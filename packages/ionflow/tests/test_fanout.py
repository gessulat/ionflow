import pytest
from flows import ChainFlow, EmptyFanFlow, FlakyFlow, RetryFlow, SideInputFlow, TwoGroupFlow, exec_counts


def _counts(d, step):
    """`flows.exec_counts` keyed by the branch index the three-item flows use."""
    return {g: exec_counts(d, step).get(str(g), 0) for g in (0, 1, 2)}


@pytest.mark.slow
def test_partial_fanout_resume_reruns_only_failed_branch(anycfg, repo, tmp_path, monkeypatch):
    d = tmp_path / "exec"
    d.mkdir()
    monkeypatch.setenv("FLAKY_DIR", str(d))
    monkeypatch.setenv("FLAKY_FAIL", "1")

    with pytest.raises(RuntimeError):
        FlakyFlow.run(anycfg, repo=repo, use_processes=False)

    con = FlakyFlow.duck(anycfg)
    origin_id, status = con.sql("SELECT run_id, status FROM ledger").fetchone()
    assert status == "failed"
    assert _counts(d, "work") == {0: 1, 1: 1, 2: 1}  # every branch attempted once
    # failed runs keep their metrics — that's when perf data matters most
    n = con.sql(f"SELECT count(*) FROM metrics WHERE run_id = '{origin_id}'").fetchone()[0]
    assert n > 0

    # resume with the fault cleared
    monkeypatch.setenv("FLAKY_FAIL", "0")
    resumed = FlakyFlow.run(anycfg, repo=repo, resume=origin_id, use_processes=False)

    # branches 0 and 2 were reused (server-side copied), only branch 1 re-ran
    assert _counts(d, "work") == {0: 1, 1: 2, 2: 1}

    con = FlakyFlow.duck(anycfg)
    row = con.sql(f"SELECT n, total FROM agg WHERE run_id = '{resumed}'").fetchone()
    assert row == (3, 6)
    st = con.sql(f"SELECT status FROM ledger WHERE run_id = '{resumed}'").fetchone()[0]
    assert st == "complete"


@pytest.mark.slow
def test_failed_branch_keeps_its_metrics(s3cfg, repo, tmp_path, monkeypatch):
    """A failing process-mode branch still writes its own metrics object."""
    monkeypatch.setenv("FLAKY_DIR", str(tmp_path))  # markers land here; not asserted on
    monkeypatch.setenv("FLAKY_FAIL", "1")

    with pytest.raises(RuntimeError):
        FlakyFlow.run(s3cfg, repo=repo, use_processes=True, max_workers=3)

    from ionflow.space import Space
    from ionflow.store import Store

    con = FlakyFlow.duck(s3cfg)
    run_id = con.sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]

    space = Space.for_flow(Store.of(s3cfg), FlakyFlow)
    keys = space.store.list(space.root + "/runs/")
    # branch 1 raised, yet its worker's final (finally) write landed; one object
    # per item, named for the fan-out — a worker covers the item's whole sub-DAG
    assert space.metrics_branch(run_id, "parts", 1) in keys
    branches = {
        r[0]
        for r in con.sql(
            f"SELECT DISTINCT branch FROM metrics WHERE run_id = '{run_id}' AND branch IS NOT NULL"
        ).fetchall()
    }
    assert branches == {0, 1, 2}  # failed branch included


def test_branch_retries_recover_transient_failures(s3cfg, repo, tmp_path, monkeypatch):
    monkeypatch.setenv("RETRY_DIR", str(tmp_path))
    run_id = RetryFlow.run(s3cfg, repo=repo, use_processes=False, branch_retries=1)

    con = RetryFlow.duck(s3cfg)
    # success entails each branch failed once first: work() only returns after its marker exists
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (2, 2)


def test_steps_read_side_inputs_via_config(s3cfg, repo):
    from ionflow.store import Store

    Store.of(s3cfg).put("side/n", b"3")
    run_id = SideInputFlow.run(s3cfg, repo=repo, use_processes=False)

    con = SideInputFlow.duck(s3cfg)
    # 3 branches (fan-out sized by the side read), each doubled by n=3 (branch-side read)
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (3, 9)


def test_each_group_collects_its_own_widths(s3cfg, repo):
    """Two groups in one thread-mode process: the second must not inherit the first's widths."""
    run_id = TwoGroupFlow.run(s3cfg, repo=repo, use_processes=False, max_workers=2)

    con = TwoGroupFlow.duck(s3cfg)

    def widths(step):
        return con.sql(f"SELECT DISTINCT doubled FROM {step} WHERE run_id = '{run_id}'").fetchall()

    assert widths("work_a") == [(2,)]  # side_a's width
    assert widths("work_b") == [(5,)]  # side_b's, not side_a's


def test_chained_branch_steps_stay_aligned(s3cfg, repo):
    """`refine` branches over `work` (itself a branch): one call per branch, same index."""
    run_id = ChainFlow.run(s3cfg, repo=repo, use_processes=False)

    con = ChainFlow.duck(s3cfg)
    rows = con.sql(f"SELECT g, seen, quadrupled FROM refine WHERE run_id = '{run_id}' ORDER BY g").fetchall()
    # one row per branch: the chained step saw only its own branch's `work` part,
    # and `seen` (from work) matches `g` (from the fan-out part) — inputs are aligned
    assert rows == [(0, 0, 0), (1, 1, 4), (2, 2, 8)]
    assert con.sql(f"SELECT n, total FROM total WHERE run_id = '{run_id}'").fetchone() == (3, 12)
    assert con.sql(f"SELECT status FROM ledger WHERE run_id = '{run_id}'").fetchone()[0] == "complete"


@pytest.mark.slow
def test_chained_branch_in_process_mode(s3cfg, repo, tmp_path, monkeypatch):
    """The chained step fans out across worker processes too, and each is measured."""
    monkeypatch.setenv("CHAIN_DIR", str(tmp_path))
    run_id = ChainFlow.run(s3cfg, repo=repo, use_processes=True, max_workers=3)

    assert _counts(tmp_path, "work") == {0: 1, 1: 1, 2: 1}
    assert _counts(tmp_path, "refine") == {0: 1, 1: 1, 2: 1}

    con = ChainFlow.duck(s3cfg)
    assert con.sql(f"SELECT n, total FROM total WHERE run_id = '{run_id}'").fetchone() == (3, 12)
    measured = {
        r
        for r in con.sql(
            f"SELECT DISTINCT step, branch FROM metrics WHERE run_id = '{run_id}' AND branch IS NOT NULL"
        ).fetchall()
    }
    # every sub-step of every item is measured; NULL labels are the worker between them
    assert measured >= {(s, b) for s in ("work", "refine") for b in (0, 1, 2)}


@pytest.mark.slow
def test_chained_branch_resume_reruns_only_the_failed_branch(s3cfg, repo, tmp_path, monkeypatch):
    """A failure in the second branch step reuses the first one's parts wholesale."""
    monkeypatch.setenv("CHAIN_DIR", str(tmp_path))
    monkeypatch.setenv("CHAIN_FAIL", "refine:1")

    with pytest.raises(RuntimeError, match="refine g=1"):
        ChainFlow.run(s3cfg, repo=repo, use_processes=False)

    con = ChainFlow.duck(s3cfg)
    origin = con.sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]
    assert _counts(tmp_path, "work") == {0: 1, 1: 1, 2: 1}

    monkeypatch.delenv("CHAIN_FAIL")
    resumed = ChainFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)

    assert _counts(tmp_path, "work") == {0: 1, 1: 1, 2: 1}  # copied, never re-run
    assert _counts(tmp_path, "refine") == {0: 1, 1: 2, 2: 1}  # only branch 1 re-ran

    con = ChainFlow.duck(s3cfg)
    rows = con.sql(f"SELECT g, seen, quadrupled FROM refine WHERE run_id = '{resumed}' ORDER BY g").fetchall()
    assert rows == [(0, 0, 0), (1, 1, 4), (2, 2, 8)]  # reused and recomputed branches agree
    assert con.sql(f"SELECT n, total FROM total WHERE run_id = '{resumed}'").fetchone() == (3, 12)


@pytest.mark.slow
def test_chained_branch_resume_after_upstream_branch_failure(s3cfg, repo, tmp_path, monkeypatch):
    """One item's failure costs that item only: the others ran their chain to the end."""
    monkeypatch.setenv("CHAIN_DIR", str(tmp_path))
    monkeypatch.setenv("CHAIN_FAIL", "work:2")

    with pytest.raises(RuntimeError, match="work g=2"):
        ChainFlow.run(s3cfg, repo=repo, use_processes=False)

    con = ChainFlow.duck(s3cfg)
    origin = con.sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]
    # no stage barrier: items 0 and 1 reached `refine` although item 2 never got past `work`
    assert _counts(tmp_path, "refine") == {0: 1, 1: 1, 2: 0}

    monkeypatch.delenv("CHAIN_FAIL")
    resumed = ChainFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)

    assert _counts(tmp_path, "work") == {0: 1, 1: 1, 2: 2}
    assert _counts(tmp_path, "refine") == {0: 1, 1: 1, 2: 1}  # only item 2 resumed
    con = ChainFlow.duck(s3cfg)
    assert con.sql(f"SELECT n, total FROM total WHERE run_id = '{resumed}'").fetchone() == (3, 12)


def test_empty_fanout_completes(s3cfg, repo):
    run_id = EmptyFanFlow.run(s3cfg, repo=repo, use_processes=False)
    con = EmptyFanFlow.duck(s3cfg)
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (0, 0)
    assert con.sql(f"SELECT status FROM ledger WHERE run_id = '{run_id}'").fetchone()[0] == "complete"
