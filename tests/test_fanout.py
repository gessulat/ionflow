import pytest
from flows import EmptyFanFlow, FlakyFlow, RetryFlow, SideInputFlow


def _exec_counts(d):
    counts = {0: 0, 1: 0, 2: 0}
    for f in d.iterdir():
        counts[int(f.name.split("_")[1])] += 1
    return counts


@pytest.mark.slow
def test_partial_fanout_resume_reruns_only_failed_branch(s3cfg, repo, tmp_path, monkeypatch):
    d = tmp_path / "exec"
    d.mkdir()
    monkeypatch.setenv("FLAKY_DIR", str(d))
    monkeypatch.setenv("FLAKY_FAIL", "1")

    with pytest.raises(RuntimeError):
        FlakyFlow.run(s3cfg, repo=repo, use_processes=False)

    con = FlakyFlow.duck(s3cfg)
    origin_id, status = con.sql("SELECT run_id, status FROM ledger").fetchone()
    assert status == "failed"
    assert _exec_counts(d) == {0: 1, 1: 1, 2: 1}  # every branch attempted once
    # failed runs keep their metrics — that's when perf data matters most
    n = con.sql(f"SELECT count(*) FROM metrics WHERE run_id = '{origin_id}'").fetchone()[0]
    assert n > 0

    # resume with the fault cleared
    monkeypatch.setenv("FLAKY_FAIL", "0")
    resumed = FlakyFlow.run(s3cfg, repo=repo, resume=origin_id, use_processes=False)

    # branches 0 and 2 were reused (server-side copied), only branch 1 re-ran
    assert _exec_counts(d) == {0: 1, 1: 2, 2: 1}

    con = FlakyFlow.duck(s3cfg)
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

    from microflow import storage

    con = FlakyFlow.duck(s3cfg)
    run_id = con.sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]

    layout = storage.Layout.for_flow(s3cfg, FlakyFlow)
    keys = storage.list_keys(storage.s3_client(s3cfg), s3cfg.bucket, layout.root + "/runs/")
    # branch 1 raised, yet its worker's final (finally) write landed
    assert layout.metrics_branch(run_id, "work", 1) in keys
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
    from microflow.storage import s3_client

    s3_client(s3cfg).put_object(Bucket=s3cfg.bucket, Key="side/n", Body=b"3")
    run_id = SideInputFlow.run(s3cfg, repo=repo, use_processes=False)

    con = SideInputFlow.duck(s3cfg)
    # 3 branches (fan-out sized by the side read), each doubled by n=3 (branch-side read)
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (3, 9)


def test_empty_fanout_completes(s3cfg, repo):
    run_id = EmptyFanFlow.run(s3cfg, repo=repo, use_processes=False)
    con = EmptyFanFlow.duck(s3cfg)
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (0, 0)
    assert con.sql(f"SELECT status FROM ledger WHERE run_id = '{run_id}'").fetchone()[0] == "complete"
