import pytest

from flows import EmptyFanFlow, FlakyFlow


def _exec_counts(d):
    counts = {0: 0, 1: 0, 2: 0}
    for f in d.iterdir():
        counts[int(f.name.split("_")[1])] += 1
    return counts


def test_partial_fanout_resume_reruns_only_failed_branch(s3cfg, clean_repo, tmp_path, monkeypatch):
    d = tmp_path / "exec"
    d.mkdir()
    monkeypatch.setenv("FLAKY_DIR", str(d))
    monkeypatch.setenv("FLAKY_FAIL", "1")

    with pytest.raises(RuntimeError):
        FlakyFlow.run(s3cfg, repo=clean_repo, use_processes=False)

    con = FlakyFlow.duck(s3cfg)
    origin_id, status = con.sql("SELECT run_id, status FROM ledger").fetchone()
    assert status == "failed"
    assert _exec_counts(d) == {0: 1, 1: 1, 2: 1}  # every branch attempted once

    # resume with the fault cleared
    monkeypatch.setenv("FLAKY_FAIL", "0")
    resumed = FlakyFlow.resume(origin_id, s3cfg, repo=clean_repo, use_processes=False)

    # branches 0 and 2 were reused (server-side copied), only branch 1 re-ran
    assert _exec_counts(d) == {0: 1, 1: 2, 2: 1}

    con = FlakyFlow.duck(s3cfg)
    row = con.sql(f"SELECT n, total FROM agg WHERE run_id = '{resumed}'").fetchone()
    assert row == (3, 6)
    st = con.sql(f"SELECT status FROM ledger WHERE run_id = '{resumed}'").fetchone()[0]
    assert st == "complete"


def test_empty_fanout_completes(s3cfg, clean_repo):
    run_id = EmptyFanFlow.run(s3cfg, repo=clean_repo, use_processes=False)
    con = EmptyFanFlow.duck(s3cfg)
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (0, 0)
    assert con.sql(f"SELECT status FROM ledger WHERE run_id = '{run_id}'").fetchone()[0] == "complete"
