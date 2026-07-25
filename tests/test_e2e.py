import pytest

from examples.etl import ETLFlow
from microflow import runner
from microflow.git import DirtyRepoError


def test_run_end_to_end(s3cfg, repo):
    run_id = ETLFlow.run(s3cfg, repo=repo, use_processes=False)
    con = ETLFlow.duck(s3cfg)

    # fan-out produced one part per region, aggregated back to a summary
    summary = con.sql(f"SELECT * FROM summary WHERE run_id = '{run_id}'").fetchall()
    assert summary[0][:2] == (2, 10.0)  # n_regions, grand_total

    # each region totalled independently across the fan-out
    totals = con.sql(
        f"SELECT region, total FROM region_totals WHERE run_id = '{run_id}' ORDER BY region"
    ).fetchall()
    assert totals == [("eu", 3.0), ("us", 7.0)]

    # ledger row complete with the custom column populated
    row = con.sql(
        f"SELECT status, finished, n_orders, len(steps_complete) FROM ledger WHERE run_id = '{run_id}'"
    ).fetchone()
    assert row[0] == "complete"
    assert row[1] is not None
    assert row[2] == 4  # n_orders custom ledger column
    assert row[3] == 6  # orders/0, by_region/0+1, region_totals/0+1, summary/0

    # resource metrics: every step sampled at least once; stats derivable
    from microflow import query

    sampled = {r[0] for r in con.sql(
        f"SELECT DISTINCT step FROM metrics WHERE run_id = '{run_id}'"
    ).fetchall()}
    assert {"orders", "by_region", "region_totals", "summary"} <= sampled
    stats = query.step_stats(con, run_id).fetchall()
    assert len(stats) == 4
    assert all(r[2] > 0 for r in stats)  # peak_rss_mb


@pytest.mark.slow
def test_run_with_processes(s3cfg, repo):
    run_id = ETLFlow.run(s3cfg, repo=repo, use_processes=True, max_workers=2)
    con = ETLFlow.duck(s3cfg)
    total = con.sql(
        f"SELECT grand_total FROM summary WHERE run_id = '{run_id}'"
    ).fetchone()[0]
    assert total == 10.0
    # branch workers (separate processes) contribute per-branch samples
    branches = con.sql(
        f"SELECT DISTINCT branch FROM metrics WHERE run_id = '{run_id}' "
        "AND step = 'region_totals' AND branch IS NOT NULL ORDER BY branch"
    ).fetchall()
    assert branches == [(0,), (1,)]
    # stats count workers only; the orchestrator's pool-wait is not the step's cost
    from microflow import query

    totals = [r for r in query.step_stats(con, run_id).fetchall() if r[0] == "region_totals"][0]
    assert totals[5] == 2  # processes


def test_dirty_git_refused(s3cfg, tmp_path):
    import subprocess

    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "dirty.txt").write_text("x")  # untracked -> dirty
    with pytest.raises(DirtyRepoError):
        ETLFlow.run(s3cfg, repo=str(tmp_path))


def test_resume_refuses_commit_drift(s3cfg, clean_repo):
    from pathlib import Path

    from conftest import _git
    from flows import EmptyFanFlow

    origin = EmptyFanFlow.run(s3cfg, repo=clean_repo, use_processes=False)
    Path(clean_repo, "flow.py").write_text("# changed\n")
    _git(clean_repo, "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-am", "fix")

    with pytest.raises(runner.CommitDriftError, match="allow_commit_drift"):
        EmptyFanFlow.run(s3cfg, repo=clean_repo, resume=origin, use_processes=False)
    with pytest.warns(UserWarning, match="different commit"):
        resumed = EmptyFanFlow.run(
            s3cfg, repo=clean_repo, resume=origin, use_processes=False, allow_commit_drift=True
        )
    assert resumed != origin


@pytest.mark.slow
def test_resume_reuses_completed_steps(s3cfg, repo):
    origin = ETLFlow.run(s3cfg, repo=repo, use_processes=False)
    resumed = runner.run(ETLFlow, s3cfg, repo=repo, resume=origin, use_processes=False)
    assert resumed != origin

    con = ETLFlow.duck(s3cfg)
    # resumed run is self-contained and identical to origin
    got = con.sql(
        f"SELECT grand_total FROM summary WHERE run_id = '{resumed}'"
    ).fetchone()[0]
    assert got == 10.0
    # ledger records the origin pointer
    origin_ptr = con.sql(
        f"SELECT origin_run_id FROM ledger WHERE run_id = '{resumed}'"
    ).fetchone()[0]
    assert origin_ptr == origin
