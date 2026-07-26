import pytest

from examples.embed import EmbedDocs
from ionflow import runner
from ionflow.git import DirtyRepoError


def test_run_end_to_end(s3cfg, repo):
    run_id = EmbedDocs.run(s3cfg, repo=repo, use_processes=False)
    con = EmbedDocs.duck(s3cfg)

    # fan-out produced one part per source, aggregated back to a summary
    summary = con.sql(f"SELECT * FROM summary WHERE run_id = '{run_id}'").fetchall()
    assert summary[0][:2] == (2, 4)  # n_sources, n_docs
    assert summary[0][2] > 0  # avg_norm

    # each source embedded independently across the fan-out
    counts = con.sql(
        f"SELECT source, count(*), min(norm) > 0 FROM vectors WHERE run_id = '{run_id}' GROUP BY source ORDER BY source"
    ).fetchall()
    assert counts == [("arxiv", 2, True), ("wiki", 2, True)]

    # the shaped embedding column round-trips as a 64-wide list
    width = con.sql(f"SELECT DISTINCT len(embedding) FROM vectors WHERE run_id = '{run_id}'").fetchall()
    assert width == [(64,)]

    # ledger row complete with the custom column populated
    row = con.sql(f"SELECT status, finished, n_docs FROM ledger WHERE run_id = '{run_id}'").fetchone()
    assert row[0] == "complete"
    assert row[1] is not None
    assert row[2] == 4  # n_docs custom ledger column
    # per-step progress (steps_complete) lives in _meta.json, not the ledger

    # resource metrics: every step sampled at least once; stats derivable
    from ionflow import query

    sampled = {r[0] for r in con.sql(f"SELECT DISTINCT step FROM metrics WHERE run_id = '{run_id}'").fetchall()}
    assert {"docs", "by_source", "vectors", "summary"} <= sampled
    stats = query.step_stats(con, run_id).fetchall()
    assert len(stats) == 4
    assert all(r[2] > 0 for r in stats)  # peak_rss_mb


@pytest.mark.slow
def test_run_with_processes(s3cfg, repo):
    run_id = EmbedDocs.run(s3cfg, repo=repo, use_processes=True, max_workers=2)
    con = EmbedDocs.duck(s3cfg)
    n_docs = con.sql(f"SELECT n_docs FROM summary WHERE run_id = '{run_id}'").fetchone()[0]
    assert n_docs == 4
    # branch workers (separate processes) contribute per-branch samples
    branches = con.sql(
        f"SELECT DISTINCT branch FROM metrics WHERE run_id = '{run_id}' "
        "AND step = 'vectors' AND branch IS NOT NULL ORDER BY branch"
    ).fetchall()
    assert branches == [(0,), (1,)]
    # stats count workers only; the orchestrator's pool-wait is not the step's cost
    from ionflow import query

    totals = [r for r in query.step_stats(con, run_id).fetchall() if r[0] == "vectors"][0]
    assert totals[5] == 2  # processes


def test_dirty_git_refused(s3cfg, tmp_path):
    import subprocess

    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "dirty.txt").write_text("x")  # untracked -> dirty
    with pytest.raises(DirtyRepoError):
        EmbedDocs.run(s3cfg, repo=str(tmp_path))


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
        resumed = EmptyFanFlow.run(s3cfg, repo=clean_repo, resume=origin, use_processes=False, allow_commit_drift=True)
    assert resumed != origin


@pytest.mark.slow
def test_resume_reuses_completed_steps(s3cfg, repo):
    origin = EmbedDocs.run(s3cfg, repo=repo, use_processes=False)
    resumed = runner.run(EmbedDocs, s3cfg, repo=repo, resume=origin, use_processes=False)
    assert resumed != origin

    con = EmbedDocs.duck(s3cfg)
    # resumed run is self-contained and identical to origin
    got = con.sql(f"SELECT n_docs FROM summary WHERE run_id = '{resumed}'").fetchone()[0]
    assert got == 4
    # ledger records the origin pointer
    origin_ptr = con.sql(f"SELECT origin_run_id FROM ledger WHERE run_id = '{resumed}'").fetchone()[0]
    assert origin_ptr == origin
