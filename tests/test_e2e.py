import pytest

from miniflow import runner
from miniflow.git import DirtyRepoError

from examples.etl import ETLFlow


def test_run_end_to_end(s3cfg, clean_repo):
    run_id = ETLFlow.run(s3cfg, repo=clean_repo, use_processes=False)
    con = ETLFlow.duck(s3cfg)

    # fan-out produced one part per group, aggregated back to a summary
    summary = con.sql(f"SELECT * FROM summary WHERE run_id = '{run_id}'").fetchall()
    assert summary[0][:2] == (2, 10.0)  # n_groups, grand_total

    # each group scored independently across the fan-out
    scores = con.sql(
        f"SELECT grp, total FROM scored WHERE run_id = '{run_id}' ORDER BY grp"
    ).fetchall()
    assert scores == [(0, 3.0), (1, 7.0)]

    # ledger row complete with the custom column populated
    row = con.sql(
        f"SELECT finished, n_rows, len(steps_complete) FROM ledger WHERE run_id = '{run_id}'"
    ).fetchone()
    assert row[0] is not None
    assert row[1] == 4  # n_rows custom ledger column
    assert row[2] == 6  # raw/0, parts/0, parts/1, scored/0, scored/1, summary/0


def test_run_with_processes(s3cfg, clean_repo):
    run_id = ETLFlow.run(s3cfg, repo=clean_repo, use_processes=True, max_workers=2)
    con = ETLFlow.duck(s3cfg)
    total = con.sql(
        f"SELECT grand_total FROM summary WHERE run_id = '{run_id}'"
    ).fetchone()[0]
    assert total == 10.0


def test_dirty_git_refused(s3cfg, tmp_path):
    import subprocess

    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "dirty.txt").write_text("x")  # untracked -> dirty
    with pytest.raises(DirtyRepoError):
        ETLFlow.run(s3cfg, repo=str(tmp_path))


def test_resume_reuses_completed_steps(s3cfg, clean_repo):
    origin = ETLFlow.run(s3cfg, repo=clean_repo, use_processes=False)
    resumed = runner.run(
        ETLFlow, s3cfg, repo=clean_repo, origin_run_id=origin, use_processes=False
    )
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
