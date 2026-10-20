"""Per-item sub-DAGs: a fan-out branch is a small DAG, run per item, not per stage."""

import pytest
from flows import PipelineFlow, exec_counts, put_n

from ionflow.store import s3_client


def _setup(cfg, tmp_path, monkeypatch, n=2):
    put_n(cfg, n)
    monkeypatch.setenv("PIPE_DIR", str(tmp_path))


def test_sub_dag_runs_siblings_concurrently_and_records_side_effects(executioncfg, repo, tmp_path, monkeypatch):
    """`upload_db` and `search` are independent, so one item runs them at once —
    and the upload's output table is its provenance: one row per item, uri + etag."""
    _setup(executioncfg, tmp_path, monkeypatch)
    run_id = PipelineFlow.run(executioncfg, repo=repo, use_processes=False)

    con = PipelineFlow.duck(executioncfg)
    # each sub-step saw its sibling in flight (they rendezvous, or time out and fail here)
    assert con.sql(f"SELECT bool_and(concurrent) FROM search WHERE run_id = '{run_id}'").fetchone()[0]
    assert con.sql(f"SELECT bool_and(concurrent) FROM upload_db WHERE run_id = '{run_id}'").fetchone()[0]
    assert con.sql(f"SELECT n, peptides FROM summary WHERE run_id = '{run_id}'").fetchone() == (2, 6)
    assert con.sql(f"SELECT status FROM ledger WHERE run_id = '{run_id}'").fetchone()[0] == "complete"

    rows = con.sql(f"SELECT raw, uri, etag FROM upload_db WHERE run_id = '{run_id}' ORDER BY raw").fetchall()
    client = s3_client(executioncfg)
    for raw, uri, etag in rows:
        assert uri == f"s3://{executioncfg.bucket}/uploads/{raw}.db"
        head = client.head_object(Bucket=executioncfg.bucket, Key=f"uploads/{raw}.db")
        assert head["ETag"].strip('"') == etag  # the recorded object is really there


def test_items_pipeline_without_a_stage_barrier(executioncfg, repo, tmp_path, monkeypatch):
    """One worker takes an item to the end before the next item starts converting."""
    _setup(executioncfg, tmp_path, monkeypatch)
    run_id = PipelineFlow.run(executioncfg, repo=repo, use_processes=False, max_workers=1)

    con = PipelineFlow.duck(executioncfg)
    converted = dict(con.sql(f"SELECT raw, started FROM convert WHERE run_id = '{run_id}'").fetchall())
    searched = dict(con.sql(f"SELECT raw, started FROM search WHERE run_id = '{run_id}'").fetchall())
    first, second = sorted(converted, key=converted.get)
    # a stage barrier would convert BOTH raws before searching either — which is
    # exactly what forces N converted .db files onto disk instead of `max_workers`
    assert searched[first] < converted[second]


@pytest.mark.slow
def test_one_item_stays_in_one_process(executioncfg, repo, tmp_path, monkeypatch):
    """Sub-steps of an item share a process, which is what lets them pass a file
    path. PIPE_MEET makes both items' converts wait for each other, so the run
    only completes if the items really ran on two workers at once — without the
    barrier a fast first item may legitimately be packed into the warm worker."""
    _setup(executioncfg, tmp_path, monkeypatch)
    monkeypatch.setenv("PIPE_MEET", "1")
    run_id = PipelineFlow.run(executioncfg, repo=repo, use_processes=True, max_workers=2)

    con = PipelineFlow.duck(executioncfg)
    pairs = con.sql(f"""
        SELECT c.raw, c.pid, s.pid FROM convert c JOIN search s USING (raw, run_id)
        WHERE run_id = '{run_id}'
    """).fetchall()
    assert len(pairs) == 2
    for _, convert_pid, search_pid in pairs:
        assert convert_pid == search_pid  # same worker: search read convert's local .db
    assert len({p[1] for p in pairs}) == 2  # and different items got different workers


def test_retries_re_run_only_the_failed_sub_step(executioncfg, repo, tmp_path, monkeypatch):
    """A flaky upload costs one upload, not the convert (nor the search) beside it."""
    _setup(executioncfg, tmp_path, monkeypatch)
    monkeypatch.setenv("PIPE_FAIL", "upload_db:raw-1")
    monkeypatch.setenv("PIPE_ONCE", "1")  # fails its first attempt only

    run_id = PipelineFlow.run(executioncfg, repo=repo, use_processes=False, branch_retries=1)

    assert exec_counts(tmp_path, "upload_db") == {"raw-0": 1, "raw-1": 2}
    assert exec_counts(tmp_path, "convert") == {"raw-0": 1, "raw-1": 1}
    assert exec_counts(tmp_path, "search") == {"raw-0": 1, "raw-1": 1}
    con = PipelineFlow.duck(executioncfg)
    assert con.sql(f"SELECT n FROM summary WHERE run_id = '{run_id}'").fetchone() == (2,)


@pytest.mark.slow
def test_resume_restarts_an_item_at_its_first_incomplete_sub_step(executioncfg, repo, tmp_path, monkeypatch):
    _setup(executioncfg, tmp_path, monkeypatch)
    monkeypatch.setenv("PIPE_FAIL", "search:raw-1")

    with pytest.raises(RuntimeError, match="search failed on raw-1"):
        PipelineFlow.run(executioncfg, repo=repo, use_processes=False)

    con = PipelineFlow.duck(executioncfg)
    origin = con.sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]
    # convert ran for every item and stays queryable although the run died
    assert {r[0] for r in con.sql(f"SELECT raw FROM convert WHERE run_id = '{origin}'").fetchall()} == {
        "raw-0",
        "raw-1",
    }

    monkeypatch.delenv("PIPE_FAIL")
    resumed = PipelineFlow.run(executioncfg, repo=repo, resume=origin, use_processes=False)

    # the 40-minute convert and the finished upload are reused; only the search re-runs
    assert exec_counts(tmp_path, "convert") == {"raw-0": 1, "raw-1": 1}
    assert exec_counts(tmp_path, "upload_db") == {"raw-0": 1, "raw-1": 1}
    assert exec_counts(tmp_path, "search") == {"raw-0": 1, "raw-1": 2}
    con = PipelineFlow.duck(executioncfg)
    assert con.sql(f"SELECT n, peptides FROM summary WHERE run_id = '{resumed}'").fetchone() == (2, 6)


@pytest.mark.slow
def test_keyed_resume_reuses_a_whole_item_and_computes_the_new_one(executioncfg, repo, tmp_path, monkeypatch):
    _setup(executioncfg, tmp_path, monkeypatch, n=1)
    origin = PipelineFlow.run(executioncfg, repo=repo, use_processes=False)
    put_n(executioncfg, 2)
    resumed = PipelineFlow.run(executioncfg, repo=repo, resume=origin, use_processes=False)

    for step in ("convert", "upload_db", "search"):  # every sub-step is key-matched
        assert exec_counts(tmp_path, step) == {"raw-0": 1, "raw-1": 1}, step
    con = PipelineFlow.duck(executioncfg)
    assert con.sql(f"SELECT n, peptides FROM summary WHERE run_id = '{resumed}'").fetchone() == (2, 6)
    old = con.sql(f"SELECT etag FROM upload_db WHERE run_id = '{origin}'").fetchone()[0]
    new = dict(con.sql(f"SELECT raw, etag FROM upload_db WHERE run_id = '{resumed}'").fetchall())
    assert new["raw-0"] == old  # raw-0's whole sub-DAG came forward untouched
