"""Local mode (decision 0014): the whole store is a directory — runs, catalog,
ledger, metrics, publish — plus `sync` to migrate a flow's history to S3."""

import duckdb
import pytest
from flows import EmptyFanFlow, FlakyFlow
from schrein.ducklake import load_ducklake

from examples.embed import EmbedDocs
from ionflow import discovery, query
from ionflow.publish import publish_lake
from ionflow.sync import sync_flow


def test_run_end_to_end_local(localcfg, repo):
    run_id = EmbedDocs.run(localcfg, repo=repo, use_processes=False)
    con = EmbedDocs.duck(localcfg)

    assert con.sql(f"SELECT n_sources, n_docs FROM summary WHERE run_id = '{run_id}'").fetchone() == (2, 4)
    counts = con.sql(
        f"SELECT source, count(*) FROM vectors WHERE run_id = '{run_id}' GROUP BY source ORDER BY source"
    ).fetchall()
    assert counts == [("arxiv", 2), ("wiki", 2)]

    row = con.sql(f"SELECT status, finished, n_docs FROM ledger WHERE run_id = '{run_id}'").fetchone()
    assert row[0] == "complete" and row[1] is not None and row[2] == 4

    # metrics + stats work off plain files exactly like off S3
    sampled = {r[0] for r in con.sql(f"SELECT DISTINCT step FROM metrics WHERE run_id = '{run_id}'").fetchall()}
    assert {"docs", "by_source", "vectors", "summary"} <= sampled
    stats = query.step_stats(con, run_id).fetchall()
    assert len(stats) == 4 and all(r[2] > 0 for r in stats)

    # discovery sees the flow; the catalog schema matches the declarations
    assert discovery.scan_runs(localcfg) == {EmbedDocs.flow_id()}
    assert discovery.schema_drift(EmbedDocs, localcfg) == []


def test_local_publish_and_vanilla_read(localcfg, repo):
    run_id = EmptyFanFlow.run(localcfg, repo=repo, use_processes=False)
    m = EmptyFanFlow.publish(run_id, "agg", "blessed_agg", config=localcfg)
    assert m["run_id"] == run_id

    # a stock-DuckDB reader attaches the lake straight off the filesystem
    lake, _ = publish_lake(localcfg)
    con = duckdb.connect()
    load_ducklake(con)
    con.execute(f"ATTACH 'ducklake:{localcfg.local_dir}/{lake.register.key}' AS lake (READ_ONLY)")
    assert con.sql("SELECT n, total FROM lake.blessed_agg").fetchall() == [(0, 0)]
    prov = con.sql("SELECT table_name, run_id FROM lake.publications").fetchall()
    assert prov == [("blessed_agg", run_id)]
    con.close()

    # publishing straight to s3:// from a local store is refused, pointing at sync
    with pytest.raises(ValueError, match="sync"):
        EmptyFanFlow.publish(run_id, "agg", "t", config=localcfg, dest="s3://bucket/prefix")


@pytest.mark.slow
def test_local_resume_reuses_completed_branches(localcfg, clean_repo, tmp_path, monkeypatch):
    monkeypatch.setenv("FLAKY_DIR", str(tmp_path / "markers"))
    (tmp_path / "markers").mkdir()
    monkeypatch.setenv("FLAKY_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom"):
        FlakyFlow.run(localcfg, repo=clean_repo, use_processes=False)

    monkeypatch.setenv("FLAKY_FAIL", "0")
    failed = FlakyFlow.duck(localcfg).sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]
    run_id = FlakyFlow.run(localcfg, repo=clean_repo, resume=failed, use_processes=False)

    con = FlakyFlow.duck(localcfg)
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (3, 6)
    # branches 0 and 2 were copied from the failed origin, only 1 re-ran
    executed = [p.name for p in (tmp_path / "markers").iterdir()]
    assert sum(1 for n in executed if n.startswith("exec_1_")) == 2
    assert sum(1 for n in executed if n.startswith("exec_0_")) == 1
    assert sum(1 for n in executed if n.startswith("exec_2_")) == 1


def test_sync_local_history_to_bucket(localcfg, s3cfg, repo):
    first = EmbedDocs.run(localcfg, repo=repo, use_processes=False)

    n = sync_flow(EmbedDocs, localcfg, s3cfg)
    assert n["runs"] == 1 and n["copied"] > 0

    con = EmbedDocs.duck(s3cfg)  # plain S3-mode read of the synced flow
    assert con.sql(f"SELECT n_docs FROM summary WHERE run_id = '{first}'").fetchone() == (4,)
    assert con.sql("SELECT status FROM ledger").fetchall() == [("complete",)]
    assert con.sql(f"SELECT count(*) FROM metrics WHERE run_id = '{first}'").fetchone()[0] > 0

    # more local history accumulates; re-sync is incremental and idempotent
    second = EmbedDocs.run(localcfg, repo=repo, use_processes=False)
    n = sync_flow(EmbedDocs, localcfg, s3cfg)
    assert n["runs"] == 2
    con = EmbedDocs.duck(s3cfg)
    runs = {r[0] for r in con.sql("SELECT DISTINCT run_id FROM summary").fetchall()}
    assert runs == {first, second}
    assert con.sql("SELECT count(*) FROM ledger").fetchone() == (2,)


def test_sync_requires_local_source(s3cfg):
    with pytest.raises(ValueError, match="local"):
        sync_flow(EmbedDocs, s3cfg, s3cfg)
