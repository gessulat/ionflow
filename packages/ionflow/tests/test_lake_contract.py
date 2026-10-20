"""Characterization of the two DuckLakes: a flow's catalog and the publish lake.

Every assertion is stated as observable behaviour — what a stock DuckDB reader
sees, the objects that end up in the store, the recovery path — rather than as
calls into internals.
"""

import re

import duckdb
import pytest
from flows import EmptyFanFlow, FlakyFlow
from schrein.ducklake import load_ducklake

from ionflow import Collect, Fanout, Flow, Table, fanout_key, primary_key, shaped
from ionflow import lake as lakemod
from ionflow.lake import publish_lake, schema_drift
from ionflow.query import duck
from ionflow.space import Space
from ionflow.store import Store, secret_sql


class LSrc(Table):
    n: int = primary_key
    label: str | None
    vec: shaped(float, 2)


class LItem(Table):
    key: str = fanout_key
    v: int


class LAgg(Table):
    total: int


class LakeFlow(Flow):
    def source(self) -> LSrc:
        return {"n": [1, 2], "label": ["a", None], "vec": [[1.0, 2.0], [3.0, 4.0]]}

    def items(self, source: LSrc) -> Fanout[LItem]:
        return [{"key": ["a"], "v": [1]}, {"key": ["b"], "v": [2]}]

    def agg(self, items: Collect[LItem]) -> LAgg:
        return {"total": [items.aggregate("sum(v)").fetchone()[0]]}


class DriftedSrc(Table):
    n: str  # BIGINT in the catalog
    extra: int  # not in the catalog at all


class DriftedFlow(Flow):
    """Declares `source` differently from LakeFlow — read against LakeFlow's space."""

    def source(self) -> DriftedSrc:
        return {"n": ["x"], "extra": [1]}


def _space(cfg, flow_cls=LakeFlow):
    return Space.for_flow(Store.of(cfg), flow_cls)


def _vanilla(cfg, target, key):
    """Stock DuckDB attached to a lake read-only — no ionflow on the read path.

    `key` is the register object's key in `target`, so the same helper reads
    the publish lake and a flow's own catalog.
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(secret_sql(cfg))
    load_ducklake(con)
    con.execute(f"ATTACH 'ducklake:s3://{target.bucket}/{key}' AS lake (READ_ONLY)")
    return con


# --- the flow catalog -------------------------------------------------------


def test_a_run_registers_and_stock_duckdb_reads_the_flow_catalog(remote_run, s3cfg, repo):
    run_id = remote_run(LakeFlow, repo=repo, use_processes=False)

    # ionflow read path: views over the attached catalog snapshot
    con = duck(LakeFlow, s3cfg)
    assert con.sql("SELECT total FROM agg").fetchone()[0] == 3
    assert con.sql("SELECT run_id, key, v FROM items ORDER BY key").fetchall() == [
        (run_id, "a", 1),
        (run_id, "b", 2),
    ]
    con.close()

    # vanilla reader: stock DuckDB, no ionflow — the headline feature
    v = _vanilla(s3cfg, Store.of(s3cfg), f"{_space(s3cfg).root}/{lakemod.CATALOG_KEY}")
    assert v.sql("SELECT sum(v) FROM lake.items").fetchone()[0] == 3
    assert v.sql("SELECT count(*) FROM lake.source").fetchone()[0] == 2
    v.close()


def test_backfill_restores_runs_manifests_and_registrations(remote_run, s3cfg, repo, tmp_path, monkeypatch):
    monkeypatch.setenv("FLAKY_DIR", str(tmp_path))
    monkeypatch.setenv("FLAKY_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom"):
        remote_run(FlakyFlow, repo=repo, use_processes=False)  # writes `source`/`parts`, then dies
    monkeypatch.setenv("FLAKY_FAIL", "0")
    good = remote_run(FlakyFlow, repo=repo, use_processes=False)

    space = _space(s3cfg, FlakyFlow)
    cat = lakemod.Catalog(space)
    with pytest.raises(FileExistsError, match="--replace"):
        lakemod.backfill(FlakyFlow, space)

    before = {r["run_id"]: r["status"] for r in cat.runs()}
    cat.discard()  # the recovery situation: run folders intact, register lost
    with pytest.raises(RuntimeError, match="catalog-backfill"):
        duck(FlakyFlow, s3cfg)  # the only read path refuses, pointing at the fix

    assert lakemod.backfill(FlakyFlow, space) == 2  # every run with a recovery record

    con = duck(FlakyFlow, s3cfg)
    assert dict(con.sql("SELECT run_id, status FROM ledger").fetchall()) == before
    assert con.sql("SELECT n, total FROM agg").fetchall() == [(3, 6)]  # from the complete run
    failed = next(r for r, s in before.items() if s == "failed")
    assert good != failed
    # the failed run's completed steps are queryable again too
    assert con.sql(f"SELECT count(*) FROM source WHERE run_id = '{failed}'").fetchone()[0] == 3
    con.close()

    assert lakemod.backfill(FlakyFlow, space, replace=True) == 2  # rebuild over an existing catalog


def test_schema_drift_reports_missing_changed_and_unexpected_columns(remote_run, s3cfg, repo):
    remote_run(LakeFlow, repo=repo, use_processes=False)
    space = _space(s3cfg)

    assert schema_drift(LakeFlow, space) == []
    flow = DriftedFlow.flow_id()
    assert schema_drift(DriftedFlow, space) == [
        f"{flow}.source: 'n' BIGINT→VARCHAR",
        f"{flow}.source: missing column 'extra'",
        f"{flow}.source: unexpected column 'label'",
        f"{flow}.source: unexpected column 'vec'",
    ]


# --- the publish lake -------------------------------------------------------


def test_publish_replaces_atomically_and_records_provenance(remote_run, s3cfg, repo):
    """One run, four publishes: atomic replace, staging, and the provenance table."""
    run_id = remote_run(LakeFlow, repo=repo, use_processes=False)
    lake, target = publish_lake(s3cfg)

    LakeFlow.publish(run_id, "items", "blessed", config=s3cfg)
    con = _vanilla(s3cfg, target, lake.register.key)
    assert con.sql("SELECT key, v FROM lake.blessed ORDER BY key").fetchall() == [("a", 1), ("b", 2)]
    assert [r[0] for r in con.sql("DESCRIBE lake.blessed").fetchall()] == ["key", "v"]  # no run_id
    con.close()

    LakeFlow.publish(run_id, "items", "blessed", config=s3cfg, where="v > 1")
    # a fresh staged file per publish, so the earlier snapshot's file stays immutable
    staged = target.list(f"{s3cfg.prefix}/published/data/blessed/")
    assert len(staged) == 2
    assert all(re.fullmatch(rf"{run_id}-[0-9a-f]{{32}}\.parquet", k.rsplit("/", 1)[1]) for k in staged)

    m = LakeFlow.publish(run_id, "agg", "blessed_agg", config=s3cfg)
    # the returned dict IS the INSERT tuple: order is load-bearing
    assert list(m) == ["table_name", "flow", "run_id", "step", "filter", "commit", "published_at"]
    assert m["filter"] is None and re.fullmatch(r"[0-9a-f]{40}", m["commit"])
    assert (m["table_name"], m["flow"], m["run_id"], m["step"]) == ("blessed_agg", LakeFlow.flow_id(), run_id, "agg")
    m2 = LakeFlow.publish(run_id, "agg", "blessed_agg", config=s3cfg, where="total > 99")

    con = _vanilla(s3cfg, target, lake.register.key)
    assert con.sql("SELECT key, v FROM lake.blessed ORDER BY key").fetchall() == [("b", 2)]  # old rows gone
    assert con.sql("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'").fetchone()[0] == 3
    assert [r[:3] for r in con.sql("DESCRIBE lake.publications").fetchall()] == [
        ("table_name", "VARCHAR", "NO"),
        ("flow", "VARCHAR", "NO"),
        ("run_id", "VARCHAR", "NO"),
        ("step", "VARCHAR", "NO"),
        ("filter", "VARCHAR", "YES"),
        ("commit", "VARCHAR", "NO"),
        ("published_at", "TIMESTAMP", "NO"),
    ]
    published = con.sql("SELECT * FROM lake.publications ORDER BY published_at").fetchall()
    assert published[-2:] == [tuple(m.values()), tuple(m2.values())]  # appends, never replaces
    con.close()


def test_publish_of_an_empty_fanout_step_is_a_typed_zero_row_table(remote_run, s3cfg, repo):
    run_id = remote_run(EmptyFanFlow, repo=repo, use_processes=False)
    EmptyFanFlow.publish(run_id, "work", "blessed_work", config=s3cfg)

    lake, target = publish_lake(s3cfg)
    con = _vanilla(s3cfg, target, lake.register.key)
    assert con.sql("SELECT count(*) FROM lake.blessed_work").fetchone()[0] == 0
    assert [r[0] for r in con.sql("DESCRIBE lake.blessed_work").fetchall()] == ["g", "doubled"]
    con.close()


def test_publish_to_another_bucket(remote_run, s3cfg, repo):
    run_id = remote_run(LakeFlow, repo=repo, use_processes=False)
    other = f"{s3cfg.bucket}-pub"
    Store.of(s3cfg).client.create_bucket(Bucket=other)
    dest = f"s3://{other}/consumers/stable"

    LakeFlow.publish(run_id, "agg", "agg", config=s3cfg, dest=dest)

    lake, target = publish_lake(s3cfg, dest)
    assert target.bucket == other
    con = _vanilla(s3cfg, target, lake.register.key)
    assert con.sql("SELECT total FROM lake.agg").fetchone()[0] == 3
    con.close()


def test_publish_refuses_unknown_step_bad_names_and_incomplete_runs(remote_run, s3cfg, repo, tmp_path, monkeypatch):
    monkeypatch.setenv("FLAKY_DIR", str(tmp_path))
    monkeypatch.setenv("FLAKY_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom"):
        remote_run(FlakyFlow, repo=repo, use_processes=False)
    failed = FlakyFlow.duck(s3cfg).sql("SELECT run_id FROM ledger").fetchone()[0]

    with pytest.raises(RuntimeError, match="did not complete"):
        FlakyFlow.publish(failed, "source", "blessed", config=s3cfg)
    with pytest.raises(RuntimeError, match="run not found"):
        FlakyFlow.publish("nope", "source", "blessed", config=s3cfg)
    with pytest.raises(ValueError, match="unknown step"):
        FlakyFlow.publish(failed, "nostep", "blessed", config=s3cfg)
    for bad in ("no-dashes", "1leading", "publications", ""):
        with pytest.raises(ValueError, match="invalid published table name"):
            FlakyFlow.publish(failed, "source", bad, config=s3cfg)
    # nothing was staged by any refusal
    assert Store.of(s3cfg).list(f"{s3cfg.prefix}/published/") == []


# --- local store ------------------------------------------------------------


def test_default_publish_from_a_local_store_stays_in_that_store(localcfg, repo):
    run_id = LakeFlow.run(localcfg, repo=repo, use_processes=False)
    lake, target = publish_lake(localcfg)
    assert target.local_dir == localcfg.local_dir and lake.data_path.startswith(localcfg.local_dir)

    assert LakeFlow.publish(run_id, "items", "blessed", config=localcfg)["run_id"] == run_id
    con = duckdb.connect()
    load_ducklake(con)
    con.execute(f"ATTACH 'ducklake:{localcfg.local_dir}/{lake.register.key}' AS lake (READ_ONLY)")
    assert con.sql("SELECT key, v FROM lake.blessed ORDER BY key").fetchall() == [("a", 1), ("b", 2)]
    prov = con.sql("SELECT table_name, run_id FROM lake.publications").fetchall()
    assert prov == [("blessed", run_id)]  # provenance lands in the local lake too
    con.close()


def test_publish_from_a_local_store_to_an_s3_dest_is_refused(localcfg, repo):
    """A local store cannot reach an s3:// destination: the target config keeps
    local_dir, so `--to` would resolve back to a DirStore. `sync` first."""
    run_id = LakeFlow.run(localcfg, repo=repo, use_processes=False)
    with pytest.raises(ValueError, match="cannot publish from a local store"):
        LakeFlow.publish(run_id, "items", "blessed", config=localcfg, dest="s3://elsewhere/consumers")
