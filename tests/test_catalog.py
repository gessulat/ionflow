"""Catalog mode: per-flow DuckLake, registered step outputs, vanilla readers.

The catalog only receives table schemas and file registrations; run
bookkeeping (ledger/meta) is untouched — see docs/specs/ionflow-on-schrein.md.
"""

import dataclasses

import duckdb
import pytest
from flows import ProtoFlow
from schrein.ducklake import load_ducklake

from ionflow import Collect, Fanout, Flow, Table, fanout_key
from ionflow import catalog as catmod
from ionflow.query import duck
from ionflow.storage import Layout, secret_sql


class CSrc(Table):
    n: int


class CItem(Table):
    key: str = fanout_key
    v: int


class CAgg(Table):
    total: int


class CatFlow(Flow):
    def source(self) -> CSrc:
        return {"n": [1, 2, 3]}

    def items(self, source: CSrc) -> Fanout[CItem]:
        return [{"key": ["a"], "v": [1]}, {"key": ["b"], "v": [2]}]

    def agg(self, items: Collect[CItem]) -> CAgg:
        return {"total": [items.aggregate("sum(v)").fetchone()[0]]}


def catcfg(s3cfg):
    return dataclasses.replace(s3cfg, catalog="ducklake")


def test_catalog_run_registers_and_vanilla_reader_sees_it(s3cfg, repo):
    cfg = catcfg(s3cfg)
    run_id = CatFlow.run(cfg, repo=repo, use_processes=False)

    # ionflow read path: views over the attached catalog snapshot
    con = duck(CatFlow, cfg)
    assert con.sql("SELECT total FROM agg").fetchone()[0] == 3
    assert con.sql("SELECT run_id, key, v FROM items ORDER BY key").fetchall() == [(run_id, "a", 1), (run_id, "b", 2)]

    # vanilla reader: stock DuckDB, no ionflow — the headline feature
    v = duckdb.connect()
    v.execute("INSTALL httpfs; LOAD httpfs")
    v.execute(secret_sql(cfg))
    load_ducklake(v)
    root = Layout.for_flow(cfg, CatFlow).root
    v.execute(f"ATTACH 'ducklake:s3://{cfg.bucket}/{root}/catalog.ducklake' AS lake (READ_ONLY)")
    assert v.sql("SELECT sum(v) FROM lake.items").fetchone()[0] == 3
    assert v.sql("SELECT count(*) FROM lake.source").fetchone()[0] == 3
    v.close()


def test_prototype_flows_stay_classic(s3cfg):
    assert catmod.for_flow(catcfg(s3cfg), ProtoFlow).mode == "none"
    assert catmod.for_flow(s3cfg, CatFlow).mode == "none"  # flag off -> classic


def test_backfill_from_classic_runs(s3cfg, repo):
    run_id = CatFlow.run(s3cfg, repo=repo, use_processes=False)  # classic: no catalog written
    cfg = catcfg(s3cfg)
    assert catmod.backfill(CatFlow, cfg) == 1
    con = duck(CatFlow, cfg)
    assert con.sql("SELECT run_id, total FROM agg").fetchall() == [(run_id, 3)]
    with pytest.raises(FileExistsError, match="--replace"):
        catmod.backfill(CatFlow, cfg)
    assert catmod.backfill(CatFlow, cfg, replace=True) == 1


@pytest.mark.slow
def test_resume_registers_copies_under_new_run_id(s3cfg, repo):
    cfg = catcfg(s3cfg)
    r1 = CatFlow.run(cfg, repo=repo, use_processes=False)
    r2 = CatFlow.run(cfg, repo=repo, use_processes=False, resume=r1)
    con = duck(CatFlow, cfg)
    assert {r[0] for r in con.sql("SELECT DISTINCT run_id FROM items").fetchall()} == {r1, r2}
    assert con.sql("SELECT sum(total) FROM agg").fetchone()[0] == 6  # both runs' agg rows
