"""The catalog: every stable flow's DuckLake, registered step outputs, vanilla
readers. Only prototype flows are catalog-less.

The catalog only receives table schemas and file registrations; run
bookkeeping (ledger/meta) is untouched — see docs/specs/ionflow-on-schrein.md.
"""

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


def test_run_registers_and_vanilla_reader_sees_it(s3cfg, repo):
    run_id = CatFlow.run(s3cfg, repo=repo, use_processes=False)

    # ionflow read path: views over the attached catalog snapshot
    con = duck(CatFlow, s3cfg)
    assert con.sql("SELECT total FROM agg").fetchone()[0] == 3
    assert con.sql("SELECT run_id, key, v FROM items ORDER BY key").fetchall() == [(run_id, "a", 1), (run_id, "b", 2)]

    # vanilla reader: stock DuckDB, no ionflow — the headline feature
    v = duckdb.connect()
    v.execute("INSTALL httpfs; LOAD httpfs")
    v.execute(secret_sql(s3cfg))
    load_ducklake(v)
    root = Layout.for_flow(s3cfg, CatFlow).root
    v.execute(f"ATTACH 'ducklake:s3://{s3cfg.bucket}/{root}/catalog.ducklake' AS lake (READ_ONLY)")
    assert v.sql("SELECT sum(v) FROM lake.items").fetchone()[0] == 3
    assert v.sql("SELECT count(*) FROM lake.source").fetchone()[0] == 3
    v.close()


def test_prototypes_have_no_catalog_and_stable_flows_always_do(s3cfg):
    assert catmod.for_flow(s3cfg, ProtoFlow).mode == "none"
    assert catmod.for_flow(s3cfg, CatFlow).mode == "ducklake"


def test_backfill_restores_a_lost_catalog(s3cfg, repo):
    run_id = CatFlow.run(s3cfg, repo=repo, use_processes=False)
    layout = Layout.for_flow(s3cfg, CatFlow)
    client = catmod.storage.s3_client(s3cfg)

    # simulate a clobbered/lost catalog: runs exist, register object gone
    client.delete_object(Bucket=s3cfg.bucket, Key=f"{layout.root}/{catmod.CATALOG_KEY}")
    with pytest.raises(RuntimeError, match="catalog-backfill"):
        duck(CatFlow, s3cfg)

    assert catmod.backfill(CatFlow, s3cfg) == 1
    con = duck(CatFlow, s3cfg)
    assert con.sql("SELECT run_id, total FROM agg").fetchall() == [(run_id, 3)]

    with pytest.raises(FileExistsError, match="--replace"):
        catmod.backfill(CatFlow, s3cfg)
    assert catmod.backfill(CatFlow, s3cfg, replace=True) == 1


@pytest.mark.slow
def test_resume_registers_copies_under_new_run_id(s3cfg, repo):
    r1 = CatFlow.run(s3cfg, repo=repo, use_processes=False)
    r2 = CatFlow.run(s3cfg, repo=repo, use_processes=False, resume=r1)
    con = duck(CatFlow, s3cfg)
    assert {r[0] for r in con.sql("SELECT DISTINCT run_id FROM items").fetchall()} == {r1, r2}
    assert con.sql("SELECT sum(total) FROM agg").fetchone()[0] == 6  # both runs' agg rows
