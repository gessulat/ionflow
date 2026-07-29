"""The lake's run bookkeeping in isolation: one CAS commit upserts the run row,
inserts manifest rows exactly once (replay-safe), and `run_state` reads one run
back. Real runs exercise it end to end (the failed→complete rows in test_fanout,
test_e2e's ledger row); `sync` depends on the upsert being idempotent by run_id.
The CAS precondition contract (412 on a stale If-Match, genesis conflicts) is
schrein's and is tested there.
"""

import datetime as dt

from ionflow import Flow, Table
from ionflow.lake import Catalog
from ionflow.space import Space
from ionflow.state import Manifest
from ionflow.store import Store


class LRows(Table):
    x: int


class LedgerFlow(Flow):
    def data(self) -> LRows: ...


def _row(run_id, day, status="running", finished=None):
    return [run_id, status, dt.datetime(2026, 1, day), finished, "abc", None, "{}", "{}"]


def test_commit_run_upserts_by_run_id_and_replays_safely(cfg):
    cat = Catalog(Space.for_flow(Store.of(cfg), LedgerFlow))
    rows = [("data", 0, "elsewhere/part_0.parquet")]  # a reference: no file registration
    cat.commit_run("r1", row=_row("r1", 1), rows=rows, genesis=LedgerFlow)
    # a replayed commit that already landed must not duplicate; the row upserts
    cat.commit_run("r1", row=_row("r1", 1, "complete", dt.datetime(2026, 1, 2)), rows=rows)
    cat.commit_run("r2", row=_row("r2", 3))

    runs = cat.runs()
    assert [r["run_id"] for r in runs] == ["r1", "r2"]  # replaced, not duplicated
    assert (runs[0]["status"], runs[0]["finished"]) == ("complete", dt.datetime(2026, 1, 2))

    row, triples = cat.run_state("r1")
    assert row["run_id"] == "r1"
    assert triples == [("data", 0, "elsewhere/part_0.parquet")]  # inserted once, not per commit
    assert Manifest.from_rows(triples).get("data", 0) == "elsewhere/part_0.parquet"

    assert cat.run_state("latest")[0]["run_id"] == "r1"  # newest COMPLETE, not newest
    assert cat.run_state("missing") == (None, [])


def test_meta_is_absent_until_written(cfg):
    s = Store.of(cfg).space("ETL")
    assert s.read_meta("r1") is None
    s.write_meta("r1", {"run_id": "r1", "manifest": {"raw": [[0, "some/key"]]}})
    assert s.read_meta("r1")["manifest"] == {"raw": [[0, "some/key"]]}
