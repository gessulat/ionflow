"""Ledger CAS semantics in isolation. Real runs exercise it end to end (the
failed→complete rows in test_fanout, test_e2e's ledger row); this pins the
upsert contract itself, which `sync` depends on being idempotent by run_id.
The CAS precondition contract (412 on a stale If-Match, genesis conflicts) is
schrein's and is tested there.
"""

import datetime as dt

import pyarrow as pa

from ionflow.store import Store


def _dt(day):
    return dt.datetime(2026, 1, day)  # naive UTC, the project's one convention


def _row(run_id, day, **extra):
    return {"run_id": run_id, "started": _dt(day), "commit": "abc", **extra}


def test_upsert_replaces_by_run_id_and_appends_others(cfg):
    s = Store.of(cfg).space("ETL")
    s.upsert_runs([_row("r1", 1)], custom_fields=[pa.field("auc", pa.float64())])
    assert s.read_rows(s.ledger)[0]["finished"] is None

    s.upsert_runs([_row("r1", 1, finished=_dt(2), auc=0.9)], custom_fields=[pa.field("auc", pa.float64())])
    s.upsert_runs([_row("r2", 2)], custom_fields=[pa.field("auc", pa.float64())])

    rows = s.read_rows(s.ledger)
    assert [r["run_id"] for r in rows] == ["r1", "r2"]  # replaced, not duplicated
    assert (rows[0]["finished"], rows[0]["auc"]) == (_dt(2), 0.9)


def test_meta_is_absent_until_written(cfg):
    s = Store.of(cfg).space("ETL")
    assert s.read_meta("r1") is None
    s.write_meta("r1", {"run_id": "r1", "manifest": {"raw": [[0, "some/key"]]}})
    assert s.read_meta("r1")["manifest"] == {"raw": [[0, "some/key"]]}
