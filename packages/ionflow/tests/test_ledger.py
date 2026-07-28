"""Ledger semantics. The CAS precondition contract itself (412 on stale
If-Match, genesis conflicts) is schrein's and is tested in its suite."""

import datetime as dt

import pyarrow as pa

from ionflow.store import Store


def _space(cfg, flow_id="ETL"):
    return Store.of(cfg).space(flow_id)


def _dt(day):
    return dt.datetime(2026, 1, day)  # naive UTC, matches runner._now()


def _row(run_id, started, **extra):
    return {"run_id": run_id, "started": started, "commit": "abc", **extra}


def test_insert_then_read(cfg):
    s = _space(cfg)
    s.upsert_runs([_row("r1", _dt(1))])
    rows = s.read_rows(s.ledger)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "r1"
    assert rows[0]["finished"] is None


def test_update_same_run_replaces(cfg):
    s = _space(cfg)
    s.upsert_runs([_row("r1", _dt(1))])
    s.upsert_runs([_row("r1", _dt(1), finished=_dt(2))])
    rows = s.read_rows(s.ledger)
    assert len(rows) == 1
    assert rows[0]["finished"] == _dt(2)


def test_multiple_runs_appended(cfg):
    s = _space(cfg)
    s.upsert_runs([_row("r1", _dt(1))])
    s.upsert_runs([_row("r2", _dt(2))])
    rows = s.read_rows(s.ledger)
    assert [r["run_id"] for r in rows] == ["r1", "r2"]


def test_custom_columns_stored(cfg):
    s = _space(cfg, "T")
    fields = [pa.field("auc", pa.float64())]
    s.upsert_runs([_row("r1", _dt(1), auc=0.9)], custom_fields=fields)
    rows = s.read_rows(s.ledger)
    assert rows[0]["auc"] == 0.9


def test_meta_roundtrip(cfg):
    s = _space(cfg)
    assert s.read_meta("r1") is None
    s.write_meta("r1", {"run_id": "r1", "steps_complete": ["raw/0"]})
    assert s.read_meta("r1")["steps_complete"] == ["raw/0"]
