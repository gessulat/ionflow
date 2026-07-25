import datetime as dt

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError

from microflow import ledger, storage


def _client(cfg):
    return storage.s3_client(cfg)


def _dt(day):
    return dt.datetime(2026, 1, day)  # naive UTC, matches runner._now()


def _row(run_id, started, **extra):
    return {"run_id": run_id, "started": started, "commit": "abc", **extra}


def test_insert_then_read(s3cfg):
    c = _client(s3cfg)
    key = "mf/ETL/ledger.parquet"
    ledger.upsert_run(c, s3cfg.bucket, key, _row("r1", _dt(1)))
    rows = ledger.read_all(c, s3cfg.bucket, key)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "r1"
    assert rows[0]["finished"] is None


def test_update_same_run_replaces(s3cfg):
    c = _client(s3cfg)
    key = "mf/ETL/ledger.parquet"
    ledger.upsert_run(c, s3cfg.bucket, key, _row("r1", _dt(1)))
    ledger.upsert_run(
        c,
        s3cfg.bucket,
        key,
        _row("r1", _dt(1), finished=_dt(2)),
    )
    rows = ledger.read_all(c, s3cfg.bucket, key)
    assert len(rows) == 1
    assert rows[0]["finished"] == _dt(2)


def test_multiple_runs_appended(s3cfg):
    c = _client(s3cfg)
    key = "mf/ETL/ledger.parquet"
    ledger.upsert_run(c, s3cfg.bucket, key, _row("r1", _dt(1)))
    ledger.upsert_run(c, s3cfg.bucket, key, _row("r2", _dt(2)))
    rows = ledger.read_all(c, s3cfg.bucket, key)
    assert [r["run_id"] for r in rows] == ["r1", "r2"]


def test_custom_columns_stored(s3cfg):
    c = _client(s3cfg)
    key = "mf/T/ledger.parquet"
    fields = [pa.field("auc", pa.float64())]
    ledger.upsert_run(
        c,
        s3cfg.bucket,
        key,
        _row("r1", _dt(1), auc=0.9),
        custom_fields=fields,
    )
    rows = ledger.read_all(c, s3cfg.bucket, key)
    assert rows[0]["auc"] == 0.9


def test_stale_etag_rejected_by_backend(s3cfg):
    """Confirms the backend enforces If-Match (CAS precondition)."""
    c = _client(s3cfg)
    key = "mf/ETL/ledger.parquet"
    ledger.upsert_run(c, s3cfg.bucket, key, _row("r1", _dt(1)))
    with pytest.raises(ClientError) as e:
        c.put_object(Bucket=s3cfg.bucket, Key=key, Body=b"x", IfMatch='"deadbeef"')
    assert e.value.response["Error"]["Code"] in ("PreconditionFailed", "412")


def test_meta_roundtrip(s3cfg):
    c = _client(s3cfg)
    key = "mf/ETL/runs/run_id=r1/_meta.json"
    assert ledger.read_meta(c, s3cfg.bucket, key) is None
    ledger.write_meta(c, s3cfg.bucket, key, {"run_id": "r1", "steps_complete": ["raw/0"]})
    assert ledger.read_meta(c, s3cfg.bucket, key)["steps_complete"] == ["raw/0"]
