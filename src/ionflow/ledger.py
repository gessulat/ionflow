import json

import pyarrow as pa

from schrein import CasRegister

from . import storage

_BASE_FIELDS = [
    pa.field("run_id", pa.string()),
    pa.field("status", pa.string()),  # running | complete | failed
    pa.field("started", pa.timestamp("us")),
    pa.field("finished", pa.timestamp("us")),
    pa.field("commit", pa.string()),
    pa.field("origin_run_id", pa.string()),
]

BASE_COLS = {f.name for f in _BASE_FIELDS}  # names params/custom columns must not take


def _rows(blob):
    return [] if blob is None else storage.table_from_bytes(blob).to_pylist()


def upsert_runs(client, bucket, key, rows, custom_fields=(), retries=5):
    """Insert or replace ledger rows by run_id, all in one CAS commit."""
    schema = pa.schema(_BASE_FIELDS + list(custom_fields))
    new_ids = {r["run_id"] for r in rows}

    def fn(old):
        merged = [r for r in _rows(old) if r["run_id"] not in new_ids]
        merged += [dict(r) for r in rows]  # from_pylist fills missing schema columns with null
        merged.sort(key=lambda r: r["started"])
        return storage.parquet_bytes(pa.Table.from_pylist(merged, schema=schema))

    CasRegister(client, bucket, key).transact(fn, max_attempts=retries, base_delay=0.05)


def upsert_run(client, bucket, key, row, custom_fields=(), retries=5):
    """Insert or replace this run's ledger row; the CAS loop is schrein's CasRegister."""
    upsert_runs(client, bucket, key, [row], custom_fields, retries)


def read_all(client, bucket, key):
    return _rows(storage.get_bytes_or_none(client, bucket, key))


def write_meta(client, bucket, key, meta):
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(meta, default=str).encode())


def read_meta(client, bucket, key):
    blob = storage.get_bytes_or_none(client, bucket, key)
    return None if blob is None else json.loads(blob)
