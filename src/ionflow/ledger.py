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
    pa.field("steps_complete", pa.list_(pa.string())),
    pa.field("origin_run_id", pa.string()),
]

BASE_COLS = {f.name for f in _BASE_FIELDS}  # names params/custom columns must not take


def _rows(blob):
    return [] if blob is None else storage.table_from_bytes(blob).to_pylist()


def upsert_run(client, bucket, key, row, custom_fields=(), retries=5):
    """Insert or replace this run's ledger row; the CAS loop is schrein's CasRegister."""
    schema = pa.schema(_BASE_FIELDS + list(custom_fields))

    def fn(old):
        rows = [r for r in _rows(old) if r["run_id"] != row["run_id"]]
        rows.append(dict(row))  # from_pylist fills any missing schema columns with null
        rows.sort(key=lambda r: r["started"])
        return storage.parquet_bytes(pa.Table.from_pylist(rows, schema=schema))

    CasRegister(client, bucket, key).transact(fn, max_attempts=retries, base_delay=0.05)


def read_all(client, bucket, key):
    return _rows(storage.get_bytes_or_none(client, bucket, key))


def write_meta(client, bucket, key, meta):
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(meta, default=str).encode())


def read_meta(client, bucket, key):
    blob = storage.get_bytes_or_none(client, bucket, key)
    return None if blob is None else json.loads(blob)
