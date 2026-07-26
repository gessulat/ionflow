import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

from schrein import CasRegister

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
    return [] if blob is None else pq.read_table(io.BytesIO(blob)).to_pylist()


def _parquet(rows, schema):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buf)
    return buf.getvalue()


def upsert_run(client, bucket, key, row, custom_fields=(), retries=5, reset_on_conflict=False):
    """Insert or replace this run's ledger row; the CAS loop is schrein's CasRegister.

    `reset_on_conflict` (prototype flows): when existing rows no longer fit
    the current schema — scratch flows churn types freely — start a fresh
    ledger holding only this run instead of failing.
    """
    schema = pa.schema(_BASE_FIELDS + list(custom_fields))

    def fn(old):
        rows = [r for r in _rows(old) if r["run_id"] != row["run_id"]]
        rows.append(dict(row))  # from_pylist fills any missing schema columns with null
        rows.sort(key=lambda r: r["started"])
        try:
            return _parquet(rows, schema)
        except (pa.ArrowInvalid, pa.ArrowTypeError):  # old rows don't fit the current schema
            if not reset_on_conflict:
                raise
            return _parquet([dict(row)], schema)

    CasRegister(client, bucket, key).transact(fn, max_attempts=retries, base_delay=0.05)


def read_all(client, bucket, key):
    return _rows(CasRegister(client, bucket, key).read()[0])


def write_meta(client, bucket, key, meta):
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(meta, default=str).encode())


def read_meta(client, bucket, key):
    blob, _ = CasRegister(client, bucket, key).read()
    return None if blob is None else json.loads(blob)
