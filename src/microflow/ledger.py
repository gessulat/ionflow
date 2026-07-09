import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from . import storage

_STD_FIELDS = [
    pa.field("run_id", pa.string()),
    pa.field("status", pa.string()),  # running | complete | failed
    pa.field("started", pa.timestamp("us")),
    pa.field("finished", pa.timestamp("us")),
    pa.field("commit", pa.string()),
    pa.field("steps_complete", pa.list_(pa.string())),
    pa.field("origin_run_id", pa.string()),
]

STD_COLS = {f.name for f in _STD_FIELDS}  # names params/custom columns must not take

_PRECONDITION = {"PreconditionFailed", "412"}


def _code(err):
    return err.response["Error"]["Code"]


def _get_or_none(client, bucket, key):
    try:
        return client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if _code(e) in storage.MISSING_KEY_CODES:
            return None
        raise


def _read(client, bucket, key):
    r = _get_or_none(client, bucket, key)
    if r is None:
        return [], None
    table = pq.read_table(io.BytesIO(r["Body"].read()))
    return table.to_pylist(), r["ETag"]


def _write(client, bucket, key, rows, custom_fields, etag):
    schema = pa.schema(_STD_FIELDS + custom_fields)
    cas = {"IfMatch" if etag else "IfNoneMatch": etag or "*"}
    storage.put_parquet(client, bucket, key, pa.Table.from_pylist(rows, schema=schema), **cas)


def upsert_run(client, bucket, key, row, custom_fields=(), retries=5):
    """Insert or replace this run's ledger row via S3 compare-and-swap."""
    for _ in range(retries):
        rows, etag = _read(client, bucket, key)
        rows = [r for r in rows if r["run_id"] != row["run_id"]]
        rows.append(dict(row))  # from_pylist fills any missing schema columns with null
        rows.sort(key=lambda r: r["started"])
        try:
            _write(client, bucket, key, rows, list(custom_fields), etag)
            return
        except ClientError as e:
            if _code(e) in _PRECONDITION:
                continue
            raise
    raise RuntimeError(f"ledger CAS failed after {retries} retries: {key}")


def read_all(client, bucket, key):
    return _read(client, bucket, key)[0]


def write_meta(client, bucket, key, meta):
    client.put_object(
        Bucket=bucket, Key=key, Body=json.dumps(meta, default=str).encode()
    )


def read_meta(client, bucket, key):
    r = _get_or_none(client, bucket, key)
    return None if r is None else json.loads(r["Body"].read())
