"""Shared test helpers, imported as `from helpers import ...` (same convention
as ionflow's tests/flows.py; pinned by pythonpath = ["tests"] in pyproject)."""

import pathlib
import threading

import duckdb

import schrein
from schrein import DuckLake

PREFIX = "lake"  # every test lake lives at s3://<bucket>/lake/


def error_code(excinfo):
    """AWS error code from a pytest.raises(ClientError) excinfo."""
    return schrein.error_code(excinfo.value)


def make_client(endpoint):
    """A fresh boto3 client against moto (one per thread in hammer tests)."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def moto_secret(endpoint):
    """DuckDB S3 secret pointing at the moto server (path-style, no SSL)."""
    host = endpoint.removeprefix("http://")
    return (
        "CREATE OR REPLACE SECRET moto (TYPE s3, KEY_ID 'test', SECRET 'test', "
        f"REGION 'us-east-1', ENDPOINT '{host}', USE_SSL false, URL_STYLE 'path')"
    )


def lake_at(s3, client=None):
    """A DuckLake on the test bucket's standard prefix; pass `client` for a per-thread handle."""
    default_client, bucket, endpoint = s3
    return DuckLake.at(client or default_client, bucket, PREFIX, secret_sql=moto_secret(endpoint))


_copy_con = duckdb.connect()  # staging COPYs all run on the main thread; one connection suffices


def upload_rows(client, bucket, key, rows, tmp_path):
    """Write rows to a local Parquet file and upload it (staging for
    ducklake_add_data_files — the register-don't-write pattern)."""
    pq = tmp_path / pathlib.Path(key).name
    values = ", ".join(f"({i}, '{s}')" for i, s in rows)
    _copy_con.execute(
        f"COPY (SELECT CAST(id AS INTEGER) AS id, CAST(name AS VARCHAR) AS name "
        f"FROM (VALUES {values}) t(id, name)) TO '{pq}' (FORMAT parquet)"
    )
    client.upload_file(str(pq), bucket, key)


def add_files(lake, bucket, name, **commit_kwargs):
    """One metadata-only commit registering staging/<name>.parquet into events."""
    lake.commit(
        lambda con: con.execute(
            f"CALL ducklake_add_data_files('lake', 'events', 's3://{bucket}/staging/{name}.parquet')"
        ),
        **commit_kwargs,
    )


def run_threads(fn, n):
    """Run fn(i) across n threads; join all, then re-raise the first worker error."""
    errors = []

    def wrapped(i):
        try:
            fn(i)
        except BaseException as e:  # noqa: BLE001 — re-raised below, not swallowed
            errors.append(e)

    threads = [threading.Thread(target=wrapped, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
