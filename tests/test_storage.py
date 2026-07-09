import datetime as dt

import duckdb
import pyarrow as pa
import pytest

from microflow import storage
from microflow.config import Config
from microflow.schema import Table, check, pk


class Features(Table):
    id: int = pk
    ts: dt.datetime
    score: float = check("score >= 0")


def test_connect_uses_credential_chain_without_keys():
    # No explicit keys -> DuckDB secret should use the AWS credential chain, not crash.
    con = storage.connect(Config(bucket="b"))
    provider = con.sql(
        "SELECT provider FROM duckdb_secrets() WHERE name = 's'"
    ).fetchone()[0]
    assert provider == "credential_chain"


def test_object_age_uses_server_clock(s3cfg):
    client = storage.s3_client(s3cfg)
    client.put_object(Bucket=s3cfg.bucket, Key="x", Body=b"1")
    age = storage.object_age(client, s3cfg.bucket, "x")
    assert age is not None and -2 <= age <= 5  # HTTP Date has 1s resolution
    assert storage.object_age(client, s3cfg.bucket, "missing") is None


def _data():
    return pa.table(
        {
            "id": [1, 2],
            "ts": [dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 2)],
            "score": [0.5, 0.9],
        }
    )


def test_write_read_roundtrip(s3cfg):
    con = storage.connect(s3cfg)
    client = storage.s3_client(s3cfg)
    storage.write_part(
        con, client, s3cfg.bucket,
        "mf/ETL/runs/run_id=r1/features/part_0.parquet", _data(), Features,
    )
    rel = con.sql(storage.read_parquet_sql(storage.uri(s3cfg, "mf/ETL/runs/*/features/*.parquet")))
    rows, cols = rel.fetchall(), rel.columns
    got = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
    assert sorted(got["id"]) == [1, 2]
    assert got["run_id"] == ["r1", "r1"]  # hive column materialized


def test_check_constraint_rejects_bad_row(s3cfg):
    con = storage.connect(s3cfg)
    client = storage.s3_client(s3cfg)
    bad = pa.table({"id": [1], "ts": [dt.datetime(2026, 1, 1)], "score": [-1.0]})
    with pytest.raises(duckdb.ConstraintException):
        storage.write_part(
            con, client, s3cfg.bucket,
            "mf/ETL/runs/run_id=r2/features/part_0.parquet", bad, Features,
        )


def test_not_null_rejects(s3cfg):
    con = storage.connect(s3cfg)
    client = storage.s3_client(s3cfg)
    bad = pa.table({"id": [1], "ts": [None], "score": [0.1]})
    with pytest.raises(duckdb.ConstraintException):
        storage.write_part(
            con, client, s3cfg.bucket,
            "mf/ETL/runs/run_id=r3/features/part_0.parquet", bad, Features,
        )
