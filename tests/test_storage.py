import datetime as dt

import duckdb
import pyarrow as pa
import pytest

from miniflow import storage
from miniflow.schema import Table, check, pk


class Features(Table):
    id: int = pk
    ts: dt.datetime
    score: float = check("score >= 0")


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
    rel = storage.read_glob(con, storage.uri(s3cfg, "mf/ETL/runs/*/features/*.parquet"))
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
