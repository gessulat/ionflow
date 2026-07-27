import datetime as dt

import duckdb
import pyarrow as pa
import pytest

from ionflow import storage
from ionflow.config import Config
from ionflow.schema import Table, check, primary_key


class Features(Table):
    id: int = primary_key
    ts: dt.datetime
    score: float = check("score >= 0")


def test_connect_uses_credential_chain_without_keys(monkeypatch):
    # No explicit keys -> DuckDB secret should use the AWS credential chain, not crash.
    # CREATE SECRET validates the chain eagerly, so pin env credentials for it to
    # find — CI runners have none anywhere in the chain.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "chain")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "chain")
    con = storage.connect(Config(bucket="b"))
    provider = con.sql("SELECT provider FROM duckdb_secrets() WHERE name = 's'").fetchone()[0]
    assert provider == "credential_chain"


def test_write_part_roundtrips_list_columns(s3cfg):
    class Emb(Table):
        id: int
        vec: list[float]

    client = storage.s3_client(s3cfg)
    data = pa.table({"id": [1, 2], "vec": [[1.0, 2.0], [3.0]]})
    key = "mf/E/runs/run_id=r/e/part_0.parquet"
    storage.write_part(client, s3cfg.bucket, key, data, Emb)
    assert storage.table_from_bytes(storage.get_bytes_or_none(client, s3cfg.bucket, key)).column("vec").to_pylist() == [
        [1.0, 2.0],
        [3.0],
    ]


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
        client,
        s3cfg.bucket,
        "mf/ETL/runs/run_id=r1/features/part_0.parquet",
        _data(),
        Features,
    )
    rel = con.sql(storage.read_parquet_sql(storage.uri(s3cfg, "mf/ETL/runs/*/features/*.parquet")))
    rows, cols = rel.fetchall(), rel.columns
    got = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
    assert sorted(got["id"]) == [1, 2]
    assert got["run_id"] == ["r1", "r1"]  # hive column materialized


def test_check_constraint_rejects_bad_row(s3cfg):
    client = storage.s3_client(s3cfg)
    bad = pa.table({"id": [1], "ts": [dt.datetime(2026, 1, 1)], "score": [-1.0]})
    with pytest.raises(storage.ConstraintViolation):
        storage.write_part(
            client,
            s3cfg.bucket,
            "mf/ETL/runs/run_id=r2/features/part_0.parquet",
            bad,
            Features,
        )


def test_not_null_rejects(s3cfg):
    client = storage.s3_client(s3cfg)
    bad = pa.table({"id": [1], "ts": [None], "score": [0.1]})
    with pytest.raises(storage.ConstraintViolation):
        storage.write_part(
            client,
            s3cfg.bucket,
            "mf/ETL/runs/run_id=r3/features/part_0.parquet",
            bad,
            Features,
        )


@pytest.mark.slow
def test_httpfs_write_unsafe_canary(s3cfg):
    """Assert the upstream behavior that forces the boto3-writes rule (ADR 0010).

    A `COPY TO s3://` that errors mid-stream still finalizes its multipart
    upload — httpfs closes the file handle with no abort-on-error path — so a
    corrupt part becomes visible on S3, which ionflow's completeness and
    resume logic would trust. If this test FAILS after a DuckDB upgrade,
    native writes have become failure-atomic: supersede ADR 0010 before
    deleting the canary.
    """
    con = storage.connect(s3cfg)
    with pytest.raises(duckdb.Error):
        con.execute(
            "COPY (SELECT CASE WHEN range = 900000 THEN error('boom') "
            "ELSE range::VARCHAR END AS v FROM range(1000000)) "
            f"TO '{storage.uri(s3cfg, 'mf/CANARY/failed.parquet')}' "
            "(FORMAT PARQUET, ROW_GROUP_SIZE 10000)"
        )
    leftovers = storage.list_keys(storage.s3_client(s3cfg), s3cfg.bucket, "mf/CANARY/")
    assert leftovers == ["mf/CANARY/failed.parquet"], (
        "failed COPY TO s3:// no longer leaves a partial object — revisit ADR 0010"
    )
