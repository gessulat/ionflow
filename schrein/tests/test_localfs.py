"""LocalClient: the boto3 client surface on a directory, CAS enforced for real."""

import datetime as dt

import pytest
from botocore.exceptions import ClientError
from helpers import run_threads
from schrein.register import RETRYABLE

from schrein import CasRegister, DuckLake, LocalClient

KEY = "pre/fix/reg/state"


@pytest.fixture
def local(tmp_path):
    return LocalClient(tmp_path / "store")


def _code(excinfo):
    return excinfo.value.response["Error"]["Code"]


def test_put_get_roundtrip_and_etag(local):
    etag = local.put_object(Key="a/b.txt", Body=b"hello")["ETag"]
    r = local.get_object(Key="a/b.txt")
    assert r["Body"].read() == b"hello"
    assert r["ETag"] == etag == '"5d41402abc4b2a76b9719d911017c592"'  # md5, quoted like S3


def test_get_missing_raises_nosuchkey(local):
    with pytest.raises(ClientError) as e:
        local.get_object(Key="nope")
    assert _code(e) == "NoSuchKey"


def test_key_cannot_escape_root(local):
    with pytest.raises(ValueError):
        local.put_object(Key="../outside", Body=b"x")


def test_conditional_genesis_and_stale_ifmatch(local):
    reg = CasRegister(local, "ignored", KEY)
    etag1 = reg.write(b"v1", None)
    with pytest.raises(ClientError) as e:  # second genesis
        reg.write(b"v2", None)
    assert _code(e) in RETRYABLE
    reg.write(b"v2", etag1)
    with pytest.raises(ClientError) as e:  # stale If-Match
        reg.write(b"v3", etag1)
    assert _code(e) in RETRYABLE
    assert reg.read()[0] == b"v2"


def test_probe_passes(local):
    CasRegister(local, "ignored", KEY).probe()


def test_list_head_copy_delete(local):
    local.put_object(Key="p/one", Body=b"1")
    local.put_object(Key="p/two", Body=b"22")
    local.put_object(Key="q/other", Body=b"3")
    pages = list(local.get_paginator("list_objects_v2").paginate(Bucket="x", Prefix="p/"))
    assert [o["Key"] for page in pages for o in page["Contents"]] == ["p/one", "p/two"]

    head = local.head_object(Key="p/two")
    assert head["ContentLength"] == 2
    assert (dt.datetime.now(dt.UTC) - head["LastModified"]).total_seconds() < 60

    local.copy_object(CopySource={"Bucket": "x", "Key": "p/one"}, Key="p/copied")
    assert local.get_object(Key="p/copied")["Body"].read() == b"1"

    local.delete_object(Key="p/one")
    local.delete_object(Key="p/one")  # idempotent, like S3
    with pytest.raises(ClientError):
        local.head_object(Key="p/one")


def test_upload_file_and_delete_objects(local, tmp_path):
    src = tmp_path / "blob"
    src.write_bytes(b"payload")
    local.upload_file(str(src), "ignored", "up/loaded")
    assert local.get_object(Key="up/loaded")["Body"].read() == b"payload"
    local.delete_objects(Bucket="x", Delete={"Objects": [{"Key": "up/loaded"}]})
    with pytest.raises(ClientError):
        local.get_object(Key="up/loaded")


def test_lost_update_hammer_threads(local):
    """N threads x M CAS commits against one directory — no update lost."""
    n_threads, n_commits = 8, 15

    def worker(i):
        reg = CasRegister(local, "ignored", KEY)
        for j in range(n_commits):
            row = f"{i}:{j}".encode()
            reg.transact(
                lambda old, row=row: old + b"\n" + row if old else row,
                max_attempts=128,
                base_delay=0.005,
            )

    run_threads(worker, n_threads)
    rows = CasRegister(local, "ignored", KEY).read()[0].split(b"\n")
    assert len(rows) == len(set(rows)) == n_threads * n_commits


def test_ducklake_on_local_store(local, tmp_path):
    """The full DuckLake cycle against a plain directory: commit an INSERT
    (rows may inline into the catalog) and register a staged parquet by path —
    the pattern ionflow's catalog uses."""
    import duckdb

    lake = DuckLake.at(local, "ignored", "lake")
    lake.data_path = str(local.root / "lake" / "data") + "/"  # local, not s3://

    lake.commit(lambda con: con.execute("CREATE TABLE lake.events (id INTEGER, name VARCHAR)"))
    lake.commit(lambda con: con.execute("INSERT INTO lake.events VALUES (1, 'a'), (2, 'b')"))

    staged = tmp_path / "staged.parquet"
    duckdb.connect().execute(f"COPY (SELECT 3 AS id, 'c' AS name) TO '{staged}' (FORMAT parquet)")
    local.upload_file(str(staged), "ignored", "lake/staging/part.parquet")
    part = local.root / "lake" / "staging" / "part.parquet"
    lake.commit(lambda con: con.execute(f"CALL ducklake_add_data_files('lake', 'events', '{part}')"))

    with lake.read() as con:
        assert con.sql("SELECT count(*), max(id) FROM lake.events").fetchone() == (3, 3)
