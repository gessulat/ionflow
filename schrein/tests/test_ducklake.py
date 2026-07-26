"""M1: DuckLake adapter — genesis, register-don't-write, replay, WAL, readers."""

import pathlib
import threading

import duckdb
import pytest
from conftest import make_client, moto_secret
from schrein.ducklake import _assert_clean

from schrein import DuckLake

TABLE_DDL = "CREATE TABLE lake.events (id INTEGER NOT NULL, name VARCHAR)"


@pytest.fixture
def lake(s3):
    client, bucket, endpoint = s3
    return DuckLake.at(client, bucket, "lake", secret_sql=moto_secret(endpoint))


def upload_rows(client, bucket, key, rows, tmp_path):
    """Write rows to a local Parquet file and upload it (staging for
    ducklake_add_data_files — the register-don't-write pattern)."""
    pq = tmp_path / pathlib.Path(key).name
    values = ", ".join(f"({i}, '{s}')" for i, s in rows)
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT CAST(id AS INTEGER) AS id, CAST(name AS VARCHAR) AS name "
        f"FROM (VALUES {values}) t(id, name)) TO '{pq}' (FORMAT parquet)"
    )
    con.close()
    client.upload_file(str(pq), bucket, key)


def test_read_before_genesis_raises(lake):
    with pytest.raises(FileNotFoundError):
        with lake.read():
            pass


def test_genesis_commit_and_read(lake):
    def fn(con):
        con.execute(TABLE_DDL)
        con.execute("INSERT INTO lake.events VALUES (1, 'a'), (2, 'b')")

    lake.commit(fn)
    with lake.read() as con:
        assert con.sql("SELECT id, name FROM lake.events ORDER BY id").fetchall() == [(1, "a"), (2, "b")]


def test_register_dont_write(lake, s3, tmp_path):
    """Preferred pattern: upload Parquet first, then a metadata-only commit."""
    client, bucket, _ = s3
    lake.commit(lambda con: con.execute(TABLE_DDL))
    upload_rows(client, bucket, "staging/x.parquet", [(1, "a"), (2, "b")], tmp_path)
    lake.commit(
        lambda con: con.execute(f"CALL ducklake_add_data_files('lake', 'events', 's3://{bucket}/staging/x.parquet')")
    )
    with lake.read() as con:
        assert con.sql("SELECT count(*) FROM lake.events").fetchone()[0] == 2


def test_replay_discipline(lake, s3, tmp_path):
    """A commit forced into one lost race replays user_fn exactly twice, and
    the result equals a clean run: both writers' rows present exactly once."""
    client, bucket, endpoint = s3
    lake.commit(lambda con: con.execute(TABLE_DDL))
    upload_rows(client, bucket, "staging/mine.parquet", [(1, "mine")], tmp_path)
    upload_rows(client, bucket, "staging/theirs.parquet", [(2, "theirs")], tmp_path)
    other = DuckLake.at(client, bucket, "lake", secret_sql=moto_secret(endpoint))
    calls = []

    def fn(con):
        calls.append(1)
        if len(calls) == 1:  # a competitor lands between our read and write
            other.commit(
                lambda c: c.execute(
                    f"CALL ducklake_add_data_files('lake', 'events', 's3://{bucket}/staging/theirs.parquet')"
                )
            )
        con.execute(f"CALL ducklake_add_data_files('lake', 'events', 's3://{bucket}/staging/mine.parquet')")

    lake.commit(fn, base_delay=0.01)
    assert len(calls) == 2
    with lake.read() as con:
        assert con.sql("SELECT id, name FROM lake.events ORDER BY id").fetchall() == [(1, "mine"), (2, "theirs")]


def test_wal_guard(tmp_path):
    """A transaction leaving an uncheckpointed WAL must fail loudly (test 7)."""
    local = tmp_path / "catalog.ducklake"
    local.write_bytes(b"catalog")
    _assert_clean(local)  # no sidecar: fine
    (tmp_path / "catalog.ducklake.wal").write_bytes(b"wal")
    with pytest.raises(RuntimeError, match="WAL"):
        _assert_clean(local)


def test_reader_sees_consistent_snapshots(lake, s3, tmp_path):
    """Readers during concurrent swaps see a committed version — counts only
    ever move forward, and no read errors."""
    client, bucket, endpoint = s3
    lake.commit(lambda con: con.execute(TABLE_DDL))
    n_commits = 4
    for k in range(n_commits):
        upload_rows(client, bucket, f"staging/r{k}.parquet", [(k, f"r{k}")], tmp_path)
    errors = []

    def writer():
        try:
            w = DuckLake.at(client, bucket, "lake", secret_sql=moto_secret(endpoint))
            for k in range(n_commits):
                w.commit(
                    lambda con, k=k: con.execute(
                        f"CALL ducklake_add_data_files('lake', 'events', 's3://{bucket}/staging/r{k}.parquet')"
                    )
                )
        except Exception as e:  # surfaced in the main thread's assert
            errors.append(e)

    t = threading.Thread(target=writer)
    t.start()
    seen = []
    while t.is_alive() or not seen or seen[-1] < n_commits:
        with lake.read() as con:
            seen.append(con.sql("SELECT count(*) FROM lake.events").fetchone()[0])
        if not t.is_alive() and seen[-1] == n_commits:
            break
    t.join()
    assert not errors
    assert seen == sorted(seen)  # monotonic: never a torn or rolled-back view
    assert seen[-1] == n_commits


@pytest.mark.slow
def test_lost_update_hammer_ducklake(s3, tmp_path):
    """The §10 hammer at the DuckLake layer: N threads x M metadata-only
    commits; final table holds exactly N*M rows."""
    client, bucket, endpoint = s3
    n_threads, n_commits = 4, 6
    DuckLake.at(client, bucket, "lake", secret_sql=moto_secret(endpoint)).commit(lambda con: con.execute(TABLE_DDL))
    for i in range(n_threads):
        for j in range(n_commits):
            upload_rows(client, bucket, f"staging/p{i}_{j}.parquet", [(i * n_commits + j, f"p{i}_{j}")], tmp_path)
    errors = []

    def worker(i):
        try:
            w = DuckLake.at(make_client(endpoint), bucket, "lake", secret_sql=moto_secret(endpoint))
            for j in range(n_commits):
                w.commit(
                    lambda con, i=i, j=j: con.execute(
                        f"CALL ducklake_add_data_files('lake', 'events', 's3://{bucket}/staging/p{i}_{j}.parquet')"
                    ),
                    max_attempts=64,
                    base_delay=0.05,
                )
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    lake = DuckLake.at(client, bucket, "lake", secret_sql=moto_secret(endpoint))
    with lake.read() as con:
        ids = [r[0] for r in con.sql("SELECT id FROM lake.events ORDER BY id").fetchall()]
    assert ids == list(range(n_threads * n_commits))
