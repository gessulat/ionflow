"""M1: DuckLake adapter — genesis, register-don't-write, replay, WAL, readers."""

import threading
import time

import pytest
from helpers import add_files, lake_at, make_client, run_threads, upload_rows
from schrein.ducklake import _assert_clean

TABLE_DDL = "CREATE TABLE lake.events (id INTEGER NOT NULL, name VARCHAR)"


@pytest.fixture
def lake(s3):
    return lake_at(s3)


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
    add_files(lake, bucket, "x")
    with lake.read() as con:
        assert con.sql("SELECT count(*) FROM lake.events").fetchone()[0] == 2


def test_replay_discipline(lake, s3, tmp_path):
    """A commit forced into one lost race replays user_fn exactly twice, and
    the result equals a clean run: both writers' rows present exactly once."""
    client, bucket, _ = s3
    lake.commit(lambda con: con.execute(TABLE_DDL))
    upload_rows(client, bucket, "staging/mine.parquet", [(1, "mine")], tmp_path)
    upload_rows(client, bucket, "staging/theirs.parquet", [(2, "theirs")], tmp_path)
    calls = []

    def fn(con):
        calls.append(1)
        if len(calls) == 1:  # a competitor lands between our read and write
            add_files(lake_at(s3), bucket, "theirs")
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
    client, bucket, _ = s3
    lake.commit(lambda con: con.execute(TABLE_DDL))
    n_commits = 4
    for k in range(n_commits):
        upload_rows(client, bucket, f"staging/r{k}.parquet", [(k, f"r{k}")], tmp_path)
    errors = []

    def writer():
        try:
            for k in range(n_commits):
                add_files(lake_at(s3), bucket, f"r{k}")
        except BaseException as e:  # noqa: BLE001 — surfaced by the asserts below
            errors.append(e)

    t = threading.Thread(target=writer)
    t.start()
    seen = []
    while True:
        with lake.read() as con:
            seen.append(con.sql("SELECT count(*) FROM lake.events").fetchone()[0])
        if seen[-1] == n_commits or (not t.is_alive() and errors):
            break
        time.sleep(0.02)
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
    lake_at(s3).commit(lambda con: con.execute(TABLE_DDL))
    for i in range(n_threads):
        for j in range(n_commits):
            upload_rows(client, bucket, f"staging/p{i}_{j}.parquet", [(i * n_commits + j, f"p{i}_{j}")], tmp_path)

    def worker(i):
        lake = lake_at(s3, make_client(endpoint))
        for j in range(n_commits):
            add_files(lake, bucket, f"p{i}_{j}", max_attempts=64, base_delay=0.05)

    run_threads(worker, n_threads)
    with lake_at(s3).read() as con:
        ids = [r[0] for r in con.sql("SELECT id FROM lake.events ORDER BY id").fetchall()]
    assert ids == list(range(n_threads * n_commits))


def test_expire_and_gc(lake, s3):
    """M2: expiry is a metadata commit; gc reaps expired + orphaned files only
    past the grace TTL, and the catalog stays readable throughout."""
    client, bucket, _ = s3
    lake.commit(lambda con: con.execute("CREATE TABLE lake.t (id INTEGER)"))
    # big enough to defeat data inlining, so a real parquet lands in data/
    lake.commit(lambda con: con.execute("INSERT INTO lake.t SELECT unnest(range(5000))::INTEGER"))
    lake.commit(lambda con: con.execute("DROP TABLE lake.t"))  # old snapshots still reference it
    client.put_object(Bucket=bucket, Key="lake/data/orphan.parquet", Body=b"junk")  # never committed

    assert lake.gc(grace_days=7, dry_run=True) == []  # everything is younger than the grace TTL
    assert lake.gc(grace_days=-1, dry_run=True) == ["lake/data/orphan.parquet"]  # data file still retained
    lake.expire_snapshots(older_than_days=0)
    # expiry may or may not physically delete the dropped table's file itself;
    # either way gc must reap whatever is left, orphan included
    dead = lake.gc(grace_days=-1, dry_run=True)
    assert "lake/data/orphan.parquet" in dead
    lake.gc(grace_days=-1, dry_run=False)
    assert client.list_objects_v2(Bucket=bucket, Prefix="lake/data/").get("Contents", []) == []
    with lake.read() as con:  # catalog stays attachable after maintenance
        assert con.sql("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'").fetchone()[0] == 0


def test_probe_passes_on_moto_and_rejects_lax_backend(s3):
    from schrein import CasRegister

    client, bucket, _ = s3
    reg = CasRegister(client, bucket, "lake/catalog.ducklake")
    reg.probe()  # moto (wrapped) enforces conditions: must pass

    class Lax:  # a backend that ignores preconditions entirely
        def __getattr__(self, name):
            return getattr(client, name)

        def put_object(self, **kw):
            kw.pop("IfMatch", None), kw.pop("IfNoneMatch", None)
            return client.put_object(**kw)

    with pytest.raises(RuntimeError, match="conditional"):
        CasRegister(Lax(), bucket, "lake/catalog.ducklake").probe()
