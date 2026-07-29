"""DuckLake catalog as a CAS-swapped immutable artifact on one S3 key.

Commit protocol: download the catalog, run the user's transaction against a
local ATTACH, conditionally upload the result; on a lost race, replay the
transaction against a fresh download (rebase). Readers download-then-attach,
so they always see exactly one committed version.
"""

import contextlib
import datetime as dt
import functools
import importlib
import pathlib
import tempfile

import duckdb

from .register import CasRegister

CATALOG_KEY = "catalog.ducklake"


def load_extension(con, name: str) -> None:
    """LOAD a DuckDB extension: installed copy, else its PyPI wheel, else INSTALL."""
    try:
        con.execute(f"LOAD {name}")
        return
    except duckdb.Error:
        pass
    ext = _wheel_extension(name)
    if ext is not None:
        con.execute(f"LOAD '{ext}'")
        return
    con.execute(f"INSTALL {name}; LOAD {name}")


def load_ducklake(con) -> None:
    load_extension(con, "ducklake")


@functools.cache
def _wheel_extension(name: str) -> pathlib.Path | None:
    """Extension binary shipped by the optional duckdb-extension-<name> wheel.

    Wheels are published in lockstep with duckdb, so only the directory
    matching the running duckdb version is trusted.
    """
    try:
        wheel = importlib.import_module(f"duckdb_extension_{name}")
    except ImportError:
        return None
    ext = pathlib.Path(wheel.__file__).parent / "extensions" / f"v{duckdb.__version__}" / f"{name}.duckdb_extension"
    return ext if ext.exists() else None


def _assert_clean(local: pathlib.Path) -> None:
    """The uploaded artifact must be one self-contained file (no WAL sidecar)."""
    wal = pathlib.Path(str(local) + ".wal")
    if wal.exists():
        raise RuntimeError(f"{wal}: catalog WAL not checkpointed on detach; refusing to upload")


class DuckLake:
    """Multi-writer DuckLake on one S3 bucket: the register holds the catalog file."""

    def __init__(self, register: CasRegister, data_path: str, secret_sql: str | None = None):
        self.register = register  # CasRegister on .../catalog.ducklake
        self.data_path = data_path  # 's3://bucket/prefix/data/'
        self.secret_sql = secret_sql  # CREATE SECRET ... for non-AWS endpoints

    @classmethod
    def at(
        cls, client, bucket: str, prefix: str, secret_sql: str | None = None, data_subdir: str = "data"
    ) -> "DuckLake":
        """Standard layout: s3://bucket/prefix/{catalog.ducklake, <data_subdir>/}."""
        register = CasRegister(client, bucket, f"{prefix}/{CATALOG_KEY}")
        return cls(register, f"s3://{bucket}/{prefix}/{data_subdir}/", secret_sql)

    def attach(self, con, local: pathlib.Path, read_only: bool = False) -> None:
        """Attach a local copy of the catalog on `con` as `lake`."""
        load_ducklake(con)
        if self.secret_sql:
            con.execute(self.secret_sql)
        ro = ", READ_ONLY" if read_only else ""
        con.execute(f"ATTACH 'ducklake:{local}' AS lake (DATA_PATH '{self.data_path}'{ro})")

    @contextlib.contextmanager
    def _open(self, blob: bytes | None, read_only: bool = False):
        """Materialize the catalog locally and attach it; yields (con, local)."""
        with tempfile.TemporaryDirectory() as tmp:
            local = pathlib.Path(tmp) / CATALOG_KEY
            if blob is not None:
                local.write_bytes(blob)  # rebase base; genesis: ATTACH creates it
            con = duckdb.connect()
            try:
                self.attach(con, local, read_only)
                yield con, local
            finally:
                con.close()

    def commit(self, user_fn, **transact_kwargs) -> str:
        """Run user_fn(con) as one catalog transaction; CAS-swap the result.

        user_fn MUST be a replayable closure: it may run once per CAS attempt,
        each time against a freshly downloaded catalog. Prefer registering
        pre-uploaded Parquet (ducklake_add_data_files) over INSERT — a rebase
        then replays metadata-only work instead of rewriting data files.
        """

        def fn(old: bytes | None) -> bytes:
            with self._open(old) as (con, local):
                user_fn(con)
                con.execute("DETACH lake")
                _assert_clean(local)
                return local.read_bytes()

        return self.register.transact(fn, **transact_kwargs)

    @contextlib.contextmanager
    def read(self):
        """Download-then-attach (atomic GET), read-only; yields the connection."""
        blob, _ = self.register.read()
        if blob is None:
            raise FileNotFoundError(f"no catalog at s3://{self.register.bucket}/{self.register.key}")
        with self._open(blob, read_only=True) as (con, _):
            yield con

    def expire_snapshots(self, older_than_days: float) -> str:
        """CAS commit expiring snapshots older than the cutoff — metadata only.

        Data files those snapshots referenced stay on S3 until gc().
        """
        cutoff = _utc_literal(older_than_days)
        return self.commit(lambda con: con.execute(f"CALL ducklake_expire_snapshots('lake', older_than => {cutoff})"))

    def _live_files(self) -> set[str]:
        """Basenames of every data-path file a retained snapshot still needs.

        Read via a plain (non-ducklake) ATTACH of the downloaded artifact, minus
        what expiry already scheduled for deletion. Basenames suffice: DuckLake
        names its files with UUIDs.
        """
        blob, _ = self.register.read()
        if blob is None:
            return set()
        with tempfile.TemporaryDirectory() as tmp:
            local = pathlib.Path(tmp) / CATALOG_KEY
            local.write_bytes(blob)
            con = duckdb.connect()
            try:
                con.execute(f"ATTACH '{local}' AS meta (READ_ONLY)")

                def names(table):
                    return {
                        pathlib.PurePosixPath(r[0]).name for r in con.sql(f"SELECT path FROM meta.{table}").fetchall()
                    }

                live = names("ducklake_data_file") | names("ducklake_delete_file")
                return live - names("ducklake_files_scheduled_for_deletion")
            finally:
                con.close()

    def gc(self, grace_days: float = 7.0, dry_run: bool = True) -> list[str]:
        """Delete (or with dry_run just list) data-path objects that are BOTH
        unneeded by every retained snapshot AND older than the grace TTL — so an
        in-flight writer's just-uploaded files can never be reaped before its commit
        lands (spec §5.5). Reads the catalog, never commits; returns the object keys.
        """
        live = self._live_files()
        bucket, _, prefix = self.data_path.removeprefix("s3://").partition("/")
        cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(days=grace_days)
        client = self.register.client
        dead = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if pathlib.PurePosixPath(obj["Key"]).name in live or obj["LastModified"] >= cutoff:
                    continue
                dead.append(obj["Key"])
        if not dry_run:
            for i in range(0, len(dead), 1000):  # DeleteObjects caps at 1000 keys
                client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in dead[i : i + 1000]]})
        return dead


def _utc_literal(days_ago: float) -> str:
    """Naive-UTC TIMESTAMP literal for `days_ago` days before now.

    Microsecond precision: truncating to seconds would round the cutoff BELOW
    snapshots committed in the same second, silently expiring nothing.
    """
    t = dt.datetime.now(dt.UTC).replace(tzinfo=None) - dt.timedelta(days=days_ago)
    return f"TIMESTAMP '{t.isoformat(sep=' ', timespec='microseconds')}'"
