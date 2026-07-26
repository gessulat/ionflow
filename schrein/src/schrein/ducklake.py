"""DuckLake catalog as a CAS-swapped immutable artifact on one S3 key.

Commit protocol: download the catalog, run the user's transaction against a
local ATTACH, conditionally upload the result; on a lost race, replay the
transaction against a fresh download (rebase). Readers download-then-attach,
so they always see exactly one committed version.
"""

import contextlib
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
    def at(cls, client, bucket: str, prefix: str, secret_sql: str | None = None) -> "DuckLake":
        """Standard layout: s3://bucket/prefix/{catalog.ducklake, data/}."""
        register = CasRegister(client, bucket, f"{prefix}/{CATALOG_KEY}")
        return cls(register, f"s3://{bucket}/{prefix}/data/", secret_sql)

    @contextlib.contextmanager
    def _open(self, blob: bytes | None, read_only: bool = False):
        """Materialize the catalog locally and attach it; yields (con, local)."""
        with tempfile.TemporaryDirectory() as tmp:
            local = pathlib.Path(tmp) / CATALOG_KEY
            if blob is not None:
                local.write_bytes(blob)  # rebase base; genesis: ATTACH creates it
            con = duckdb.connect()
            try:
                load_ducklake(con)
                if self.secret_sql:
                    con.execute(self.secret_sql)
                ro = ", READ_ONLY" if read_only else ""
                con.execute(f"ATTACH 'ducklake:{local}' AS lake (DATA_PATH '{self.data_path}'{ro})")
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
