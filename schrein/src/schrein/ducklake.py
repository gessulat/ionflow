"""DuckLake catalog as a CAS-swapped immutable artifact on one S3 key.

Commit protocol: download the catalog, run the user's transaction against a
local ATTACH, conditionally upload the result; on a lost race, replay the
transaction against a fresh download (rebase). Readers download-then-attach,
so they always see exactly one committed version.
"""

import contextlib
import pathlib
import tempfile

import duckdb

from .register import CasRegister

CATALOG_KEY = "catalog.ducklake"


def load_ducklake(con) -> None:
    """LOAD ducklake: already-installed copy, else the PyPI wheel, else INSTALL."""
    try:
        con.execute("LOAD ducklake")
        return
    except duckdb.Error:
        pass
    ext = _wheel_extension()
    if ext is not None:
        con.execute(f"LOAD '{ext}'")
        return
    con.execute("INSTALL ducklake; LOAD ducklake")


def _wheel_extension() -> pathlib.Path | None:
    """Extension binary shipped by the optional duckdb-extension-ducklake wheel."""
    try:
        import duckdb_extension_ducklake
    except ImportError:
        return None
    root = pathlib.Path(duckdb_extension_ducklake.__file__).parent / "extensions"
    exact = root / f"v{duckdb.__version__}" / "ducklake.duckdb_extension"
    if exact.exists():
        return exact
    hits = sorted(root.glob("*/ducklake.duckdb_extension"))
    return hits[-1] if hits else None


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

    def _attach(self, con, local: pathlib.Path, read_only: bool = False) -> None:
        load_ducklake(con)
        if self.secret_sql:
            con.execute(self.secret_sql)
        ro = ", READ_ONLY" if read_only else ""
        con.execute(f"ATTACH 'ducklake:{local}' AS lake (DATA_PATH '{self.data_path}'{ro})")

    def commit(self, user_fn, **transact_kwargs) -> str:
        """Run user_fn(con) as one catalog transaction; CAS-swap the result.

        user_fn MUST be a replayable closure: it may run once per CAS attempt,
        each time against a freshly downloaded catalog. Prefer registering
        pre-uploaded Parquet (ducklake_add_data_files) over INSERT — a rebase
        then replays metadata-only work instead of rewriting data files.
        """

        def fn(old: bytes | None) -> bytes:
            with tempfile.TemporaryDirectory() as tmp:
                local = pathlib.Path(tmp) / CATALOG_KEY
                if old is not None:
                    local.write_bytes(old)  # rebase base; genesis: ATTACH creates it
                con = duckdb.connect()
                try:
                    self._attach(con, local)
                    user_fn(con)
                    con.execute("DETACH lake")
                finally:
                    con.close()
                _assert_clean(local)
                return local.read_bytes()

        return self.register.transact(fn, **transact_kwargs)

    @contextlib.contextmanager
    def read(self):
        """Download-then-attach (atomic GET), read-only; yields the connection."""
        blob, _ = self.register.read()
        if blob is None:
            raise FileNotFoundError(f"no catalog at s3://{self.register.bucket}/{self.register.key}")
        with tempfile.TemporaryDirectory() as tmp:
            local = pathlib.Path(tmp) / CATALOG_KEY
            local.write_bytes(blob)
            con = duckdb.connect()
            try:
                self._attach(con, local, read_only=True)
                yield con
            finally:
                con.close()
