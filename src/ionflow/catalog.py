"""Optional per-flow DuckLake catalog — "catalog mode" (docs/specs/ionflow-on-schrein.md).

Roles stay strictly separate: the catalog only ever receives table schemas and
data-file registrations; the ledger keeps sole authority over run bookkeeping.
One catalog per flow, one table per step, rows across runs keyed by `run_id`,
which `ducklake_add_data_files` infers from the parts' hive `run_id=` path
segments (decision 0015) — parts are never rewritten, and resume's server-side
copies register under the new run's id for free.
"""

import atexit
import pathlib
import shutil
import tempfile

from schrein.ducklake import load_ducklake

from schrein import CasRegister, DuckLake

from . import ledger, storage
from .storage import Layout

CATALOG_KEY = "catalog.ducklake"  # at the flow root, next to ledger.parquet


def for_flow(config, flow_cls):
    """The flow's catalog per config — NullCatalog unless catalog mode applies."""
    if getattr(config, "catalog", "none") != "ducklake" or flow_cls.prototype:
        return NullCatalog()
    return DuckLakeCatalog(config, Layout.for_flow(config, flow_cls))


class NullCatalog:
    """Classic mode (and prototype flows): every catalog call is a no-op."""

    mode = "none"

    def ensure_tables(self, flow_cls):
        pass

    def register_step(self, run_id, step, part_keys):
        pass

    def attach_readonly(self, con):
        return False


class DuckLakeCatalog:
    """schrein DuckLake at the flow root; its data_path is catalog-written files
    only (`_lake/`) — registered step parts stay in their immutable run folders."""

    mode = "ducklake"

    def __init__(self, config, layout):
        self.bucket = config.bucket
        register = CasRegister(storage.s3_client(config), config.bucket, f"{layout.root}/{CATALOG_KEY}")
        self.lake = DuckLake(register, f"s3://{config.bucket}/{layout.root}/_lake/", storage.secret_sql(config))

    def ensure_tables(self, flow_cls):
        """Genesis, idempotent: one table per typed step, skipped when all exist."""
        ddls = {name: _table_ddl(name, step.output_table) for name, step in flow_cls._dag.steps.items()}
        try:
            with self.lake.read() as con:
                have = {
                    r[0]
                    for r in con.sql("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'").fetchall()
                }
            if set(ddls) <= have:
                return
        except FileNotFoundError:
            pass
        self.lake.commit(lambda con: [con.execute(ddl) for ddl in ddls.values()])

    def register_step(self, run_id, step, part_keys):
        """One CAS commit registering a completed step's parts (metadata-only)."""
        if not part_keys:
            return  # empty fan-out: the typed table exists from genesis
        uris = [f"s3://{self.bucket}/{k}" for k in part_keys]

        def fn(con):
            for uri in uris:
                con.execute(f"CALL ducklake_add_data_files('lake', '{step}', '{uri}', hive_partitioning => true)")

        self.lake.commit(fn)

    def attach_readonly(self, con):
        """Download-then-attach the catalog on `con` as `lake` (snapshot-consistent).

        The local copy must outlive the connection; the temp dir is reclaimed
        at process exit. Returns False when no catalog exists yet.
        """
        blob, _ = self.lake.register.read()
        if blob is None:
            return False
        tmp = pathlib.Path(tempfile.mkdtemp(prefix="ionflow-catalog-"))
        atexit.register(shutil.rmtree, tmp, ignore_errors=True)
        local = tmp / CATALOG_KEY
        local.write_bytes(blob)
        load_ducklake(con)
        con.execute(f"ATTACH 'ducklake:{local}' AS lake (DATA_PATH '{self.lake.data_path}', READ_ONLY)")
        return True


def _table_ddl(step_name, table):
    """CREATE TABLE from a step's Table: persisted types (what a reader sees),
    NOT NULL only (all DuckLake enforces) — PK/CHECK stay ionflow-side (0003)."""
    if table is None:
        raise RuntimeError(f"step '{step_name}': catalog mode requires a typed return (Table)")
    if "run_id" in table.columns():
        raise RuntimeError(f"step '{step_name}': column 'run_id' is reserved in catalog mode")
    cols = ", ".join(
        f"{n} {col.persisted_type()}{'' if col.nullable else ' NOT NULL'}" for n, col in table.columns().items()
    )
    return f"CREATE TABLE IF NOT EXISTS lake.{step_name} (run_id VARCHAR NOT NULL, {cols})"


def backfill(flow_cls, config, replace=False):
    """Register every complete run of a flow into a fresh catalog.

    Both the adoption path (enable catalog mode on an existing flow) and the
    recovery path (rebuild a corrupt/clobbered catalog from the run folders).
    Returns the number of runs registered.
    """
    if flow_cls.prototype:
        raise RuntimeError("prototype flows are always classic; nothing to backfill")
    layout = Layout.for_flow(config, flow_cls)
    client = storage.s3_client(config)
    cat = DuckLakeCatalog(config, layout)
    if cat.lake.register.read()[0] is not None:
        if not replace:
            raise FileExistsError(f"catalog exists at {layout.root}/{CATALOG_KEY}; pass --replace to rebuild")
        client.delete_object(Bucket=config.bucket, Key=f"{layout.root}/{CATALOG_KEY}")
    cat.ensure_tables(flow_cls)
    runs = [r for r in ledger.read_all(client, config.bucket, layout.ledger) if r["status"] == "complete"]
    for r in runs:
        for step in flow_cls._dag.steps:
            keys = sorted(storage.list_keys(client, config.bucket, layout.step_dir(r["run_id"], step) + "/"))
            cat.register_step(r["run_id"], step, keys)
    return len(runs)
