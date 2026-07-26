"""Per-flow DuckLake catalog — every flow's read/registration substrate
(docs/specs/ionflow-on-schrein.md; decisions 0015, 0016, 0017).

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

from schrein.ducklake import CATALOG_KEY

from schrein import CasRegister, DuckLake

from . import ledger, storage
from .storage import Layout


class Catalog:
    """schrein DuckLake at the flow root; its data_path is catalog-written files
    only (`_lake/`) — registered step parts stay in their immutable run folders."""

    def __init__(self, config, flow_cls, client=None):
        root = Layout.for_flow(config, flow_cls).root
        self.bucket = config.bucket
        self.key = f"{root}/{CATALOG_KEY}"
        register = CasRegister(client or storage.s3_client(config), config.bucket, self.key)
        self.lake = DuckLake(register, f"s3://{config.bucket}/{root}/_lake/", storage.secret_sql(config))

    def tables(self):
        """{table: {column: type}} from the current catalog; {} when none exists."""
        try:
            with self.lake.read() as con:
                names = [
                    r[0]
                    for r in con.sql("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'").fetchall()
                ]
                return {t: {r[0]: r[1] for r in con.sql(f"DESCRIBE lake.{t}").fetchall()} for t in names}
        except FileNotFoundError:
            return {}

    def ensure_tables(self, flow_cls):
        """Genesis, idempotent: one table per step, skipped when all exist."""
        ddls = {name: _table_ddl(name, step.output_table) for name, step in flow_cls._dag.steps.items()}
        if set(ddls) <= set(self.tables()):
            return
        self.lake.commit(lambda con: [con.execute(ddl) for ddl in ddls.values()])

    def register_step(self, step, part_keys):
        """One CAS commit registering a completed step's parts (metadata-only);
        each part's run_id comes from its hive path (0015)."""
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
        self.lake.attach(con, local, read_only=True)
        return True

    def discard(self):
        """Delete the register object (the --replace path; run folders untouched)."""
        self.lake.register.client.delete_object(Bucket=self.bucket, Key=self.key)


def _table_ddl(step_name, table):
    """CREATE TABLE from a step's Table: persisted types (what a reader sees),
    NOT NULL only (all DuckLake enforces) — PK/CHECK stay ionflow-side (0003)."""
    cols = ", ".join(
        f"{n} {col.persisted_type()}{'' if col.nullable else ' NOT NULL'}" for n, col in table.columns().items()
    )
    return f"CREATE TABLE IF NOT EXISTS lake.{step_name} (run_id VARCHAR NOT NULL, {cols})"


def backfill(flow_cls, config, replace=False):
    """Rebuild a flow's catalog from its run folders — the recovery path for a
    lost or corrupt register. One LIST, genesis, one registration commit.

    Returns the number of complete runs registered.
    """
    layout = Layout.for_flow(config, flow_cls)
    client = storage.s3_client(config)
    cat = Catalog(config, flow_cls, client=client)
    if cat.lake.register.read()[0] is not None:
        if not replace:
            raise FileExistsError(f"catalog exists at {cat.key}; pass --replace to rebuild")
        cat.discard()
    cat.ensure_tables(flow_cls)
    complete = {r["run_id"] for r in ledger.read_all(client, config.bucket, layout.ledger) if r["status"] == "complete"}
    steps = set(flow_cls._dag.steps)
    by_step = {}
    run_prefix = layout.root + "/runs/run_id="
    for key in storage.list_keys(client, config.bucket, run_prefix):
        run_id, _, tail = key[len(run_prefix) :].partition("/")
        step, _, part = tail.partition("/")
        if run_id in complete and step in steps and part:
            by_step.setdefault(step, []).append(key)

    def fn(con):
        for step in sorted(by_step):
            for k in sorted(by_step[step]):
                con.execute(
                    f"CALL ducklake_add_data_files('lake', '{step}', 's3://{config.bucket}/{k}', "
                    "hive_partitioning => true)"
                )

    if by_step:
        cat.lake.commit(fn)
    return len(complete)
