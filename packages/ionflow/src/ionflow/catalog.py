"""Per-flow DuckLake catalog — every flow's read/registration substrate
(docs/specs/ionflow-on-schrein.md; decision 0012).

Roles stay strictly separate: the catalog only ever receives table schemas and
data-file registrations; the ledger keeps sole authority over run bookkeeping.
One catalog per flow, one table per step, rows across runs keyed by `run_id`,
which `ducklake_add_data_files` infers from the parts' hive `run_id=` path
segments (decision 0012) — parts are never rewritten, and resume's server-side
copies register under the new run's id for free.
"""

import atexit
import pathlib
import shutil
import tempfile

from schrein.ducklake import CATALOG_KEY


class Catalog:
    """schrein DuckLake at the flow root; its data_path is catalog-written files
    only (`_lake/`) — registered step parts stay in their immutable run folders."""

    def __init__(self, space):
        self.space = space
        self.lake = space.lake()
        self.key = self.lake.register.key

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
        each part's run_id comes from its hive path (0012)."""
        if not part_keys:
            return  # empty fan-out: the typed table exists from genesis
        uris = [self.space.uri(k) for k in part_keys]

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
        self.space.store.delete(self.key)


def _table_ddl(step_name, table):
    """CREATE TABLE from a step's Table (persisted types; run_id partition first)."""
    return f"CREATE TABLE IF NOT EXISTS lake.{step_name} (run_id VARCHAR NOT NULL, {table.persisted_ddl()})"


def backfill(flow_cls, space, replace=False):
    """Rebuild a flow's catalog from its run folders — the recovery path for a
    lost or corrupt register. One LIST, genesis, one registration commit.

    Returns the number of complete runs registered.
    """
    cat = Catalog(space)
    if cat.lake.register.read()[0] is not None:
        if not replace:
            raise FileExistsError(f"catalog exists at {cat.key}; pass --replace to rebuild")
        cat.discard()
    cat.ensure_tables(flow_cls)
    complete = {r["run_id"] for r in space.read_rows(space.ledger) if r["status"] == "complete"}
    steps = set(flow_cls._dag.steps)
    by_step = {}
    for key in space.store.list(space.root + "/runs/run_id="):
        parsed = space.parse_part(key)
        if parsed and parsed[0] in complete and parsed[1] in steps:
            by_step.setdefault(parsed[1], []).append(key)

    def fn(con):
        for step in sorted(by_step):
            for k in sorted(by_step[step]):
                con.execute(
                    f"CALL ducklake_add_data_files('lake', '{step}', '{space.uri(k)}', hive_partitioning => true)"
                )

    if by_step:
        cat.lake.commit(fn)
    return len(complete)
