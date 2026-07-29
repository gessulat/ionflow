"""The two DuckLakes ionflow keeps: every flow's catalog (decision 0012) and the
publish lake (decision 0009).

Both are schrein DuckLakes driven by the same three operations — genesis (typed
tables), registering pre-uploaded parquet, attaching a snapshot read-only. Roles
stay separate: a lake only ever receives table schemas and data-file
registrations; the ledger keeps sole authority over run bookkeeping.
"""

import atexit
import datetime as dt
import pathlib
import re
import shutil
import tempfile
import uuid
from dataclasses import replace

from schrein.ducklake import CATALOG_KEY

from .schema import Table
from .space import Space, utc_now
from .store import Store, parse_dest

PUBLISH_PREFIX = "published"  # default lake: s3://<bucket>/<config.prefix>/published/
PROVENANCE = "publications"  # per-lake provenance table, appended on every publish


def _create(name, cols):
    """CREATE TABLE IF NOT EXISTS for a lake table, from its column DDL."""
    return f"CREATE TABLE IF NOT EXISTS lake.{name} ({cols})"


def _add_files(con, table, uris, hive=False):
    """Register pre-uploaded parquet into `lake.<table>` — metadata only.

    `hive` reads run_id out of each part's `run_id=` path segment (0012); the
    publish lake's staged file has no hive path, so it registers without it.
    """
    flag = ", hive_partitioning => true" if hive else ""
    for uri in uris:
        con.execute(f"CALL ducklake_add_data_files('lake', '{table}', '{uri}'{flag})")


class Catalog:
    """schrein DuckLake at the flow root; its data_path is catalog-written files
    only (`_lake/`) — registered step parts stay in their immutable run folders."""

    def __init__(self, space):
        self.space = space
        self.lake = space.lake()
        self.key = self.lake.register.key

    def _blob(self):
        """The catalog artifact's bytes, or None when this flow has no catalog —
        the one existence check."""
        return self.lake.register.read()[0]

    def tables(self):
        """{table: {column: type}} from the current catalog; {} when none exists.

        Reads through schrein's context manager, NOT `attach_readonly`: a
        transient lookup's local copy must be freed on the way out, not held to
        process exit the way a returned connection's must be.
        """
        if self._blob() is None:
            return {}
        with self.lake.read() as con:
            names = [
                r[0] for r in con.sql("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'").fetchall()
            ]
            return {t: {r[0]: r[1] for r in con.sql(f"DESCRIBE lake.{t}").fetchall()} for t in names}

    def ensure_tables(self, flow_cls):
        """Genesis, idempotent: one table per step, skipped when all exist."""
        ddls = {
            name: _create(name, f"run_id VARCHAR NOT NULL, {step.output_table.persisted_ddl()}")
            for name, step in flow_cls._dag.steps.items()
        }
        if set(ddls) <= set(self.tables()):
            return
        self.lake.commit(lambda con: [con.execute(ddl) for ddl in ddls.values()])

    def register_parts(self, parts):
        """One CAS commit registering {table: [part keys]} (metadata-only), in the
        given order; nothing to register commits nothing.
        """
        uris = {t: [self.space.uri(k) for k in keys] for t, keys in parts.items() if keys}
        if not uris:
            return
        self.lake.commit(lambda con: [_add_files(con, t, u, hive=True) for t, u in uris.items()])

    def attach_readonly(self, con):
        """Download-then-attach the catalog on `con` as `lake` (snapshot-consistent);
        False when no catalog exists yet. The local copy must outlive the
        connection, so its temp dir is only reclaimed at process exit.
        """
        blob = self._blob()
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


def backfill(flow_cls, space, replace=False):
    """Rebuild a flow's catalog from the run folders' `_meta.json` recovery
    records — the path for a lost or corrupt register; returns the number of
    complete runs registered. Steps no longer in the flow's code are dropped.
    """
    from .state import Manifest  # state builds on Space; local import breaks the cycle

    cat = Catalog(space)
    if cat._blob() is not None:
        if not replace:
            raise FileExistsError(f"catalog exists at {cat.key}; pass --replace to rebuild")
        cat.discard()
    cat.ensure_tables(flow_cls)
    complete = {r["run_id"] for r in space.read_rows(space.ledger) if r["status"] == "complete"}
    steps = set(flow_cls._dag.steps)
    by_step = {}
    for run_id in sorted(complete):
        manifest = Manifest.from_meta((space.read_meta(run_id) or {}).get("manifest"))
        for step in steps:
            by_step.setdefault(step, []).extend(manifest.paths(step))
    cat.register_parts({step: by_step[step] for step in sorted(by_step)})
    return len(complete)


def schema_drift(flow_cls, space):
    """Messages where a step's declared schema differs from its catalog table.

    No catalog (flow never ran) is no drift; nor is a step added since genesis,
    which gets its table on the next run.
    """
    issues = []
    tables = Catalog(space).tables()
    for name, step in flow_cls._dag.steps.items():
        if name not in tables:
            continue
        declared = {n: c.persisted_type() for n, c in step.output_table.columns().items()}
        persisted = {c: t for c, t in tables[name].items() if c != "run_id"}
        for col, dtype in declared.items():
            if col not in persisted:
                issues.append(f"{flow_cls.flow_id()}.{name}: missing column '{col}'")
            elif persisted[col] != dtype:
                issues.append(f"{flow_cls.flow_id()}.{name}: '{col}' {persisted[col]}→{dtype}")
        for col in persisted:
            if col not in declared:
                issues.append(f"{flow_cls.flow_id()}.{name}: unexpected column '{col}'")
    return issues


class Publication(Table):
    """One provenance row, appended to the publish lake on every publish (0009)."""

    table_name: str
    flow: str
    run_id: str
    step: str
    filter: str | None
    commit: str
    published_at: dt.datetime


def publish_lake(config, dest=None):
    """The publish DuckLake at `dest` ('s3://bucket/prefix') or the project
    default; returns (lake, target store).

    A custom `dest` publishes across buckets: the target store is `config` with
    the bucket swapped, so consumers need read access to that location only.
    """
    if dest:
        if config.local_dir:
            raise ValueError("cannot publish from a local store to s3://…; `ionflow sync` the flow to a bucket first")
        bucket, prefix = parse_dest(dest, require_prefix=True)
        target = Store.of(replace(config, bucket=bucket))
        return target.lake(prefix), target
    store = Store.of(config)
    return store.lake(f"{store.prefix}/{PUBLISH_PREFIX}"), store


def publish(flow_cls, config, run_id, step, table, where=None, dest=None):
    """Copy `step`'s output from a complete run into the publish lake as `table`.

    Atomic replace: previous contents stay reachable as DuckLake snapshots, and
    the returned `publications` provenance row lands in the same commit.
    """
    if step not in flow_cls._dag.steps:
        raise ValueError(f"unknown step '{step}' (steps: {', '.join(flow_cls._dag.steps)})")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) or table == PROVENANCE:
        raise ValueError(f"invalid published table name '{table}'")
    store = Store.of(config)
    space = Space.for_flow(store, flow_cls)
    meta = space.read_meta(run_id)
    if meta is None:
        raise RuntimeError(f"run not found: {run_id}")
    if not meta.get("finished"):
        raise RuntimeError(f"run {run_id} did not complete; only complete runs publish")

    lake, target = publish_lake(config, dest)
    out_table = flow_cls._dag.steps[step].output_table
    keys = space.part_keys(run_id, step)
    con = store.connect()
    rel = space.relation(con, keys) if keys else out_table.empty(con)
    if where:
        rel = rel.filter(where)
    # stage the frozen artifact inside the lake's data path; unique name, so
    # snapshots referencing an earlier publish keep their (immutable) file.
    # data_path is built from the target store's uri(), so key_for_uri() inverts
    # it; the relation stays on the source connection, which is what makes a
    # cross-store publish work
    staged_uri = f"{lake.data_path}{table}/{run_id}-{uuid.uuid4().hex}.parquet"
    staged = target.key_for_uri(staged_uri)
    target.space(flow_cls.flow_id()).write_part(staged, rel, out_table)

    ddl_cols = out_table.persisted_ddl()
    row = {
        "table_name": table,
        "flow": flow_cls.flow_id(),
        "run_id": run_id,
        "step": step,
        "filter": where,
        "commit": meta.get("commit"),
        "published_at": utc_now(),
    }

    def fn(con):
        con.execute("BEGIN")
        con.execute(_create(table, ddl_cols))
        con.execute(f"DELETE FROM lake.{table}")
        _add_files(con, table, [staged_uri])
        con.execute(_create(PROVENANCE, Publication.persisted_ddl()))
        con.execute(f"INSERT INTO lake.{PROVENANCE} VALUES (?, ?, ?, ?, ?, ?, ?)", list(row.values()))
        con.execute("COMMIT")

    lake.commit(fn)
    return row
