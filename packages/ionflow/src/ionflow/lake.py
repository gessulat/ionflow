"""The two DuckLakes ionflow keeps: every flow's catalog (decisions 0012/0019)
and the publish lake (decision 0009).

Both are schrein DuckLakes driven by the same operations — genesis (typed
tables), registering pre-uploaded parquet, attaching a snapshot read-only. The
flow catalog is the run's ONE metadata authority (0019): beyond the per-step
data tables it holds `runs` (the ledger) and `manifest` (run -> its parts), so
a step's completion, its data registration and the run's bookkeeping land in a
single CAS commit. `_meta.json` in the run folders is a write-only recovery
export; `backfill` is its only reader.
"""

import atexit
import datetime as dt
import json
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

# The system tables' fixed DDL — never migrated; params/custom are JSON text
# that the `ledger` view flattens against the flow's declarations at read time.
RUNS_DDL = (
    "run_id VARCHAR NOT NULL, status VARCHAR NOT NULL, started TIMESTAMP NOT NULL, finished TIMESTAMP, "
    '"commit" VARCHAR, origin_run_id VARCHAR, params VARCHAR, custom VARCHAR'
)
MANIFEST_DDL = "run_id VARCHAR NOT NULL, step VARCHAR NOT NULL, item_key VARCHAR NOT NULL, path VARCHAR NOT NULL"
RUN_COLS = ("run_id", "status", "started", "finished", "commit", "origin_run_id", "params", "custom")
_RUNS_SELECT = 'SELECT run_id, status, started, finished, "commit", origin_run_id, params, custom FROM lake.runs'


def _decode_run(row):
    row = dict(zip(RUN_COLS, row, strict=True))
    row["params"] = json.loads(row["params"] or "{}")
    row["custom"] = json.loads(row["custom"] or "{}")
    return row


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
    """schrein DuckLake at the flow root — the run's one metadata authority.

    Per-step data tables plus the `runs` and `manifest` system tables; its own
    data_path (`_lake/`) holds only catalog-written files — registered step
    parts stay in their immutable run folders, and the tiny system rows inline
    into the register artifact itself."""

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

    def _ddls(self, flow_cls):
        """CREATE TABLE IF NOT EXISTS for every table genesis owes this flow."""
        steps = {
            name: _create(name, f"run_id VARCHAR NOT NULL, {step.output_table.persisted_ddl()}")
            for name, step in flow_cls._dag.steps.items()
        }
        return steps | {"runs": _create("runs", RUNS_DDL), "manifest": _create("manifest", MANIFEST_DDL)}

    def _apply(self, con, run_id, row=None, rows=()):
        """One run's upsert, inside an open lake transaction: replace its ledger
        row, insert the manifest rows not already present (a replayed commit
        whose predecessor actually landed must not duplicate), and register the
        data files of the newly created parts among them — a path inside this
        run's own folder means "created here", anything else is a reference.
        """
        if row is not None:
            con.execute("DELETE FROM lake.runs WHERE run_id = ?", [run_id])
            con.execute(f"INSERT INTO lake.runs VALUES ({', '.join('?' * len(RUN_COLS))})", row)
        if not rows:
            return
        have = set(con.execute("SELECT step, item_key FROM lake.manifest WHERE run_id = ?", [run_id]).fetchall())
        # sorted = key order per step, the one registration (hence reducer) order
        fresh = [(s, json.dumps(k), p) for s, k, p in sorted(rows) if (s, json.dumps(k)) not in have]
        own = self.space.run_dir(run_id) + "/"
        files = {}
        for s, k, p in fresh:
            con.execute("INSERT INTO lake.manifest VALUES (?, ?, ?, ?)", [run_id, s, k, p])
            if p.startswith(own):
                files.setdefault(s, []).append(self.space.uri(p))
        for s, uris in files.items():
            _add_files(con, s, uris, hive=True)

    def commit_run(self, run_id, row=None, rows=(), genesis=None):
        """One CAS commit of a run's progress: optional genesis (pass the flow
        class), the run's ledger `row`, and `rows` — (step, item key, path)
        manifest triples with their new parts' registration. Atomic, so a
        reader never sees a part without its manifest row, a run without its
        tables, or a partially registered step. Replay-safe (see `_apply`).
        """

        def fn(con):
            con.execute("BEGIN")
            if genesis is not None:
                for ddl in self._ddls(genesis).values():
                    con.execute(ddl)
            self._apply(con, run_id, row, rows)
            con.execute("COMMIT")

        self.lake.commit(fn)

    def runs(self):
        """Ledger rows as dicts (params/custom decoded), oldest first; [] when
        this flow has no catalog yet."""
        if self._blob() is None:
            return []
        with self.lake.read() as con:
            return [_decode_run(r) for r in con.sql(_RUNS_SELECT + " ORDER BY started").fetchall()]

    def run_state(self, run_id):
        """(ledger row, manifest triples) for one run, off one snapshot;
        `"latest"` picks the newest complete run. (None, []) when absent."""
        if self._blob() is None:
            return None, []
        with self.lake.read() as con:
            if run_id == "latest":
                r = con.execute(_RUNS_SELECT + " WHERE status = 'complete' ORDER BY started DESC LIMIT 1").fetchone()
            else:
                r = con.execute(_RUNS_SELECT + " WHERE run_id = ?", [run_id]).fetchone()
            if r is None:
                return None, []
            triples = con.execute("SELECT step, item_key, path FROM lake.manifest WHERE run_id = ?", [r[0]]).fetchall()
        return _decode_run(r), [(s, json.loads(k), p) for s, k, p in triples]

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


def _meta_row(meta, run_id):
    """A `runs` row reconstructed from one `_meta.json` recovery record."""

    def ts(v):
        return dt.datetime.fromisoformat(v) if v else None

    return [
        run_id,
        meta.get("status") or "failed",
        ts(meta.get("started")) or utc_now(),
        ts(meta.get("finished")),
        meta.get("commit"),
        meta.get("origin_run_id"),
        json.dumps(meta.get("params") or {}),
        json.dumps(meta.get("custom") or {}, default=str),
    ]


def backfill(flow_cls, space, replace=False):
    """Rebuild a flow's catalog — ledger rows, manifests, data registrations —
    from the run folders' `_meta.json` recovery records, in one CAS commit.
    The only path out of a lost or corrupt register; also how `sync` derives
    the target-side catalog. Returns the number of runs restored; steps no
    longer in the flow's code are dropped.
    """
    from .state import Manifest  # state builds on Space; local import breaks the cycle

    cat = Catalog(space)
    if cat._blob() is not None:
        if not replace:
            raise FileExistsError(f"catalog exists at {cat.key}; pass --replace to rebuild")
        cat.discard()
    steps = set(flow_cls._dag.steps)
    runs = []
    for run_id in space.run_ids():
        meta = space.read_meta(run_id) or {}
        manifest = Manifest.from_meta(meta.get("manifest"))
        rows = [(slot.step, slot.key, path) for slot, path in manifest.slots() if slot.step in steps]
        runs.append((run_id, _meta_row(meta, run_id), rows))

    def fn(con):
        con.execute("BEGIN")
        for ddl in cat._ddls(flow_cls).values():
            con.execute(ddl)
        for run_id, row, rows in runs:
            cat._apply(con, run_id, row, rows)
        con.execute("COMMIT")

    cat.lake.commit(fn)
    return len(runs)


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
    row, _ = Catalog(space).run_state(run_id)
    if row is None:
        raise RuntimeError(f"run not found: {run_id}")
    if row["status"] != "complete":
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
        "commit": row["commit"],
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
