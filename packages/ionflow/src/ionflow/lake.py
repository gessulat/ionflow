"""Run-data DuckLakes: every flow's catalog (decisions 0012/0019)
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
import hashlib
import json
import pathlib
import re
import shutil
import tempfile
import uuid
from dataclasses import replace
from typing import Any

from schrein.ducklake import CATALOG_KEY

from .schema import Table
from .space import Space, utc_now
from .state import Manifest
from .store import Store, parse_dest

PUBLISH_PREFIX = "published"  # default lake: s3://<bucket>/<config.prefix>/published/
PROVENANCE = "publications"  # per-lake provenance table, appended on every publish

# The system tables' fixed DDL — never migrated; params/custom are JSON text
# that the `ledger` view flattens against the flow's declarations at read time.
RUNS_DDL = (
    "run_id VARCHAR NOT NULL, status VARCHAR NOT NULL, started TIMESTAMP NOT NULL, finished TIMESTAMP, "
    '"commit" VARCHAR, origin_run_id VARCHAR, params VARCHAR, custom VARCHAR, artifacts VARCHAR, parent VARCHAR'
)
MANIFEST_DDL = "run_id VARCHAR NOT NULL, step VARCHAR NOT NULL, item_key VARCHAR NOT NULL, path VARCHAR NOT NULL"
RUN_COLS = (
    "run_id",
    "status",
    "started",
    "finished",
    "commit",
    "origin_run_id",
    "params",
    "custom",
    "artifacts",
    "parent",
)
_RUNS_SELECT = (
    'SELECT run_id, status, started, finished, "commit", origin_run_id, '
    "params, custom, artifacts, parent FROM lake.runs"
)
ACTIVITY_DDL = "run_id VARCHAR, step VARCHAR, item_key VARCHAR, record VARCHAR"
SYSTEM_DDLS = {
    "runs": RUNS_DDL,
    "manifest": MANIFEST_DDL,
    "artifact_activity": ACTIVITY_DDL,
    "run_versions": "run_id VARCHAR, origin VARCHAR, revision BIGINT, token VARCHAR, present BOOLEAN",
    "part_files": "path VARCHAR, size BIGINT, sha256 VARCHAR",
    "registered_parts": "path VARCHAR",
    "peer_runs": "peer VARCHAR, run_id VARCHAR, token VARCHAR, present BOOLEAN",
}


def _token(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def _decode_run(row):
    row = dict(zip(RUN_COLS, row, strict=True))
    row["params"] = json.loads(row["params"] or "{}")
    row["custom"] = json.loads(row["custom"] or "{}")
    row["artifacts"] = json.loads(row["artifacts"] or "{}")
    row["parent"] = json.loads(row["parent"] or "null")
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
            if not step.is_artifact
        }
        return steps | {name: _create(name, ddl) for name, ddl in SYSTEM_DDLS.items()}

    def _apply(self, con, run_id, row=None, rows=(), activities=()):
        """One run's upsert, inside an open lake transaction: replace its ledger
        row, insert the manifest rows not already present (a replayed commit
        whose predecessor actually landed must not duplicate), and register the
        data files of the newly created parts among them — a path inside this
        run's own folder means "created here", anything else is a reference.
        """
        if row is not None:
            con.execute("DELETE FROM lake.runs WHERE run_id = ?", [run_id])
            con.execute(f"INSERT INTO lake.runs VALUES ({', '.join('?' * len(RUN_COLS))})", row)
        for record in activities:
            args = [run_id, record["step"], json.dumps(record["key"])]
            con.execute("DELETE FROM lake.artifact_activity WHERE run_id=? AND step=? AND item_key=?", args)
            con.execute("INSERT INTO lake.artifact_activity VALUES (?, ?, ?, ?)", [*args, json.dumps(record)])
        if not rows:
            return
        have = set(con.execute("SELECT step, item_key FROM lake.manifest WHERE run_id = ?", [run_id]).fetchall())
        # sorted = key order per step, the one registration (hence reducer) order
        fresh = [(s, json.dumps(k), p) for s, k, p in sorted(rows) if (s, json.dumps(k)) not in have]
        own = self.space.run_dir(run_id) + "/"
        files = {}
        for s, k, p in fresh:
            con.execute("INSERT INTO lake.manifest VALUES (?, ?, ?, ?)", [run_id, s, k, p])
        registered = {r[0] for r in con.execute("SELECT path FROM lake.registered_parts").fetchall()}
        for s, _, p in rows:
            if p.startswith(own) and p not in registered:
                if con.execute("SELECT 1 FROM lake.part_files WHERE path = ?", [p]).fetchone() is None:
                    info = self.space.store.describe(p)
                    con.execute("INSERT INTO lake.part_files VALUES (?, ?, ?)", [p, info["size"], info["sha256"]])
                con.execute("INSERT INTO lake.registered_parts VALUES (?)", [p])
                files.setdefault(s, []).append(self.space.uri(p))
                registered.add(p)
        for s, uris in files.items():
            _add_files(con, s, uris, hive=True)

    def commit_run(self, run_id, row=None, rows=(), genesis=None, activities=()):
        """One CAS commit of a run's progress: optional genesis (pass the flow
        class), the run's ledger `row`, and `rows` — (step, item key, path)
        manifest triples with their new parts' registration. Atomic, so a
        reader never sees a part without its manifest row, a run without its
        tables, or a partially registered step. Replay-safe (see `_apply`).
        """

        committed = {}

        def fn(con):
            con.execute("BEGIN")
            if genesis is not None:
                for ddl in self._ddls(genesis).values():
                    con.execute(ddl)
            self._apply(con, run_id, row, rows, activities)
            snapshot = self._snapshot(con, run_id)
            token = _token(snapshot)
            old = con.execute(
                "SELECT origin, revision, token FROM lake.run_versions WHERE run_id = ?", [run_id]
            ).fetchone()
            if old and old[0] != self.space.store.identity:
                raise ValueError(f"only the execution origin can advance run {run_id}")
            if old is None or old[2] != token:
                con.execute("DELETE FROM lake.run_versions WHERE run_id = ?", [run_id])
                con.execute(
                    "INSERT INTO lake.run_versions VALUES (?, ?, ?, ?, true)",
                    [run_id, self.space.store.identity, 1 if old is None else old[1] + 1, token],
                )
            committed.update(
                snapshot
                | {
                    "version": {
                        "origin": self.space.store.identity,
                        "revision": 1 if old is None else old[1] + int(old[2] != token),
                        "token": token,
                    }
                }
            )
            con.execute("COMMIT")

        self.lake.commit(fn)
        return committed

    def _snapshot(self, con: Any, run_id: str) -> dict:
        """Portable metadata from one catalog snapshot; never reads a data file."""
        row = con.execute(_RUNS_SELECT + " WHERE run_id = ?", [run_id]).fetchone()
        record = _decode_run(row)
        record["started"] = record["started"].isoformat()
        record["finished"] = record["finished"].isoformat() if record["finished"] else None
        root = self.space.root + "/"
        record["manifest"] = sorted(
            [
                [s, json.loads(k), p.removeprefix(root)]
                for s, k, p in con.execute(
                    "SELECT step, item_key, path FROM lake.manifest WHERE run_id = ?", [run_id]
                ).fetchall()
            ]
        )
        record["activities"] = [
            json.loads(r[0])
            for r in con.execute(
                "SELECT record FROM lake.artifact_activity WHERE run_id = ? ORDER BY step, item_key", [run_id]
            ).fetchall()
        ]
        record["files"] = [
            {"path": p.removeprefix(root), "size": size, "sha256": digest}
            for p, size, digest in con.execute(
                "SELECT DISTINCT f.path, f.size, f.sha256 FROM lake.part_files f "
                "JOIN lake.manifest m ON m.path=f.path WHERE m.run_id = ? ORDER BY f.path",
                [run_id],
            ).fetchall()
        ]
        return record

    def inventory(self) -> list[dict]:
        """Portable run records and physical availability, read without payloads."""
        if self._blob() is None:
            return []
        with self.lake.read() as con:
            return [
                self._snapshot(con, rid)
                | {"version": {"origin": origin, "revision": revision, "token": token}, "present": present}
                for rid, origin, revision, token, present in con.execute(
                    "SELECT run_id, origin, revision, token, present FROM lake.run_versions ORDER BY run_id"
                ).fetchall()
            ]

    def merge_metadata(self, records: list[dict], tables: dict[str, dict[str, str]], peer: str | None = None) -> None:
        """Merge known runs without registering or fetching missing data files."""
        if not records:
            return
        from .storage import _safe_path

        for record in records:
            rid = record["run_id"]
            if not re.fullmatch(r"[A-Za-z0-9._-]+", rid) or rid in (".", ".."):
                raise ValueError(f"invalid run ID: {rid}")
            for _, _, path in record["manifest"]:
                _safe_path(path)
                if not path.startswith("run_id=") and not re.fullmatch(r"[a-z][a-z0-9-]*:[0-9a-f]{64}", path):
                    raise ValueError(f"invalid manifest path: {path}")
            for f in record["files"]:
                _safe_path(f["path"])
                if not f["path"].startswith("run_id=") or type(f["size"]) is not int or f["size"] < 0:
                    raise ValueError("invalid run file descriptor")
        if self._blob() is not None:
            with self.lake.read() as con:
                versions = {
                    r[0]: r[1:]
                    for r in con.execute("SELECT run_id, origin, revision, token FROM lake.run_versions").fetchall()
                }
                observed = {
                    r[0]: r[1:]
                    for r in con.execute(
                        "SELECT run_id, token, present FROM lake.peer_runs WHERE peer = ?", [peer]
                    ).fetchall()
                }
            records = [
                r
                for r in records
                if versions.get(r["run_id"])
                != (r["version"]["origin"], r["version"]["revision"], r["version"]["token"])
                or (peer and observed.get(r["run_id"]) != (r["version"]["token"], r["present"]))
            ]
            if not records:
                return

        def commit(con):
            con.execute("BEGIN")
            for name, ddl in SYSTEM_DDLS.items():
                con.execute(_create(name, ddl))
            for name, columns in tables.items():
                if name not in SYSTEM_DDLS:
                    from .schema import _q

                    con.execute(_create(_q(name), ", ".join(f"{_q(n)} {t}" for n, t in columns.items())))
            for record in records:
                rid, version = record["run_id"], record["version"]
                body = {k: v for k, v in record.items() if k not in ("version", "present")}
                if _token(body) != version["token"]:
                    raise ValueError(f"run metadata checksum mismatch: {rid}")
                old = con.execute(
                    "SELECT origin, revision, token FROM lake.run_versions WHERE run_id = ?", [rid]
                ).fetchone()
                if old and (
                    old[0] != version["origin"] or (old[1] == version["revision"] and old[2] != version["token"])
                ):
                    raise ValueError(f"run identity or revision conflict: {rid}")
                if peer:
                    con.execute("DELETE FROM lake.peer_runs WHERE peer = ? AND run_id = ?", [peer, rid])
                    con.execute(
                        "INSERT INTO lake.peer_runs VALUES (?, ?, ?, ?)",
                        [peer, rid, version["token"], record["present"]],
                    )
                if old and old[1] >= version["revision"]:
                    continue
                for table in ("runs", "manifest", "artifact_activity", "run_versions"):
                    con.execute(f"DELETE FROM lake.{table} WHERE run_id = ?", [rid])
                con.execute(f"INSERT INTO lake.runs VALUES ({', '.join('?' * len(RUN_COLS))})", _meta_row(record, rid))
                for step, key, path in record["manifest"]:
                    con.execute(
                        "INSERT INTO lake.manifest VALUES (?, ?, ?, ?)",
                        [
                            rid,
                            step,
                            json.dumps(key),
                            self.space.root + "/" + path if path.startswith("run_id=") else path,
                        ],
                    )
                for f in record["files"]:
                    path = self.space.root + "/" + f["path"]
                    con.execute("DELETE FROM lake.part_files WHERE path = ?", [path])
                    con.execute("INSERT INTO lake.part_files VALUES (?, ?, ?)", [path, f["size"], f["sha256"]])
                for activity in record["activities"]:
                    con.execute(
                        "INSERT INTO lake.artifact_activity VALUES (?, ?, ?, ?)",
                        [rid, activity["step"], json.dumps(activity["key"]), json.dumps(activity)],
                    )
                con.execute(
                    "INSERT INTO lake.run_versions VALUES (?, ?, ?, ?, false)",
                    [rid, version["origin"], version["revision"], version["token"]],
                )
            con.execute("COMMIT")

        self.lake.commit(commit)
        for record in records:
            self.export_metadata(record)

    def export_metadata(self, record: dict) -> None:
        """Keep a recoverable ledger, manifest, and lineage without requiring payloads."""
        key = f"{self.space.root}/_inventory/run_id={record['run_id']}/{record['version']['token']}.json"
        self.space.store.put(key, json.dumps(record | {"present": False}).encode())

    def mark_present(self, record: dict) -> None:
        """Register a verified receiving snapshot without advancing its execution."""
        rid, version = record["run_id"], record["version"]

        def commit(con):
            current = con.execute("SELECT token FROM lake.run_versions WHERE run_id = ?", [rid]).fetchone()
            if current is None or current[0] != version["token"]:
                raise ValueError(f"run changed during transfer: {rid}; refresh and retry")
            rows = [
                (s, k, self.space.root + "/" + p if p.startswith("run_id=") else p) for s, k, p in record["manifest"]
            ]
            self._apply(con, rid, rows=rows)
            con.execute("UPDATE lake.run_versions SET present = true WHERE run_id = ?", [rid])

        self.lake.commit(commit)

    def availability(self, peer: str | None = None) -> dict[str, dict[str, bool]]:
        """Presence at this store and the last observed peer, keyed by run ID."""
        if self._blob() is None:
            return {}
        with self.lake.read() as con:
            return {
                rid: {"local": present, "s3": bool(remote)}
                for rid, present, remote in con.execute(
                    "SELECT v.run_id, v.present, p.present AND p.token = v.token FROM lake.run_versions v "
                    "LEFT JOIN lake.peer_runs p ON p.run_id = v.run_id AND p.peer = ?",
                    [peer],
                ).fetchall()
            }

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
            activities = {
                json.dumps([s, json.loads(k)]): json.loads(v)
                for s, k, v in con.execute(
                    "SELECT step, item_key, record FROM lake.artifact_activity WHERE run_id = ?", [r[0]]
                ).fetchall()
            }
        return _decode_run(r) | {"activities": activities}, [(s, json.loads(k), p) for s, k, p in triples]

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
        json.dumps(meta.get("artifacts") or {}),
        json.dumps(meta.get("parent")),
    ]


def backfill(flow_cls, space, replace=False):
    """Rebuild a flow's catalog — ledger rows, manifests, data registrations —
    from the run folders' `_meta.json` recovery records, in one CAS commit.
    The only path out of a lost or corrupt register; also how `sync` derives
    the target-side catalog. Returns the number of runs restored; steps no
    longer in the flow's code are dropped.
    """
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
        runs.append((run_id, _meta_row(meta, run_id), rows, list(meta.get("activities", {}).values()), meta))

    def fn(con):
        con.execute("BEGIN")
        for ddl in cat._ddls(flow_cls).values():
            con.execute(ddl)
        for run_id, row, rows, activities, meta in runs:
            for f in meta.get("files", []):
                path = space.root + "/" + f["path"]
                if con.execute("SELECT 1 FROM lake.part_files WHERE path = ?", [path]).fetchone() is None:
                    con.execute("INSERT INTO lake.part_files VALUES (?, ?, ?)", [path, f["size"], f["sha256"]])
            cat._apply(con, run_id, row, rows, activities)
            body = cat._snapshot(con, run_id)
            version = meta.get("version") or {"origin": space.store.identity, "revision": 1, "token": _token(body)}
            con.execute(
                "INSERT INTO lake.run_versions VALUES (?, ?, ?, ?, true)",
                [run_id, version["origin"], version["revision"], version["token"]],
            )
        con.execute("COMMIT")

    cat.lake.commit(fn)
    snapshots = {}
    for key in space.store.list(space.root + "/_inventory/"):
        record = json.loads(space.store.get(key))
        current = snapshots.get(record["run_id"])
        if current is None or current["version"]["revision"] < record["version"]["revision"]:
            snapshots[record["run_id"]] = record
    cat.merge_metadata(list(snapshots.values()), {})
    return len(cat.runs())


def schema_drift(flow_cls, space):
    """Messages where a step's declared schema differs from its catalog table.

    No catalog (flow never ran) is no drift; nor is a step added since genesis,
    which gets its table on the next run.
    """
    issues = []
    tables = Catalog(space).tables()
    for name, step in flow_cls._dag.steps.items():
        if step.is_artifact:
            continue
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
    if flow_cls._dag.steps[step].is_artifact:
        raise ValueError("artifacts are already global; publish accepts table outputs only")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) or table == PROVENANCE:
        raise ValueError(f"invalid published table name '{table}'")
    store = Store.of(config)
    space = Space.for_flow(store, flow_cls)
    row, triples = Catalog(space).run_state(run_id)
    if row is None:
        raise RuntimeError(f"run not found: {run_id}")
    if row["status"] != "complete":
        raise RuntimeError(f"run {run_id} did not complete; only complete runs publish")

    lake, target = publish_lake(config, dest)
    out_table = flow_cls._dag.steps[step].output_table
    keys = Manifest.from_rows(triples).paths(step)  # the run's parts, wherever they live (0020)
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
