import datetime as dt
import hashlib
import json
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from schrein.ducklake import CATALOG_KEY

from .arrow import BATCH_ROWS, parquet_bytes, to_arrow
from .schema import _q
from .store import COPY_WORKERS, Store

METRICS_PREFIX = "_metrics"  # basename prefix of all metrics objects (run-dir depth, like _meta.json)
METRICS_FILE = METRICS_PREFIX + ".parquet"  # the orchestrator's object — also the run heartbeat
LAKE_SUBDIR = "_lake"  # catalog-written data files at the flow root; sync skips it
META_FILE = "_meta.json"  # per-run recovery export; the catalog is the authority (0019)


class ConstraintViolation(RuntimeError):
    """A step output violated its declared Table constraints (nothing uploaded)."""


def utc_now():
    """Naive UTC now — the project's single timestamp convention."""
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


_SLUG_BAD = re.compile(r"[^A-Za-z0-9._-]")


def key_slug(key):
    """Filename-safe form of an item key: exact when already clean, else
    sanitized + content-hashed. The manifest, not the filename, is the
    authoritative key -> part mapping; the slug only keeps run folders legible.

    >>> key_slug(3), key_slug("file-7")
    ('3', 'file-7')
    >>> key_slug("data/a.raw")
    'data-a.raw-7ffc3f78'
    """
    s = str(key)
    clean = _SLUG_BAD.sub("-", s)[:80]
    return clean if clean == s else f"{clean}-{hashlib.sha1(s.encode()).hexdigest()[:8]}"


def flow_id_of_catalog_key(prefix, key):
    """The flow a `<prefix>/<flow_id>/catalog.ducklake` key names, else None."""
    suffix = "/" + CATALOG_KEY
    if not key.startswith(prefix) or not key.endswith(suffix):
        return None
    name = key[len(prefix) : -len(suffix)]
    return name if "/" not in name else None


def _validate_part(con, table, path, key):
    """One aggregate scan of the written file; raise listing every violation."""
    scans = table.violation_scans()
    if not scans:
        return
    counts = con.execute(f"SELECT {', '.join(sql for _, sql in scans)} FROM read_parquet('{path}')").fetchone()
    if bad := [f"{label}: {n} rows" for (label, _), n in zip(scans, counts, strict=True) if n]:
        raise ConstraintViolation(f"{key}: " + "; ".join(bad))


def _extract_fanout_key(con, column, path, key):
    """The single distinct value of a fan-out item's key column — its identity."""
    col = _q(column)
    rows, nulls, distinct, value = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE {col} IS NULL), "
        f"count(DISTINCT {col}), any_value({col}) FROM read_parquet('{path}')"
    ).fetchone()
    if rows == 0:
        raise ConstraintViolation(f"{key}: keyed fan-out item is empty; every item needs one '{column}' value")
    if nulls or distinct != 1:
        raise ConstraintViolation(
            f"{key}: fanout key column '{column}' must hold exactly one distinct non-NULL "
            f"value per item (got {distinct} distinct values, {nulls} NULLs)"
        )
    return value


@dataclass(frozen=True)
class Space:
    """One flow's region of a store: its key scheme, and the verbs over those keys.

    Identity is the `flow_id` STRING, never a class — branch workers re-import the
    flow module under a loader name, so their `flow_cls.flow_id()` is wrong.

    >>> s = Space(None, "mf", "ETL")
    >>> s.meta("r1")
    'mf/runs/ETL/run_id=r1/_meta.json'
    >>> s.part("r1", "raw")
    'mf/runs/ETL/run_id=r1/raw/part_0.parquet'
    >>> s.part("r1", "work", "file-7")
    'mf/runs/ETL/run_id=r1/work/part_file-7.parquet'
    """

    store: Store
    prefix: str
    flow_id: str

    @classmethod
    def for_flow(cls, store, flow_cls):
        """The space a flow class writes to — the one place identity is read from a class."""
        return store.space(flow_cls.flow_id())

    @property
    def root(self):
        return f"{self.prefix}/runs/{self.flow_id}"

    def run_dir(self, run_id):
        return f"{self.root}/run_id={run_id}"

    def meta(self, run_id):
        return f"{self.run_dir(run_id)}/{META_FILE}"

    def run_ids(self):
        """run_ids of every run folder holding a `_meta.json`, from one LIST."""
        prefix = f"{self.root}/run_id="
        out = []
        for key in self.store.list(prefix):
            run_id, _, tail = key.removeprefix(prefix).partition("/")
            if tail == META_FILE:
                out.append(run_id)
        return sorted(out)

    def metrics(self, run_id):
        return f"{self.run_dir(run_id)}/{METRICS_FILE}"

    def metrics_branch(self, run_id, step, branch):
        """An item worker's own metrics object — one writer per key, no CAS needed."""
        return f"{self.run_dir(run_id)}/{METRICS_PREFIX}_{step}_{branch}.parquet"

    def metrics_node(self, run_id, step):
        """A whole-step task's metrics object. Dot-separated: step names cannot
        contain dots, so it can never collide with an item's `_metrics_...` key."""
        return f"{self.run_dir(run_id)}/{METRICS_PREFIX}.{step}.parquet"

    @property
    def metrics_glob(self):
        return f"{self.root}/run_id=*/{METRICS_PREFIX}*.parquet"  # orchestrator + branch objects

    def step_dir(self, run_id, step):
        return f"{self.run_dir(run_id)}/{step}"

    def part(self, run_id, step, key=0):
        """The part key for one (step, item key) — see `key_slug` for the naming."""
        return f"{self.step_dir(run_id, step)}/part_{key_slug(key)}.parquet"

    def is_internal(self, key):
        """True for the flow's catalog register or its catalog-written data files."""
        tail = key.removeprefix(self.root)
        return tail.startswith((f"/{CATALOG_KEY}", f"/{LAKE_SUBDIR}/"))

    def uri(self, key):
        return self.store.uri(key)

    def relation(self, con, keys):
        """Lazy relation over the given part keys (pushdown, no download).

        hive_partitioning stays off: part paths contain `run_id=...`, which would
        otherwise inject a run_id column into step inputs.
        """
        return con.read_parquet([self.uri(k) for k in keys], hive_partitioning=False)

    def lake(self, data_subdir=LAKE_SUBDIR):
        """The flow's catalog DuckLake at its root (catalog-written files only)."""
        return self.store.lake(self.root, data_subdir=data_subdir)

    def put(self, key, table):
        """Write a small pyarrow Table as one parquet object (whole-object PUT)."""
        self.store.put(key, parquet_bytes(table))

    def write_meta(self, run_id, meta):
        self.store.put(self.meta(run_id), json.dumps(meta, default=str).encode())

    def read_meta(self, run_id):
        blob = self.store.get(self.meta(run_id))
        return None if blob is None else json.loads(blob)

    def write_part(self, dest, data, table, threads=None, memory_mb=None, fanout_key=None):
        """Stream `data` to a local parquet via DuckDB COPY, validate, upload via boto3.

        The COPY runs on a fresh local connection fed by an arrow batch reader —
        its pipeline must never read httpfs (COPY over a partially consumed httpfs
        source deadlocks), and the write itself goes through boto3, never httpfs
        (a failed `COPY TO s3://` leaves a corrupt part visible, ADR 0010).
        Nothing touches S3 until the staged file is complete and validated, so a
        part object's existence implies the part is whole. `dest` is the target
        key, or — for keyed fan-out items, whose name derives from the data — a
        callable mapping the extracted key value to it (raising there, e.g. on a
        duplicate key, still uploads nothing). Returns the item's `fanout_key`
        value when a key column is given, else None.
        """
        # capability dispatch: covers relations and StepInput (which delegates
        # to_arrow_reader to its relation) without per-wrapper unwrap branches
        src = data.to_arrow_reader(BATCH_ROWS) if hasattr(data, "to_arrow_reader") else to_arrow(data, table)
        label = dest if isinstance(dest, str) else "step output"
        if missing := set(table.columns()) - set(src.schema.names):
            raise ConstraintViolation(f"{label}: step output missing declared columns: {sorted(missing)}")
        con = self.store.local_connection(threads, memory_mb)
        con.register("_src", src)
        select = table.cast_sql("_src")
        with tempfile.TemporaryDirectory(prefix="ionflow-part-") as tmp_dir:
            path = os.path.join(tmp_dir, "part.parquet")
            con.execute(f"COPY ({select}) TO '{path}' (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {BATCH_ROWS})")
            _validate_part(con, table, path, label)
            key_value = None if fanout_key is None else _extract_fanout_key(con, fanout_key, path, label)
            self.store.upload(dest(key_value) if callable(dest) else dest, path)
        return key_value

    def sync_to(self, other, flow_cls, run_id=None):
        """Copy run reference closures, then merge their exported records atomically.

        Destination catalogs derive from records, never copied database bytes.
        `run_id` selects one run and the creating runs of its referenced parts.
        """
        from .artifact import Artifacts
        from .lake import Catalog, _meta_row

        if not self.flow_id == other.flow_id == flow_cls.flow_id():
            raise ValueError(
                f"sync_to: source, target and flow must name one flow, got "
                f"{self.flow_id!r} -> {other.flow_id!r} for {flow_cls.flow_id()!r}"
            )
        selected = set(self.run_ids() if run_id is None else [run_id])
        metas = {}
        while selected - metas.keys():
            rid = sorted(selected - metas.keys())[0]
            meta = self.read_meta(rid)
            if meta is None:
                raise ValueError(f"run not found: {rid}")
            metas[rid] = meta
            for pairs in meta.get("manifest", {}).values():
                for _, path in pairs:
                    prefix = self.root + "/run_id="
                    if path.startswith(prefix):
                        selected.add(path.removeprefix(prefix).split("/", 1)[0])
        cat = Catalog(other)
        target_rows = {row["run_id"]: row for row in cat.runs()}
        skip = set()
        for rid, meta in metas.items():
            if rid not in target_rows:
                continue
            current = target_rows[rid]
            expected = {
                "commit": meta.get("commit"),
                "params": meta.get("params") or {},
                "artifacts": meta.get("artifacts") or {},
            }
            if any(current[field] != expected[field] for field in expected):
                raise ValueError(f"run identity conflicts at destination: {rid}")
            if current["status"] != "running":
                if meta["status"] == "running":
                    skip.add(rid)
                elif current["status"] != meta["status"]:
                    raise ValueError(f"run outcome conflicts at destination: {rid}")
        artifact_ids = set()
        for meta in metas.values():
            for value in meta.get("artifacts", {}).values():
                artifact_ids.update(r["id"] for r in (value if isinstance(value, list) else [value]))
            for activity in meta.get("activities", {}).values():
                artifact_ids.update(activity["ancestors"])
                artifact_ids.update(u["artifact"]["id"] for u in activity["uses"])
                artifact_ids.update(o["artifact"]["id"] for o in activity["outputs"])
        if artifact_ids:
            Artifacts(self.store).sync_to(Artifacts(other.store), sorted(artifact_ids))
        existing = set(other.store.list(other.root + "/"))
        todo = []
        for rid in sorted(selected):
            for key in self.store.list(self.run_dir(rid) + "/"):
                target = other.root + key.removeprefix(self.root)
                if target not in existing and key != self.meta(rid):
                    todo.append((key, target))

        def transfer(pair):
            source, target = pair
            with tempfile.TemporaryDirectory(prefix="ionflow-sync-") as tmp:
                path = os.path.join(tmp, "object")
                self.store.download(source, path)
                other.store.upload(target, path)

        with ThreadPoolExecutor(max_workers=COPY_WORKERS) as ex:
            list(ex.map(transfer, todo))
        # Expose records only after the entire reference closure has arrived.
        for rid, meta in sorted(metas.items()):
            if rid in skip:
                continue
            meta = meta | {
                "manifest": {
                    step: [
                        [k, other.root + p.removeprefix(self.root) if p.startswith(self.root + "/") else p]
                        for k, p in pairs
                    ]
                    for step, pairs in meta.get("manifest", {}).items()
                }
            }
            from .state import Manifest

            rows = [(s.step, s.key, p) for s, p in Manifest.from_meta(meta["manifest"]).slots()]
            cat.commit_run(
                rid,
                row=_meta_row(meta, rid),
                rows=rows,
                genesis=flow_cls,
                activities=list(meta.get("activities", {}).values()),
            )
            other.write_meta(rid, meta)
        return {"copied": len(todo) + len(metas), "runs": len(cat.runs())}
