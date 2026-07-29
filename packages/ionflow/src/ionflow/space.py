import datetime as dt
import json
import os
import pathlib
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pyarrow as pa
from schrein.ducklake import CATALOG_KEY

from .arrow import BATCH_ROWS, parquet_bytes, table_from_bytes, to_arrow
from .schema import _q
from .store import COPY_WORKERS, Store

METRICS_PREFIX = "_metrics"  # basename prefix of all metrics objects (run-dir depth, like _meta.json)
METRICS_FILE = METRICS_PREFIX + ".parquet"  # the orchestrator's object — also the run heartbeat
LEDGER_FILE = "ledger.parquet"
LAKE_SUBDIR = "_lake"  # catalog-written data files at the flow root; sync skips it

LEDGER_FIELDS = [
    pa.field("run_id", pa.string()),
    pa.field("status", pa.string()),  # running | complete | failed
    pa.field("started", pa.timestamp("us")),
    pa.field("finished", pa.timestamp("us")),
    pa.field("commit", pa.string()),
    pa.field("origin_run_id", pa.string()),
]

BASE_COLS = {f.name for f in LEDGER_FIELDS}  # names params/custom columns must not take


class ConstraintViolation(RuntimeError):
    """A step output violated its declared Table constraints (nothing uploaded)."""


def utc_now():
    """Naive UTC now — the project's single timestamp convention."""
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def flow_id_of_ledger_key(prefix, key):
    """The flow a `<prefix>/<flow_id>/ledger.parquet` key names, else None."""
    suffix = "/" + LEDGER_FILE
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
    >>> s.ledger
    'mf/ETL/ledger.parquet'
    >>> s.part("r1", "raw")
    'mf/ETL/runs/run_id=r1/raw/part_0.parquet'
    >>> s.parse_part(s.part("r1", "raw"))
    ('r1', 'raw', 'part_0.parquet')
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
        return f"{self.prefix}/{self.flow_id}"

    @property
    def ledger(self):
        return f"{self.root}/{LEDGER_FILE}"

    def run_dir(self, run_id):
        return f"{self.root}/runs/run_id={run_id}"

    def meta(self, run_id):
        return f"{self.run_dir(run_id)}/_meta.json"

    def metrics(self, run_id):
        return f"{self.run_dir(run_id)}/{METRICS_FILE}"

    def metrics_branch(self, run_id, step, branch):
        """A branch worker's own metrics object — one writer per key, no CAS needed."""
        return f"{self.run_dir(run_id)}/{METRICS_PREFIX}_{step}_{branch}.parquet"

    @property
    def metrics_glob(self):
        return f"{self.root}/runs/*/{METRICS_PREFIX}*.parquet"  # orchestrator + branch objects

    def step_dir(self, run_id, step):
        return f"{self.run_dir(run_id)}/{step}"

    def part(self, run_id, step, branch=0):
        return f"{self.step_dir(run_id, step)}/part_{branch}.parquet"

    def parse_part(self, key):
        """Inverse of part(): (run_id, step, tail) for a key under this space's run
        folders, else None. `tail` is everything after the step segment, so nested
        keys parse like the flat ones the layout writes."""
        prefix = f"{self.root}/runs/run_id="
        if not key.startswith(prefix):
            return None
        run_id, _, rest = key[len(prefix) :].partition("/")
        step, _, tail = rest.partition("/")
        return (run_id, step, tail) if tail else None

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

    def part_keys(self, run_id, step):
        """A step's part keys for one run, sorted — the one registration/read order."""
        return sorted(self.store.list(self.step_dir(run_id, step) + "/"))

    def lake(self, data_subdir=LAKE_SUBDIR):
        """The flow's catalog DuckLake at its root (catalog-written files only)."""
        return self.store.lake(self.root, data_subdir=data_subdir)

    def put(self, key, table):
        """Write a small pyarrow Table as one parquet object (whole-object PUT)."""
        self.store.put(key, parquet_bytes(table))

    def read_rows(self, key):
        """Rows of a parquet object, or [] when it does not exist."""
        blob = self.store.get(key)
        return [] if blob is None else table_from_bytes(blob).to_pylist()

    def write_meta(self, run_id, meta):
        self.store.put(self.meta(run_id), json.dumps(meta, default=str).encode())

    def read_meta(self, run_id):
        blob = self.store.get(self.meta(run_id))
        return None if blob is None else json.loads(blob)

    def amend(self, key, schema, merge, retries=5):
        """CAS-rewrite a parquet object: `merge(rows)` returns the FULL new row list.

        `merge` runs inside the CAS closure, against a fresh read on every attempt
        (decision 0011) — it must be replayable and must not do I/O.
        """

        def fn(old):
            rows = merge([] if old is None else table_from_bytes(old).to_pylist())
            return parquet_bytes(pa.Table.from_pylist(rows, schema=schema))

        self.store.cas(key).transact(fn, max_attempts=retries, base_delay=0.05)

    def upsert_runs(self, rows, custom_fields=(), retries=5):
        """Insert or replace ledger rows by run_id, all in one CAS commit."""
        schema = pa.schema(LEDGER_FIELDS + list(custom_fields))
        new_ids = {r["run_id"] for r in rows}

        def merge(old):
            merged = [r for r in old if r["run_id"] not in new_ids]
            merged += [dict(r) for r in rows]  # from_pylist fills missing schema columns with null
            merged.sort(key=lambda r: r["started"])
            return merged

        self.amend(self.ledger, schema, merge, retries=retries)

    def copy_parts(self, items):
        """Server-side copies of (src_key, dst_key, tag) triples; yields each tag as
        its copy lands, in submission order (consume it as `Store.copy_many` says).
        """
        tag_of = {dst: tag for _, dst, tag in items}
        return (tag_of[dst] for dst in self.store.copy_many([(s, d) for s, d, _ in items]))

    def write_part(self, key, data, table, threads=None, memory_mb=None, fanout_key=None):
        """Stream `data` to a local parquet via DuckDB COPY, validate, upload via boto3.

        The COPY runs on a fresh local connection fed by an arrow batch reader —
        its pipeline must never read httpfs (COPY over a partially consumed httpfs
        source deadlocks), and the write itself goes through boto3, never httpfs
        (a failed `COPY TO s3://` leaves a corrupt part visible, ADR 0010).
        Nothing touches S3 until the staged file is complete and validated, so a
        part object's existence implies the part is whole. Returns the item's
        `fanout_key` value when a key column is given, else None.
        """
        # capability dispatch: covers relations and StepInput (which delegates
        # to_arrow_reader to its relation) without per-wrapper unwrap branches
        src = data.to_arrow_reader(BATCH_ROWS) if hasattr(data, "to_arrow_reader") else to_arrow(data, table)
        if missing := set(table.columns()) - set(src.schema.names):
            raise ConstraintViolation(f"{key}: step output missing declared columns: {sorted(missing)}")
        con = self.store.local_connection(threads, memory_mb)
        con.register("_src", src)
        select = table.cast_sql("_src")
        with tempfile.TemporaryDirectory(prefix="ionflow-part-") as tmp_dir:
            path = os.path.join(tmp_dir, "part.parquet")
            con.execute(f"COPY ({select}) TO '{path}' (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {BATCH_ROWS})")
            _validate_part(con, table, path, key)
            key_value = None if fanout_key is None else _extract_fanout_key(con, fanout_key, path, key)
            self.store.upload(key, path)
        return key_value

    def sync_to(self, other, flow_cls):
        """Copy this local space's runs + ledger into `other`, then rebuild the
        catalog there (decision 0016). Returns {copied, runs}.

        Ledger rows MERGE through the target's CAS ledger; the catalog is
        re-derived rather than copied — it stores absolute data-file locations.
        """
        from .lake import backfill  # lake builds on Space; the import stays local to break the cycle

        if not self.store.local_dir:
            raise ValueError("sync sources a local store; the active config has no local_dir")
        if not self.flow_id == other.flow_id == flow_cls.flow_id():
            raise ValueError(
                f"sync_to: source, target and flow must name one flow, got "
                f"{self.flow_id!r} -> {other.flow_id!r} for {flow_cls.flow_id()!r}"
            )
        root = pathlib.Path(self.store.local_dir)
        existing = set(other.store.list(other.root + "/"))
        todo = []
        for key in self.store.list(self.root + "/"):
            tail = key.removeprefix(self.root)
            if key == self.ledger or self.is_internal(key) or other.root + tail in existing:
                continue  # run folders are immutable: a key already on the target is done
            todo.append((str(root / key), other.root + tail))
        with ThreadPoolExecutor(max_workers=COPY_WORKERS) as ex:  # many small objects, latency-bound
            list(ex.map(lambda t: other.store.upload(t[1], t[0]), todo))

        runs = 0
        blob = self.store.get(self.ledger)
        if blob is not None:
            table = table_from_bytes(blob)
            custom = [table.schema.field(n) for n in table.schema.names if n not in BASE_COLS]
            other.upsert_runs(table.to_pylist(), custom)
            runs = table.num_rows

        if runs:
            backfill(flow_cls, other, replace=True)
        return {"copied": len(todo), "runs": runs}
