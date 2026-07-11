import datetime as dt
import io
import os
import shutil
import tempfile
import weakref
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from itertools import batched
from urllib.parse import urlparse

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

try:  # numpy is optional in core; step returns convert when it's installed
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover
    np = None

METRICS_PREFIX = "_metrics"  # basename prefix of all metrics objects; they sit at run-dir
# depth (like _meta.json), which is what keeps them invisible to parse_key
METRICS_FILE = METRICS_PREFIX + ".parquet"  # the orchestrator's object — also the run heartbeat
MISSING_KEY_CODES = {"NoSuchKey", "404"}  # ClientError codes meaning "object absent"
BATCH_ROWS = 122880  # arrow reader batch = one parquet row group
MULTIPART_THRESHOLD = 8 * 2**20  # boto3's default; below it, skip TransferManager churn


class ConstraintViolation(RuntimeError):
    """A step output violated its declared Table constraints (nothing uploaded)."""


def utc_now():
    """Naive UTC now — the project's single timestamp convention."""
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def error_code(err):
    """AWS error code from a botocore ClientError."""
    return err.response["Error"]["Code"]


@dataclass
class Layout:
    """S3 key scheme for one flow.

    Prototype flows live under a `_proto/` scratch namespace: disposable,
    invisible to the flow's stable views, wiped by `microflow clean`.

    >>> l = Layout("mf", "ETL")
    >>> l.ledger
    'mf/ETL/ledger.parquet'
    >>> l.part("r1", "raw")
    'mf/ETL/runs/run_id=r1/raw/part_0.parquet'
    >>> l.table_glob("raw")
    'mf/ETL/runs/*/raw/*.parquet'
    >>> Layout("mf", "ETL", proto=True).ledger
    'mf/_proto/ETL/ledger.parquet'
    """

    prefix: str
    flow: str
    proto: bool = False

    @classmethod
    def for_flow(cls, config, flow_cls):
        """The layout a flow class writes to — the one place identity and flag are read.

        `flow` is the module-qualified `flow_id()`, so same-named classes in
        different modules never share a namespace or ledger.
        """
        return cls(config.prefix, flow_cls.flow_id(), getattr(flow_cls, "prototype", False))

    @property
    def root(self):
        return f"{self.prefix}/_proto/{self.flow}" if self.proto else f"{self.prefix}/{self.flow}"

    @property
    def ledger(self):
        return f"{self.root}/ledger.parquet"

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

    def table_glob(self, step):
        return f"{self.root}/runs/*/{step}/*.parquet"


def s3_client(cfg):
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        region_name=cfg.region,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )


def aws_credentials_available():
    """True if the standard AWS chain resolves credentials (offline check)."""
    return boto3.Session().get_credentials() is not None


def verify_access(cfg):
    """Best-effort reachability check; returns (ok, message)."""
    try:
        s3_client(cfg).head_bucket(Bucket=cfg.bucket)
        return True, f"bucket '{cfg.bucket}' is reachable"
    except Exception as e:  # noqa: BLE001 — surface any failure to the user
        return False, f"cannot reach bucket '{cfg.bucket}': {e}"


def uri(cfg, key):
    return f"s3://{cfg.bucket}/{key}"


def _local_connection(threads=None, memory_mb=None):
    """Fresh DuckDB instance with a private spill dir (removed with the connection).

    `threads` / `memory_mb` cap the instance for pooled workers — each DuckDB
    instance would otherwise claim all cores and 80% of RAM.
    """
    con = duckdb.connect()
    spill = tempfile.mkdtemp(prefix="microflow-duck-")
    weakref.finalize(con, shutil.rmtree, spill, ignore_errors=True)
    con.execute(f"SET temp_directory = '{spill}'")
    if threads:
        con.execute(f"SET threads TO {threads}")
    if memory_mb:
        con.execute(f"SET memory_limit = '{memory_mb}MB'")
    return con


def connect(cfg, threads=None, memory_mb=None):
    """Return a DuckDB connection wired to the configured S3 endpoint (httpfs).

    Uses explicit keys when the config carries them (MinIO/tests), otherwise the
    standard AWS credential chain.
    """
    con = _local_connection(threads, memory_mb)
    con.execute("INSTALL httpfs; LOAD httpfs")
    parts = ["TYPE s3", f"REGION '{cfg.region}'"]
    if cfg.access_key:
        parts += [f"KEY_ID '{cfg.access_key}'", f"SECRET '{cfg.secret_key}'"]
    else:
        parts.append("PROVIDER credential_chain")
    if cfg.endpoint_url:
        u = urlparse(cfg.endpoint_url)
        parts += [
            f"ENDPOINT '{u.netloc}'",
            "URL_STYLE 'path'",
            f"USE_SSL {str(u.scheme == 'https').lower()}",
        ]
    con.execute(f"CREATE OR REPLACE SECRET s ({', '.join(parts)})")
    return con


def _column_array(name, value):
    """Arrow-compatible column from one value; 2D+ ndarrays nest as fixed-size lists.

    NaN is a value, not NULL — a float column full of NaNs passes NOT NULL.
    """
    if np is None or not isinstance(value, np.ndarray):
        return value  # pa.table handles lists, arrow arrays, 1D ndarrays
    if value.dtype.kind == "c":
        raise TypeError(
            f"column '{name}': complex arrays have no parquet type; "
            "store np.real/np.imag as two columns"
        )
    if value.ndim <= 1:
        return value
    arr = pa.array(np.ascontiguousarray(value).reshape(-1))
    for size in value.shape[:0:-1]:  # wrap innermost dimension first
        arr = pa.FixedSizeListArray.from_arrays(arr, size)
    return arr


def _ndarray_table(arr, table):
    """Table from a bare ndarray, taking column names from the declared schema.

    Structured arrays carry their own names/dtypes. A plain array maps onto a
    single declared column (rows = vectors when it's a list column), or
    positionally onto k columns for an (N, k) array.
    """
    if arr.dtype.names:
        return pa.table({n: _column_array(n, arr[n]) for n in arr.dtype.names})
    if table is None:
        raise TypeError(
            "bare ndarray from an untyped step: return {'col': array} or a "
            "structured array so columns have names"
        )
    cols = table.columns()
    names = list(cols)
    if len(names) == 1 and (arr.ndim == 1 or cols[names[0]].is_vector()):
        return pa.table({names[0]: _column_array(names[0], arr)})
    if arr.ndim == 2 and arr.shape[1] == len(names):
        return pa.table({n: _column_array(n, arr[:, i]) for i, n in enumerate(names)})
    raise TypeError(
        f"cannot map a {arr.shape} ndarray onto {table.__name__}'s columns "
        f"({', '.join(names)}); return a dict of named arrays instead"
    )


def to_arrow(obj, table=None) -> pa.Table:
    """Materialize a step return value into a pyarrow Table.

    Accepts a pyarrow Table, a dict of columns (ndarrays — 2D+ become
    fixed-size-list columns — lists, or arrow arrays), a numpy structured or
    bare array (mapped onto `table`'s declared columns), or a polars/pandas
    DataFrame. numpy dtypes are preserved, so a float32 array stays FLOAT.
    DuckDB relations are intercepted by `write_part` and streamed, never
    materialized.

    >>> import numpy as np
    >>> to_arrow({"x": np.arange(2, dtype=np.int16)}).schema.types
    [DataType(int16)]
    >>> str(to_arrow({"v": np.zeros((2, 3), dtype=np.float32)}).schema.types[0])
    'fixed_size_list<item: float>[3]'
    """
    if isinstance(obj, pa.Table):
        return obj
    if np is not None and isinstance(obj, np.ndarray):
        return _ndarray_table(obj, table)
    if isinstance(obj, dict):
        return pa.table({n: _column_array(n, v) for n, v in obj.items()})
    if hasattr(obj, "to_arrow"):  # polars
        return obj.to_arrow()
    if hasattr(obj, "__dataframe__"):  # pandas
        return pa.Table.from_pandas(obj)
    raise TypeError(f"cannot convert {type(obj)} to a table")


def _validate_part(con, table, path, key):
    """One aggregate scan of the written file; raise listing every violation."""
    scans = table.violation_scans()
    if not scans:
        return
    counts = con.execute(
        f"SELECT {', '.join(sql for _, sql in scans)} FROM read_parquet('{path}')"
    ).fetchone()
    if bad := [f"{label}: {n} rows" for (label, _), n in zip(scans, counts, strict=True) if n]:
        raise ConstraintViolation(f"{key}: " + "; ".join(bad))


def write_part(client, bucket, key, data, table, threads=None, memory_mb=None):
    """Stream `data` to a local parquet via DuckDB COPY, validate, upload via boto3.

    The COPY runs on a fresh local connection fed by an arrow batch reader —
    its pipeline must never read httpfs (COPY over a partially consumed httpfs
    source deadlocks), and the write itself goes through boto3 (httpfs's write
    path is unreliable against some S3 stores; upload_file does multipart, so
    parts can exceed S3's 5GB single-PUT cap). Types are enforced by the cast
    projection, NOT NULL / CHECK / PRIMARY KEY by one scan of the written file.
    `table=None` (untyped prototype step) skips both — the schema is whatever
    the data infers. `threads`/`memory_mb` keep pooled workers within their
    share during the write, the pipeline's memory/CPU peak.
    """
    # capability dispatch: covers relations and StepInput (which delegates
    # to_arrow_reader to its relation) without per-wrapper unwrap branches
    src = data.to_arrow_reader(BATCH_ROWS) if hasattr(data, "to_arrow_reader") else to_arrow(data, table)
    if table is not None and (missing := set(table.columns()) - set(src.schema.names)):
        raise ConstraintViolation(f"{key}: step output missing declared columns: {sorted(missing)}")
    con = _local_connection(threads, memory_mb)
    con.register("_src", src)
    select = "SELECT * FROM _src" if table is None else table.cast_sql("_src")
    with tempfile.TemporaryDirectory(prefix="microflow-part-") as tmp_dir:
        path = os.path.join(tmp_dir, "part.parquet")
        con.execute(
            f"COPY ({select}) TO '{path}' (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {BATCH_ROWS})"
        )
        if table is not None:
            _validate_part(con, table, path, key)
        if os.path.getsize(path) < MULTIPART_THRESHOLD:
            with open(path, "rb") as f:
                client.put_object(Bucket=bucket, Key=key, Body=f.read())
        else:
            client.upload_file(path, bucket, key)


def put_parquet(client, bucket, key, table, **extra):
    """Upload a pyarrow Table as parquet; `extra` passes PutObject kwargs (e.g. IfMatch)."""
    buf = io.BytesIO()
    pq.write_table(table, buf)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), **extra)


def get_table(client, bucket, key):
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def delete_prefix(client, bucket, prefix):
    """Delete every object under `prefix`; returns the number deleted."""
    keys = list_keys(client, bucket, prefix)
    for batch in batched(keys, 1000):  # DeleteObjects caps at 1000 keys
        client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]}
        )
    return len(keys)


def object_age(client, bucket, key):
    """Seconds since the object was last written, by S3's own clock (None if absent).

    Compares the HEAD response's Date header with the object's LastModified,
    so no client clock is involved.
    """
    try:
        r = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if error_code(e) in MISSING_KEY_CODES:
            return None
        raise
    server_now = parsedate_to_datetime(r["ResponseMetadata"]["HTTPHeaders"]["date"])
    return (server_now - r["LastModified"]).total_seconds()


def list_keys(client, bucket, prefix):
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for page in pages for o in page.get("Contents", [])]


def parse_key(prefix, key):
    """Reverse of Layout's key scheme, for keys under `prefix` (ends with "/").

    Returns (flow, None) for a flow's ledger, (flow, step) for a run part,
    or None for anything else (e.g. _meta.json, or prototype `_proto/` keys —
    scratch must stay invisible to stable scans; namespace-scoped readers pass
    the `_proto/` prefix themselves).

    >>> parse_key("mf/", "mf/ETL/ledger.parquet")
    ('ETL', None)
    >>> parse_key("mf/", "mf/ETL/runs/run_id=r1/raw/part_0.parquet")
    ('ETL', 'raw')
    >>> parse_key("mf/", "mf/_proto/ETL/ledger.parquet") is None
    True
    >>> parse_key("mf/_proto/", "mf/_proto/ETL/ledger.parquet")
    ('ETL', None)
    >>> parse_key("mf/", "mf/ETL/runs/run_id=r1/_meta.json") is None
    True
    >>> parse_key("mf/", "mf/ETL/runs/run_id=r1/_metrics.parquet") is None
    True
    >>> parse_key("mf/", "mf/ETL/runs/run_id=r1/_metrics_scored_0.parquet") is None
    True
    """
    parts = key.removeprefix(prefix).split("/")
    if parts[1:] == ["ledger.parquet"]:
        return parts[0], None
    if len(parts) == 5 and parts[1] == "runs" and parts[4].endswith(".parquet"):
        return parts[0], parts[3]
    return None


def is_metrics_key(key):
    """True for metrics objects — the orchestrator's or a branch worker's.

    The positive complement of parse_key's exclusion, kept beside the key
    scheme so recognition can't drift from construction.

    >>> is_metrics_key("mf/ETL/runs/run_id=r1/_metrics.parquet")
    True
    >>> is_metrics_key("mf/ETL/runs/run_id=r1/_metrics_scored_0.parquet")
    True
    >>> is_metrics_key("mf/ETL/runs/run_id=r1/raw/part_0.parquet")
    False
    """
    name = key.rsplit("/", 1)[-1]
    return name.startswith(METRICS_PREFIX) and name.endswith(".parquet")


def _np_column(name, arr):
    """ndarray from an arrow (chunked) array; list columns stack to (N, d, ...)."""
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if arr.null_count:
        raise ValueError(
            f"column '{name}' has NULLs, which have no ndarray slot; "
            "filter them out first (or materialize with .to_arrow_table())"
        )
    t = arr.type
    if pa.types.is_fixed_size_list(t):
        d = t.list_size
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        lengths = arr.value_lengths().to_numpy(zero_copy_only=False)
        d = int(lengths[0]) if len(lengths) else 0
        if (lengths != d).any():
            raise ValueError(
                f"column '{name}' is ragged (list lengths {lengths.min()}..{lengths.max()}); "
                ".np() needs a rectangular column"
            )
    else:
        return arr.to_numpy(zero_copy_only=False)
    inner = _np_column(name, arr.flatten())
    return inner.reshape(len(arr), d, *inner.shape[1:])


class StepInput:
    """A step's input: the lazy DuckDB relation plus a numpy accessor.

    Everything a relation offers delegates through (`.filter`, `.aggregate`,
    `.project`, `.df()`, ...) and returns plain relations. `.np()` is the one
    addition, and it MATERIALIZES what it touches — the relation itself stays
    lazy, so filter/project first when the input is large.
    """

    def __init__(self, rel):
        self.rel = rel

    def __getattr__(self, name):
        return getattr(self.rel, name)

    def np(self, column=None):
        """One column as an ndarray, or every column as {name: ndarray}.

        Scalar columns come back 1D with their stored dtype (FLOAT -> float32);
        rectangular list columns stack to (N, d, ...). Ragged or NULL-bearing
        columns raise rather than degrade to object arrays.
        """
        if np is None:
            raise ModuleNotFoundError("numpy is required for .np(); pip install numpy")
        if column is None:  # one scan for all columns, not one pipeline per column
            tbl = self.rel.to_arrow_table()
            return {n: _np_column(n, tbl.column(i)) for i, n in enumerate(tbl.column_names)}
        arr = self.rel.project(f'"{column}"').to_arrow_table().column(0)
        return _np_column(column, arr)


def input_relation(con, cfg, keys):
    """Lazy httpfs relation over the given part keys (pushdown, no download).

    hive_partitioning stays off: part paths contain `run_id=...`, which would
    otherwise inject a run_id column into step inputs.
    """
    return con.read_parquet([uri(cfg, k) for k in keys], hive_partitioning=False)


def empty_relation(con, table):
    """Typed, zero-row DuckDB relation for a table that produced no parts."""
    return con.from_arrow(table.arrow_schema().empty_table())


def read_parquet_sql(glob_uri):
    """SELECT over a glob, with hive `run_id=` partitioning surfaced as a column."""
    return (
        f"SELECT * FROM read_parquet('{glob_uri}', "
        "hive_partitioning=true, union_by_name=true)"
    )
