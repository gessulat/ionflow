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

METRICS_PREFIX = "_metrics"  # basename prefix of all metrics objects; invisible to parse_key
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


def to_arrow(obj) -> pa.Table:
    """Materialize a step return value into a pyarrow Table.

    Accepts a polars/pandas DataFrame or pyarrow Table (DuckDB relations are
    intercepted by `write_part` and streamed, never materialized).
    """
    if isinstance(obj, pa.Table):
        return obj
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
    src = data.to_arrow_reader(BATCH_ROWS) if isinstance(data, duckdb.DuckDBPyRelation) else to_arrow(data)
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
