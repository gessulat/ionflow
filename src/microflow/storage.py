import datetime as dt
import io
import os
import shutil
import tempfile
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from itertools import batched
from urllib.parse import urlparse

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

METRICS_FILE = "_metrics.parquet"  # per-run resource samples; invisible to parse_key
MISSING_KEY_CODES = {"NoSuchKey", "404"}  # ClientError codes meaning "object absent"
BATCH_ROWS = 122880  # arrow reader batch = one parquet row group


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
        """The layout a flow class writes to — the one place the flag is read."""
        return cls(config.prefix, flow_cls.__name__, getattr(flow_cls, "prototype", False))

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

    @property
    def metrics_glob(self):
        return f"{self.root}/runs/*/{METRICS_FILE}"

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


def connect(cfg, threads=None, memory_mb=None):
    """Return a DuckDB connection wired to the configured S3 endpoint (httpfs).

    Uses explicit keys when the config carries them (MinIO/tests), otherwise the
    standard AWS credential chain. Every connection gets a private temp_directory
    so operators spill to disk instead of erroring; `threads` / `memory_mb` cap
    the instance for pooled workers (each connection is its own DuckDB instance
    that would otherwise claim all cores and 80% of RAM).
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(f"SET temp_directory = '{tempfile.mkdtemp(prefix='microflow-duck-')}'")
    if threads:
        con.execute(f"SET threads TO {threads}")
    if memory_mb:
        con.execute(f"SET memory_limit = '{memory_mb}MB'")
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

    Accepts a DuckDB relation, polars/pandas DataFrame, or pyarrow Table.
    """
    if isinstance(obj, pa.Table):
        return obj
    if hasattr(obj, "to_arrow_table"):  # duckdb relation
        return obj.to_arrow_table()
    if hasattr(obj, "to_arrow"):  # polars
        return obj.to_arrow()
    if hasattr(obj, "__dataframe__"):  # pandas
        return pa.Table.from_pandas(obj)
    raise TypeError(f"cannot convert {type(obj)} to a table")


def _constraint_scans(table):
    """(label, count-of-violations SQL) per declared constraint, for one scan.

    A CHECK passes on NULL (SQL semantics), so violations are `(expr) IS FALSE`;
    PRIMARY KEY NULLs are caught by its NOT NULL, so DISTINCT-counting handles
    uniqueness alone (the row-value form covers composite keys).
    """
    scans = []
    for n, c in table.columns().items():
        if not c.nullable:
            scans.append((f"{n} NOT NULL", f"count(*) FILTER (WHERE {n} IS NULL)"))
        if c.check:
            scans.append((f"CHECK ({c.check})", f"count(*) FILTER (WHERE ({c.check}) IS FALSE)"))
    if pk := [n for n, c in table.columns().items() if c.primary_key]:
        cols = ", ".join(pk)
        scans.append((f"PRIMARY KEY ({cols})", f"count(*) - count(DISTINCT ({cols}))"))
    return scans


def _validate_part(con, table, path, key):
    """One aggregate scan of the written file; raise listing every violation."""
    scans = _constraint_scans(table)
    if not scans:
        return
    counts = con.execute(
        f"SELECT {', '.join(sql for _, sql in scans)} FROM read_parquet('{path}')"
    ).fetchone()
    if bad := [f"{label}: {n} rows" for (label, _), n in zip(scans, counts, strict=True) if n]:
        raise ConstraintViolation(f"{key}: " + "; ".join(bad))


def write_part(client, bucket, key, data, table):
    """Stream `data` to a local parquet via DuckDB COPY, validate, upload via boto3.

    The COPY runs on a fresh local connection fed by an arrow batch reader —
    its pipeline must never read httpfs (COPY over a partially consumed httpfs
    source deadlocks), and the write itself goes through boto3 (httpfs's write
    path is unreliable against some S3 stores; upload_file does multipart, so
    parts can exceed S3's 5GB single-PUT cap). Types are enforced by the cast
    projection, NOT NULL / CHECK / PRIMARY KEY by one scan of the written file.
    `table=None` (untyped prototype step) skips both — the schema is whatever
    the data infers.
    """
    if isinstance(data, duckdb.DuckDBPyRelation):
        names, src = data.columns, data.to_arrow_reader(BATCH_ROWS)
    else:
        src = to_arrow(data)
        names = src.column_names
    if table is not None and (missing := set(table.columns()) - set(names)):
        raise ConstraintViolation(f"{key}: step output missing declared columns: {sorted(missing)}")
    con = duckdb.connect()
    con.register("_src", src)
    if table is None:
        select = "SELECT * FROM _src"
    else:
        cols = ", ".join(f"CAST({n} AS {c.duckdb_type()}) AS {n}" for n, c in table.columns().items())
        select = f"SELECT {cols} FROM _src"
    tmp_dir = tempfile.mkdtemp(prefix="microflow-part-")
    try:
        path = os.path.join(tmp_dir, "part.parquet")
        con.execute(
            f"COPY ({select}) TO '{path}' (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {BATCH_ROWS})"
        )
        if table is not None:
            _validate_part(con, table, path, key)
        client.upload_file(path, bucket, key)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


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
    uris = [uri(cfg, k) for k in keys]
    return con.read_parquet(uris if len(uris) > 1 else uris[0], hive_partitioning=False)


def empty_relation(con, table):
    """Typed, zero-row DuckDB relation for a table that produced no parts."""
    return con.from_arrow(table.arrow_schema().empty_table())


def read_parquet_sql(glob_uri):
    """SELECT over a glob, with hive `run_id=` partitioning surfaced as a column."""
    return (
        f"SELECT * FROM read_parquet('{glob_uri}', "
        "hive_partitioning=true, union_by_name=true)"
    )
