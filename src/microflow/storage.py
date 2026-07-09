import datetime as dt
import io
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


METRICS_FILE = "_metrics.parquet"  # per-run resource samples; invisible to parse_key
MISSING_KEY_CODES = {"NoSuchKey", "404"}  # ClientError codes meaning "object absent"


def utc_now():
    """Naive UTC now — the project's single timestamp convention."""
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


@dataclass
class Layout:
    """S3 key scheme for one flow.

    >>> l = Layout("mf", "ETL")
    >>> l.ledger
    'mf/ETL/ledger.parquet'
    >>> l.part("r1", "raw")
    'mf/ETL/runs/run_id=r1/raw/part_0.parquet'
    >>> l.table_glob("raw")
    'mf/ETL/runs/*/raw/*.parquet'
    """

    prefix: str
    flow: str

    @property
    def root(self):
        return f"{self.prefix}/{self.flow}"

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


def compute_connection():
    """In-memory DuckDB connection for validation/compute; no S3 access."""
    return duckdb.connect()


def connect(cfg):
    """Return a DuckDB connection wired to the configured S3 endpoint (httpfs).

    Uses explicit keys when the config carries them (MinIO/tests), otherwise the
    standard AWS credential chain.
    """
    con = duckdb.connect()
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


def write_part(con, client, bucket, key, data, table):
    """Validate `data` against `table` schema, then upload parquet via boto3.

    DuckDB enforces types / NOT NULL / CHECK by inserting into a temp table
    built from the schema DDL; the write itself goes through boto3 (httpfs's
    write path is unreliable against some S3 stores).
    """
    con.register("_src", to_arrow(data))
    try:
        cols = ", ".join(table.columns())
        con.execute("DROP TABLE IF EXISTS _v")
        con.execute(table.ddl("_v", temp=True))
        con.execute(f"INSERT INTO _v ({cols}) SELECT {cols} FROM _src")
        out = con.sql("SELECT * FROM _v").to_arrow_table()
        con.execute("DROP TABLE _v")
    finally:
        con.unregister("_src")
    put_parquet(client, bucket, key, out)


def put_parquet(client, bucket, key, table, **extra):
    """Upload a pyarrow Table as parquet; `extra` passes PutObject kwargs (e.g. IfMatch)."""
    buf = io.BytesIO()
    pq.write_table(table, buf)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), **extra)


def get_table(client, bucket, key):
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def object_age(client, bucket, key):
    """Seconds since the object was last written, by S3's own clock (None if absent).

    Compares the HEAD response's Date header with the object's LastModified,
    so no client clock is involved.
    """
    try:
        r = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in MISSING_KEY_CODES:
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
    or None for anything else (e.g. _meta.json).

    >>> parse_key("mf/", "mf/ETL/ledger.parquet")
    ('ETL', None)
    >>> parse_key("mf/", "mf/ETL/runs/run_id=r1/raw/part_0.parquet")
    ('ETL', 'raw')
    >>> parse_key("mf/", "mf/ETL/runs/run_id=r1/_meta.json") is None
    True
    >>> parse_key("mf/", "mf/ETL/runs/run_id=r1/_metrics.parquet") is None
    True
    """
    parts = key[len(prefix):].split("/")
    if parts[1:] == ["ledger.parquet"]:
        return parts[0], None
    if len(parts) == 5 and parts[1] == "runs" and parts[4].endswith(".parquet"):
        return parts[0], parts[3]
    return None


def load_relation(con, client, bucket, keys):
    """In-memory DuckDB relation over the given parts (read via boto3)."""
    tables = [get_table(client, bucket, k) for k in keys]
    at = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    return con.from_arrow(at)


def empty_relation(con, table):
    """Typed, zero-row DuckDB relation for a table that produced no parts."""
    return con.from_arrow(table.arrow_schema().empty_table())


def read_parquet_sql(glob_uri):
    """SELECT over a glob, with hive `run_id=` partitioning surfaced as a column."""
    return (
        f"SELECT * FROM read_parquet('{glob_uri}', "
        "hive_partitioning=true, union_by_name=true)"
    )
