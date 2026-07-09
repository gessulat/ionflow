import io
from dataclasses import dataclass
from urllib.parse import urlparse

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


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


def uri(cfg, key):
    return f"s3://{cfg.bucket}/{key}"


def compute_connection():
    """In-memory DuckDB connection for validation/compute; no S3 access."""
    return duckdb.connect()


def connect(cfg):
    """Return a DuckDB connection wired to the configured S3 endpoint (httpfs)."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    parts = [
        "TYPE s3",
        f"KEY_ID '{cfg.access_key}'",
        f"SECRET '{cfg.secret_key}'",
        f"REGION '{cfg.region}'",
    ]
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
    buf = io.BytesIO()
    pq.write_table(out, buf)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def get_table(client, bucket, key):
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def list_keys(client, bucket, prefix):
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for page in pages for o in page.get("Contents", [])]


def load_relation(con, client, bucket, keys):
    """In-memory DuckDB relation over the given parts (read via boto3)."""
    tables = [get_table(client, bucket, k) for k in keys]
    at = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    return con.from_arrow(at)


def read_parquet_sql(glob_uri):
    """SELECT over a glob, with hive `run_id=` partitioning surfaced as a column."""
    return (
        f"SELECT * FROM read_parquet('{glob_uri}', "
        "hive_partitioning=true, union_by_name=true)"
    )


def read_glob(con, glob_uri):
    return con.sql(read_parquet_sql(glob_uri))
