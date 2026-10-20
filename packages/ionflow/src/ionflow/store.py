import builtins
import os
import shutil
import tempfile
import weakref
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import boto3
import duckdb
from botocore.exceptions import ClientError
from schrein import MISSING as MISSING_KEY_CODES
from schrein import CasRegister, DuckLake, LocalClient, error_code
from schrein.ducklake import CATALOG_KEY, load_extension

MULTIPART_THRESHOLD = 8 * 2**20  # boto3's default; below it, skip TransferManager churn
COPY_WORKERS = 16  # sync uploads are latency-bound, not CPU-bound


def s3_client(cfg):
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        region_name=cfg.region,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )


def secret_sql(cfg):
    """CREATE SECRET for the configured S3 endpoint: explicit keys when the config
    carries them (MinIO/tests), else the standard AWS credential chain.
    """
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
    return f"CREATE OR REPLACE SECRET s ({', '.join(parts)})"


def parse_dest(dest, require_prefix=False):
    """'s3://bucket[/prefix]' -> (bucket, prefix | None) — the one parser of --to.

    >>> parse_dest("s3://b/pre/fix/")
    ('b', 'pre/fix')
    """
    bucket, _, prefix = dest.removeprefix("s3://").partition("/")
    if not (dest.startswith("s3://") and bucket and (prefix or not require_prefix)):
        want = "s3://bucket/prefix" if require_prefix else "s3://bucket[/prefix]"
        raise ValueError(f"destination must be {want}, got: {dest}")
    return bucket, prefix.rstrip("/") or None


def _local_connection(threads=None, memory_mb=None):
    """Fresh DuckDB instance with a private spill dir (removed with the connection).

    `threads` / `memory_mb` cap the instance for pooled workers — each DuckDB
    instance would otherwise claim all cores and 80% of RAM.
    """
    con = duckdb.connect()
    spill = tempfile.mkdtemp(prefix="ionflow-duck-")
    weakref.finalize(con, shutil.rmtree, spill, ignore_errors=True)
    con.execute(f"SET temp_directory = '{spill}'")
    if threads:
        con.execute(f"SET threads TO {threads}")
    if memory_mb:
        con.execute(f"SET memory_limit = '{memory_mb}MB'")
    return con


class Store:
    """One object store, as ionflow uses it: bytes by key, plus the DuckDB wiring
    to read them. `Store.of` is the one place local-vs-S3 is decided."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.prefix = cfg.prefix
        self.bucket = cfg.bucket  # None in local mode: passed through, never validated
        self.local_dir = cfg.local_dir  # None in S3 mode
        self.client = self._client()

    @classmethod
    def of(cls, cfg):
        """The store this config describes (decision 0016: exactly one mode)."""
        return DirStore(cfg) if cfg.local_dir else S3Store(cfg)

    def space(self, flow_id):
        """This store's region for a flow, named by its id string."""
        from .space import Space  # Space builds on Store; the import stays local to break the cycle

        return Space(self, self.prefix, flow_id)

    def flow_ids(self) -> builtins.list[str]:
        """Discover flow catalogs without importing their Python definitions."""
        from .space import flow_id_of_catalog_key

        prefix = self.prefix + "/runs/"
        return sorted({n for k in self.list(prefix) if (n := flow_id_of_catalog_key(prefix, k)) is not None})

    def _client(self):
        raise NotImplementedError

    def get(self, key):
        """GET an object's bytes, or None if it doesn't exist."""
        try:
            return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as e:
            if error_code(e) in MISSING_KEY_CODES:
                return None
            raise

    def put(self, key, body):
        """Whole-object PUT — no CAS, so repeated writes of one key are safe."""
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body)

    def upload(self, key, path):
        """Upload a local file; multipart above the threshold, so parts can
        exceed S3's 5GB single-PUT cap."""
        if os.path.getsize(path) < MULTIPART_THRESHOLD:
            with open(path, "rb") as f:
                self.put(key, f.read())
        else:
            self.client.upload_file(path, self.bucket, key)

    def delete(self, key):
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def exists(self, key: str) -> bool:
        """Whether a complete object is present, without fetching its body."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if error_code(e) in MISSING_KEY_CODES:
                return False
            raise

    def download(self, key: str, path: str) -> None:
        """Stream an object into a caller-owned staging file."""
        self.client.download_file(self.bucket, key, path)

    def list(self, prefix):
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        return [o["Key"] for page in pages for o in page.get("Contents", [])]

    def age(self, key):
        """Seconds since the object was last written, by S3's own clock (None if absent).

        Compares the HEAD response's Date header with the object's LastModified,
        so no client clock is involved.
        """
        try:
            r = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if error_code(e) in MISSING_KEY_CODES:
                return None
            raise
        server_now = parsedate_to_datetime(r["ResponseMetadata"]["HTTPHeaders"]["date"])
        return (server_now - r["LastModified"]).total_seconds()

    def uri(self, key):
        raise NotImplementedError

    def key_for_uri(self, uri):
        """Inverse of uri(): the key a store URI names."""
        root = self.uri("")
        if not uri.startswith(root):
            raise ValueError(f"{uri} is not under {root}")
        return uri.removeprefix(root)

    def local_connection(self, threads=None, memory_mb=None):
        """A fresh store-free DuckDB connection (no httpfs, no secret)."""
        return _local_connection(threads, memory_mb)

    def connect(self, threads=None, memory_mb=None):
        """A DuckDB connection wired to this store; local mode needs neither httpfs
        nor a secret (DuckDB reads plain paths natively), so S3Store overrides.
        """
        return self.local_connection(threads, memory_mb)

    def lake(self, prefix, data_subdir="data"):
        raise NotImplementedError

    @staticmethod
    def verify(cfg):
        """Best-effort reachability check; returns (ok, message)."""
        if cfg.local_dir:
            try:
                LocalClient(cfg.local_dir)  # creates the root; raises if it can't
                return True, f"local store '{cfg.local_dir}' is writable"
            except Exception as e:  # noqa: BLE001 — surface any failure to the user
                return False, f"cannot use local store '{cfg.local_dir}': {e}"
        try:
            s3_client(cfg).head_bucket(Bucket=cfg.bucket)
            return True, f"bucket '{cfg.bucket}' is reachable"
        except Exception as e:  # noqa: BLE001 — surface any failure to the user
            return False, f"cannot reach bucket '{cfg.bucket}': {e}"

    @staticmethod
    def credentials():
        """True if the standard AWS chain resolves credentials (offline check)."""
        return boto3.Session().get_credentials() is not None


class S3Store(Store):
    def _client(self):
        return s3_client(self.cfg)

    def uri(self, key):
        return f"s3://{self.bucket}/{key}"

    def connect(self, threads=None, memory_mb=None):
        con = self.local_connection(threads, memory_mb)
        load_extension(con, "httpfs")  # schrein's loader: cache, then wheel, then INSTALL
        con.execute(secret_sql(self.cfg))
        return con

    def lake(self, prefix, data_subdir="data"):
        return DuckLake.at(self.client, self.bucket, prefix, secret_sql=secret_sql(self.cfg), data_subdir=data_subdir)


class DirStore(Store):
    def _client(self):
        return LocalClient(self.cfg.local_dir)

    def uri(self, key):
        return f"{self.local_dir}/{key}"

    def download(self, key: str, path: str) -> None:
        shutil.copyfile(os.path.join(self.local_dir, key), path)

    def lake(self, prefix, data_subdir="data"):
        # "local" is a display label only (LocalClient ignores Bucket); it keeps
        # CAS error messages readable where the bucket would print None
        register = CasRegister(self.client, "local", f"{prefix}/{CATALOG_KEY}")
        return DuckLake(register, data_path=self.uri(f"{prefix}/{data_subdir}/"))
