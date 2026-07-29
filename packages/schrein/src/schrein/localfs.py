"""A boto3-shaped S3 client over a local directory — the zero-bucket backend.

Conditional PUTs (If-Match / If-None-Match) are genuinely enforced, so
`CasRegister` and `DuckLake` run unchanged against a plain directory; failures
raise botocore `ClientError` with the S3 codes callers already handle. `Bucket`
is accepted and ignored: one client, one root directory, keys relative to it.

Conditional writes serialize on one advisory file lock at the root, which makes
CAS linearizable across processes ON ONE HOST — this backend is for prototyping
without infrastructure, not multi-host coordination (decision 0005).
"""

import datetime as dt
import hashlib
import io
import os
import pathlib
import shutil
import tempfile
from contextlib import contextmanager
from email.utils import format_datetime

from botocore.exceptions import ClientError

try:  # POSIX; on Windows fall back to msvcrt region locking
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None
    import msvcrt

LOCK_FILE = ".schrein.lock"  # serializes conditional writes; hidden from listings


def _err(code, op, msg=""):
    return ClientError({"Error": {"Code": code, "Message": msg}}, op)


def _quote(digest):
    return f'"{digest.hexdigest()}"'


def _etag(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return _quote(h)


def _mtime(path):
    return dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC)


class _Paginator:
    def __init__(self, root):
        self.root = root

    def paginate(self, Bucket=None, Prefix=""):  # noqa: N803 — boto3 casing
        # walk only the prefix's directory subtree, not the whole store: keys
        # are posix paths, so every key starting with Prefix lives under the
        # directory named by Prefix's last '/' (S3 prefix semantics preserved
        # by the startswith filter below)
        start = (self.root / Prefix.rpartition("/")[0]).resolve()
        if not start.is_relative_to(self.root):  # a prefix must stay inside the store
            start = self.root
        contents = []
        for path in sorted(start.rglob("*")) if start.is_dir() else []:
            if not path.is_file():
                continue
            key = path.relative_to(self.root).as_posix()
            # lock file and in-flight atomic-write temps are not objects
            if key == LOCK_FILE or path.name.startswith(".schrein-") or not key.startswith(Prefix):
                continue
            try:
                stat = path.stat()
            except FileNotFoundError:  # deleted between rglob and stat
                continue
            last = dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.UTC)
            contents.append({"Key": key, "LastModified": last, "Size": stat.st_size})
        yield {"Contents": contents} if contents else {}


class LocalClient:
    """Filesystem-backed stand-in for a boto3 S3 client (see module docstring)."""

    def __init__(self, root):
        self.root = pathlib.Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key):
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root):  # a key must stay inside the store
            raise ValueError(f"key escapes the store root: {key}")
        return path

    @contextmanager
    def _lock(self):
        fd = os.open(self.root / LOCK_FILE, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_EX)
            else:  # pragma: no cover — Windows
                msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
            else:  # pragma: no cover
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            os.close(fd)

    def _atomic_write(self, path, write_fn):
        """Same-directory temp + rename; `write_fn(tmp_path)` produces the bytes."""
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".schrein-put-")
        os.close(fd)
        try:
            result = write_fn(tmp)
            os.replace(tmp, path)
        except BaseException:
            pathlib.Path(tmp).unlink(missing_ok=True)
            raise
        return result

    def _put_bytes(self, path, body):
        """Atomic write of bytes or a file-like; returns the content ETag."""

        def write(tmp):
            h = hashlib.md5()
            with open(tmp, "wb") as f:
                if hasattr(body, "read"):
                    while chunk := body.read(1 << 20):
                        h.update(chunk)
                        f.write(chunk)
                else:
                    h.update(body)
                    f.write(body)
            return _quote(h)

        return self._atomic_write(path, write)

    # --- object operations (boto3 signatures, keyword casing included) ---

    def get_object(self, Bucket=None, Key=None):  # noqa: N803
        path = self._path(Key)
        try:
            blob = path.read_bytes()
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            raise _err("NoSuchKey", "GetObject", Key) from None
        etag = _quote(hashlib.md5(blob))
        return {"Body": io.BytesIO(blob), "ETag": etag, "ContentLength": len(blob), "LastModified": _mtime(path)}

    def put_object(self, Bucket=None, Key=None, Body=b"", IfMatch=None, IfNoneMatch=None):  # noqa: N803
        path = self._path(Key)
        if IfMatch is None and IfNoneMatch is None:
            return {"ETag": self._put_bytes(path, Body)}
        with self._lock():  # compare and swap must be one critical section
            if IfNoneMatch is not None and path.exists():
                raise _err("PreconditionFailed", "PutObject", Key)
            if IfMatch is not None:
                if not path.exists():
                    raise _err("NoSuchKey", "PutObject", Key)
                if _etag(path) != IfMatch:
                    raise _err("PreconditionFailed", "PutObject", Key)
            return {"ETag": self._put_bytes(path, Body)}

    def upload_file(self, Filename, Bucket=None, Key=None):  # noqa: N803
        # no md5 pass: nothing consumes an upload's ETag, and this is the
        # hottest write path (every step/branch part in ionflow's local mode)
        self._atomic_write(self._path(Key), lambda tmp: shutil.copyfile(Filename, tmp))

    def head_object(self, Bucket=None, Key=None):  # noqa: N803
        # only the fields callers read (LastModified + the server date for
        # object_age); notably no ETag — that would mean hashing the whole file
        try:
            stat = self._path(Key).stat()
        except (FileNotFoundError, NotADirectoryError):
            raise _err("404", "HeadObject", Key) from None
        return {
            "LastModified": dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.UTC),
            "ContentLength": stat.st_size,
            "ResponseMetadata": {"HTTPHeaders": {"date": format_datetime(dt.datetime.now(dt.UTC), usegmt=True)}},
        }

    def head_bucket(self, Bucket=None):  # noqa: N803
        if not self.root.is_dir():
            raise _err("404", "HeadBucket", str(self.root))
        return {}

    def copy_object(self, Bucket=None, CopySource=None, Key=None):  # noqa: N803
        src = self._path(CopySource["Key"])
        if not src.is_file():
            raise _err("NoSuchKey", "CopyObject", CopySource["Key"])
        self.upload_file(str(src), Bucket, Key)

    def delete_object(self, Bucket=None, Key=None):  # noqa: N803
        self._path(Key).unlink(missing_ok=True)  # S3 delete is idempotent
        return {}

    def delete_objects(self, Bucket=None, Delete=None):  # noqa: N803
        for obj in Delete["Objects"]:
            self.delete_object(Key=obj["Key"])
        return {}

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise NotImplementedError(name)
        return _Paginator(self.root)
