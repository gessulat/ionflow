"""A boto3-shaped S3 client over a local directory — the zero-bucket backend.

Implements exactly the client surface schrein (and its users) touch:
get/put/head/copy/delete/list plus `upload_file`, with conditional PUTs
(If-Match / If-None-Match) genuinely enforced, so `CasRegister` and
`DuckLake` run unchanged against a plain directory. Failures raise botocore
`ClientError` with the S3 error codes callers already handle.

Semantics: the `Bucket` argument is accepted and ignored — one client, one
root directory; keys are paths relative to it. ETags are content MD5 (quoted,
like S3). Writes are atomic (`os.replace` of a same-directory temp file), and
conditional writes serialize on one advisory file lock at the root, which
makes CAS linearizable across processes ON ONE HOST — the point of this
backend is prototyping without infrastructure, not multi-host coordination.
"""

import datetime as dt
import hashlib
import os
import pathlib
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


def _etag(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return f'"{h.hexdigest()}"'


def _mtime(path):
    return dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC)


class _Paginator:
    def __init__(self, root):
        self.root = root

    def paginate(self, Bucket=None, Prefix=""):  # noqa: N803 — boto3 casing
        contents = []
        for path in sorted(p for p in self.root.rglob("*") if p.is_file()):
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

    def _atomic_write(self, path, body):
        """Write body to path via a same-directory temp + rename; return its ETag."""
        path.parent.mkdir(parents=True, exist_ok=True)
        h = hashlib.md5()
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".schrein-put-")
        try:
            with os.fdopen(fd, "wb") as f:
                if hasattr(body, "read"):
                    while chunk := body.read(1 << 20):
                        h.update(chunk)
                        f.write(chunk)
                else:
                    h.update(body)
                    f.write(body)
            os.replace(tmp, path)
        except BaseException:
            pathlib.Path(tmp).unlink(missing_ok=True)
            raise
        return f'"{h.hexdigest()}"'

    # --- object operations (boto3 signatures, keyword casing included) ---

    def get_object(self, Bucket=None, Key=None):  # noqa: N803
        path = self._path(Key)
        try:
            blob = path.read_bytes()
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            raise _err("NoSuchKey", "GetObject", Key) from None
        etag = f'"{hashlib.md5(blob).hexdigest()}"'
        return {"Body": _Body(blob), "ETag": etag, "ContentLength": len(blob), "LastModified": _mtime(path)}

    def put_object(self, Bucket=None, Key=None, Body=b"", IfMatch=None, IfNoneMatch=None):  # noqa: N803
        path = self._path(Key)
        if IfMatch is None and IfNoneMatch is None:
            return {"ETag": self._atomic_write(path, Body)}
        with self._lock():  # compare and swap must be one critical section
            if IfNoneMatch is not None and path.exists():
                raise _err("PreconditionFailed", "PutObject", Key)
            if IfMatch is not None:
                if not path.exists():
                    raise _err("NoSuchKey", "PutObject", Key)
                if _etag(path) != IfMatch:
                    raise _err("PreconditionFailed", "PutObject", Key)
            return {"ETag": self._atomic_write(path, Body)}

    def upload_file(self, Filename, Bucket=None, Key=None):  # noqa: N803
        with open(Filename, "rb") as f:
            self.put_object(Bucket=Bucket, Key=Key, Body=f)

    def head_object(self, Bucket=None, Key=None):  # noqa: N803
        path = self._path(Key)
        if not path.is_file():
            raise _err("404", "HeadObject", Key)
        return {
            "LastModified": _mtime(path),
            "ContentLength": path.stat().st_size,
            "ETag": _etag(path),
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
        with open(src, "rb") as f:
            self.put_object(Key=Key, Body=f)

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


class _Body:
    def __init__(self, blob):
        self._blob = blob

    def read(self):
        return self._blob
