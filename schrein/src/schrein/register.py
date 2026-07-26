"""A linearizable CAS register on one S3 key, via conditional writes.

The whole coordination protocol: every commit is one compare-and-swap on a
single strongly-consistent S3 object, so commits are totally ordered. No
locks, no leases, no clocks — a stale writer just fails its If-Match and
rebases.
"""

import random
import time

from botocore.exceptions import ClientError

MISSING = {"NoSuchKey", "404"}
# 412 = etag mismatch; 409 ConditionalRequestConflict = concurrent conditional
# PUTs raced on the key (AWS returns this; both mean "re-read and retry").
RETRYABLE = {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}


class CommitConflict(RuntimeError):
    """CAS lost the race more than max_attempts times."""


def _error_code(e: ClientError) -> str:
    return e.response["Error"]["Code"]


class CasRegister:
    def __init__(self, client, bucket: str, key: str):
        self.client, self.bucket, self.key = client, bucket, key

    def read(self) -> tuple[bytes | None, str | None]:
        """-> (bytes | None, etag | None). Strongly consistent, atomic."""
        try:
            r = self.client.get_object(Bucket=self.bucket, Key=self.key)
        except ClientError as e:
            if _error_code(e) in MISSING:
                return None, None
            raise
        return r["Body"].read(), r["ETag"]

    def write(self, body: bytes, expect_etag: str | None) -> str:
        """Conditional PUT: If-Match when updating, If-None-Match:* on genesis.

        Raises ClientError (code in RETRYABLE) when the precondition fails.
        Single put_object, never multipart — keeps ETag semantics simple.
        """
        cas = {"IfMatch": expect_etag} if expect_etag else {"IfNoneMatch": "*"}
        r = self.client.put_object(Bucket=self.bucket, Key=self.key, Body=body, **cas)
        return r["ETag"]

    def transact(self, fn, max_attempts: int = 8, base_delay: float = 0.5, cap: float = 30.0) -> str:
        """fn(old_bytes | None) -> new_bytes; replayed until the CAS lands.

        fn MUST be replayable: it may run once per attempt, each against a
        fresh read. Returns the committed object's new etag.
        """
        for attempt in range(max_attempts):
            old, etag = self.read()
            try:
                return self.write(fn(old), etag)
            except ClientError as e:
                if _error_code(e) not in RETRYABLE:
                    raise
                if attempt + 1 < max_attempts:  # no backoff after the final attempt
                    time.sleep(random.uniform(0, min(cap, base_delay * 2**attempt)))
        raise CommitConflict(f"s3://{self.bucket}/{self.key}: lost CAS race {max_attempts} times")
