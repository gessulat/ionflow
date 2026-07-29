# 0010 — S3 writes go through boto3, never DuckDB `COPY TO s3://`

Status: accepted (2026-07-26)

## Context

DuckDB can write parquet to S3 directly (`COPY TO s3://` via httpfs), and
examined empirically against moto with DuckDB 1.5.4, basic `COPY TO s3://`
works fine — a blanket "the httpfs write path is unreliable" claim would be
false. What the experiment did find is worse and specific: when a
`COPY TO s3://` fails mid-stream, httpfs finalizes the multipart upload
anyway and leaves a corrupt object visible on S3. The cause is structural —
`S3FileHandle::Close()` unconditionally calls `Finalize()`; there is no
abort-on-error path in duckdb-httpfs (confirmed in `main`, ahead of 1.5.5).

## Decision

Every S3 object write goes through boto3. `write_part` stages a COPY to a
local temp file, runs the constraint scan on that file, and only then
uploads (`put_object` under 8MB, multipart `upload_file` above). Grounds:

- **Failure atomicity.** In ionflow, part-object existence *is* the
  completion record: fan-out completeness, resume trust, and immutable run
  folders all read it that way. An httpfs write that dies mid-stream would
  make a failed branch look complete; resume would copy the corrupt part
  into a new run. boto3's `put_object` and a completed multipart upload
  are atomic — S3 never shows a partial.
- **Validate before publish.** Step outputs arrive as one-shot streams
  (arrow readers, consumed relations), so the constraint scan needs a
  rescannable artifact; the local temp file is the only single-pass-safe
  one. Direct `COPY TO s3://` could only validate after the possibly-bad
  object already exists in an immutable folder.
- **One write path.** The catalog CAS (If-Match), publish, and deletes are
  boto3 regardless; httpfs writes would add a second path, not replace the
  first.

## Consequences

- Upload is not overlapped with encoding: wall time ≈ encode + upload,
  where httpfs approaches max(encode, upload) — measured 6.97s vs 4.16s on
  a 65MB incompressible part. Plus local disk equal to the part size
  (the spill `temp_directory` already assumes local disk). Recovering the
  overlap would mean streaming multipart during encode — reimplementing
  httpfs and reintroducing the partial-object failure mode. Rejected.
- boto3 does transport only; DuckDB still does the cast projection,
  parquet encode, compression, and validation scan. This is not a
  parallel parquet implementation.
- Expiry: `tests/test_storage.py::test_httpfs_write_unsafe_canary` (slow)
  asserts the disqualifying upstream behavior. When a DuckDB upgrade makes
  that test fail, native writes have become failure-atomic — revisit this
  record (supersede, don't edit) before deleting the canary.
