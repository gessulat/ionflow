# 0005 — A local-filesystem client, not a local-catalog special case

## Status

Accepted.

## Context

ionflow wants a zero-bucket prototyping mode: run flows, keep a DuckLake
catalog, and a CAS ledger — all on a laptop, no S3. schrein sits under both
of those (CasRegister, DuckLake), so the question is where "local" enters
the stack.

Options considered:

1. Teach `CasRegister`/`DuckLake` a second storage dialect (paths next to
   client/bucket/key everywhere).
2. Run a local S3 server (moto/minio) behind the scenes.
3. Implement the *client* — a boto3-shaped object over a directory, with
   conditional writes genuinely enforced.

## Decision

Option 3: `schrein.LocalClient(root)`. Every schrein (and ionflow) storage
call already goes through one injected client object, so a filesystem client
is the single point where "local" can be introduced without touching any
protocol code. `CasRegister`, `probe()`, and `DuckLake.commit` run unchanged.

Semantics: the `Bucket` argument is ignored (one client = one root
directory), ETags are content MD5 quoted like S3's, writes are atomic
(same-directory temp + `os.replace`), and conditional PUTs serialize on one
advisory `flock` at the store root. That makes CAS linearizable across
processes **on one host** — which is the whole ambition; multi-host
coordination is what real S3 conditional writes are for.

Option 1 would smear a second dialect through every layer. Option 2 drags a
server subprocess into production code paths and contradicts
"zero-infrastructure" (and moto's conditional PUTs race — decision 0003).

## Consequences

- `CasRegister.probe()` passes against `LocalClient`, so callers need no
  local-mode branches around the safety check.
- The lock file (`.schrein.lock`) and in-flight temp files are filtered out
  of listings; keys never see them.
- ETag-by-content means `get_object` and conditional `put_object` hash the
  object; register objects are small, so this is fine. The hot paths skip
  hashing entirely: `upload_file`/`copy_object` are plain atomic file copies
  (nothing consumes their ETags), and `head_object` answers from one `stat`
  and returns no ETag at all.
- Windows falls back to `msvcrt` locking (untested in CI; POSIX is the
  supported platform).
