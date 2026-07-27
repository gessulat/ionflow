# 0016 — Local mode is a client swap, and migration is a rebuild, not a copy

## Status

Accepted.

## Context

Prototyping should not require provisioning an S3 bucket. We wanted a mode
where ionflow + schrein run entirely against the local filesystem — full
guarantees intact (typed steps, ledger, per-flow DuckLake catalog, resume,
publish; no scratch mode, per 0012) — with a later, explicit migration onto
a bucket once one exists.

## Decision

**One dispatch point, not a storage abstraction layer.** Every storage
operation already flows through three narrow seams: the injected boto3-shaped
client, `storage.uri()` (where DuckDB reads), and `storage.secret_sql()`
(only meaningful for httpfs). Local mode swaps those seams — schrein's
`LocalClient` (conditional writes enforced via one file lock; schrein 0005),
plain paths from `uri()`, and no httpfs/secret at all — behind
`Config.local_dir`, which is mutually exclusive with `bucket`. Nothing in
runner/ledger/catalog/publish/query branches on the mode beyond
`storage.ducklake_at()`, the one constructor that must know the data path
dialect.

**Migration = `ionflow sync`, per flow, and the catalog is re-derived.** A
DuckLake catalog records absolute data-file locations, so the local catalog
object is meaningless on S3. `sync` therefore copies the immutable run
folders byte-for-byte, MERGES ledger rows through the target's CAS ledger
(bucket-side runs survive; re-sync is idempotent because rows key by
`run_id`), and rebuilds the target catalog with `catalog.backfill` — the
same recovery path 0012 established, now doing double duty. The local store
is never mutated by sync; cutover is an explicit `ionflow setup` re-run.

Rejected alternatives: auto-running a local S3 server (infrastructure in
disguise, and moto's conditional PUTs race — schrein 0003); a config holding
both `bucket` and `local_dir` with implicit precedence (silent
wrong-store writes; the sync target is a CLI argument instead); rewriting
catalog paths during sync (fragile string surgery on a database file when a
rebuild is already a supported, tested path).

## Consequences

- Local mode is single-host: CAS linearizes via `flock`, so two machines on
  a network drive are NOT coordinated. The README says so.
- `publish --to s3://…` from a local store is refused (one client per
  operation; cross-store publish would need two) — sync first.
- The TUI's setup form still only configures S3; `ionflow setup --local` is
  the local entry point (the form points at it).
- `sync` does not carry the publish lake; re-publish from the synced runs if
  consumers need it on the bucket.
