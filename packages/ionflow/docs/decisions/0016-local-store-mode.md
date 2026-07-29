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
folders byte-for-byte and rebuilds the whole target catalog — ledger rows,
manifests, registrations — with `lake.backfill` from the copied
`_meta.json` records (re-sync is idempotent: backfill rebuilds from all of
them) — the same recovery path 0012/0019 established, doing double duty. The local store
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

## Note (2026-07-28) — the seams moved into `Store`

The three seams named above are no longer module-level functions in
`storage.py`. `storage.uri()`, `storage.secret_sql()`, `storage.connect()`,
`storage.make_client()` and `storage.ducklake_at()` were replaced by
`store.Store` with two subclasses, `S3Store` and `DirStore` (`store.py`);
`Store.of(config)` is the single dispatch, and `uri()`, `connect()`,
`lake()` and `verify()` are the methods that differ between them.
`storage.py` retains only the documented `s3_client` escape hatch.

This strengthens the decision rather than changing it: the claim that
"nothing branches on the mode beyond one constructor" had decayed to nine
mode branches across five modules, and is now enforced by construction —
no store dispatch exists outside the two subclasses. The migration clause
below is unchanged, except that `sync.py` is now `Space.sync_to`.

## Note (2026-07-28) — the publish consequence was over-broad

The consequence "`publish --to s3://…` from a local store is refused (one
client per operation; cross-store publish would need two) — sync first" said
more than the code ever did, and gave a reason that no longer holds.

The **default** local publish was never refused: `publish` drives two stores
already (it drains the relation on the source connection and uploads through
the target store — that is what makes the cross-bucket S3 case work), and with
`Store`/`Space` a second lake in the same store is ordinary. A local flow
publishing into its own local publish lake is supported, and
`test_local.py::test_local_publish_and_vanilla_read` now asserts the target
store and data path stay inside `local_dir`, rather than leaving it incidental.

The refusal survives, but on a narrower and honest ground: a local-mode config
carries no endpoint, region or credentials for an `s3://` destination, and
`Store.of` dispatches on `local_dir` first — so a `--to` target derived from a
local config would silently resolve back to the local directory rather than to
the named bucket. Giving `publish` its own `--endpoint`/`--region` is what
would be needed, and that is exactly what `ionflow sync` already offers;
`sync` therefore remains the path.
