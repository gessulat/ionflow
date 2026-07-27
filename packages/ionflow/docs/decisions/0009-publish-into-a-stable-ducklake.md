# 0009 — publish blesses outputs into a stable DuckLake

Status: accepted (2026-07-26)

`ionflow publish <flow> <run_id> <step> <table>` copies one complete run's
step output into the **publish lake** — a schrein DuckLake independent of
every flow catalog (default `s3://<bucket>/<prefix>/published/`, or any
`s3://bucket/prefix` via `--to`, including another bucket) — and swaps it
in as `<table>` in one CAS commit.

Why a lake and not a bare parquet path:

- **Atomic replace.** DELETE + register + provenance append happen in one
  catalog swap: consumers see the previous version or the new one, never
  a mix — across every statement of the publish, which a plain object PUT
  could only promise per file.
- **History without --force.** Republishing replaces the current
  contents; previous versions stay reachable through DuckLake snapshots
  (time travel) until expiry, so the old refuse-unless-forced dance is
  gone.
- **Queryable provenance.** A `publications` table (table_name, flow,
  run_id, step, filter, commit, published_at) is appended in the same
  commit — the JSON sidecar's job, but SQL.
- **Still a copy, never a reference.** The data is rewritten into the
  lake's own data path (unique filenames, so snapshots keep immutable
  files). A published table needs no access to the pipeline bucket and
  survives run-folder retention.

Consumers attach read-only with stock DuckDB and see only blessed tables:

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:s3://bucket/prefix/published/catalog.ducklake' AS pub (READ_ONLY);
```

The publish lake is exactly as recoverable as any schrein lake (bucket
versioning; the data files are self-describing). Retention of old
published snapshots goes through schrein `expire_snapshots`/`gc` — wiring
that into a command is open, alongside run-folder retention.
