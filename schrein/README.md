# schrein

Zero-infrastructure lakehouse catalogs on S3 conditional writes.

*Schrein* (German: shrine/reliquary — a sealed casket built to preserve its
contents unchanged) makes a **multi-writer
[DuckLake](https://ducklake.select/) catalog** (and later an Apache Iceberg
catalog) work with **no infrastructure other than one S3-compatible bucket**
— no Postgres, no catalog server, no lock service.

Full spec: [docs/specs/cas-catalog-library.md](https://github.com/gessulat/ionflow/blob/main/docs/specs/cas-catalog-library.md)
(an absolute link so it survives packaging and extraction).
Status: M0 (CAS core) and M1 (DuckLake adapter) implemented.

## The idea

DuckLake keeps data as Parquet on object storage and metadata in a SQL
catalog database — but multi-writer officially means "run PostgreSQL". For
workloads where the catalog changes rarely (batch pipelines, published
datasets), you don't need a database *server* for coordination; you need one
linearizable compare-and-swap register, and S3 conditional writes
(`If-Match` / `If-None-Match`) provide exactly that:

> Don't make the catalog database *live* on S3. Treat the whole catalog as
> an immutable artifact and CAS-swap it atomically on commit: download →
> transact locally → conditional upload → on conflict, rebase and retry.

Every commit serializes on one S3 key. That is the entire coordination
protocol. No locks, no leases, no clocks — a stale or crashed writer blocks
nobody, ever.

```
s3://bucket/<prefix>/
  catalog.ducklake      <- THE register. Only mutable object. CAS-swapped.
  data/                 <- immutable Parquet written by DuckLake / by clients
```

## Usage

```python
import boto3
from schrein import DuckLake

lake = DuckLake.at(boto3.client("s3"), "mybucket", "lake")

# writer: replayable closure; retried (rebased) on CAS conflict
lake.commit(lambda con: con.execute(
    "CALL ducklake_add_data_files('lake', 'events', 's3://mybucket/staging/e1.parquet')"
))

# reader: download-then-attach (atomic); context manager cleans the temp copy
with lake.read() as con:
    con.sql("SELECT count(*) FROM lake.events").show()
```

Readers need no library at all — a vanilla DuckDB client attaches the
catalog read-only (Frozen-DuckLake style):

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:s3://mybucket/lake/catalog.ducklake' AS lake (READ_ONLY);
```

The transaction closure passed to `commit` may run once per CAS attempt,
each time against a freshly downloaded catalog — it must be a pure function
of `(con, external inputs)`. Prefer the **register, don't write** pattern:
upload Parquet yourself, then let the closure register it with
`ducklake_add_data_files` (metadata-only, trivially replayable).

Not a high-throughput catalog: commits serialize on one key — comfortable
at commits-per-minute. Backblaze B2 is unsupported (no `If-Match`).

## Development

Part of the ionflow repo as a uv workspace member (see
`docs/decisions/0001`):

```bash
cd schrein
uv run pytest          # fast set (parallel, moto subprocess)
uv run pytest -m ""    # full suite incl. the slow CAS-contention hammer
```
