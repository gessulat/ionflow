# Spec: a zero-infrastructure lakehouse catalog on S3 conditional writes

**Status:** implemented — `packages/schrein/src/` and `packages/schrein/tests/`
are the code of record; this document is the durable design reference
**Origin:** design discussion in the [ionflow](https://github.com/gessulat/ionflow) repo, 2026-07-25
**Name:** `schrein` (German: shrine/reliquary — a sealed casket built to preserve its contents unchanged)

A small Python library that makes a **multi-writer DuckLake catalog** work
with **no infrastructure other than one S3-compatible bucket** — no
Postgres, no catalog server, no lock service. DuckLake is the only target
format (schrein decision 0004 — an Apache Iceberg adapter was considered
and scrapped).

The original draft's build-time sections — a prior-art landscape survey,
code listings extracted from ionflow, the Iceberg adapter design, test
plan, milestones, and source list — were removed once the library shipped;
git history keeps them. The 2026-07 landscape check found the niche
validated but unoccupied: the only direct occupant (Boring Catalog) is
dormant, the Iceberg dev list discusses the same design without an
implementation, and the DuckDB team's Frozen DuckLake makes the *read*
side official — this library is "frozen, plus a safe multi-writer commit
protocol."

---

## 1. Problem statement

[DuckLake](https://ducklake.select/) is a lakehouse format from the DuckDB
team: data as Parquet on object storage, metadata (snapshots, schemas, file
lists) in a SQL catalog database. It gives you ACID multi-table
transactions, snapshots/time travel, and schema evolution over plain
Parquet. The catch is the catalog database. Per the
[official guidance](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database):

- **DuckDB file** as catalog → single client only.
- **SQLite file** as catalog → multiple clients *on one machine*.
- **Multiple users / remote clients → run PostgreSQL.**

For teams whose entire infrastructure is "one S3 bucket," the Postgres
requirement kills the whole idea.

**Observation that unlocks the design:** for many workloads (batch
pipelines, published datasets, ML feature tables) the catalog changes
*rarely* — commits per day or per month, not per second — and commit
latency of seconds-to-minutes is acceptable. In that regime you do not need
a database server for coordination. You need one **linearizable
compare-and-swap (CAS) register**, and S3 itself now provides exactly that:

- S3 reads/writes have been **strongly consistent** since Dec 2020.
- S3 supports **conditional writes**: `If-None-Match: *` on PUT
  (create-only, Aug 2024) and `If-Match: <etag>` on PUT
  (compare-and-swap, Nov 2024). See
  [AWS docs: conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

The library's core idea:

> **Don't make the catalog database *live* on S3 (page-level VFS access,
> live locks — the dead end every "SQLite on S3" project hits). Treat the
> whole catalog as an immutable artifact and CAS-swap it atomically on
> commit.** Download → transact locally → conditional upload → on conflict,
> rebase and retry.

Every commit serializes on one S3 key. That is the entire coordination
protocol.

What is eliminated is the always-on *server*, not the database: the
catalog artifact is still a real database file. A catalog server's two
jobs get split — *storing* metadata stays in the database (as an
immutable artifact), *coordinating* writers is delegated to S3's CAS.
The honest price (§2, §6): every commit transfers the whole catalog and
commits serialize. "Zero infrastructure within a stated low-write-rate
regime" is the claim.

### 1.1 Scope guard: schrein stays generic

schrein's register holds a lakehouse *catalog* — table schemas, snapshots,
file lists — and knows nothing about flows or runs. Its one consumer today,
ionflow, keeps a schrein DuckLake per flow (plus a publish lake) and stores
its own run bookkeeping as ordinary tables *inside* that catalog (ionflow
decisions 0012/0019); the CAS commit loop itself was originally extracted
from ionflow and generalized as `register.py::CasRegister.transact`. None
of that leaks back: schrein never imports ionflow, and nothing here is
specific to any consumer's semantics.

## 2. Non-goals

- **Not a high-throughput catalog.** Commits serialize on one key. Fine up
  to roughly commits-per-minute; wrong tool at commits-per-second (§6 has
  the upgrade path).
- **Not a security boundary.** Nothing stops a client with bucket write
  access from an unconditional PUT. Discipline is enforced by tooling, same
  trust model as any file-based catalog.
- **No constraint enforcement beyond DuckLake's own** (NOT NULL only).
- **No Apache Iceberg support** (decision 0004): no adapter, no managed
  REST-catalog presets. One format, one adapter.
- **No Backblaze B2 support** — B2 lacks `If-Match` conditional PUT. (AWS
  S3, Cloudflare R2, MinIO, Tigris, and moto all support it.) Detect and
  refuse loudly at init.
- **No live/page-level remote SQLite access.** Whole-file artifact swap
  only.

## 3. Architecture

Two components: the register (coordination) and the DuckLake adapter
(what the register holds).

```
┌────────────────────────────────────────────────────────────┐
│ schrein.register  —  CAS core (~150 lines, no deps > boto3)│
│  A linearizable register on one S3 key:                    │
│    read() -> (bytes|None, etag|None)                       │
│    write(bytes, expect_etag) -> etag     # If-Match CAS    │
│    create(bytes) -> etag                 # If-None-Match * │
│    transact(fn)   # read→fn→write loop, backoff+jitter     │
└─────────────┬──────────────────────────────────────────────┘
              │
┌─────────────▼─────────────┐
│ schrein.ducklake          │
│ register holds the        │
│ .ducklake catalog file    │
│ (DuckDB-format);          │
│ transactions = replayable │
│ closures over local ATTACH│
└───────────────────────────┘
```

Bucket layout:

```
s3://bucket/<prefix>/
  catalog.ducklake          <- THE register. Only mutable object. CAS-swapped.
  data/                     <- immutable Parquet written by DuckLake / by clients
    <table>/...parquet
```

Readers need **no library at all** — a plain DuckDB client attaches the
catalog file read-only
([Frozen-DuckLake](https://ducklake.select/2025/10/24/frozen-ducklake/)
style). Writers use the library.

## 4. The commit protocol (DuckLake adapter)

```
1. GET  catalog.ducklake            -> (blob, etag)     [absent -> genesis path]
2. Write blob to a temp dir; open a fresh local DuckDB connection:
     INSTALL ducklake; LOAD ducklake;
     CREATE SECRET (...)            -- S3 creds/endpooint for the data path
     ATTACH 'ducklake:<tmp>/catalog.ducklake' AS lake (DATA_PATH 's3://bucket/prefix/data/')
3. Run the user's transaction fn(con)  -- MUST be a replayable closure (§4.3)
4. DETACH; close connection; verify the catalog is one clean file (no WAL sidecar)
5. PUT catalog.ducklake with If-Match: <etag>   (genesis: If-None-Match: *)
     200 -> committed, globally visible. Done.
     412/409 -> someone else committed first:
        discard temp file, jittered exponential backoff, goto 1
        (fn is REPLAYED against the fresh catalog = rebase)
```

### 4.1 Why this is correct

- **Serializability.** A strongly-consistent conditional PUT makes the
  catalog key a linearizable CAS register. Every commit is one CAS on that
  register, so commits are totally ordered → serializable catalog
  transactions, with S3 as the consensus layer.
- **No clocks, no leases, no fencing tokens.** A writer that stalls
  mid-commit (GC pause, laptop sleep, partition) and wakes hours later
  simply fails its `If-Match` and rebases. This is why the design
  deliberately has **no lock object**: a lock-file variant (create
  `catalog.lock` with TTL) would need heartbeats, clock trust, and fencing
  to handle zombies — the pure CAS variant needs none, and is simpler.
- **Lock-free progress.** A 412 implies another writer's commit succeeded;
  the system always advances. A crashed writer blocks nobody, ever.
- **No torn states.** Data files are uploaded *before* the catalog
  references them (write-data-first, commit-metadata-second — the
  Iceberg/Delta ordering). A crash between the two leaves only invisible
  orphan Parquet, removed later by GC (§4.5). A whole-object GET is atomic,
  so a reader sees the old catalog or the new one, never a hybrid.
- **Rebase, not merge.** No CRDTs or byte-level merging: catalog operations
  are logical and mostly commutative ("add a snapshot referencing these
  files"). Concurrent writers touching different tables replay cleanly;
  writers mutating the same rows conflict, and last-committer-rebases is
  the right semantics there anyway.

### 4.2 Failure matrix

| Failure | Outcome |
|---|---|
| Crash before step 5 | Nothing visible. Possibly orphan Parquet in `data/` → GC. |
| Crash after step 5 | Fully committed. |
| CAS 412 (lost race) | Rebase: re-download, replay `fn`, retry. Bounded attempts, then raise `CommitConflict`. |
| CAS 409 `ConditionalRequestConflict` (concurrent conditional PUTs racing on the key) | Retryable, same path as 412. |
| Zombie writer resumes with stale etag | Its `If-Match` fails → harmless rebase. |
| Reader GET during a swap | Atomic: sees exactly one version. |
| Reader doing HTTP *range* reads across a swap (direct `ATTACH 's3://…'`) | Could mix versions mid-attach → the library's read path **downloads then attaches** (single atomic GET). Direct remote ATTACH is fine for convenience but momentarily racy during a swap; with bucket versioning, pin a VersionId. |
| Rogue unconditional PUT | Undetectable data loss. Out of scope (§2); mitigate with bucket versioning (every clobbered catalog version remains recoverable). |

### 4.3 The transaction contract (`fn`)

`commit(fn)` may call `fn(con)` **multiple times** (once per CAS attempt),
each time against a freshly downloaded catalog. Therefore `fn` must be a
pure function of `(con, external inputs)`:

- **Preferred pattern — register, don't write.** Upload Parquet to
  `data/` yourself (or in a prior step), then `fn` only registers it:
  `CALL ducklake_add_data_files('lake', 'tbl', 's3://…/x.parquet')` —
  metadata-only, reads just the Parquet footer, trivially replayable.
  (This is exactly the Frozen-DuckLake ingestion pattern. The function
  accepts `hive_partitioning => true` and infers partition values, e.g.
  `run_id=`, from the file path; types must match the table's persisted
  types.)
- Plain `INSERT INTO lake.tbl SELECT …` also works, but on a rebase
  DuckLake will write the data files *again* (previous attempt's files
  become GC-able orphans). Acceptable; wasteful for big writes.
- `fn` must not keep state across calls, must not assume snapshot ids, and
  must not touch the register out-of-band.

### 4.4 Catalog file hygiene

- The uploaded artifact must be **one self-contained file**. After a clean
  `DETACH` + connection close, DuckDB checkpoints and removes its `.wal`
  sidecar — assert no `.wal` exists before upload, fail loudly if it does.
- Upload via a **single `put_object`**, never multipart/TransferManager
  (catalogs are KBs–MBs; single PUT keeps ETag semantics simple; S3 single
  PUT limit is 5 GB — far beyond any sane catalog).
- Treat ETags as **opaque strings**. Never parse or compare them as MD5.
- Catalog format choice: **DuckDB-format `.ducklake` file** (default;
  compatible with Frozen-DuckLake vanilla readers) vs **SQLite**
  (`ATTACH 'ducklake:sqlite:file.sqlite'`; more stable on-disk format
  across versions, but readers then need the sqlite ducklake flavor too).
  Ship DuckDB-format first; keep the choice pluggable.

### 4.5 Garbage collection

Two sources of dead files in `data/`: DuckLake snapshot expiry (DuckLake
never deletes data on DROP/DELETE; only
[expiring snapshots](https://ducklake.select/faq) frees files), and orphans
from failed/rebased commits. Rules:

- Expiry runs as an ordinary CAS commit (`fn` calls DuckLake's expiry
  functions).
- Physical deletion (of expired or orphaned files) only for objects that
  are **both** unreferenced by the current catalog **and** older than a
  grace TTL (default: 7 days), so an in-flight writer's just-uploaded files
  can never be reaped before its commit lands. This is standard Iceberg
  orphan-cleanup practice.

### 4.6 Schema migrations

Schema evolution is a first-class use case, not an extension: in DuckLake a
schema change is a **metadata-only catalog operation** — no data files are
rewritten — so under this design a migration is just another `commit(fn)`
whose closure runs `ALTER TABLE`. Per the
[schema evolution docs](https://ducklake.select/docs/stable/duckdb/usage/schema_evolution),
DuckLake supports adding columns (with `DEFAULT`, old files read the
default/NULL), dropping and renaming columns, **lossless type promotion**
(e.g. INT32→INT64; narrowing is rejected), and all of the above on nested
struct fields. Columns are tracked by field id, so a rename does not orphan
existing data, and time travel keeps working — old snapshots retain the
schema they were written with.

Properties the CAS register adds on top:

- **Migrations are serialized against all other commits** — a migration can
  never interleave with a half-written data commit.
- **Migration + backfill is one atomic commit**: `fn` can `ALTER TABLE` and
  populate the new column in the same transaction; readers see either the
  old schema or the fully migrated one. This extends across tables
  (DuckLake multi-table transactions), which most server-based setups
  cannot do atomically.
- **Concurrent conflicts fail loudly at commit time, not silently.** If a
  migration lands first, a concurrent writer's rebase replays its closure
  against the *migrated* catalog: a `ducklake_add_data_files` registering
  Parquet with the old schema, or a second conflicting `ALTER`, errors
  inside `fn` instead of corrupting anything.

Two rules for migration closures: they must stay replayable like any other
`fn` (an `ALTER` re-applied on rebase should either succeed or fail loudly
— never check-then-skip in a way that masks a conflicting concurrent
migration), and changes beyond lossless promotion (semantic type changes,
narrowing) must be expressed as add-new-column + backfill + drop-old —
which the atomic-commit property above makes safe to do in one step.

## 5. Public API

```python
import boto3
from schrein import DuckLake, LocalClient

client = boto3.client("s3")               # or LocalClient(dir) for a plain directory
lake = DuckLake.at(client, "mybucket", "lake")  # -> s3://mybucket/lake/catalog.ducklake

# writer: replayable closure; retried on CAS conflict
lake.commit(lambda con: con.execute(
    "CALL ducklake_add_data_files('lake', 'events', 's3://mybucket/staging/e1.parquet')"
))

# reader: download-then-attach (atomic); context manager cleans the temp copy
with lake.read() as con:
    con.sql("SELECT count(*) FROM lake.events").show()

lake.expire_snapshots(older_than_days=30)    # CAS commit
lake.gc(grace_days=7, dry_run=True)          # list/delete unreferenced+aged files
```

Vanilla reader, no library (Frozen-DuckLake style; racy only during an
active swap — see §4.2):

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:s3://mybucket/lake/catalog.ducklake' AS lake (READ_ONLY);
```

## 6. Scaling limits and the upgrade path

Whole-file CAS degrades on two axes, with a known fix that is an
evolution, not a rewrite:

- **Contention.** Commits serialize; each conflict costs a
  download+replay+upload. Comfortable at commits-per-minute; at sustained
  multi-writer commits-per-second, switch to a **Delta-style commit log**:
  numbered changeset objects claimed with `If-None-Match`
  (`log/00000042.json`) plus periodic compacted checkpoints of the full
  catalog. Same primitive, log-structured; Delta Lake proved it at scale.
- **Catalog size.** Whole-file transfer per commit. A DuckLake catalog with
  ~10⁴ snapshots is single-digit MBs — fine. At ~100 MB (≈ millions of
  snapshots: expire them!) move to the log design.

For the stated target workload (catalog changes monthly, minutes of
latency acceptable), whole-file CAS has orders of magnitude of headroom.

Open questions that survive the build: the catalog file format (DuckDB
vs SQLite, revisit if DuckDB storage-version skew bites), coordinating a
writer fleet across ducklake extension versions, GCS/Azure backends (both
have equivalent conditional-write primitives; the register is the only
backend-specific code — worth an interface when needed), and client-side
encryption of the catalog object.
