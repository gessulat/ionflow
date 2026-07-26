# Spec: a zero-infrastructure lakehouse catalog on S3 conditional writes

**Status:** draft for extraction into a fresh repository
**Origin:** design discussion in the [ionflow](https://github.com/gessulat/ionflow) repo, 2026-07-25
**Landscape checked:** 2026-07-26 (§2.1 — the niche is validated and unoccupied)
**Working name:** `schrein` (German: shrine/reliquary — a sealed casket built to preserve its contents unchanged)

This document is self-contained. It specifies a small Python library that
makes a **multi-writer DuckLake catalog** (and later an **Apache Iceberg
catalog**) work with **no infrastructure other than one S3-compatible
bucket** — no Postgres, no catalog server, no lock service. An agent should
be able to build the library from this document alone, in a blank
repository, without access to ionflow.

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
requirement kills the whole idea. The same shape of problem exists for
Iceberg: the format itself is files-on-object-storage, but a *catalog
service* traditionally holds the mutable pointer.

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

### 1.1 Scope guard: schrein is not ionflow's ledger

Because both designs are "one mutable S3 object, CAS-swapped," it is easy
to conflate them. They share a *mechanism*, not a *role*:

- **ionflow's ledger** is run bookkeeping: an append-only record of flow
  runs (`run_id`, status, timestamps, commit, completed steps, params).
  It has no schema authority — in ionflow, table schemas live in `Table`
  class annotations and drift checking lives in `discovery.py`. The
  ledger never performs or tracks schema changes.
- **schrein's register** holds a lakehouse *catalog*: table schemas,
  snapshots, and file lists for a datalake. Schema evolution is a
  first-class catalog operation here (§5.6) — the opposite of the
  ledger's role.

What §7 extracts from `ledger.py` is only the proven CAS commit loop
(`upsert_run` → `CasRegister`), i.e. the coordination primitive. Nothing
of the ledger's run-tracking semantics carries over.

ionflow flows do **not** run on schrein, and the ledger stays in ionflow
unchanged after schrein exists. The only planned integration is M4:
`ionflow publish --to-ducklake / --to-iceberg` — ionflow *publishes
into* a schrein-managed lake; it does not use schrein as its run
substrate. (Reimplementing the ledger on `CasRegister` would be a
mechanical refactor ionflow could adopt later; it would not merge the
two roles.) An *optional* mode where ionflow registers completed step
outputs into a schrein lake is explored separately in the ionflow repo
(`docs/specs/ionflow-on-schrein.md`); even there the roles stay
distinct — the ledger remains a separate object and keeps sole
authority over run bookkeeping; the catalog only ever receives table
schemas and data-file registrations.

## 2. Prior art (read these before building)

| Project | What it proves | Link |
|---|---|---|
| **Frozen DuckLake** (DuckDB team, Oct 2025) | The *read* side is official: a DuckLake catalog written as a single DuckDB-format file, copied to object storage, attached read-only by any vanilla DuckDB client. Our library is "frozen, plus a safe multi-writer commit protocol." | [blog post](https://ducklake.select/2025/10/24/frozen-ducklake/), [guide](https://ducklake.select/docs/stable/duckdb/guides/public_ducklake_on_object_storage), [PyPI helper](https://pypi.org/project/frozen-ducklake/) |
| **ionflow `ledger.py`** | The exact CAS loop (read → modify → `If-Match` PUT → retry on 412) running in production against S3/moto/MinIO. Code reproduced in §7. | `src/ionflow/ledger.py` in the ionflow repo |
| **Delta Lake S3 commit protocol** | Optimistic commits over object storage at scale. Historically needed DynamoDB for put-if-absent; delta-rs now uses native S3 conditional writes. This is the log-structured "second gear" of our design (§9). | [Delta storage docs](https://docs.delta.io/latest/delta-storage.html), [delta-rs](https://delta-io.github.io/delta-rs/) |
| **Boring Catalog** | An Iceberg catalog as a *single JSON file on S3* using conditional writes, implemented as a PyIceberg custom catalog. Direct prior art for our Iceberg adapter (§8) — but **dormant**: 12 commits by a single author, 2025-04→2025-05, nothing since (§2.1). A design reference to read, not a dependency to adopt. | [GitHub](https://github.com/boringdata/boring-catalog), [design writeup](https://juhache.substack.com/p/boring-iceberg-catalog) |
| **Quack** (DuckDB team, May 2026) | The server-shaped alternative: a protocol/extension making a DuckDB-file catalog safely shared via a server process. Proves demand; still requires running a process, which is what we avoid. | [overview](https://www.definite.app/blog/duckdb-quack-ducklake-catalog) |
| **Litestream / Cloud-Backed SQLite** | Why *live* SQLite-on-blob-storage is the wrong framing: both end up single-writer or lock-based. Rare writes let us skip all of it. | [litestream.io](https://litestream.io), [sqlite.org/cloudsqlite](https://sqlite.org/cloudsqlite/doc/trunk/www/index.wiki) |
| **AWS S3 Tables** | Managed Iceberg with built-in catalog — the "buy" option on native AWS; irrelevant for MinIO/R2/self-hosted. | [first look](https://meltware.com/2024/12/04/s3-tables.html) |

DuckLake background reading: [manifesto](https://ducklake.select/manifesto/),
[FAQ](https://ducklake.select/faq),
[unsupported features](https://ducklake.select/docs/stable/duckdb/unsupported_features)
(NOT NULL is the only constraint; no PK/CHECK/UNIQUE — enforce constraints
in your own tooling),
[MotherDuck architecture deep dive](https://motherduck.com/blog/ducklake-architecture-deep-dive/).

### 2.1 Landscape check (2026-07-26): validated, unoccupied

A deliberate web sweep (survey articles, GitHub, the Iceberg dev list,
cloud-provider announcements) found **no active, maintained OSS project**
offering "Iceberg catalog as a file on plain S3 via conditional writes."
Distilled findings, and why the niche is empty despite the idea being
everywhere:

- **Boring Catalog is the only direct occupant, and it is dormant.** Full
  history: 12 commits, one author (Julien Hurault / Boring Data),
  2025-04-23 → 2025-05-21. Its outsized visibility (every 2025/2026
  catalog-survey article cites it) comes from a widely shared writeup, not
  from sustained development. It is a well-publicized proof of concept
  that validated the design, then stopped.
- **The Apache Iceberg community itself validates the design.** It
  deprecated `HadoopCatalog` (the old file-based catalog) over exactly the
  atomicity problem, and a dev-list thread —
  ["Storing catalog directly on object store"](https://www.mail-archive.com/dev@iceberg.apache.org/msg08495.html)
  — proposes a `StorageCatalog`/`ObjectStorageCatalog` built on `If-Match`
  now that all major object stores support CAS. As of mid-2026 it is
  discussion only, no implementation. This is both the strongest
  validation and a potential upstreaming path for this library.
- **The managed clouds absorbed the demand — this is why no OSS project
  took off.** AWS **S3 Tables** (actively developed; Iceberg v3, built-in
  maintenance), Cloudflare **R2 Data Catalog** (managed catalog built into
  the bucket), and Google **BigLake metastore** all bundle an Iceberg
  catalog with storage and expose the **standard Iceberg REST catalog
  interface**. Anyone on a major cloud who wants zero-infrastructure
  Iceberg uses these (§8.1 — we can front them with thin config adapters).
- **Interop standardized on the REST protocol, so serious OSS went
  server-shaped.** Lakekeeper, Polaris, Nessie, Gravitino all ship REST
  *servers*, because REST works with every engine (Spark, Trino, DuckDB,
  Snowflake) while a file-based catalog is a per-language *client library*
  each engine would have to reimplement. On the DuckLake side the same
  pattern: **Quack** (DuckDB team, 2026-05, beta) makes a DuckDB-file
  catalog multi-writer — via a server process.
- **Closest active adjacent work:**
  [JanKaul/iceberg-rust](https://github.com/JanKaul/iceberg-rust)
  (unofficial Rust Iceberg impl, active, ships a "Filesystem" catalog
  among REST/Glue/S3Tables — CAS semantics undocumented, Rust-only);
  [icepick](https://github.com/smithclay/icepick) (experimental Rust/WASM
  *client for* the managed serverless catalogs, not a catalog).

**Consequence for this spec:** the unserved segment is exactly the target
workload — self-hosted / any-S3-compatible storage (MinIO, moto, on-prem)
with library-based writers. The CAS register is only needed there; on the
major clouds, the managed REST catalogs already solve the problem and
deserve nothing more than thin adapters (§8.1). And since Boring Catalog
is unmaintained, the §8 plan builds on `CasRegister` rather than adopting
it.

### 2.2 Why DuckLake exists at all (and why that argument leads here)

Background worth internalizing before building either adapter — it
explains why the two adapters have such different shapes, and why this
library is a natural extension of DuckLake's own argument rather than a
workaround.

**The catalog paradox.** Iceberg and Delta were designed to need *no*
database: all metadata encoded into immutable files on blob storage, at
the cost of many compromises (two-layer Avro manifests batched solely
because blob stores punish small files; a hard latency floor of many
sequential HTTP round trips before the smallest query touches data; a
new metadata-file spray per commit — the "small changes" problem).
Atomic commits then forced both formats to bolt on a catalog — a
database — anyway, but one that holds only a pointer, while the real
metadata still lives in the file labyrinth built to avoid it. The
DuckDB team hit this first-hand implementing `duckdb-iceberg`, and the
[DuckLake manifesto](https://ducklake.select/manifesto/) is the
conclusion: the designs "never revisited the rest of their design
constraints" after conceding the database. If you must run one, let it
hold *everything* — schemas, snapshots, file lists, stats — and keep
only data as Parquet on S3. Reads drop from ~7 round trips to ~2 (one
SQL query does all pruning and returns the file list); commits become
SQL transactions with free cross-table ACID; tiny inserts can be
[inlined into the catalog](https://ducklake.select/2026/04/02/data-inlining-in-ducklake/)
instead of spraying small files. The stated justification: the 2010s
constraints died — S3 is strongly consistent, running a database is
trivial, and the Hadoop-era database-avoidance reflex is a hangover
(Mühleisen and Tigani's framing: "Iceberg is the new Hadoop").

**The counterattack, and this spec's move.** The standard objection to
DuckLake is that the catalog database reintroduces exactly the
always-on infrastructure the lakehouse was supposed to eliminate —
hence "multi-user → run PostgreSQL" in the DuckLake docs, and hence
Quack (§2, §2.1: their server-shaped beta answer). This library is the
missing move in that debate: keep DuckLake's "all metadata in SQL" win
and delete the infrastructure by making the database itself an
immutable artifact CAS-swapped on S3 (§5). It also explains the adapter
asymmetry: DuckLake's catalog is *designed* to be one swappable
database file — the register holds it directly — while Iceberg's
metadata is deliberately smeared across immutable files, so its
register can only hold the pointer map (§8).

**It's playing out as a debate, not a war.** DuckLake 0.3 shipped
[bidirectional Iceberg metadata/data copy](https://ducklake.select/2025/09/17/ducklake-03/),
and Iceberg's v4 discussions are absorbing the critique (moving
metadata responsibility catalog-side, single-file commits). Supporting
both adapters is therefore not hedging between rivals; it is covering
two converging designs with one commit protocol.

### 2.3 Why Iceberg never did this, what "no database" really means, and which criticisms each adapter answers

Three questions that come up when reading §2.2 next to this spec, with
honest answers — they sharpen what the library does and does not claim.

**Why didn't Iceberg build on S3 CAS from the start? Timing, not
oversight.** CAS on a single mutable pointer *is* Iceberg's commit model
— every commit is "swap the current-metadata pointer, fail if someone
beat you." But when Iceberg was designed (Netflix, 2017–2018), S3 had
nowhere to put that CAS: no atomic rename (HDFS's had made
`HadoopCatalog` work — on HDFS only), no strong consistency until
Dec 2020, and no conditional PUT until Aug 2024 (`If-None-Match`) /
Nov 2024 (`If-Match`). For Iceberg's entire formative decade the
linearizable register had to live elsewhere — Hive metastore locks,
DynamoDB (Delta's S3 answer for years), SQL databases, then the REST
catalog spec. The catalog-service ecosystem is scar tissue from S3
lacking a conditional write. This spec's premise is only viable since
late 2024 — which is exactly when the Iceberg dev list started
discussing a `StorageCatalog` (§2.1) and Boring Catalog appeared. Being
early, not uniquely clever, is the reassuring position: the primitive
is new, the idea is convergent.

**Does this design actually work "without a database"? Precision:
without a database *server*.** The catalog artifact is a real database
(a DuckDB/SQLite file) — what is eliminated is the always-on process:
no connections, no credentials store, no ops, nothing that can be down.
A catalog server's two jobs get split: *storing* metadata stays in the
database (as an immutable artifact), *coordinating* writers is
delegated to S3's CAS. The honest price (§3, §9): every commit
transfers the whole catalog and commits serialize — seconds of latency,
comfortable at commits-per-minute, versus milliseconds and high
concurrency for a Postgres catalog. Readers fetch the whole catalog
instead of sending targeted queries to a server — fine while catalogs
are MBs. "Zero infrastructure within a stated low-write-rate regime" is
the defensible claim.

**Does it answer the DuckLake manifesto's criticism of Iceberg? Only
the DuckLake adapter does.** The two adapters answer different
questions:

- The **DuckLake adapter** inherits the manifesto's answers by
  construction — it *is* DuckLake: one GET of the catalog, pruning in
  local SQL (arguably better than DuckLake-with-Postgres, where pruning
  is a network query), commits as SQL transactions, no Avro layers,
  cross-table ACID free. What this spec adds is the reply to the
  criticism *of DuckLake* — "you reintroduced the server" (§2.2).
- The **Iceberg adapter** does not and cannot: the register replaces
  the catalog *service*, but the metadata labyrinth (metadata.json,
  Avro manifests, the round-trip floor, per-commit small-file spray) is
  the format itself. Its honest pitch is narrower: zero-infrastructure
  *coordination*, plus atomic multi-table commits (§8) that most real
  Iceberg catalogs don't offer. You build it for ecosystem
  compatibility, not because it answers the manifesto.

Prioritization follows: the DuckLake adapter is the main event; the
Iceberg adapter is a compatibility bridge (consistent with the §12
milestone order).

## 3. Non-goals

- **Not a high-throughput catalog.** Commits serialize on one key. Fine up
  to roughly commits-per-minute; wrong tool at commits-per-second (§9 has
  the upgrade path).
- **Not a security boundary.** Nothing stops a client with bucket write
  access from an unconditional PUT. Discipline is enforced by tooling, same
  trust model as any file-based catalog.
- **No constraint enforcement beyond the formats' own** (DuckLake:
  NOT NULL only; Iceberg: none enforced).
- **No Backblaze B2 support** — B2 lacks `If-Match` conditional PUT. (AWS
  S3, Cloudflare R2, MinIO, Tigris, and moto all support it.) Detect and
  refuse loudly at init.
- **No live/page-level remote SQLite access.** Whole-file artifact swap
  only.

## 4. Architecture

Three components; the adapters differ only in *what the register holds*.

```
┌───────────────────────────────────────────────────────────┐
│ schrein.register  —  CAS core (~150 lines, no deps > boto3)│
│  A linearizable register on one S3 key:                    │
│    read() -> (bytes|None, etag|None)                       │
│    write(bytes, expect_etag) -> etag     # If-Match CAS    │
│    create(bytes) -> etag                 # If-None-Match * │
│    transact(fn)   # read→fn→write loop, backoff+jitter     │
└─────────────┬─────────────────────────┬───────────────────┘
              │                         │
┌─────────────▼─────────────┐ ┌─────────▼───────────────────┐
│ schrein.ducklake (phase 1)│ │ schrein.iceberg  (phase 2)  │
│ register holds the        │ │ register holds a JSON       │
│ .ducklake catalog file    │ │ pointer map:                │
│ (DuckDB-format);          │ │   {table -> metadata.json}  │
│ transactions = replayable │ │ implemented as a PyIceberg  │
│ closures over local ATTACH│ │ custom Catalog; rebase via  │
│                           │ │ PyIceberg's own retry logic │
└───────────────────────────┘ └─────────────────────────────┘
```

Bucket layout (DuckLake adapter):

```
s3://bucket/<prefix>/
  catalog.ducklake          <- THE register. Only mutable object. CAS-swapped.
  data/                     <- immutable Parquet written by DuckLake / by clients
    <table>/...parquet
```

Readers need **no library at all** — a plain DuckDB client attaches the
catalog file read-only (Frozen-DuckLake style). Writers use the library.

## 5. The commit protocol (DuckLake adapter)

```
1. GET  catalog.ducklake            -> (blob, etag)     [absent -> genesis path]
2. Write blob to a temp dir; open a fresh local DuckDB connection:
     INSTALL ducklake; LOAD ducklake;
     CREATE SECRET (...)            -- S3 creds/endpooint for the data path
     ATTACH 'ducklake:<tmp>/catalog.ducklake' AS lake (DATA_PATH 's3://bucket/prefix/data/')
3. Run the user's transaction fn(con)  -- MUST be a replayable closure (§5.3)
4. DETACH; close connection; verify the catalog is one clean file (no WAL sidecar)
5. PUT catalog.ducklake with If-Match: <etag>   (genesis: If-None-Match: *)
     200 -> committed, globally visible. Done.
     412/409 -> someone else committed first:
        discard temp file, jittered exponential backoff, goto 1
        (fn is REPLAYED against the fresh catalog = rebase)
```

### 5.1 Why this is correct

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
  orphan Parquet, removed later by GC (§5.5). A whole-object GET is atomic,
  so a reader sees the old catalog or the new one, never a hybrid.
- **Rebase, not merge.** No CRDTs or byte-level merging: catalog operations
  are logical and mostly commutative ("add a snapshot referencing these
  files"). Concurrent writers touching different tables replay cleanly;
  writers mutating the same rows conflict, and last-committer-rebases is
  the right semantics there anyway.

### 5.2 Failure matrix

| Failure | Outcome |
|---|---|
| Crash before step 5 | Nothing visible. Possibly orphan Parquet in `data/` → GC. |
| Crash after step 5 | Fully committed. |
| CAS 412 (lost race) | Rebase: re-download, replay `fn`, retry. Bounded attempts, then raise `CommitConflict`. |
| CAS 409 `ConditionalRequestConflict` (concurrent conditional PUTs racing on the key) | Retryable, same path as 412. |
| Zombie writer resumes with stale etag | Its `If-Match` fails → harmless rebase. |
| Reader GET during a swap | Atomic: sees exactly one version. |
| Reader doing HTTP *range* reads across a swap (direct `ATTACH 's3://…'`) | Could mix versions mid-attach → the library's read path **downloads then attaches** (single atomic GET). Direct remote ATTACH is fine for convenience but momentarily racy during a swap; with bucket versioning, pin a VersionId. |
| Rogue unconditional PUT | Undetectable data loss. Out of scope (§3); mitigate with bucket versioning (every clobbered catalog version remains recoverable). |

### 5.3 The transaction contract (`fn`)

`commit(fn)` may call `fn(con)` **multiple times** (once per CAS attempt),
each time against a freshly downloaded catalog. Therefore `fn` must be a
pure function of `(con, external inputs)`:

- **Preferred pattern — register, don't write.** Upload Parquet to
  `data/` yourself (or in a prior step), then `fn` only registers it:
  `CALL ducklake_add_data_files('lake', 'tbl', 's3://…/x.parquet')` —
  metadata-only, reads just the Parquet footer, trivially replayable.
  (This is exactly the Frozen-DuckLake ingestion pattern.)
- Plain `INSERT INTO lake.tbl SELECT …` also works, but on a rebase
  DuckLake will write the data files *again* (previous attempt's files
  become GC-able orphans). Acceptable; wasteful for big writes.
- `fn` must not keep state across calls, must not assume snapshot ids, and
  must not touch the register out-of-band.

### 5.4 Catalog file hygiene

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

### 5.5 Garbage collection

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

### 5.6 Schema migrations

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

## 6. Public API (target)

```python
from schrein import CasRegister, DuckLake

lake = DuckLake(
    bucket="mybucket", prefix="lake",     # -> s3://mybucket/lake/catalog.ducklake
    endpoint_url=None,                    # MinIO/R2/moto endpoints supported
    region="us-east-1",
    grace_ttl_days=7,
)

# writer: replayable closure; retried on CAS conflict
lake.commit(lambda con: con.execute(
    "CALL ducklake_add_data_files('lake', 'events', 's3://mybucket/staging/e1.parquet')"
))

# reader: download-then-attach (atomic); context manager cleans the temp copy
with lake.read() as con:
    con.sql("SELECT count(*) FROM lake.events").show()

lake.expire_snapshots(older_than_days=30)   # CAS commit
lake.gc(dry_run=True)                        # list/delete unreferenced+aged files
```

Vanilla reader, no library (Frozen-DuckLake style; racy only during an
active swap — see §5.2):

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:s3://mybucket/lake/catalog.ducklake' AS lake (READ_ONLY);
```

## 7. What to extract from ionflow (with code)

ionflow (this spec's origin repo) already runs the CAS pattern in
production. Three things are worth carrying over — reproduced here so a
fresh repo needs no access to ionflow.

### 7.1 The CAS loop — generalize `ledger.py::upsert_run` into `CasRegister`

ionflow's ledger is an append-only Parquet file swapped by CAS
(`src/ionflow/ledger.py`). The essence, plus the helpers it uses from
`storage.py`:

```python
# ionflow's proven kernel (condensed):
MISSING_KEY_CODES = {"NoSuchKey", "404"}      # ClientError codes: object absent
_PRECONDITION    = {"PreconditionFailed", "412"}

def error_code(err):                           # AWS error code from ClientError
    return err.response["Error"]["Code"]

# write with CAS: IfMatch when updating, IfNoneMatch="*" when creating
cas = {"IfMatch" if etag else "IfNoneMatch": etag or "*"}
client.put_object(Bucket=bucket, Key=key, Body=body, **cas)

# the loop: read -> modify -> conditional write -> on 412, re-read and retry
for _ in range(retries):
    rows, etag = _read(client, bucket, key)    # returns ([], None) if absent
    rows = modify(rows)
    try:
        _write(client, bucket, key, rows, etag)
        return
    except ClientError as e:
        if error_code(e) in _PRECONDITION:
            continue
        raise
raise RuntimeError("CAS failed after retries")
```

Generalized core for the new library (complete, runnable):

```python
# src/schrein/register.py
"""A linearizable CAS register on one S3 key, via conditional writes."""
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

    def read(self):
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
        Raises ClientError (code in RETRYABLE) when the precondition fails."""
        cas = {"IfMatch": expect_etag} if expect_etag else {"IfNoneMatch": "*"}
        r = self.client.put_object(Bucket=self.bucket, Key=self.key, Body=body, **cas)
        return r["ETag"]

    def transact(self, fn, max_attempts: int = 8, base_delay: float = 0.5, cap: float = 30.0):
        """fn(old_bytes | None) -> new_bytes; replayed until the CAS lands.
        fn MUST be replayable: it may run once per attempt, each against a
        fresh read. Returns the committed object's new etag."""
        for attempt in range(max_attempts):
            old, etag = self.read()
            try:
                return self.write(fn(old), etag)
            except ClientError as e:
                if _error_code(e) not in RETRYABLE:
                    raise
                time.sleep(random.uniform(0, min(cap, base_delay * 2**attempt)))
        raise CommitConflict(f"s3://{self.bucket}/{self.key}: lost CAS race {max_attempts} times")
```

The DuckLake adapter wraps `transact`: `fn(old_bytes)` materializes the
blob to a temp file (or creates a fresh catalog on genesis), attaches,
runs the user closure, detaches, asserts no `.wal`, and returns the new
file's bytes.

```python
# src/schrein/ducklake.py  (sketch; the register does the concurrency work)
import pathlib
import tempfile

import duckdb


class DuckLake:
    def __init__(self, register, data_path: str, secret_sql: str | None = None):
        self.register = register          # CasRegister on .../catalog.ducklake
        self.data_path = data_path        # 's3://bucket/prefix/data/'
        self.secret_sql = secret_sql      # CREATE SECRET ... for non-AWS endpoints

    def _attach(self, con, local: pathlib.Path, read_only: bool = False):
        con.execute("INSTALL ducklake; LOAD ducklake;")
        if self.secret_sql:
            con.execute(self.secret_sql)
        ro = ", READ_ONLY" if read_only else ""
        con.execute(
            f"ATTACH 'ducklake:{local}' AS lake (DATA_PATH '{self.data_path}'{ro})"
        )

    def commit(self, user_fn, **transact_kwargs):
        def fn(old: bytes | None) -> bytes:
            with tempfile.TemporaryDirectory() as tmp:
                local = pathlib.Path(tmp) / "catalog.ducklake"
                if old is not None:
                    local.write_bytes(old)          # rebase base; genesis: ATTACH creates it
                con = duckdb.connect()
                try:
                    self._attach(con, local)
                    user_fn(con)                    # replayable closure (§5.3)
                    con.execute("DETACH lake")
                finally:
                    con.close()
                wal = local.with_suffix(local.suffix + ".wal")
                if wal.exists():                    # must be one clean file (§5.4)
                    raise RuntimeError("catalog WAL not checkpointed on detach")
                return local.read_bytes()

        return self.register.transact(fn, **transact_kwargs)
```

### 7.2 The moto test harness — copy `tests/conftest.py` nearly verbatim

ionflow's suite fakes S3 with a **moto server subprocess** started and
warmed at conftest import, killed atexit; each test gets a unique bucket
and no teardown. Two hard-won rules encoded in it:

1. **Never run moto in-process.** DuckDB httpfs holds relations open while
   reading; an in-process werkzeug server starves and wedges tests for
   minutes. Subprocess only.
2. moto implements S3 conditional writes, so the CAS paths (412 on stale
   `If-Match`, `If-None-Match` create) are genuinely exercised — ionflow's
   ledger tests prove this daily.

```python
# tests/conftest.py  (for the new repo — adapted from ionflow)
import atexit
import socket
import subprocess
import sys
import threading
import time

import pytest

_moto = {}


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _warm():
    import boto3

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    atexit.register(proc.kill)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    _moto["endpoint"] = f"http://127.0.0.1:{port}"
    boto3.client(
        "s3", endpoint_url=_moto["endpoint"], region_name="us-east-1",
        aws_access_key_id="warm", aws_secret_access_key="warm",
    ).list_buckets()


_warmup = threading.Thread(target=_warm, daemon=True)
_warmup.start()


@pytest.fixture(scope="session")
def moto_endpoint():
    _warmup.join()
    return _moto["endpoint"]


@pytest.fixture
def s3(moto_endpoint, request):
    import boto3

    client = boto3.client(
        "s3", endpoint_url=moto_endpoint, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
    )
    bucket = f"b-{abs(hash(request.node.nodeid)) % 10**8}"   # unique per test, no cleanup
    client.create_bucket(Bucket=bucket)
    return client, bucket, moto_endpoint
```

DuckDB inside tests needs a secret pointing at moto (path-style, no SSL):

```python
SECRET_SQL = """
CREATE OR REPLACE SECRET moto (
    TYPE s3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1',
    ENDPOINT '{host_port}', USE_SSL false, URL_STYLE 'path'
);
"""  # host_port = moto endpoint without the scheme, e.g. 127.0.0.1:5000
```

### 7.3 Design lessons to inherit (ionflow conventions that transfer)

- **Backblaze B2 is unsupported** (no `If-Match`). Probe at init: attempt a
  conditional PUT on a scratch key; refuse with a clear error if the
  backend ignores or rejects preconditions. (A backend that *ignores*
  `If-Match` silently is worse than one that errors — probe for correct
  412 behavior, not just for success.)
- **DuckDB extension install race:** `INSTALL ducklake` downloads once per
  DuckDB version. If the library ever spawns concurrent workers, install
  from a single connection before fanning out (ionflow's runner does
  this for httpfs).
- **Timestamps naive UTC** if you store any in Parquet metadata you expect
  DuckDB to read without pytz.
- Bucket versioning is cheap insurance: every catalog version a rogue or
  buggy writer clobbers remains recoverable.

## 8. Phase 2 — the Iceberg adapter

Iceberg is structurally an even better fit: *all* metadata (snapshots,
manifests, file lists) already lives in immutable files; a catalog holds
only a tiny mutable pointer per table ("current `metadata.json` is X").
The register therefore holds a small JSON pointer map, not a database file
— and rebase-on-conflict is *native* to Iceberg's spec (optimistic commit
retries are how every Iceberg catalog works; they just assume a server
holds the pointer).

Plan:

1. **Read [Boring Catalog](https://github.com/boringdata/boring-catalog)
   first — an evening, not an evaluation.** It proves the shape ("single
   `catalog.json` on S3 + conditional writes + PyIceberg custom catalog"),
   but it is dormant (§2.1), so the earlier "adopt it behind the library's
   interface" option is off the table: an unmaintained single-author POC
   must not hold the correctness-critical commit path. Build on
   `CasRegister`; port its PyIceberg-integration details where they check
   out, and run the §10 hammer against our implementation from day one.
2. Building it: subclass `pyiceberg.catalog.Catalog`
   ([PyIceberg docs](https://py.iceberg.apache.org/configuration/)); state
   is `{namespace.table: metadata_location}` in the register; the
   CAS-relevant method is `_commit_table` — PyIceberg writes the new
   immutable `metadata.json`, then:

```python
def _commit_table(self, request):            # sketch: the pointer swap
    def fn(old_bytes):
        state = json.loads(old_bytes) if old_bytes else {"tables": {}}
        current = state["tables"].get(ident)
        # delegate conflict detection/rebase to PyIceberg's update machinery
        new_metadata_location = write_new_metadata(request, base=current)
        state["tables"][ident] = new_metadata_location
        return json.dumps(state).encode()
    self.register.transact(fn)
```

3. A bonus most real Iceberg catalogs don't offer: all pointers in one
   register → **atomic multi-table commits** via a single CAS.

Ecosystem notes (as of mid-2026):

- DuckDB *reads* Iceberg without any catalog
  (`iceberg_scan('s3://…/metadata.json')`) once the pointer is resolved.
- DuckDB *writes* Iceberg only through a REST catalog
  ([writes announcement](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb),
  [v1.5.3 features](https://duckdb.org/2026/05/29/new-iceberg-features),
  [REST catalog docs](https://duckdb.org/docs/lts/core_extensions/iceberg/iceberg_rest_catalogs)).
  DuckDB-native writes through our catalog would need a small **in-process
  localhost REST shim** wrapping the PyIceberg catalog — still zero shared
  infrastructure. Otherwise, write via PyIceberg directly.
- Iceberg metadata sprawls (every commit = new `metadata.json` +
  manifests); snapshot expiry and orphan cleanup are routine maintenance —
  PyIceberg has the tooling; wire it to the §5.5 grace-TTL rule.

### 8.1 Thin adapters to the managed catalogs (S3 Tables, R2, BigLake)

All three managed offerings speak the **standard Iceberg REST catalog
protocol**, so an "adapter" here is *not* a catalog implementation — it is
a ~30-line config preset over PyIceberg's built-in `RestCatalog` plus
credential wiring. No CAS, no register: the service serializes commits
server-side. What the presets buy is one uniform entry point where the
backend is just a URI:

```python
from schrein import open_catalog

open_catalog("cas+s3://bucket/prefix")            # self-hosted: this spec's CAS catalog
open_catalog("s3tables://<table-bucket-arn>")     # AWS S3 Tables
open_catalog("r2://<account_id>/<bucket>")        # Cloudflare R2 Data Catalog
open_catalog("biglake://<project>/<region>/<cat>")# Google BigLake metastore
```

Per-preset sketch (endpoint spellings drift — **verify against current
provider docs at build time**, same caveat as open question 3):

- **AWS S3 Tables** — `RestCatalog` with SigV4 request signing:
  `uri=https://s3tables.<region>.amazonaws.com/iceberg`,
  `rest.sigv4-enabled=true`, `rest.signing-name=s3tables`,
  `warehouse=<table-bucket ARN>`; credentials from the AWS default chain
  (matching ionflow's "never store credentials" convention). Maintenance
  (compaction, snapshot expiry) is built into the service — the §5.5 GC
  surface no-ops here.
- **Cloudflare R2 Data Catalog** — `RestCatalog` with bearer token:
  `uri=https://catalog.cloudflarestorage.com/<account_id>/<bucket>`,
  `warehouse=<account_id>_<bucket>`, `token=<R2 API token>`. Zero egress
  fees make it the natural cross-cloud read target.
- **Google BigLake metastore** — `RestCatalog` with Google OAuth:
  `uri=https://biglake.googleapis.com/iceberg/v1/restcatalog`, bearer
  token from `google.auth` application-default credentials plus the
  `x-goog-user-project` header.

Notes:

- **DuckDB-native writes work on the managed backends today** — DuckDB
  writes Iceberg through REST catalogs (see the ecosystem notes above), and
  managed = REST. The localhost REST shim is only ever needed for the
  `cas+s3` backend.
- The presets cannot be tested against moto (it fakes none of these
  services); smoke tests are manual against real accounts. The §10 hammer
  applies only to the `cas+s3` backend.
- **Positioning consequence:** with these presets the library's pitch
  becomes "one zero-ops Iceberg catalog API — managed REST where your
  cloud provides one, CAS-on-conditional-writes where it doesn't (MinIO,
  on-prem, moto, any plain S3-compatible bucket)." The CAS core stays the
  novel part; the presets make the same user code portable across all four
  backends by swapping a URI.

## 9. Scaling limits and the upgrade path

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

## 10. Test plan

All against the moto subprocess harness (§7.2). The CAS core tests need
only boto3; adapter tests need duckdb.

1. **Register basics:** genesis via `If-None-Match` (second genesis
   fails); read-your-write; stale `If-Match` → 412 surfaces as retry.
2. **Lost-update hammer (the crown jewel):** N threads × M commits, each
   appending one unique row via `transact` (threads suffice — CAS is
   server-side; separate boto3 clients per thread). Assert the final state
   holds exactly N×M rows, none lost, no duplicates. Repeat at the
   DuckLake layer with `commit()` + `ducklake_add_data_files`.
3. **Zombie writer:** read etag, let another writer commit, then attempt
   the stale write → must rebase and land, and the intervening commit must
   survive.
4. **Crash mid-commit:** upload data Parquet, never commit → catalog
   unchanged; file is invisible; `gc(dry_run)` lists it only after the
   grace TTL.
5. **Replay discipline:** a `commit(fn)` forced into one 412 must call
   `fn` exactly twice, and the result must equal a clean single run
   (idempotence of the register-don't-write pattern).
6. **Reader atomicity:** reader downloads-then-attaches while a writer
   swaps; reader sees a consistent snapshot (old or new, never an error).
7. **WAL hygiene:** a transaction that leaves the WAL uncheckpointed must
   fail the commit loudly, uploading nothing.
8. **Conditional-write probe:** against a mock that ignores `If-Match`,
   init must refuse (simulate by monkeypatching put_object).

CI note: run tests with `pytest -n auto --dist worksteal` (pytest-xdist);
the harness is parallel-safe by construction (one server, unique buckets).

## 11. Fresh-repo skeleton

```
schrein/
  pyproject.toml
  README.md                 <- distill §1, §4, §6 of this spec
  src/schrein/
    __init__.py             <- exports CasRegister, DuckLake, CommitConflict
    register.py             <- §7.1 (complete)
    ducklake.py             <- §7.1 sketch -> full adapter (genesis, read(), gc)
    iceberg.py              <- phase 2 (§8), behind an optional extra
  tests/
    conftest.py             <- §7.2
    test_register.py
    test_ducklake.py
  docs/
    spec.md                 <- this document
    decisions/              <- ADRs, numbered, one per non-obvious choice
```

```toml
# pyproject.toml
[project]
name = "schrein"
version = "0.0.1"
description = "Zero-infrastructure lakehouse catalogs on S3 conditional writes (DuckLake, Iceberg)"
requires-python = ">=3.12"
dependencies = [
  "boto3>=1.35",           # conditional-write (If-Match) support
  "duckdb>=1.4",           # ducklake extension requires modern duckdb
]

[project.optional-dependencies]
iceberg = ["pyiceberg>=0.9"]

[dependency-groups]
dev = ["pytest>=8", "pytest-xdist>=3", "moto[server,s3]>=5"]

[tool.pytest.ini_options]
addopts = "-n auto --dist worksteal"
testpaths = ["tests"]
```

Bootstrap: `uv init --lib schrein && uv add boto3 duckdb && uv add --group dev pytest pytest-xdist "moto[server,s3]"`.

## 12. Milestones

- **M0 — CAS core.** `register.py` + tests 1–3 (register-level). Exit:
  hammer test green at N=8, M=25.
- **M1 — DuckLake adapter.** `commit`/`read`/genesis + secret handling +
  WAL hygiene + tests 2, 5–7 at the DuckLake layer. Exit: two processes on
  one moto bucket interleave 50 commits, zero lost updates, vanilla-DuckDB
  reader sees the final state.
- **M2 — Maintenance.** `expire_snapshots`, `gc` with grace TTL, test 4;
  conditional-write probe at init (test 8); optional real-backend smoke
  (MinIO container; R2/AWS manual).
- **M3 — Iceberg.** Read Boring Catalog as prior art (§8.1 note: do not
  adopt — dormant); implement `iceberg.py` on `CasRegister` under the §10
  hammer; optional localhost REST shim for DuckDB writers.
- **M3b — Managed presets (§8.1).** `open_catalog()` URI dispatch +
  S3 Tables / R2 / BigLake `RestCatalog` presets, endpoint spellings
  verified against current provider docs; manual smoke tests (real
  accounts — moto fakes none of these).
- **M4 — (back in ionflow, out of scope here).** `publish --to-ducklake`
  / `--to-iceberg` targets using this library.

## 13. Open questions

1. **Catalog file format:** DuckDB-format (frozen-reader compatible) vs
   SQLite (stabler on-disk format across DuckDB versions). Default
   DuckDB-format; revisit if version-skew bites (a client on a newer
   DuckDB storage version could write a catalog older clients can't read —
   consider recording min-reader-version in object metadata).
2. **DuckLake extension version pinning:** catalog schema migrations
   across ducklake extension versions are themselves CAS commits; how to
   coordinate a fleet of writers on mixed versions? (Likely: record the
   ducklake spec version in the catalog, refuse older writers.)
3. **`ducklake_add_data_files` semantics** — verify current signature and
   constraints (types must match; check behavior with partial/hive paths)
   against [current docs](https://ducklake.select/docs/stable) at build
   time; the function is load-bearing for cheap replays.
4. **GCS/Azure:** both have equivalent primitives (GCS
   `x-goog-if-generation-match`, Azure ETag If-Match). The register is the
   only backend-specific code — worth an interface, not worth building
   until needed.
5. **Encryption:** DuckLake supports encrypted data files; decide whether
   the catalog object itself needs client-side encryption (it contains
   schema + paths, not data).

## 14. Complete source list

DuckLake:
[site](https://ducklake.select/) ·
[manifesto](https://ducklake.select/manifesto/) ·
[FAQ](https://ducklake.select/faq) ·
[choosing a catalog database](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database) ·
[schema evolution](https://ducklake.select/docs/stable/duckdb/usage/schema_evolution) ·
[unsupported features](https://ducklake.select/docs/stable/duckdb/unsupported_features) ·
[Frozen DuckLakes](https://ducklake.select/2025/10/24/frozen-ducklake/) ·
[public DuckLake on object storage](https://ducklake.select/docs/stable/duckdb/guides/public_ducklake_on_object_storage) ·
[frozen-ducklake on PyPI](https://pypi.org/project/frozen-ducklake/) ·
[MotherDuck architecture deep dive](https://motherduck.com/blog/ducklake-architecture-deep-dive/) ·
[DuckLake in production (catalog & storage)](https://thefulldatastack.substack.com/p/ducklake-in-production-catalog-storage) ·
[Quack as the DuckLake catalog](https://www.definite.app/blog/duckdb-quack-ducklake-catalog)

DuckLake-vs-Iceberg background (§2.2):
[DuckLake announcement](https://ducklake.select/2025/05/27/ducklake-01/) ·
[data inlining in DuckLake](https://ducklake.select/2026/04/02/data-inlining-in-ducklake/) ·
[DuckLake 0.3 — Iceberg interoperability](https://ducklake.select/2025/09/17/ducklake-03/) ·
[Iceberg vs DuckLake, summary of a conversation](https://selectstarfrom.substack.com/p/iceberg-vs-ducklake-summary-of-a) ·
[MotherDuck: a duck walks into a lake](https://motherduck.com/blog/ducklake-motherduck/) ·
[InfoQ on DuckLake 1.0](https://www.infoq.com/news/2026/05/ducklake-sql-catalog/) ·
[The Register on the small-changes problem](https://www.theregister.com/software/2026/04/16/duckdb-uses-rdbms-to-tackle-lakehouse-small-changes-issue/5222357) ·
[lakehouse table formats in 2026](https://amdatalakehouse.substack.com/p/lakehouse-table-formats-in-2026-iceberg) ·
[Data Engineering Podcast: Duck Lake](https://www.dataengineeringpodcast.com/episodepage/duck-lake-simplifying-the-lakehouse-ecosystem)

S3 primitives:
[conditional writes user guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
(If-None-Match GA Aug 2024; If-Match GA Nov 2024; strong consistency since Dec 2020)

Iceberg:
[Boring Catalog](https://github.com/boringdata/boring-catalog) ·
[boring iceberg catalog — design writeup](https://juhache.substack.com/p/boring-iceberg-catalog) ·
[PyIceberg configuration](https://py.iceberg.apache.org/configuration/) ·
[DuckDB-Iceberg writes](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb) ·
[DuckDB-Iceberg v1.5.3 features](https://duckdb.org/2026/05/29/new-iceberg-features) ·
[DuckDB Iceberg REST catalogs](https://duckdb.org/docs/lts/core_extensions/iceberg/iceberg_rest_catalogs) ·
[AWS S3 Tables first look](https://meltware.com/2024/12/04/s3-tables.html)

Catalog landscape (checked 2026-07-26, §2.1):
[State of Apache Iceberg Catalogs, June 2026](https://amdatalakehouse.substack.com/p/the-state-of-apache-iceberg-catalogs) ·
[Iceberg dev list: storing catalog directly on object store](https://www.mail-archive.com/dev@iceberg.apache.org/msg08495.html) ·
[conditional operations in object stores](https://cdouglas.github.io/posts/2026/01/conditional) ·
[R2 Data Catalog announcement](https://blog.cloudflare.com/r2-data-catalog-public-beta/) ·
[emerging Iceberg catalogs (e6data)](https://www.e6data.com/blog/iceberg-catalogs-2025-emerging-catalogs-modern-metadata-management) ·
[Lakekeeper](https://github.com/lakekeeper/lakekeeper) ·
[JanKaul/iceberg-rust](https://github.com/JanKaul/iceberg-rust) ·
[icepick](https://github.com/smithclay/icepick)

Commit-protocol prior art & contrast:
[Delta Lake storage configuration](https://docs.delta.io/latest/delta-storage.html) ·
[delta-rs](https://delta-io.github.io/delta-rs/) ·
[Litestream](https://litestream.io) ·
[Cloud-Backed SQLite](https://sqlite.org/cloudsqlite/doc/trunk/www/index.wiki)

ionflow (origin repo; code reproduced in §7 so this doc stands alone):
`src/ionflow/ledger.py` (CAS loop) ·
`src/ionflow/storage.py` (`error_code`, `MISSING_KEY_CODES`, conditional `put_parquet`) ·
`tests/conftest.py` (moto subprocess harness) ·
`docs/decisions/` (ADR practice worth copying)
