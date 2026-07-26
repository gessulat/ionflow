# 0015 — catalog mode registers parts as-is; run_id comes from hive paths

Catalog mode (`catalog = "ducklake"` in config, `IONFLOW_CATALOG` override;
spec: `docs/specs/ionflow-on-schrein.md`) gives each stable flow a schrein
DuckLake at its root — one table per step, rows across runs keyed by
`run_id`. The I0 spike settled the spec's open question 1 cleanly:
`ducklake_add_data_files(..., hive_partitioning => true)` infers partition
values from the file's path, so a part at `runs/run_id=<id>/<step>/...`
registers with that run_id **without containing a run_id column**. Parts are
byte-identical to classic mode, and resume's server-side copies (0002) land
under the new run's path — they register under the new run_id for free.

Shape of the integration (all of it in `catalog.py`):

- **One CAS commit per completed step**, at the runner's per-step completion
  point — so a fan-out's parts register atomically and a partial fan-out is
  invisible to lake readers, mirroring the runner's all-or-nothing
  invariant. Registration failure fails the run: a silently unregistered
  step would break the mode's one guarantee, and `ionflow catalog-backfill`
  is the recovery path (also the adoption path for pre-existing runs).
- **Genesis is idempotent** (`CREATE TABLE IF NOT EXISTS` from each step's
  `Table`, skipped entirely when all tables exist): persisted types — the
  read-side rule — NOT NULL only (all DuckLake enforces; PK/CHECK stay
  ionflow-side per 0003). `run_id` is a reserved column; untyped steps are
  impossible here (prototype-only, and prototypes are always classic).
- **The catalog's own data_path is `_lake/`** at the flow root, used only
  for catalog-written files (none today; future migrations/backfills).
  Registered parts stay in their immutable run folders — the catalog never
  copies or rewrites data.
- **Reads:** `query.duck()` attaches a downloaded catalog snapshot
  read-only and views its tables; `ledger`/`metrics` keep their non-catalog
  plumbing. `discovery`'s scan and the drift check stay classic — parts
  still exist on S3, so those paths remain correct in catalog mode; moving
  them to one catalog GET is a deferred optimization, not a correctness
  gap. Vanilla readers need no ionflow at all:
  `ATTACH 'ducklake:s3://…/catalog.ducklake' (READ_ONLY)`.
- Run meta records the mode (`"catalog"`), per the spec's optionality
  section. The ledger is untouched — the catalog holds no run bookkeeping.

Deferred to I3: retention (`clean` wiring over schrein's
`expire_snapshots`/`gc`) and `ionflow migrate` (needs its own small spec —
which `Table` diffs map to which `ALTER`s). This decision widens ionflow's
schrein imports from `CasRegister` (0014) to `CasRegister + DuckLake`; the
dependency still points one way only.
