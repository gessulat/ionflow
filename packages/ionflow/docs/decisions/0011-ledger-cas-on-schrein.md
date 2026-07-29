# 0011 — the ledger's CAS loop delegates to schrein's CasRegister

Status: superseded by 0019 (2026-07-29) — ledger.parquet is gone; run bookkeeping lives in the catalog DuckLake

The ledger's one mutation — append a run row, conditionally, retrying on
412 — needs a read → modify → conditional-PUT → retry loop plus its
helpers (get-or-absent, precondition codes, an If-Match-forwarding
parquet PUT). schrein's `CasRegister` (`packages/schrein/src/schrein/register.py`)
is exactly that kernel, generalized and hammer-tested: its suite proves
N×M concurrent commits lose nothing. Carrying a private copy of the same
loop in `ledger.py` was rejected — a duplicate of a kernel the workspace
already maintains, and one that would never see contention testing of its
own. So:

- `upsert_run` builds the new ledger bytes in a replayable closure and
  hands it to `CasRegister.transact`; `read_all`/`read_meta` use
  `CasRegister.read` (absent → `(None, None)`).
- ionflow gains a dependency on schrein (uv workspace source today, an
  ordinary package dependency once schrein is extracted). The dependency
  points one way only: schrein must never import ionflow.
- **Roles do not merge** (schrein spec §1.1): the ledger remains sole
  authority over run bookkeeping and never touches a lakehouse catalog;
  schrein contributes only the coordination primitive.

Conflict behavior, deliberate: retries back off with jitter (base 50ms —
an immediate retry is fine against moto but hostile to a real S3 under
contention); exhausting retries raises schrein's `CommitConflict` (a
`RuntimeError` subclass). Because the ledger is the only conditional
writer, `storage.put_parquet` does plain uploads with no conditional-write
passthrough — every If-Match lives inside `CasRegister`.
