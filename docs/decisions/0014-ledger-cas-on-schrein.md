# 0014 — the ledger's CAS loop delegates to schrein's CasRegister

`ledger.py` carried its own read → modify → conditional-PUT → retry-on-412
loop, plus the helpers it needed (`_get_or_none`, precondition codes, a
`put_parquet` that forwarded `IfMatch`). schrein's `CasRegister`
(`schrein/src/schrein/register.py`) is that exact kernel, generalized and
hammer-tested (its suite proves N×M concurrent commits lose nothing). The
schrein spec anticipated this as "a mechanical refactor ionflow could adopt
later" — adopted now:

- `upsert_run` builds the new ledger bytes in a replayable closure and
  hands it to `CasRegister.transact`; `read_all`/`read_meta` use
  `CasRegister.read` (absent → `(None, None)`).
- ionflow gains a dependency on schrein (uv workspace source today, an
  ordinary package dependency once schrein is extracted). The dependency
  points one way only: schrein must never import ionflow.
- **Roles do not merge** (schrein spec §1.1): the ledger remains sole
  authority over run bookkeeping and never touches a lakehouse catalog;
  schrein contributes only the coordination primitive.

Behavior deltas, all deliberate: conflict retries now back off with jitter
(base 50ms — the old loop retried immediately, fine for moto, hostile to a
real S3 under contention); exhausting retries raises schrein's
`CommitConflict` (a `RuntimeError` subclass, so existing handlers — there
are none — would still match); `storage.put_parquet` lost its `**extra`
passthrough, whose only consumer was the ledger's conditional write.

`reset_on_conflict` (prototype schema churn) survives unchanged
inside the closure: if the merged rows no longer fit the current schema, it
returns a fresh single-row ledger instead — still CAS-guarded, so a lost
race replays against the winner's state.
