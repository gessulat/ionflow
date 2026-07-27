# 0015 — A fan-out branch is a sub-DAG, executed per item instead of per stage

Status: accepted (2026-07-27)

Builds on 0014 (branch identity travels down a chain).

## Context

One level of fan-out where a branch is a single method is too coarse for items
whose processing is itself a small DAG with a fork in it. The motivating shape
is a peptide search: per raw file, convert a 10 GB `.raw` to a `.db`, then
concurrently upload the `.db` and run a CPU-bound search over it, with a second
upload of the `.raw` alongside both. The uploads exist to overlap network with
compute and are independently retryable — a flaky upload must not force a re-run
of a 40-minute search. Collapsing that into one step body means hand-rolling
threads inside a step and losing per-stage retry and resume granularity.

The DAG model already allowed the shape: `Branch[T]` names the upstream *step*,
so branch steps chain. Execution did not. Steps ran in one global topological
order, so every branch of step N finished before step N+1 began. Applied to the
chain, that converts every raw file before searching any — N `.db` files on disk
at once instead of `max_workers` — and a failure anywhere costs the whole stage.

## Decision

Every branch step belongs to the group of the fan-out at the root of its chain
(`Dag.groups`), and the group executes **per item, not per stage**: the worker
for item i runs that item's whole sub-DAG, resolving intra-item dependencies
from the same method signatures and running independent members concurrently in
threads. Item A can be in `search` while item B is still in `convert`.

- `Dag.plan` is the step order with each group contracted into its fan-out node,
  so members never execute on their own and the contraction is what validates
  the model: a cycle in the contracted graph is a deadlock at run time and is
  refused at class definition.
- Completion is tracked per (item, sub-step). `branch_retries` re-runs the
  failed sub-step alone, and `resume` restarts an item at its first incomplete
  sub-step. Sub-steps that finished for every item are registered in the catalog
  even when a later one fails the run, so a dead run's converts stay queryable.
- Members of one item are same-host, same-process **by construction** (one
  worker owns one item), so they may hand each other large local artifacts by
  path — the converted `.db` never round-trips through S3.
- Three shapes are rejected at class definition rather than silently
  reinterpreted: a fan-out inside a branch (nesting stays one level), a per-item
  step reading a sibling per-item step whole (`Collect[...]` or plain — it can
  only ever see its own item), and a fan-in of a group feeding that same group's
  per-item steps (the contracted cycle).
- Side-effect sub-steps keep returning a table: `-> None` remains a
  class-definition error, and an upload returns one row of provenance (uri,
  etag) instead of nothing. An invisible side effect becomes queryable history.

## Consequences

- Disk and memory scale with `max_workers`, not with the work-list: at most
  `max_workers` items hold intermediate artifacts at once.
- Retry and resume are as granular as the DAG is: the unit is a sub-step of an
  item, so a flaky upload costs one upload.
- A corpus-wide barrier *inside* a fan-out is no longer expressible. Keeping both
  models (per-item where possible, per-stage where not) was rejected: a silent
  fallback to stage execution would make disk use and failure blast radius depend
  on a subtlety of the signature. The shape stays reachable by spelling the
  barrier out as a second fan-out, which is the honest cost rather than a hidden
  one — the two groups are two workers, so an item's local artifacts do not
  survive the boundary and the second group re-reads from S3:

  ```python
  def by_job(self) -> Fanout[Job]: ...
  def convert(self, by_job: Branch[Job]) -> Converted: ...
  def stats(self, convert: Collect[Converted]) -> Stats: ...            # the barrier
  def by_db(self, convert: Collect[Converted]) -> Fanout[Converted]: ...  # fan out again
  def search(self, by_db: Branch[Converted], stats: Stats) -> Psm: ...
  # plan: by_job -> [convert] -> stats -> by_db -> [search]
  ```
- Reused sub-steps come back from S3, not from the local disk: an item resumed
  at a late sub-step gets its upstream's *table* (a path column), not the file
  that path names. Steps handing over local artifacts must tolerate re-deriving
  them, or key the fan-out so the whole item is matched forward instead.
- Metrics: one object per (item, fan-out), and members that run concurrently in
  one process share one sample label (`upload_db+search`) — a process's RSS and
  CPU cannot be split between two threads. In thread mode the whole group
  answers to one label, since one process interleaves every item.
