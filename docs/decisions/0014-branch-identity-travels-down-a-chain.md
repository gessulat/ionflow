# 0014 — A fan-out item's key names every branch step in the chain, not just the first

Status: accepted (2026-07-27)

Refines 0008 (keyed incremental resume).

## Context

A step may branch over another branch step's output, not only over a fan-out:
`Fanout[Doc] -> vectors: Branch[Doc] -> reranked: Branch[Vector] -> Collect`.
The DAG has always allowed this — `Branch[T]` names the upstream *step*, and a
branch step's output is a plain table — and the runner has always sized the
chained step from its branch dep's parts.

Keyed resume (0008) matched origin parts by item key only where the step's
branch dep *was* the keyed fan-out. A chained step fell into the positional
"reuse the whole step when the key set is unchanged" bucket, which is wrong for
anything indexed by branch: the fan-out re-runs on resume and may emit the same
items in a different order (a directory listing, an S3 page). Branch parts of
the first step then move with their keys while the chained step's parts stay
where they were — origin `refine/0` copied onto a branch now holding a different
item. With a partially complete origin the mismatch is visible in the output:
one item computed twice, another silently dropped. It also meant a chained step
recomputed every branch whenever the work-list changed at all.

## Decision

Branch identity is inherited: branch *i* of a step that branches over a branch
step is still the fan-out's *i*-th item. `runner._key_roots` walks the DAG in
topological order and maps every branch step to the keyed fan-out at the root of
its chain; each such step is matched by item key against the origin, and none of
them is ever positionally reused. Positional reuse stays for steps whose branch
position means nothing (collectors, plain steps) and for chains rooted at an
unkeyed fan-out, which have no names to match by.

The existing taint rule is unchanged and does the rest: a key-matched part is
only reused when every *other* keyed ancestor's work-list is also unchanged.

## Consequences

- Reordering a work-list is safe. Fan-out steps are free to list their items in
  whatever order the source hands them over, at any chain depth.
- Incrementality reaches the whole chain: adding one item to a corpus recomputes
  that item's branch in every branch step, not the entire fan-out width from the
  second step on.
- The rule rests on the invariant that a branch step's part *i* always holds the
  fan-out's *i*-th item — true because branches are computed at their own index
  and copies are placed at the destination index. Any future "compact the branch
  space" optimisation would have to record per-step keys instead of deriving
  them from the root fan-out.
- A step with two branch deps is matched through the first one (the same dep the
  runner sizes the fan-out from). Divergent ancestors are not silently trusted:
  they are covered by the taint check.
