# 0012 — The item-identity marker is named `fanout_key`, paired with `Fanout`

Status: accepted (2026-07-25). Renames the `branch_key` marker introduced
in [0008](0008-keyed-incremental-resume.md); the mechanics are unchanged.

## Context

The two halves of keyed resume did not sound related: users declare a
fan-out with `Fanout[...]`, but the column marking each fan-out item's
identity was called `branch_key` (declared as `field(branch_key=True)` —
the only constraint without a shortcut, despite anchoring a flagship
feature). "Branch" is runner vocabulary for one parallel execution;
nothing in the authoring surface calls the feature "branching", so the
name forced newcomers to connect two words the docs never connect.

## Decision

The marker is `fanout_key`, exported as a shortcut symmetric with
`primary_key`:

```python
class Doc(Table):
    path: str = fanout_key   # this Fanout item's identity
```

`field(fanout_key=True)` remains for combining constraints.
`Table.branch_key_column()` becomes `Table.fanout_key_column()`, and the
per-run record in `_meta.json` is `fanout_keys` (was `branch_keys`).

## Consequences

- Declaring a fan-out and declaring its item identity now share the word
  `fanout` — the pairing is visible without reading the docs.
- Pre-1.0 breaking rename: code using `field(branch_key=True)` must
  switch. Runs whose meta predates `fanout_keys` lose key matching once
  (resume warns and recomputes the keyed subgraph — the degradation path
  0008 already defined for key-less origins).
- `Branch[...]`/`Collect[...]` keep their names: "branch" still names one
  parallel execution of a fan-out item, which is a runner concept, not an
  authoring one.
