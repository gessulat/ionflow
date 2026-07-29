# 0018 — Item keys are the storage identity; positional branch indices are gone

Status: accepted (2026-07-29); builds on 0008 (keyed resume) and 0014 (branch
identity travels down a chain)

## Context

Parts used to be stored positionally (`part_<index>.parquet`), while resume
identity was the `fanout_key` value. Reconciling those two identity systems
was the runner's worst complexity: `fanout_keys` recorded index->key lists in
`_meta.json`, and resume remapped origin position *j* onto new position *i*
(`reuse_matching_branches`), with `Progress.keys` insisting on numeric branch
order so reducers stayed deterministic. Two identity systems, one of them
(position) meaningless for keyed work.

## Decision

A part is identified by `(step, item key)` everywhere — in memory
(`Slot(step, key)`, `Manifest`), on disk (`part_<key_slug>.parquet`), in
`_meta.json` (`manifest: {step: [[key, path], ...]}`), and in the worker
protocol (items are dispatched and reported by key). An unkeyed fan-out's
items are keyed by position ints — positions ARE their identity — and a keyed
fan-out's by its `fanout_key` values; a chained branch step answers to the
same key (0014). Consequences of making the pun literal:

- **Keyed resume is a lookup, not a remap.** Reuse copies origin part
  `(step, k)` to new part `(step, k)` — same name both sides. The
  `fanout_keys` meta field, the position-remap machinery and its "origin
  predates fanout keys" compatibility path are deleted.
- **Duplicate keys collide structurally.** Two items with one key would name
  one object; the write path refuses before uploading the second.
- **The deterministic order is sorted-by-key.** Registration (hence a
  reducer's scan order) is `Manifest.paths()` = sorted key order — stable
  across runs, resumes and reordered work-lists, which numeric emission
  order never was for keyed work.
- **Filenames stay legible, the manifest stays authoritative.** `key_slug`
  passes clean keys through verbatim and sanitizes+hashes the rest; the
  manifest's key->path mapping is what resume and the catalog read, so slug
  mangling can never corrupt identity.
- **Workers receive paths, not derivation rules.** The orchestrator resolves
  every input path from the live manifest (shared per-group inputs travel
  once per process via the pool initializer); a worker derives only the
  paths it writes itself. The `widths` protocol — both sides re-deriving key
  lists by one positional rule — is gone.
- Metrics `branch` labels become the item key (VARCHAR), one more place a
  position used to stand in for an identity.

## Consequences

- Pre-0018 stores (positional part names, `steps_complete`/`fanout_keys`
  meta) are not readable by this version. Consistent with the no-migration
  stance (0012): keep the old ionflow for old data, or re-run.
- Nested fan-out is no longer structurally unreachable (a composite key
  could name it); still refused at DAG level, deliberately.
