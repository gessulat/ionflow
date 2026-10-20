# 0024 — Artifact lineage commits with run progress

Status: accepted (2026-10-20); extends 0019 and 0021.

## Context

An artifact can be produced by several executions and consumed across flows.
A global creator column loses provenance. Separate run and artifact catalogs
cannot participate in one atomic transaction.

## Decision

The global artifact catalog owns inventory, bundle membership, metadata and
explicitly asserted import relationships. Flow catalogs own run input bindings
and activity records for artifact use, generation, reuse and ancestry. An
activity is a run, step and optional item key, with named roles.

Register complete artifacts before committing run references. The orchestrator
commits activity records with the run's manifest and completion state. Workers
return those records even when a later sub-step fails. Run recovery exports
include the same records. No transaction spans catalogs.

Artifact inputs are declared alongside Params. Artifact outputs use step return
annotations. Tracked runtime use/publication handles artifacts selected inside
table-producing steps. Artifact ancestry propagates through table steps and
retains branch scope. Identical outputs retain every producing activity.
Sibling outputs do not become parents merely because they were published first.
An explicit use or parent declaration records a dependency between them.

Resume inherits the origin's input IDs and metadata snapshots. Changed inputs
require a fresh run. Reused outputs retain original generation provenance.
Cross-flow queries derive consumer records from flow catalogs initially.

## Consequences

A crash may leave an unreferenced complete artifact, but never a completed run
output pointing at incomplete content. Declared use is dependency tracking, not
filesystem read tracing. There is no global consumer index to keep consistent.
