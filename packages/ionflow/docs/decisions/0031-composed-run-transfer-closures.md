# 0031 — Transfer composed executions under one approval

Status: accepted (2026-10-22).

## Context

A composed parent keeps orchestration tables and selected child outputs. Its
scientific results and telemetry can live in child catalogs. Transferring only
the parent leaves those results unavailable at the receiving workstation.

## Decision

Run selections follow persisted parent/child links, table reuse, and origin
records. The nearest parent attempt selects each call site's latest child.
Origin records retain that relationship through repeated resume and transfer.
Each selected flow keeps its own catalog and table schemas.

A transfer plan combines files and artifact choices across the execution graph.
Shared blobs and reused parts appear once. Directional artifact defaults apply
to the combined inventory; explicit choices override them. The approval token
pins every selected run version. Execution validates all source run revisions
before copying files, then registers child catalogs before the parent catalog.

Preview traverses only local metadata and never constructs remote clients.
Selecting a child directly retains a narrow scope. Artifact producer evidence
does not expand selection to unrelated producer runs.

## Consequences

One upload or download retains a composed run's results and telemetry. Each
child remains independently runnable, queryable, transferable, and evictable.
No global catalog or external scheduler is introduced. Metadata-only ancestors
with no table payloads can be retained without recovering optional telemetry.
