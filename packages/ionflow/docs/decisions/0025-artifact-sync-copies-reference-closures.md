# 0025 — Sync copies immutable reference closures before run records

Status: accepted (2026-10-20); extends 0016 and 0020.

## Context

An individual run can reference another run's table parts and global artifacts.
Copying only its folder creates a destination with missing dependencies.

## Decision

Sync transfers referenced artifact descriptors, blobs, metadata and asserted
parents before exposing receiving runs. Per-run transfer also follows concrete
table-part paths outside its own folder. IDs survive prefix changes. Catalogs
are derived at the destination; metadata conflicts are reported, never silently
resolved. Artifact-only sync supports preparing shared inputs before runs exist.

Artifact-only transfers carry filtered producer activity snapshots when the
originating flow history is absent. Merge these imported evidence records and
retain immutable exports. An authoritative flow activity overrides its imported
snapshot when that flow history becomes available. This is portable provenance,
not another authority for live run progress.

## Consequences

Sync and recovery share serialized records. Neither operation deletes payloads.
Recovery validates descriptors and missing members. Store-specific copying stays
behind the existing Store boundary.
