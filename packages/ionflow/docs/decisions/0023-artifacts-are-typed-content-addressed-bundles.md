# 0023 — Artifacts are typed content-addressed files or bundles

Status: accepted (2026-10-20).

## Context

Runs share RAW files, FASTAs, spectral libraries and indexes. Libraries and
indexes contain several files and can share large prediction files. Build recipe
hashes do not identify completed content.

## Decision

Artifact schemas declare explicit stable type names and metadata fields. Each
kind has one payload kind and metadata schema; incompatible schemas need a new
kind. File identity is its complete SHA-256. Bundle identity hashes a canonical,
versioned manifest of relative member names, sizes and SHA-256 values.
IDs combine type and digest; blobs are shared across types and
bundles at `artifacts/_blobs/sha256/<digest>`.

Hash staged bytes, then upload those same bytes. A registry entry becomes visible
only after all members exist. Mutable names and metadata do not affect identity.
Metadata edits require a revision; runs retain snapshots. Duplicate publication
does not silently overwrite conflicting properties.

Recovery exports retain immutable numbered metadata revisions. A delayed export
cannot erase a newer edit's recovery record. File materializations use hard-link
aliases inside the verified cache, so pinned source names can differ across
stores without duplicating bytes or depending on original filesystem links.

Materialization verifies blob bytes and publishes a complete cache entry by
atomic rename. Cache entries contain no source-path symlinks. Builders write to
work directories, never to stored payloads. Resolve explicitly selected file
symlinks on import; reject escaping member names and directory symlinks.

## Consequences

Storage reuse does not imply computation reuse. Bundles share file bytes without
depending on the original host. Full-file hashing and staging are intentional
ingestion costs. Permanent deletion and automatic recipe caching are deferred.
