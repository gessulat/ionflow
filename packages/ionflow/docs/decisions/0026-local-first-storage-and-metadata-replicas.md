# 0026 — Local execution and metadata-only replicas

Status: accepted (2026-10-20); supersedes the exclusive-store choice in 0016;
amends 0019 and 0025.

## Context

Users need a complete inventory without downloading its payloads. Snage already
combines local and remote listings, but this policy belongs in ionflow.

## Decision

Every workspace has a persistent local store and one optional remote. Local
execution writes locally. The remote can be S3 or a directory for offline use
and testing. Both stores retain the runs and artifacts namespaces from 0022.

Catalogs accept metadata-only run and artifact records. A manifest describes
known outputs, not necessarily available files. Only available files enter
DuckLake data tables. Metadata refresh never reads step outputs, metrics or
artifact blobs. It merges logical records, never database files.

Run revisions have one execution origin. Replicas cannot advance that origin's
progress. Artifact edits retain revision ancestry; divergent edits are conflicts,
not a timestamp-based election. Run input snapshots remain immutable.

A small storage coordinator refreshes inventories and plans explicit transfers.
Refresh is incremental, offline-safe and eventually consistent. Failed refreshes
do not fail runs. Each connected store owns its availability evidence. The local
projection always describes the current workstation, not another workstation.

## Consequences

Ledger and artifact views include local and S3 availability. Metadata can exist
where its payload does not. Recovery retains this distinction. There is no legacy
configuration migration. No permanent daemon or additional database is required.
