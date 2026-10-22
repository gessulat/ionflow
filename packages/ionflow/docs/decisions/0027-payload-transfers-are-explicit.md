# 0027 — Payload transfers require an explicit direction

Status: accepted (2026-10-20); extends 0025 and 0026.

## Decision

Browsing synchronizes metadata only. Local execution and artifact materialization
never download remote payloads implicitly. Missing inputs produce an actionable
error before execution. A transfer plan specifies source, destination, missing
files and bytes. Interfaces ask for confirmation before executing that plan.

Run transfers include reused table parts, required artifacts and their parents.
Artifact transfers include required parents, not arbitrary descendants. Callers
may explicitly select additional related artifacts, such as Snage indexes.

Callers may select artifact payloads within the reference closure with
`include_artifacts`. Unselected artifacts retain metadata and lineage, but
their payloads need not exist at the source. An empty selection transfers run
results only. Presence is recorded only for selected, verified payloads.

Transfers stage and verify files before registration. An interrupted transfer
does not claim complete availability. Retries reuse verified files. A run's
presence refers to its committed revision; newer progress invalidates an older
receiving snapshot. Execution status is independent of storage presence.

The local and S3 columns use two indicators: present and absent or incomplete.
Progress, errors and metadata freshness appear separately. An unreachable remote
retains last-known presence. No remote means disabled remote actions, not proof
that a remote payload was deleted. Presence indicators never delete content.

## Consequences

Query views expose local data only. Run-specific reads reject unavailable data.
The TUI and Snage use the same transfer API. No default preview reads remote
step tables. Automatic eviction, deletion propagation and multiple remotes remain
outside this decision.

2026-10-22: decision 0032 permits verified output downloads when a run explicitly
uses the auto reuse policy. Browsing and ordinary materialization remain unchanged.
