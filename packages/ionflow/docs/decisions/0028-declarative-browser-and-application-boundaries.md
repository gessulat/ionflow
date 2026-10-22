# 0028 — Framework contracts for application browsers

Status: accepted (2026-10-22).

## Decision

Applications declare artifact types, step tables, flows, and optional view hints.
Ionflow owns inventory, lineage, local table queries, transfer previews, and local
payload removal. Applications must not reconstruct blob paths or edit catalog tables.
The browser contract is a Python API. HTTP serving and browser components can use it
later without introducing another metadata authority.

View hints select labels, columns, formats, and artifact-reference targets. They
describe presentation; they neither create execution dependencies nor replace
tracked artifact use. Table and artifact schemas remain the validation contracts.
References must name declared string or string-list fields and an artifact type.

Transfer previews use local metadata only. They are estimates, carry no approval
token, and never authenticate or contact a peer. Exact plans still check both
endpoints and pin the approved file selection. Browsing never downloads payloads.
Local removal preserves metadata, remote copies, dependencies, and shared blobs.
It refuses active runs and removal of table parts used by other available runs.
Cache eviction remains distinct from canonical payload removal.

Flows can record named phases inside a step. Ionflow stores their boundaries,
nesting, branch identity, and success or failure alongside resource telemetry.
Phases are observations, not steps, resumable units, or application output tables.

## Application boundary: Snage

Snage declares Fasta, Raw, RawDB, SpectralLibrary, and SpectralIndex artifacts.
There is no generic Files category. Drop the SpectrumFile/SpectrumTable metadata
table instead of duplicating RawDB metadata or persisting spectrum arrays.
RawDB owns the searchable spectra and their intrinsic metadata. Assignment and
result tables describe searches; organism and sample labels belong to those
assignments when they are experiment-specific.

File preparation, library construction, index construction, and identification
are explicit flows. Search kernels consume prepared artifacts. Snage owns
proteomics, build compatibility rules, domain summaries, and credential integration.
Ionflow owns execution, provenance, storage, and generic presentation contracts.

Existing development runs can be discarded. This work adds no flow-identity
aliases, historical readers, table migration, or production-store deletion.

## Consequences

Applications can replace bespoke inventory and storage implementations incrementally.
The initial contracts require no web framework or frontend dependencies.
Domain renderers can supplement generated defaults without redefining storage policy.
