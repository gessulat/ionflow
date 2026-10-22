# 0032 — Declared artifact identity and execution reuse

Status: accepted (2026-10-22). Extends 0023, 0027, and 0030.

## Context

Content hashes identify exact bytes, but cannot predict an output before building
it. Equivalent scientific builds on CUDA and Metal can differ in their bytes.
Applications should declare when those results are interchangeable and reuse
them without scanning payload files or embedding discovery in their kernels.

## Decision

An artifact has one ID. By default, that ID hashes the file or bundle descriptor.
An artifact may instead declare `identity=("library", "method", "nces", "losses")`.
Its ID then hashes the canonical JSON of those validated metadata fields, within
its existing type namespace. Names, platforms, and other metadata do not enter
that ID. Developers must include every scientifically significant input and an
explicit version field when the equivalence contract changes. Identity fields
cannot be edited after publication. File checksums still verify every payload.

Artifact metadata uses strict Pydantic validation. Artifact-typed metadata fields
store typed reference IDs. FlowParams uses Pydantic models, including validators,
defaults, lists, and dictionaries. Parameters remain JSON-portable. Table rows
retain vectorized Arrow and DuckDB validation; no row-model objects are created.

An artifact-producing step resolves identity fields by exact name from its typed
flow inputs, resolved parameters, or explicit artifact dependencies. Missing or
incompatible sources fail at declaration. There is no resolver method or decorator.
Whole steps and branch steps support lookup; a producer emitting several artifacts
must expose individual branch invocations to calculate their identities first.
Returned artifacts must match the identity resolved before the body executes.
Declaring identity opts the producer into reuse and promises interchangeable output.
Side effects that must always execute belong in separate steps.

Every run accepts `reuse="auto"`, `"local"`, or `"never"`. Auto checks local payloads
first, then queries the configured peer for the exact ID and performs a verified
download. Local permits local reuse only. Never executes every body, including
completed resume nodes and nested children. Children inherit their parent's policy.
The CLI exposes `--reuse` and `--no-reuse`. Runs persist the policy; activities
record the source, ID, and transferred bytes when a producer is reused.

Identity lookup precedes input payload validation. Bindings still pin and validate
metadata at run start. A step that computes requires its consumed payloads locally;
a reused producer does not require its otherwise-unused build inputs. Auto is an
explicit execution authorization for downloading a cached output. Browsing and
ordinary materialization retain the explicit-transfer policy in 0027. Archival
checkbox defaults remain independent of execution reuse.

Stores retain concrete file manifests under the same artifact ID. Local forced
builds become the preferred local copy. A receiving peer retains its first
complete shared copy. Different content alone does not constitute an identity
conflict. Descriptive metadata stays with the preferred concrete copy. Edits
to the same concrete copy retain explicit conflict resolution.
Run bindings and activities pin their exact manifests. Transfer plans, previews,
cache paths, recovery, and eviction account for those concrete files rather than
substituting the store's current preferred copy. Internal manifest checksums are
storage checksums, not additional artifact IDs.

## Consequences

Applications can ask `Index.identity(...)` before producing any files, then use
the ordinary registry lookup. Snage can remove library and index discovery from
preparation bodies after adopting matching typed parameter and metadata names.
Automatic upload, distributed build leases, cost prediction, and historical
layout migration remain outside this contract. Concurrent misses may compute
twice; publication retains one shared artifact and preserves exact run evidence.
