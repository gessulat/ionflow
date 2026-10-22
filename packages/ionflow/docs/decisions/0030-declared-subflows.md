# 0030 — Declared subflows with durable child runs

Status: accepted (2026-10-22).

## Context

Applications need independently runnable preparation and search flows, composed
into one visible execution graph. Calling `Flow.run()` from a CLI loses the
relationship between runs. Calling it from a step without a durable relationship
also repeats child work when the parent fails before persisting that step.

## Decision

A parent declares `@subflow(Child, output="step")` on a step returning
`Call[OutputSchema]`. Its method supplies child parameters and artifact bindings.
The DAG validates the selected output and schema at class definition. Existing
dependency and branch markers connect the parent nodes.

The runner executes children. Child genesis records the parent flow, run, step,
and branch in the catalog and recovery metadata before any child work starts.
The child catalog is the call journal; no second registry or result authority
is introduced. A child must finish before the parent exposes its selected output.
Each child keeps its own manifest, telemetry, and artifact producer evidence.
Artifact references retain their pinned metadata across bindings.

Parent resume searches its origin chain for previous child attempts. A completed
child is reused; a failed child resumes through the existing planner. Changed
arguments and still-running children are refused. Worker imports preserve flow
identity at every nesting level. Child scheduling inherits the parent task's
resource allocation and execution mode.

The browser registers declared descendants and exposes child attempts, including
attempts retained from resumed parents. Metadata replication and catalog recovery
preserve parent links. Payload transfers retain their existing run scope.

## Consequences

Applications compose flows without embedding execution policy in their CLI.
Selected child outputs must be whole tables or artifacts; branch outputs can be
collected in the child first. Tables materialize as parent parts; artifacts remain
shared references. Additional child tables and telemetry transfer separately.
This contract adds no takeover of running children, cancellation protocol,
cross-store execution, or historical-store migration.
