# 0004 — Flow identity should be module-qualified

Status: accepted, not yet implemented (2026-07-10)

## Context

Today a flow's durable identity is its bare class name: `Layout` builds S3
keys from `flow_cls.__name__` and `Flow._registry` is keyed the same way.
Renaming a class silently orphans its S3 data, and two same-named Flow
classes in different modules collide — last import wins in the registry, and
both would share one S3 namespace and one ledger.

## Decision

Flow identity becomes module-qualified (e.g. `examples.etl.ETLFlow`) in both
the S3 layout and the registry, so renames are explicit and collisions are
impossible.

## Consequences

- Migration-worthy: existing buckets use bare-name keys, so implementation
  needs a key-scheme migration story (or a layout-version marker).
- Until implemented: never define two Flow classes with the same name, and
  treat a class rename as creating a new flow (old runs stay reachable only
  under the old name).
