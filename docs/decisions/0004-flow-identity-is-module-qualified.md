# 0004 — Flow identity is the module-qualified class name

Status: accepted and implemented (2026-07-10)

## Context

Originally a flow's durable identity was its bare class name: `Layout` built
S3 keys from `flow_cls.__name__` and `Flow._registry` was keyed the same way.
Renaming a class silently orphaned its S3 data, and two same-named Flow
classes in different modules collided — last import won in the registry, and
both shared one S3 namespace and one ledger. Pre-release, no existing data,
so no migration was needed.

## Decision

Flow identity is `flow_id()` = `{__module__}.{__name__}` (e.g.
`examples.embed.EmbedDocs`), used for the S3 namespace (`Layout.for_flow`) and
the registry key. Branch workers receive the flow_id in their payload rather
than deriving it — the worker re-imports the flow by file path, which gives
the class a loader module name (`_mf_flow`), so its own `flow_id()` is wrong
there.

## Consequences

- Same-named flows in different modules can never share data; collisions are
  structurally impossible, so no registry guard is needed.
- Renaming a class OR moving it to another module is now explicitly "new
  flow" — old runs stay under the old id (visible as an orphan warning).
- Flows defined in `__main__` or a REPL get an unstable identity; flows must
  live in importable modules (already required by process-mode fan-out).
- Textual widget ids forbid dots, so the TUI slugs flow_ids ("." → "-";
  injective since neither module nor class names may contain "-").
