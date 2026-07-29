# 0004 — Flow identity is the module-qualified class name

Status: accepted (2026-07-26)

## Context

The obvious identity for a flow is its bare class name — `Layout` building
S3 keys from `flow_cls.__name__`, `Flow._registry` keyed the same way. That
was considered and rejected: renaming a class would silently orphan its S3
data, and two same-named Flow classes in different modules would collide —
last import wins in the registry, and both share one S3 namespace and one
ledger.

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

Note (2026-07-28): `Layout.for_flow` is now `Space.for_flow` (`space.py`),
and a flow_id string can build a Space directly (`Store.space(flow_id)`) —
which is what branch workers use, for the reason given above.
