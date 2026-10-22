# 0029 — Directional artifact payload defaults

Status: accepted (2026-10-22).

## Decision

Artifact declarations accept boolean `sync_up` and `sync_down` keywords.
The type name is derived from the class when `kind=` is omitted (0023).
Both flags default to true and inherit from the parent type. Publication captures the defaults
in the portable artifact record, outside payload identity and domain metadata.
Changing a declaration does not rewrite existing records.

Workspace previews and exact plans apply these hints when no explicit payload
selection is supplied. `include_artifacts` replaces the default selection,
including an empty list. Source payload validation happens after selection.
Metadata, ancestry, and producer evidence transfer for the entire reference
closure, regardless of payload selection. Run tables keep their existing transfer
semantics.

Browser descriptions expose the declaration defaults. Transfer previews expose
the resulting checkbox selection. The TUI allows users to change each choice
before confirming a new exact plan. Generation checks prevent stale planning
responses from enabling confirmation for an obsolete selection.

Low-level transfers infer direction only between different storage backends.
Same-backend callers can provide a logical direction; otherwise all payloads
remain selected. Workspace transfers always provide their explicit direction.

## Consequences

Snage can archive RawDB without uploading Raw by default, while clients download
results without either large input. Generic clients retain these defaults without
importing Snage. Defaults do not prohibit an explicit transfer or trigger one.
Existing development stores can be recreated; this adds no record migration.
