# 0005 — Resume across commits refuses by default

Status: accepted (2026-07-26)

## Context

When a resume runs at a different commit than the origin run, reused steps
keep their old-code outputs while re-run steps use new code, so one run can
mix outputs from two commits; provenance records only the new commit plus
`origin_run_id`. Merely warning about the mismatch was considered and
rejected — it would sit oddly next to the git gate, which refuses a dirty
tree outright.

## Decision

A commit mismatch on resume raises `CommitDriftError` unless
`allow_commit_drift=True` (CLI: `--allow-commit-drift`).

## Consequences

- Resume-after-fix — the most common resume — changes the commit by
  definition, so it requires the flag every time. That friction is the point:
  mixed-commit runs are a visible, deliberate choice.
- The ledger still records only the new commit; the drift itself remains
  visible via `origin_run_id` plus the origin's own ledger row.
