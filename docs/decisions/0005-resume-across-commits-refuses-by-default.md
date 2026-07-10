# 0005 — Resume across commits refuses by default

Status: accepted and implemented (2026-07-10)

## Context

Resuming at a different commit than the origin run used to only warn
(`runner.py`). Reused steps keep their old-code outputs while re-run steps
use new code, so one run can mix outputs from two commits; provenance records
only the new commit plus `origin_run_id`. This sat oddly next to the git
gate, which refuses a dirty tree outright.

## Decision

A commit mismatch on resume raises `CommitDriftError` unless
`allow_commit_drift=True` (CLI: `--allow-commit-drift`). Prototype flows stay
warn-only — they already relinquish provenance (dirty trees allowed).

## Consequences

- Resume-after-fix — the most common resume — changes the commit by
  definition, so it requires the flag every time. That friction is the point:
  mixed-commit runs are a visible, deliberate choice.
- The ledger still records only the new commit; the drift itself remains
  visible via `origin_run_id` plus the origin's own ledger row.
