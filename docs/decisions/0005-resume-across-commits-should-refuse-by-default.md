# 0005 — Resume across commits should refuse by default

Status: accepted, not yet implemented (2026-07-10)

## Context

Resuming at a different commit than the origin run currently only warns
(`runner.py`). Reused steps keep their old-code outputs while re-run steps
use new code, so one run can mix outputs from two commits; provenance records
only the new commit plus `origin_run_id`. This sits oddly next to the git
gate, which refuses a dirty tree outright.

## Decision

A commit mismatch on resume should be an error by default, with an explicit
opt-in flag (e.g. `allow_commit_drift=True`) to proceed and get today's
warning behavior.

## Consequences

- Resume-after-fix — the most common resume — changes the commit by
  definition, so it will require the flag every time. That friction is the
  point: mixed-commit runs become a visible, deliberate choice.
- Prototype flows already relinquish provenance (dirty trees allowed); the
  gate may reasonably stay warn-only there.
