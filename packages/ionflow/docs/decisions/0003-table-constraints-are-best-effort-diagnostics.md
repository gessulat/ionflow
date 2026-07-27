# 0003 — Table constraints are best-effort diagnostics, not guarantees

Status: accepted (2026-07-26)

## Context

NOT NULL / CHECK / PRIMARY KEY are validated by one aggregate scan of each
written part file before upload (`write_part` → `violation_scans`). The scan
is per part in isolation: a fan-out step's PRIMARY KEY is only checked within
each branch's part, and nothing is ever checked across runs.

## Decision

Constraint validation is an early-warning debugging aid, not an enforced
integrity guarantee. The per-part scope stays: no cross-branch scan after a
fan-out, no cross-worker coordination, no global uniqueness.

## Consequences

- PRIMARY KEY on a fan-out output means per-branch uniqueness only;
  duplicates across branches (or across runs) pass silently.
- A flow that needs hard uniqueness must enforce it itself, e.g. in a
  `Collect` step over the branch outputs.
- Rejected alternative: a post-fan-out scan over all parts — it would re-read
  everything just written on every fan-out to guard against a rare bug class.
