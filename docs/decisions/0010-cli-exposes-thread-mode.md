# 0010 — The CLI exposes thread mode (`--threads`), partially superseding 0001

Status: accepted (2026-07-24). Supersedes the "the CLI does not expose it"
clause of [0001](0001-fan-out-spawn-processes-and-thread-mode.md); everything
else in 0001 (spawn, never fork; thread mode's known limits) stands.

## Context

0001 kept `use_processes=False` as an internal/testing affordance. Since then
the sizing knobs became the operational bottleneck: peak RAM ≈ workers × one
branch's working set, and users cron or script runs from the CLI, where none
of `max_workers` / `use_processes` / `branch_retries` were reachable. Exposing
two of the three while hiding the one that shares their signature would leave
the Python API and CLI asymmetric for no safety gain — thread mode is not
dangerous, just limited.

## Decision

`microflow run` and `microflow resume` take `--max-workers`, `--branch-retries`,
and `--processes/--threads` (default `--processes`), passing straight through
to `runner.run`. Thread mode's limits are unchanged and now user-facing:
GIL-bound pure-Python steps, no per-branch BLAS caps or metrics rows, no crash
isolation.

## Consequences

- One flag pair to document instead of a Python launcher script for anyone
  who wants in-process branches (debugging, tiny corpora, constrained boxes).
- Users can now pick thread mode in production and hit its accepted limits;
  the default stays process mode, so this is opt-in.
