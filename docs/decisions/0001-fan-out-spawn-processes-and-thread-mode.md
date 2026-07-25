# 0001 — Fan-out: spawn processes in production, keep thread mode for tests

Status: accepted (2026-07-10). The "CLI does not expose it" clause is
superseded by [0010](0010-cli-exposes-thread-mode.md).

## Context

Fan-out branches run user code that typically imports numpy/torch. The
orchestrator is multi-threaded (metrics sampler, DuckDB workers) and has
already run user code in-process for non-branch steps, so `fork`ed children
would inherit OpenMP/BLAS/CUDA state mid-flight — hangs, "cannot re-initialize
CUDA". On Linux the stdlib `ProcessPoolExecutor` default is still fork
(Python 3.12/3.13).

## Decision

- Process-mode fan-out **always uses `get_context("spawn")`**, on every
  platform (`runner.py`, `_run_branches`). Never fork.
- Spawn is load-bearing beyond safety: each worker sets
  `OMP/OPENBLAS/MKL_NUM_THREADS` to its fair share *before* the flow module
  imports numpy — only possible in a fresh interpreter — and runs its own
  metrics sampler (the per-branch rows in the `metrics` view).
- **Thread mode (`use_processes=False`) stays, but as an internal/testing
  affordance, not a supported user option.** The CLI does not expose it. It
  exists because it keeps the default test suite at ~1.6s (no spawn + full
  import chain per branch) and makes branch code debuggable in one process
  (`pytest -n 0` + breakpoint). Its known limits are accepted: GIL for pure-
  Python step code, no per-branch BLAS caps or metrics rows, no crash
  isolation.

## Consequences

- Flows must be importable from a file for process mode (workers re-import via
  `inspect.getfile`); notebook/REPL-defined flows can't fan out in process mode.
- Each spawned worker pays interpreter startup + fresh imports (can be seconds
  with heavy deps); acceptable for production runs, which is why tests opt out.
- The two modes share `_branch_worker` entirely; the only forks are which
  executor is built and the `sample` flag (sampler + env caps). Removing thread
  mode was considered and rejected — it would delete ~15 lines and slow the
  suite several-fold.
