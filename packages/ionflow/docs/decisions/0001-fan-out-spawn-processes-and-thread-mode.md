# 0001 — Fan-out: spawn processes by default, thread mode as the opt-in

Status: accepted (2026-07-26)

## Context

Fan-out branches run user code that typically imports numpy/torch. The
orchestrator is multi-threaded (metrics sampler, DuckDB workers) and has
already run user code in-process for non-branch steps, so `fork`ed children
would inherit OpenMP/BLAS/CUDA state mid-flight — hangs, "cannot re-initialize
CUDA". On Linux the stdlib `ProcessPoolExecutor` default is still fork
(Python 3.12/3.13).

Separately, the sizing knobs are the operational bottleneck: peak RAM ≈
workers × one branch's working set, and users cron or script runs from the
CLI — so the execution-mode choice has to be reachable wherever
`max_workers` and `branch_retries` are.

## Decision

- Process-mode fan-out **always uses `get_context("spawn")`**, on every
  platform (`runner.py`, `_run_branches`). Never fork.
- Spawn is load-bearing beyond safety: each worker sets
  `OMP/OPENBLAS/MKL_NUM_THREADS` to its fair share *before* the flow module
  imports numpy — only possible in a fresh interpreter — and runs its own
  metrics sampler (the per-branch rows in the `metrics` view).
- **Thread mode (`use_processes=False`) is a supported opt-in, not just a
  test affordance.** `ionflow run` takes `--processes/--threads` (default
  `--processes`) alongside `--max-workers` and `--branch-retries`, passing
  straight through to `runner.run` — hiding the one knob that shares the
  sizing signature would leave the Python API and CLI asymmetric for no
  safety gain; thread mode is not dangerous, just limited. Its accepted
  limits are user-facing: GIL for pure-Python step code, no per-branch BLAS
  caps or metrics rows, no crash isolation. What it buys: in-process
  branches for debugging (`pytest -n 0` + breakpoint), tiny corpora,
  constrained boxes — and it keeps the default test suite at ~1.6s (no
  spawn + full import chain per branch).

## Consequences

- Flows must be importable from a file for process mode (workers re-import via
  `inspect.getfile`); notebook/REPL-defined flows can't fan out in process mode.
- Each spawned worker pays interpreter startup + fresh imports (can be seconds
  with heavy deps); acceptable for production runs, which is why tests opt out.
- Users can pick thread mode in production and hit its accepted limits; the
  default stays process mode, so this is opt-in.
- The two modes share `_branch_worker` entirely; the only forks are which
  executor is built and the `sample` flag (sampler + env caps). Removing
  thread mode was considered and rejected — it would delete ~15 lines and
  slow the suite several-fold.
