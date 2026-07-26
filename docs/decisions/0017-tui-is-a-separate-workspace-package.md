# 0017 — the TUI is a separate workspace package

## Context

The TUI was one module (`ionflow/tui.py`), but it pulled textual — a full
terminal-UI framework, by far the heaviest core dependency — into every
install of ionflow, including headless environments (CI, workers,
schedulers) that only ever run flows. The TUI is strictly a consumer of
ionflow's public surface (`discovery`, `git`, `query`, `config`); nothing
in core imports it except the CLI's bare-command launcher, and that import
was already lazy. The repo already is a uv workspace (schrein).

## Decision

Factor the TUI out as workspace member `ionflow-tui/` (package
`ionflow_tui`), depending on `ionflow` + `textual`. Core drops textual.
The dependency points one way only: ionflow must never import
`ionflow_tui` at module scope — the CLI's bare command imports it lazily
and prints an install hint (`pip install 'ionflow[tui]'`) when absent.

Distribution is the `ionflow[tui]` extra (a deliberate benign cycle:
`ionflow[tui]` → `ionflow-tui` → `ionflow`; resolvers handle extras
cycles), so the one-line install keeps working. The workspace dev group
includes `ionflow-tui`, so bare `uv run ionflow` still opens the TUI when
developing.

## Consequences

- `pip install ionflow` alone no longer includes the TUI — the bare
  command tells you what to install instead of tracebacking.
- Versions stay in lockstep informally (both 0.1.0, released from one
  repo); `ionflow-tui` declares no minimum ionflow version until the core
  API it uses actually moves.
- The TUI keeps its no-tests policy; root pytest doesn't collect the
  package (testpaths), and there is no test setup inside it.
- Rejected: a plain optional extra with textual still in the ionflow
  sdist (`ionflow[tui]` gating only the dependency) — keeps one package
  but the UI code then ships in every wheel and stays inside the
  type-checked core surface with an unguarded import of an optional dep.
- Rejected: an `ionflow-tui` console script — one launcher (`ionflow`)
  is enough; a second entry point would drift.
