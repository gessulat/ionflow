# ionflow-tui

Terminal UI for [ionflow](https://github.com/gessulat/ionflow): one tab per
flow, a runs table per tab (ledger + params + custom columns), and a per-run
drill-down with resource stats and step-output previews. If the repo isn't
configured yet it opens ionflow's setup screen first.

Factored out of ionflow core so the library carries no UI dependency
(ionflow decision 0017). Install via the core extra and launch with the bare
command:

```bash
uv add "ionflow[tui]"   # or: pip install "ionflow[tui]"
ionflow                  # no subcommand opens the TUI
```

Screenshots and key bindings are in the
[ionflow README](https://github.com/gessulat/ionflow#browse-in-the-terminal-tui).

No automated tests, deliberately — the TUI is human-tested.
