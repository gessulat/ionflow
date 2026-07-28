# 0006 — schrein is a sibling package, not a subdirectory of ionflow

Supersedes [0001](0001-uv-workspace-in-ionflow-repo.md), which is otherwise
still accurate: schrein is developed inside the ionflow repository as an
independent uv workspace member, and extraction remains mechanical.

What changes is only *where*. 0001 put schrein under `schrein/` at the repo
root, next to ionflow's `src/`, `tests/` and `pyproject.toml` — a nesting that
read as "part of ionflow" when the two are peers with a one-way dependency.
The repo root is now a workspace root only, and both packages are siblings:

```
packages/ionflow/
packages/schrein/     # src/, tests/, docs/{decisions,specs}/, README, LICENSE
```

Consequences for schrein:

- The spec moved from the repo root into the package it specifies:
  `packages/schrein/docs/specs/cas-catalog-library.md`. Everything schrein
  now lives under one directory — the spec's original skeleton, verbatim.
- Tests run as `uv run --directory packages/schrein pytest` from the repo
  root. `uv sync` at the root installs schrein's dev group too (the ducklake
  extension wheel, moto), so there is one environment for the repo instead of
  one per member. CI still runs schrein's suite as its own job.
- `license-files = ["LICENSE"]` and a LICENSE copy were added, so the wheel
  ships the license text the metadata already claimed.
- Inside schrein, `schrein` is first-party again: the package declares its own
  ruff `src` and `extend`s the root config for the shared rules. Extraction
  drops that one `extend` line — ruff refuses to run and names the missing
  file if it is forgotten.
- The boundary from 0001 is unchanged and is now also the layout: schrein
  must never import ionflow; ionflow depends on schrein
  (`{ workspace = true }` today, an ordinary package dependency once
  extracted).

The repo-level rationale is ionflow decision 0017
(`packages/ionflow/docs/decisions/0017-flat-uv-workspace-of-sibling-packages.md`).
