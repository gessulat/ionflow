# 0016 — the repo is a flat uv workspace; ionflow and schrein are siblings

## Context

ionflow was the repository *and* the uv workspace root: `pyproject.toml`,
`src/ionflow/`, `tests/`, `examples/` and `docs/` all sat at the top level,
with schrein nested one level down in `schrein/` as the only workspace member
(schrein decision 0001).

That nesting encoded a hierarchy the code does not have. schrein is a
standalone library with its own spec, tests and release story — it must never
import ionflow — yet the layout read as "schrein is a subdirectory of
ionflow". The asymmetry leaked into everything built on the tree: root-level
configuration was really ionflow's, `uv sync` at the root installed ionflow's
dev group but not schrein's (so the two suites needed two environments), and
`uv add <dep>` silently meant "add to ionflow".

It also quietly mis-taught the linter. With `schrein/` sitting at a source
root, isort resolved bare `schrein` as first-party but `schrein.ducklake` as
third-party, so one dependency was split across two import blocks in
`catalog.py`, `ledger.py` and schrein's own tests.

## Decision

The repository root is a **workspace root only** — no `[project]`, no package.
Both packages are siblings under `packages/`:

```
pyproject.toml            # [tool.uv.workspace] + repo-wide ruff/ty config
packages/ionflow/         # src/, tests/, examples/, docs/, README, LICENSE
packages/schrein/         # src/, tests/, docs/, README, LICENSE
```

Each member is self-contained: its own `pyproject.toml`, dependency group,
pytest configuration, README, LICENSE, `docs/decisions/` and `docs/specs/`
(the CAS catalog spec moved to `packages/schrein/docs/specs/`, where its
subject lives). ionflow declares `schrein = { workspace = true }` itself.

Repo-wide concerns stay at the root only where they genuinely span both: the
lint/type-check dev group, and the ruff *rules* (`line-length`,
`target-version`, `select`). Source roots do not span — `src` is what tells
isort who is first-party, and the answer differs per member — so each member
declares its own and `extend`s the root for the rules. ty's scope is
unchanged, still ionflow's `src` only.

`uv sync` at a virtual workspace root installs *all* members and *all* their
dev groups, so the repo now shares one environment. `--directory` selects
whose pytest configuration applies:

```bash
uv run --directory packages/ionflow pytest
uv run --directory packages/schrein pytest
```

## Consequences

- **No root test suite.** The durable reason is that the two suites are
  deliberately different — different addopts (`--doctest-modules` only for
  ionflow), different markers, different moto harnesses, independent release.
  They also happen to collide mechanically today (both define a top-level
  `conftest.py`, so the basenames clash and `from conftest import _git`
  resolves into the wrong package), but that collision is a symptom. Removing
  it would not make a merged suite a good idea. CI keeps one job per member.
- One environment for the repo instead of two. schrein's dev group (the
  ducklake extension wheel) is present when running ionflow's tests too, which
  makes them hermetic: `LOAD ducklake` from the wheel instead of `INSTALL` over
  the network. The session-start hook no longer seeds that extension.
- `uv add` must name its target: `uv add --package ionflow <dep>`.
- The root `[tool.ruff]` needs `target-version` spelled out; with no
  `[project]` there is no `requires-python` to infer from, and the default
  demotes `tomllib` to a third-party import.
- Each member's ruff config `extend`s the root by relative path. Extracting
  schrein means deleting that one line — ruff fails loudly and names the file
  if it is forgotten, so it cannot rot silently.
- ionflow's README moved to `packages/ionflow/README.md`; the root README is
  now a short index of the workspace.

Rejected: keeping ionflow at the root and moving only schrein deeper. It
preserves the hierarchy the dependency graph does not have.

See schrein decision 0005 for the same move from schrein's side.
