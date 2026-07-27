# 0016 — the repo is a flat uv workspace; ionflow and schrein are siblings

## Context

ionflow was the repository *and* the uv workspace root: `pyproject.toml`,
`src/ionflow/`, `tests/`, `examples/` and `docs/` all sat at the top level,
with schrein nested one level down in `schrein/` as the only workspace member
(schrein decision 0001).

That nesting encoded a hierarchy the code does not have. schrein is a
standalone library with its own spec, tests and release story — it must never
import ionflow — yet the layout read as "schrein is a subdirectory of
ionflow". The asymmetry leaked into everything built on top of the tree:
root-level configuration was really ionflow's configuration; `uv sync` at the
root installed ionflow's dev group but not schrein's, so the two suites needed
two environments; and `uv add <dep>` silently meant "add to ionflow".

It also quietly mis-taught the linter. With `schrein/` sitting at a source
root, isort resolved bare `schrein` as first-party but `schrein.ducklake` as
third-party, so the same dependency was split across two import blocks in
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
subject lives). ionflow declares `schrein = { workspace = true }` itself,
rather than inheriting the source from the root.

Repo-wide concerns stay at the root, because they genuinely span both:
`ruff` and `ty` configuration and their dev group. ty's scope is unchanged —
still ionflow's `src` only; widening it to schrein is a separate decision, not
a side effect of moving directories.

`uv sync` at a virtual workspace root installs *all* members and *all* their
dev groups, so the whole repo now shares one environment. `--directory` selects
whose pytest configuration applies:

```bash
uv run --directory packages/ionflow pytest
uv run --directory packages/schrein pytest
```

## Consequences

- No root test suite, deliberately. The members' suites each define a
  top-level `conftest.py` and their own moto harness; collected together the
  conftest basenames collide and `from conftest import _git` resolves to the
  wrong module. CI keeps one job per member, as before.
- One environment for the repo instead of two. schrein's dev group (the
  ducklake extension wheel) is now present when running ionflow's tests too —
  harmless, and it matches what the session-start hook already seeds.
- `uv add` must name its target: `uv add --package ionflow <dep>`.
- The root `[tool.ruff]` needs `target-version` and `src` spelled out; with no
  `[project]` at the root there is no `requires-python` to infer from, and the
  package roots are no longer at the default `./src`. `schrein` now classifies
  as third-party everywhere, which is what it is — a dependency of ionflow.
- ionflow's README moved to `packages/ionflow/README.md`; the root README is
  now a short index of the workspace, which is what a repository front page
  for two packages should be.
- Extracting schrein into its own repository stays what schrein decision 0001
  promised — copy the directory, drop the workspace entry — and is now a
  strictly smaller move, since nothing of schrein's lives outside its package.

Rejected: keeping ionflow at the root and only moving schrein deeper, or
promoting the root to a package that re-exports both. Both preserve the
hierarchy the dependency graph does not have.

Supersedes nothing on the ionflow side; on schrein's, see schrein decision
0005, which supersedes its 0001.
