# 0001 — schrein starts as a uv workspace member inside the ionflow repo

> Superseded by [0006](0006-sibling-workspace-member.md): schrein now sits at
> `packages/schrein/`. Paths below are historical.

The spec (`docs/specs/cas-catalog-library.md` at the repo root) is written
for a fresh repository. The first stages (M0 CAS core, M1 DuckLake adapter)
are instead developed here, as an independent uv workspace member under
`schrein/`.

Why:

- The spec's origin, its companion exploration
  (`ionflow-on-schrein.md`, since retired), and the proven patterns being
  extracted (the CAS loop from `ledger.py`, the moto subprocess harness from
  `tests/conftest.py`) all live in this repo — co-locating keeps spec and
  implementation evolving together while the design is young.
- A uv workspace member keeps schrein fully self-contained: own
  `pyproject.toml`, own dependencies, own test suite, no imports from
  ionflow in either direction. `uv sync`/`uv run` at the repo root are
  unaffected; schrein commands run from `schrein/`.
- Extraction later is mechanical: the `schrein/` directory is already the
  spec's repository skeleton; moving it out means copying the tree and
  dropping the workspace entry.

Boundaries this imposes: schrein must never import ionflow — the spec is
deliberately self-contained and the package must stay buildable from it
alone. The other direction is allowed and exists: ionflow depends on
schrein (a `{ workspace = true }` uv source today, an ordinary package
dependency once schrein is extracted) for `CasRegister` under its ledger —
ionflow decision 0011. That dependency carries no role-mixing: the ledger
stays run bookkeeping, the catalog stays schrein's.
