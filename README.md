```
|  |   _
|  |  /
|  |  +-
|__|  |
|     /
```

# ionflow

A [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/) with two
sibling packages under `packages/`. Neither is the root: the repository root holds
only the workspace, the shared lint/type-check configuration, and this page.

| Package | What it is |
| --- | --- |
| [**ionflow**](packages/ionflow) | A barebones [Metaflow](https://metaflow.org) alternative. A flow is a class, a step is a method, and every step returns a typed table persisted as Parquet on S3. No Kubernetes, no scheduler, no metadata server — the only thing you configure is one S3-compatible bucket. |
| [**schrein**](packages/schrein) | Zero-infrastructure [DuckLake](https://ducklake.select/) catalogs on S3 conditional writes — no Postgres, no catalog server, no lock service. Stands alone; ionflow depends on it for its ledger and per-flow catalog. |

The dependency runs one way only: ionflow depends on schrein, schrein never
imports ionflow. See
[ionflow decision 0016](packages/ionflow/docs/decisions/0016-flat-uv-workspace-of-sibling-packages.md)
and [schrein decision 0005](packages/schrein/docs/decisions/0005-sibling-workspace-member.md)
for why the layout is flat.

## Working in the repo

```bash
uv sync                                          # one env: both packages + every dev group

uv run --directory packages/ionflow pytest       # ionflow's suite (fast set)
uv run --directory packages/ionflow pytest -m "" # ...including the slow integration tests
uv run --directory packages/schrein pytest       # schrein's suite (fast set)

uv run ruff check && uv run ty check             # repo-wide, from here
uv build --all-packages                          # both wheels into dist/
```

Each package owns its own test suite, dependency group, docs and decision
records. There is no root test suite, deliberately — see decision 0016.

## License

MIT — see [LICENSE](LICENSE).
