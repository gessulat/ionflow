# 0022 — Runs and artifacts have separate store namespaces

Status: accepted (2026-10-20); amends the paths in 0012, 0016 and 0020.

## Context

Global artifacts must be discoverable independently of flow classes and runs.
The current prefix mixes flow roots with the publish lake. There are no
production stores or users, so layout compatibility has no value yet.

## Decision

Flow roots live at `<prefix>/runs/<flow_id>`. Each root holds its catalog,
`_lake/`, and `run_id=<id>/` directories directly. Artifacts live at
`<prefix>/artifacts/`; publication remains at `<prefix>/published/`.
Discovery lists only the runs namespace. Local and S3 stores use identical keys.
There is no legacy reader or migration command.

## Consequences

All run access changes through `Space`, including metrics, sync and recovery.
Development stores can be recreated. Artifact IDs do not encode store locations.
