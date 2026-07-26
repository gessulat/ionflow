---
name: verify
description: Drive ionflow end-to-end as a user would — CLI against a moto S3 subprocess — to verify a change at its real surface.
---

# Verifying ionflow changes

The surface is the CLI (`ionflow run/query/ls/stats`) plus the library API,
against any S3-compatible endpoint. Recipe that works (no AWS account needed):

1. Start a moto server **subprocess** (never in-process — httpfs wedges it):
   `python -m moto.server -H 127.0.0.1 -p <port>`, then create a bucket via
   boto3 with `endpoint_url`.
2. Make a scratch "user project" dir: a `flows.py` with Flow subclasses, a
   `.ionflow/config` (TOML: bucket/prefix/endpoint/region), and a **clean git
   repo** — runs refuse dirty trees, and `__pycache__/` counts, so commit a
   `.gitignore` (`__pycache__/`, `.ionflow/`) first.
3. Drive from that dir with env creds + module path:
   `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test PYTHONPATH=. \
    uv run --project /home/user/ionflow ionflow run flows:MyFlow`
   DuckDB's `credential_chain` resolves the env vars fine.
4. `ionflow query flows:MyFlow "DESCRIBE step"` shows persisted column types;
   `ionflow ls` shows ledger status (failed probe runs should read `failed`).

CLI runs default to process-mode branch workers — a fan-out flow exercises the
spawn path with no extra flags. Kill the moto pid when done.
