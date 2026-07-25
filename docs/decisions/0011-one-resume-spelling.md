# 0011 — One resume spelling: `run --resume <run_id|latest>`

Status: accepted (2026-07-25). Supersedes the `run --incremental` sugar
clause of [0008](0008-keyed-incremental-resume.md) (the keyed-resume
mechanics there are unchanged) and removes the `resume` command that had
existed since 0002.

## Context

Resuming had three spellings for one mechanism: the `resume` command
(explicit origin run_id), `run --incremental` (origin = latest complete
run), and the runner's mutually exclusive `origin_run_id` / `incremental`
parameters guarded by a ValueError. All of them funnel into the same code
path — the only difference is how the origin run is chosen. A newcomer
reading the CLI saw two commands and a flag and had to learn they are one
feature; the sizing knobs (`--max-workers` etc.) were duplicated across
both commands to boot. Separately, `--allow-commit-drift` was accepted on
a fresh `run`, where it silently did nothing — and sat confusingly close
to the dirty-tree gate, which deliberately has no override flag.

## Decision

One parameter, end to end: `runner.run(..., resume=...)` takes an origin
run_id or the string `"latest"`. `"latest"` resolves to the newest
complete run and falls through to a fresh full run when there is none, so
cron jobs can pass it unconditionally; an explicit run_id that doesn't
exist is an error. The CLI exposes it as `run --resume RUN_ID|latest`;
the `resume` command, the `--incremental` flag, and `Flow.resume` are
gone. `allow_commit_drift` without `resume` is now a ValueError instead
of a silent no-op.

A run_id can never collide with the sentinel: ids are
`<epoch-ms>-<hex8>`, so the literal string `latest` is unambiguous.

## Consequences

- One concept to learn and document; every sizing flag exists exactly
  once; the runner loses a two-parameter invariant.
- Scripts using `microflow resume` or `--incremental` must switch to
  `--resume` (pre-1.0, no deprecation shim).
- "Resume a failed run" and "incremental corpus run" are now visibly the
  same operation with a different origin — which they always were.
