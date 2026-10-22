import importlib
import json
import sys
import typing
from pathlib import Path

import typer

from . import git
from .artifact import Artifacts
from .config import (
    CONFIG_DIR,
    CONFIG_FILE,
    SETUP_FIELDS,
    Config,
    ConfigMissing,
    config_from_setup,
    read_config,
    write_config,
)
from .query import ledger_with_liveness, step_stats
from .space import Space
from .storage import Storage, execute_transfer, plan_transfer, remote_config
from .store import Store, parse_dest

app = typer.Typer(add_completion=False, help="ionflow: local-first typed flows with optional S3 sharing")
artifact_app = typer.Typer(help="Shared, typed files and bundles")
app.add_typer(artifact_app, name="artifacts")


@app.callback(invoke_without_command=True)
def _default(ctx: typer.Context):
    """Launch the TUI when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        from . import tui as _tui

        _tui.run_tui()  # config resolution (incl. the flows package) is the TUI's own job


def _load(spec):
    """Resolve 'package.module:ClassName' to the Flow class."""
    mod, name = spec.split(":")
    return getattr(importlib.import_module(mod), name)


def _config_or_setup():
    """Load config, or guide the user to `setup`. Used by every real command."""
    try:
        cfg = Config.load()
    except ConfigMissing:
        if sys.stdin.isatty():
            typer.echo("ionflow isn't configured for this repo yet — let's set it up.\n")
            setup()
            typer.echo("\nSetup complete. Commit the change shown above, then re-run.")
            raise typer.Exit(0) from None
        typer.echo("No ionflow config found. Run `ionflow setup`.")
        raise typer.Exit(1) from None
    return cfg


def _add_to_gitignore(root):
    gi = root / ".gitignore"
    lines = gi.read_text().splitlines() if gi.exists() else []
    if f"{CONFIG_DIR}/" not in lines:
        lines.append(f"{CONFIG_DIR}/")
        gi.write_text("\n".join(lines) + "\n")


@app.command()
def setup(
    local: bool = typer.Option(
        False,
        "--local",
        help="Skip the optional remote prompt",
    ),
):
    """Configure the store for this repo (writes .ionflow/config)."""
    root = git.repo_root()
    if root is None:
        typer.echo("Warning: not inside a git repository; ionflow runs require one.")
        root = Path.cwd()
    else:
        root = Path(root)

    cur = read_config(str(root))
    vals = {}
    for key, label, default in SETUP_FIELDS:
        if local and key in ("remote", "endpoint", "region", "prefix"):
            vals[key] = cur.get(key, default)
            continue
        preset = cur.get(key, default)
        vals[key] = typer.prompt(label, default=preset, show_default=bool(preset))
    cfg = config_from_setup(vals)

    typer.echo(f"\nWrote {write_config(root, cfg)}")
    ok, msg = Store.verify(Config.load(root))
    typer.echo(("OK: " if ok else "Warning: ") + msg)

    if typer.confirm(f"Add {CONFIG_DIR}/ to .gitignore (keep it local)?", default=True):
        _add_to_gitignore(root)
        typer.echo(f"Added {CONFIG_DIR}/ to .gitignore — commit the .gitignore change, then run your flow.")
    else:
        typer.echo(f"Run `git add {CONFIG_DIR}/{CONFIG_FILE}` and commit to share it with your team.")


def _parse_params(cols, pairs):
    """Parse repeated `-p KEY=VALUE` flags using the flow's declared param types."""
    out = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        if not sep:
            raise typer.BadParameter(f"expected KEY=VALUE, got '{pair}'")
        if key not in cols:
            raise typer.BadParameter(f"unknown param '{key}' (declared: {', '.join(cols) or 'none'})")
        dtype = cols[key].dtype
        try:
            if typing.get_origin(dtype) in (list, dict):
                out[key] = json.loads(raw)
            else:
                out[key] = {"true": True, "false": False}[raw.lower()] if dtype is bool else dtype(raw)
        except (KeyError, ValueError):
            raise typer.BadParameter(f"param '{key}': expected {dtype.__name__}, got '{raw}'") from None
    return out


def _parse_artifacts(cls, pairs):
    declared = cls.Inputs.declarations() if cls.Inputs else {}
    out = {}
    for pair in pairs:
        name, sep, value = pair.partition("=")
        if not sep or name not in declared:
            raise typer.BadParameter(f"expected declared artifact NAME=ID, got {pair!r}")
        if typing.get_origin(declared[name]) is list:
            out.setdefault(name, []).append(value)
        elif name in out:
            raise typer.BadParameter(f"artifact {name!r} accepts one ID")
        else:
            out[name] = value
    for name, hint in declared.items():
        if typing.get_origin(hint) is list:
            out.setdefault(name, [])
    return out


@app.command()
def run(
    flow: str,
    param: list[str] = typer.Option([], "--param", "-p", help="Flow param as KEY=VALUE (repeatable)"),
    resume: str = typer.Option(
        None,
        "--resume",
        metavar="RUN_ID|latest",
        help="Resume from a previous run, reusing its completed steps: an origin "
        "run_id, or 'latest' for the newest complete run (keyed fan-outs "
        "recompute only changed items; a first run falls through fresh)",
    ),
    allow_commit_drift: bool = typer.Option(
        False,
        "--allow-commit-drift",
        help="Resume even though HEAD differs from the origin run's commit (reused steps keep their old-code outputs)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="With --resume: report what each step would reuse vs compute (assuming unchanged work-lists), then exit",
    ),
    max_workers: int = typer.Option(
        None, "--max-workers", help="Parallel item workers, one per fan-out item (default: CPU count)"
    ),
    use_processes: bool = typer.Option(
        True, "--processes/--threads", help="Run fan-out items in worker processes or threads"
    ),
    branch_retries: int = typer.Option(0, "--branch-retries", help="In-worker retries for a failed branch sub-step"),
    artifact: list[str] = typer.Option([], "--artifact", "-a", help="Artifact NAME=ID; repeat for collection inputs"),
    reuse: str = typer.Option("auto", "--reuse", help="Artifact reuse: auto (local or download), local, or never"),
    no_reuse: bool = typer.Option(False, "--no-reuse", help="Execute every step, including all child flows"),
):
    """Run FLOW (given as 'module:ClassName'); the current directory is the git repo."""
    from . import runner as _runner

    if reuse not in ("auto", "local", "never"):
        raise typer.BadParameter("--reuse must be auto, local, or never")
    cls = _load(flow)
    if dry_run:
        if artifact or param:
            raise typer.BadParameter("resume inherits its params and artifacts; omit -p and -a")
        if not resume:
            raise typer.BadParameter("--dry-run previews a resume; pass --resume as well")
        origin, table = _runner.preview(cls, _config_or_setup(), resume, reuse="never" if no_reuse else reuse)
        typer.echo(f"resume from {origin} (assuming unchanged work-lists):")
        for step, (reused, computed) in table.items():
            typer.echo(f"  {step}: reuse {reused}, compute {'?' if computed < 0 else computed}")
        raise typer.Exit(0)
    params = _parse_params(cls.params_columns(), param)
    typer.echo(
        cls.run(
            _config_or_setup(),
            params=params,
            artifacts=_parse_artifacts(cls, artifact) if artifact or not resume else None,
            resume=resume,
            allow_commit_drift=allow_commit_drift,
            max_workers=max_workers,
            use_processes=use_processes,
            branch_retries=branch_retries,
            reuse="never" if no_reuse else reuse,
        )
    )


@app.command()
def publish(
    flow: str,
    run_id: str,
    step: str,
    table: str,
    where: str = typer.Option(None, "--where", help="SQL predicate selecting the rows to publish"),
    to: str = typer.Option(None, "--to", help="Publish lake location (s3://bucket/prefix); default <prefix>/published"),
):
    """Bless STEP's output from RUN_ID as TABLE in the publish DuckLake (atomic replace)."""
    m = _load(flow).publish(run_id, step, table, config=_config_or_setup(), where=where, dest=to)
    typer.echo(f"published {m['flow']}/{m['step']} from run {m['run_id']} as table '{table}'")


@app.command()
def ls(flow: str):
    """List runs recorded in the ledger (stale running runs are marked)."""
    cfg = _config_or_setup()
    if cfg.local_dir:
        Storage(cfg).refresh()
    rel = ledger_with_liveness(_load(flow), cfg)
    rel.project("run_id, status, started, finished, commit, local, s3").order("started").show()


@app.command()
def query(flow: str, sql: str):
    """Run a SQL query against FLOW's tables (views: each step + `ledger` + `metrics`)."""
    _load(flow).duck(_config_or_setup()).sql(sql).show()


@app.command()
def stats(flow: str, run_id: str = typer.Argument(None, help="defaults to the latest run")):
    """Per-step resource usage (duration, peak memory, cores, threads) for a run."""
    con = _load(flow).duck(_config_or_setup())
    if run_id is None:
        # newest by start time, ANY status — a failed run's resource usage is
        # exactly what stats gets asked about. Deliberately different from
        # `run --resume latest`, which needs the newest COMPLETE run
        # (runner._latest_complete).
        latest = con.sql("SELECT run_id FROM ledger ORDER BY started DESC LIMIT 1").fetchone()
        if latest is None:
            typer.echo("no runs recorded")
            raise typer.Exit(1)
        run_id = latest[0]
    typer.echo(f"run {run_id}")
    step_stats(con, run_id).show()


@app.command()
def sync(
    flow: str = typer.Argument(None, help="Flow as module:ClassName; omit for artifact-only transfers"),
    to: str = typer.Option(None, "--to", help="Explicit upload destination; otherwise use the configured remote"),
    endpoint: str = typer.Option(None, "--endpoint", help="S3 endpoint URL (blank for AWS)"),
    region: str = typer.Option("us-east-1", "--region"),
    run_id: str = typer.Option(None, "--run", help="Copy one run and its complete reference closure"),
    artifact: list[str] = typer.Option([], "--artifact", help="Artifact ID to transfer (repeatable)"),
    include_artifact: list[str] | None = typer.Option(
        None, "--include-artifact", help="Exact artifact payload selection, overriding defaults (repeatable)"
    ),
    metadata_only: bool = typer.Option(
        False, "--metadata-only", help="Exclude all artifact payloads; run tables still copy"
    ),
    down: bool = typer.Option(False, "--down", help="Download from the configured remote"),
    plan: bool = typer.Option(False, "--plan", help="Print the transfer plan without copying payloads"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Approve the displayed transfer direction"),
):
    """Explicitly upload or download a run or selected artifacts."""
    cfg = _config_or_setup()
    if not flow and not artifact:
        raise typer.BadParameter("select a flow or at least one --artifact")
    if down and to:
        raise typer.BadParameter("--to selects an upload; omit it with --down")
    if metadata_only and include_artifact:
        raise typer.BadParameter("--metadata-only and --include-artifact cannot be combined")
    included = [] if metadata_only else include_artifact
    flow_id = _load(flow).flow_id() if flow else None
    workspace = Storage(cfg)
    selection = (
        plan_transfer(
            workspace.local,
            Store.of(_destination(cfg, to, endpoint, region)),
            flow_id,
            run_id,
            artifact,
            include_artifacts=included,
            direction="up",
        )
        if to
        else workspace.plan("down" if down else "up", flow_id, run_id, artifact, include_artifacts=included)
    )
    typer.echo(json.dumps(selection.summary()))
    if plan:
        return
    if not yes and not typer.confirm("Transfer these payloads in the displayed direction?", default=False):
        raise typer.Abort()
    result = execute_transfer(selection) if to else workspace.transfer(selection)
    typer.echo(json.dumps(result))


@app.command()
def refresh():
    """Synchronize ledger, inventory and lineage metadata without payloads."""
    result = Storage(_config_or_setup()).refresh()
    typer.echo(json.dumps(result))
    if result["error"] or result["conflicts"]:
        raise typer.Exit(1)


def _destination(cfg, to, endpoint=None, region="us-east-1"):
    if not to.startswith("s3://"):
        return Config(local_dir=str(Path(to).expanduser().resolve()), prefix=cfg.prefix)
    bucket, prefix = parse_dest(to)
    if not cfg.access_key and not Store.credentials():
        raise typer.BadParameter("No AWS credentials found for the target bucket")
    return Config(
        bucket=bucket,
        prefix=prefix or cfg.prefix,
        endpoint_url=endpoint,
        region=region,
        access_key=cfg.access_key,
        secret_key=cfg.secret_key,
    )


@artifact_app.command("add")
def artifact_add(
    schema: str,
    path: Path,
    metadata: str = typer.Option("{}", "--metadata", help="JSON object matching the artifact schema"),
    name: str = typer.Option(None, "--name"),
):
    """Publish PATH using an artifact schema given as module:ClassName."""
    candidate = _load(schema).from_path(path, name=name, **json.loads(metadata))
    typer.echo(Artifacts(_config_or_setup()).put(candidate).id)


@artifact_app.command("ls")
def artifact_ls(kind: str = typer.Option(None, "--kind")):
    """List inventory without importing a consuming flow."""
    workspace = Storage(_config_or_setup())
    workspace.refresh()
    typer.echo(json.dumps(workspace.artifacts(kind), indent=2))


@artifact_app.command("show")
def artifact_show(
    artifact_id: str, s3: bool = typer.Option(False, "--s3", help="Inspect the remote metadata revision")
):
    """Show a reference and its current metadata."""
    config = _config_or_setup()
    typer.echo(json.dumps(Artifacts(remote_config(config) if s3 else config).get(artifact_id).snapshot(), indent=2))


@artifact_app.command("update")
def artifact_update(
    artifact_id: str,
    revision: int = typer.Option(..., "--revision"),
    name: str = typer.Option(None, "--name"),
    metadata: str = typer.Option(None, "--metadata"),
):
    """Edit metadata only if the supplied revision is still current."""
    ref = Artifacts(_config_or_setup()).update(
        artifact_id, revision=revision, name=name, metadata=json.loads(metadata) if metadata is not None else None
    )
    typer.echo(json.dumps(ref.snapshot(), indent=2))


@artifact_app.command("resolve")
def artifact_resolve(
    artifact_id: str,
    prefer: str = typer.Option(..., "--prefer", help="Metadata to retain: local or s3"),
    local_revision: int = typer.Option(..., "--local-revision"),
    s3_revision: int = typer.Option(..., "--s3-revision"),
):
    """Resolve a metadata conflict after inspecting both revisions; copy no payloads."""
    record = Storage(_config_or_setup()).resolve_artifact(
        artifact_id, prefer=prefer, local_revision=local_revision, remote_revision=s3_revision
    )
    typer.echo(json.dumps(record, indent=2))


@artifact_app.command("path")
def artifact_path(artifact_id: str):
    """Materialize and print a verified local path."""
    typer.echo(Artifacts(_config_or_setup()).get(artifact_id).local_path())


@artifact_app.command("lineage")
def artifact_lineage(artifact_id: str):
    """Show parents, original producers, and step-scoped consumers."""
    registry = Artifacts(_config_or_setup())
    typer.echo(
        json.dumps(
            {
                "parents": registry.parents(artifact_id),
                "producers": registry.producers(artifact_id),
                "consumers": registry.consumers(artifact_id),
            },
            indent=2,
        )
    )


@artifact_app.command("query")
def artifact_query(sql: str):
    """Query artifact inventory, files, and cross-flow activity views."""
    Artifacts(_config_or_setup()).duck().sql(sql).show()


@artifact_app.command("sync")
def artifact_sync(
    artifact_id: list[str] = typer.Argument(None),
    to: str = typer.Option(..., "--to"),
    endpoint: str = typer.Option(None, "--endpoint"),
    region: str = typer.Option("us-east-1", "--region"),
    include_artifact: list[str] | None = typer.Option(
        None, "--include-artifact", help="Exact artifact payload selection, overriding defaults (repeatable)"
    ),
    metadata_only: bool = typer.Option(False, "--metadata-only", help="Copy artifact metadata without payloads"),
    plan: bool = typer.Option(False, "--plan", help="Show the transfer without copying payloads"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Approve this upload"),
):
    """Copy selected artifacts and their parents; omit IDs to copy the inventory."""
    cfg = _config_or_setup()
    if metadata_only and include_artifact:
        raise typer.BadParameter("--metadata-only and --include-artifact cannot be combined")
    ids = artifact_id or [r["id"] for r in Artifacts(cfg).records()]
    selection = plan_transfer(
        Store.of(cfg),
        Store.of(_destination(cfg, to, endpoint, region)),
        artifact_ids=ids,
        include_artifacts=[] if metadata_only else include_artifact,
        direction="up",
    )
    typer.echo(json.dumps(selection.summary()))
    if plan:
        return
    if not yes and not typer.confirm("Upload these payloads in the displayed direction?", default=False):
        raise typer.Abort()
    typer.echo(json.dumps(execute_transfer(selection)))


@artifact_app.command("recover")
def artifact_recover(replace: bool = typer.Option(False, "--replace")):
    """Rebuild the registry from descriptors and revision exports."""
    registry = Artifacts(_config_or_setup())
    if registry.lake.register.read()[0] is not None and not replace:
        raise typer.BadParameter("registry exists; pass --replace to rebuild")
    typer.echo(f"recovered {registry.recover()} artifact(s)")


@app.command("catalog-backfill")
def catalog_backfill(
    flow: str,
    replace: bool = typer.Option(False, "--replace", help="Rebuild an existing catalog from the run folders"),
):
    """Rebuild a flow's catalog from its run folders (recovery for a lost register)."""
    from .lake import backfill

    cls = _load(flow)
    n = backfill(cls, Space.for_flow(Store.of(_config_or_setup()), cls), replace=replace)
    typer.echo(f"registered {n} complete run(s) into the catalog")


def main():
    app()
