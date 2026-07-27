import importlib
import sys
from pathlib import Path

import typer

from . import git, storage
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

app = typer.Typer(add_completion=False, help="ionflow: minimal flows on S3")


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
    if not storage.aws_credentials_available():
        typer.echo(
            "No AWS credentials found. Provide them via environment variables, ~/.aws/credentials, or an IAM role."
        )
        raise typer.Exit(1)
    return cfg


def _add_to_gitignore(root):
    gi = root / ".gitignore"
    lines = gi.read_text().splitlines() if gi.exists() else []
    if f"{CONFIG_DIR}/" not in lines:
        lines.append(f"{CONFIG_DIR}/")
        gi.write_text("\n".join(lines) + "\n")


@app.command()
def setup():
    """Configure the S3 bucket for this repo (writes .ionflow/config)."""
    root = git.repo_root()
    if root is None:
        typer.echo("Warning: not inside a git repository; ionflow runs require one.")
        root = Path.cwd()
    else:
        root = Path(root)

    cur = read_config(str(root))
    vals = {}
    for key, label, default in SETUP_FIELDS:
        preset = cur.get(key, default)  # None (required) prompts until non-empty
        vals[key] = typer.prompt(label, default=preset, show_default=bool(preset))
    cfg = config_from_setup(vals)

    if not storage.aws_credentials_available():
        typer.echo("Warning: no AWS credentials found (env / ~/.aws / IAM). Set them before running.")
    else:
        ok, msg = storage.verify_access(cfg)
        typer.echo(("OK: " if ok else "Warning: ") + msg)

    typer.echo(f"\nWrote {write_config(root, cfg)}")

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
            out[key] = {"true": True, "false": False}[raw.lower()] if dtype is bool else dtype(raw)
        except (KeyError, ValueError):
            raise typer.BadParameter(f"param '{key}': expected {dtype.__name__}, got '{raw}'") from None
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
    max_workers: int = typer.Option(
        None, "--max-workers", help="Parallel item workers, one per fan-out item (default: CPU count)"
    ),
    use_processes: bool = typer.Option(
        True, "--processes/--threads", help="Run fan-out items in worker processes or threads"
    ),
    branch_retries: int = typer.Option(0, "--branch-retries", help="In-worker retries for a failed branch sub-step"),
):
    """Run FLOW (given as 'module:ClassName'); the current directory is the git repo."""
    cls = _load(flow)
    params = _parse_params(cls.params_columns(), param)
    typer.echo(
        cls.run(
            _config_or_setup(),
            params=params,
            resume=resume,
            allow_commit_drift=allow_commit_drift,
            max_workers=max_workers,
            use_processes=use_processes,
            branch_retries=branch_retries,
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
    rel = ledger_with_liveness(_load(flow), _config_or_setup())
    rel.project("run_id, status, started, finished, commit").order("started").show()


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


@app.command("catalog-backfill")
def catalog_backfill(
    flow: str,
    replace: bool = typer.Option(False, "--replace", help="Rebuild an existing catalog from the run folders"),
):
    """Rebuild a flow's catalog from its run folders (recovery for a lost register)."""
    from . import catalog

    cls = _load(flow)
    n = catalog.backfill(cls, _config_or_setup(), replace=replace)
    typer.echo(f"registered {n} complete run(s) into the catalog")


def main():
    app()
