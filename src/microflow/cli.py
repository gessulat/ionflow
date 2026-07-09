import importlib
import sys
from pathlib import Path

import typer

from . import git, storage
from .config import CONFIG_DIR, CONFIG_FILE, Config, ConfigMissing, read_config, write_config
from .query import step_stats

app = typer.Typer(add_completion=False, help="microflow: minimal flows on S3")


@app.callback(invoke_without_command=True)
def _default(ctx: typer.Context):
    """Launch the TUI when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        _launch_tui()


def _launch_tui(flows=None):
    from . import tui as _tui

    _tui.run_tui(flows_override=flows)  # config resolution is the TUI's own job


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
            typer.echo("microflow isn't configured for this repo yet — let's set it up.\n")
            setup()
            typer.echo("\nSetup complete. Commit the change shown above, then re-run.")
            raise typer.Exit(0)
        typer.echo("No microflow config found. Run `microflow setup`.")
        raise typer.Exit(1)
    if not storage.aws_credentials_available():
        typer.echo(
            "No AWS credentials found. Provide them via environment variables, "
            "~/.aws/credentials, or an IAM role."
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
    """Configure the S3 bucket for this repo (writes .microflow/config)."""
    root = git.repo_root()
    if root is None:
        typer.echo("Warning: not inside a git repository; microflow runs require one.")
        root = Path.cwd()
    else:
        root = Path(root)

    cur = read_config(str(root))
    bucket = typer.prompt("S3 bucket", default=cur.get("bucket"))
    prefix = typer.prompt("Key prefix", default=cur.get("prefix", "microflow"))
    endpoint = typer.prompt(
        "S3 endpoint URL (blank for AWS)", default=cur.get("endpoint", ""), show_default=False
    )
    region = typer.prompt("Region", default=cur.get("region", "us-east-1"))
    flows = typer.prompt(
        "Flows package for the TUI (e.g. myproject.flows, blank to skip)",
        default=cur.get("flows", ""), show_default=False,
    )
    cfg = Config(
        bucket=bucket, prefix=prefix, endpoint_url=endpoint or None,
        region=region, flows=flows or None,
    )

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
            raise typer.BadParameter(f"param '{key}': expected {dtype.__name__}, got '{raw}'")
    return out


@app.command()
def run(
    flow: str,
    repo: str = ".",
    param: list[str] = typer.Option([], "--param", "-p", help="Flow param as KEY=VALUE (repeatable)"),
):
    """Run FLOW (given as 'module:ClassName')."""
    cls = _load(flow)
    params = _parse_params(cls.params_columns(), param)
    typer.echo(cls.run(_config_or_setup(), repo=repo, params=params))


@app.command()
def resume(flow: str, origin_run_id: str, repo: str = "."):
    """Resume FLOW from a previous run, reusing its completed steps."""
    typer.echo(_load(flow).resume(origin_run_id, _config_or_setup(), repo=repo))


@app.command()
def ls(flow: str):
    """List runs recorded in the ledger."""
    con = _load(flow).duck(_config_or_setup())
    con.sql(
        "SELECT run_id, status, started, finished, commit FROM ledger ORDER BY started"
    ).show()


@app.command()
def query(flow: str, sql: str):
    """Run a SQL query against FLOW's tables (views: each step + `ledger` + `metrics`)."""
    _load(flow).duck(_config_or_setup()).sql(sql).show()


@app.command()
def stats(flow: str, run_id: str = typer.Argument(None, help="defaults to the latest run")):
    """Per-step resource usage (duration, peak memory, cores, threads) for a run."""
    con = _load(flow).duck(_config_or_setup())
    if run_id is None:
        latest = con.sql("SELECT run_id FROM ledger ORDER BY started DESC LIMIT 1").fetchone()
        if latest is None:
            typer.echo("no runs recorded")
            raise typer.Exit(1)
        run_id = latest[0]
    typer.echo(f"run {run_id}")
    step_stats(con, run_id).show()


@app.command()
def tui(flows: str = typer.Argument(None, help="Flows package (overrides config)")):
    """Launch the interactive terminal UI."""
    _launch_tui(flows)


def main():
    app()
