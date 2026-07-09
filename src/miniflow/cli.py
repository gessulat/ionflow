import importlib
import sys
from pathlib import Path

import typer

from . import git, storage
from .config import CONFIG_DIR, CONFIG_FILE, Config, ConfigMissing, read_config, write_config

app = typer.Typer(add_completion=False, help="miniflow: minimal flows on S3")


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
            typer.echo("miniflow isn't configured for this repo yet — let's set it up.\n")
            setup()
            typer.echo("\nSetup complete. Commit the change shown above, then re-run.")
            raise typer.Exit(0)
        typer.echo("No miniflow config found. Run `miniflow setup`.")
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
    """Configure the S3 bucket for this repo (writes .miniflow/config)."""
    root = git.repo_root()
    if root is None:
        typer.echo("Warning: not inside a git repository; miniflow runs require one.")
        root = Path.cwd()
    else:
        root = Path(root)

    cur = read_config(str(root))
    bucket = typer.prompt("S3 bucket", default=cur.get("bucket"))
    prefix = typer.prompt("Key prefix", default=cur.get("prefix", "miniflow"))
    endpoint = typer.prompt(
        "S3 endpoint URL (blank for AWS)", default=cur.get("endpoint", ""), show_default=False
    )
    region = typer.prompt("Region", default=cur.get("region", "us-east-1"))
    cfg = Config(bucket=bucket, prefix=prefix, endpoint_url=endpoint or None, region=region)

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


@app.command()
def run(flow: str, repo: str = "."):
    """Run FLOW (given as 'module:ClassName')."""
    typer.echo(_load(flow).run(_config_or_setup(), repo=repo))


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
    """Run a SQL query against FLOW's tables (views: each step + `ledger`)."""
    _load(flow).duck(_config_or_setup()).sql(sql).show()


def main():
    app()
