import importlib

import typer

from .config import Config

app = typer.Typer(add_completion=False, help="miniflow: minimal flows on S3")


def _load(spec):
    """Resolve 'package.module:ClassName' to the Flow class."""
    mod, name = spec.split(":")
    return getattr(importlib.import_module(mod), name)


@app.command()
def run(flow: str, repo: str = "."):
    """Run FLOW (given as 'module:ClassName')."""
    typer.echo(_load(flow).run(Config.from_env(), repo=repo))


@app.command()
def resume(flow: str, origin_run_id: str, repo: str = "."):
    """Resume FLOW from a previous run, reusing its completed steps."""
    typer.echo(_load(flow).resume(origin_run_id, Config.from_env(), repo=repo))


@app.command()
def ls(flow: str):
    """List runs recorded in the ledger."""
    con = _load(flow).duck(Config.from_env())
    con.sql(
        "SELECT run_id, status, started, finished, commit FROM ledger ORDER BY started"
    ).show()


@app.command()
def query(flow: str, sql: str):
    """Run a SQL query against FLOW's tables (views: each step + `ledger`)."""
    _load(flow).duck(Config.from_env()).sql(sql).show()


def main():
    app()
