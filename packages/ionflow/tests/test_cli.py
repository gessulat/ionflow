import tomllib

import pytest
from typer.testing import CliRunner

from ionflow import cli, config

runner = CliRunner()


@pytest.mark.slow
def test_cli_run_and_query(s3cfg, clean_repo, monkeypatch):
    monkeypatch.chdir(clean_repo)  # the CLI treats cwd as the git repo
    monkeypatch.setattr(config.Config, "load", classmethod(lambda cls, start=".": s3cfg))
    monkeypatch.setattr("ionflow.store.Store.credentials", staticmethod(lambda: True))

    r = runner.invoke(cli.app, ["run", "examples.embed:EmbedDocs"])
    assert r.exit_code == 0, r.output
    run_id = r.output.strip()

    q = runner.invoke(
        cli.app,
        ["query", "examples.embed:EmbedDocs", f"SELECT n_docs FROM summary WHERE run_id='{run_id}'"],
    )
    assert q.exit_code == 0, q.output
    assert "4" in q.output

    ls = runner.invoke(cli.app, ["ls", "examples.embed:EmbedDocs"])
    assert ls.exit_code == 0, ls.output
    assert run_id in ls.output

    st = runner.invoke(cli.app, ["stats", "examples.embed:EmbedDocs"])  # defaults to latest
    assert st.exit_code == 0, st.output
    assert run_id in st.output and "peak_rss_mb" in st.output and "summary" in st.output


def test_cli_forwards_worker_knobs(s3cfg, monkeypatch):
    """run passes the resume/sizing knobs through to runner.run (no flow executed)."""
    monkeypatch.setattr(config.Config, "load", classmethod(lambda cls, start=".": s3cfg))
    monkeypatch.setattr("ionflow.store.Store.credentials", staticmethod(lambda: True))
    seen = {}
    monkeypatch.setattr(
        "ionflow.runner.run",
        lambda flow_cls, cfg, repo=".", **kw: seen.update(kw) or "rid",
    )

    r = runner.invoke(
        cli.app,
        [
            "run",
            "examples.embed:EmbedDocs",
            "--resume",
            "latest",
            "--allow-commit-drift",
            "--max-workers",
            "2",
            "--threads",
            "--branch-retries",
            "1",
        ],
    )
    assert r.exit_code == 0, r.output
    assert seen["resume"] == "latest" and seen["max_workers"] == 2
    assert seen["use_processes"] is False and seen["branch_retries"] == 1
    assert seen["allow_commit_drift"] is True

    seen.clear()
    r = runner.invoke(cli.app, ["run", "examples.embed:EmbedDocs", "--resume", "some-run", "--processes"])
    assert r.exit_code == 0, r.output
    assert seen["resume"] == "some-run" and seen["use_processes"] is True


def test_cli_setup_writes_config(clean_repo, monkeypatch):
    monkeypatch.chdir(clean_repo)  # own repo: setup writes .ionflow/ + .gitignore
    monkeypatch.setattr("ionflow.store.Store.credentials", staticmethod(lambda: False))

    # bucket, prefix, endpoint(blank), region, flows(blank), gitignore? -> yes
    r = runner.invoke(cli.app, ["setup"], input="mybucket\nionflow\n\nus-east-1\n\ny\n")
    assert r.exit_code == 0, r.output

    path = config.find_config(clean_repo)  # resolves symlinks; robust on macOS
    assert path is not None
    cfg = tomllib.loads(path.read_text())
    assert cfg == {"bucket": "mybucket", "prefix": "ionflow", "region": "us-east-1"}
    assert ".ionflow/" in (path.parent.parent / ".gitignore").read_text()


def test_cli_setup_local_writes_config(clean_repo, monkeypatch):
    monkeypatch.chdir(clean_repo)
    monkeypatch.setattr("ionflow.store.Store.credentials", staticmethod(lambda: False))  # irrelevant locally

    # local dir (default), flows(blank), gitignore? -> yes
    r = runner.invoke(cli.app, ["setup", "--local"], input="\n\ny\n")
    assert r.exit_code == 0, r.output
    assert "OK" in r.output  # store verified without any AWS credentials

    path = config.find_config(clean_repo)
    cfg = tomllib.loads(path.read_text())
    assert cfg == {"local_dir": ".ionflow/data", "prefix": "ionflow"}
    loaded = config.Config.load(str(path.parent.parent))
    assert loaded.bucket is None and loaded.local_dir.endswith(".ionflow/data")


def test_cli_run_without_config_errors(clean_repo, monkeypatch):
    monkeypatch.chdir(clean_repo)  # own repo: must contain no .ionflow/config
    monkeypatch.delenv("IONFLOW_BUCKET", raising=False)

    r = runner.invoke(cli.app, ["run", "examples.embed:EmbedDocs"])  # CliRunner stdin is not a tty
    assert r.exit_code == 1
    assert "ionflow setup" in r.output
