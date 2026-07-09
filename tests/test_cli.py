import pytest
import tomllib

from typer.testing import CliRunner

from miniflow import cli, config

runner = CliRunner()


@pytest.mark.slow
def test_cli_run_and_query(s3cfg, repo, monkeypatch):
    monkeypatch.setattr(config.Config, "load", classmethod(lambda cls, start=".": s3cfg))
    monkeypatch.setattr("miniflow.storage.aws_credentials_available", lambda: True)

    r = runner.invoke(cli.app, ["run", "examples.etl:ETLFlow", "--repo", repo])
    assert r.exit_code == 0, r.output
    run_id = r.output.strip()

    q = runner.invoke(
        cli.app,
        ["query", "examples.etl:ETLFlow", f"SELECT grand_total FROM summary WHERE run_id='{run_id}'"],
    )
    assert q.exit_code == 0, q.output
    assert "10.0" in q.output

    ls = runner.invoke(cli.app, ["ls", "examples.etl:ETLFlow"])
    assert ls.exit_code == 0, ls.output
    assert run_id in ls.output


def test_cli_setup_writes_config(clean_repo, monkeypatch):
    monkeypatch.chdir(clean_repo)  # own repo: setup writes .miniflow/ + .gitignore
    monkeypatch.setattr("miniflow.storage.aws_credentials_available", lambda: False)

    # bucket, prefix, endpoint(blank), region, flows(blank), gitignore? -> yes
    r = runner.invoke(cli.app, ["setup"], input="mybucket\nminiflow\n\nus-east-1\n\ny\n")
    assert r.exit_code == 0, r.output

    path = config.find_config(clean_repo)  # resolves symlinks; robust on macOS
    assert path is not None
    cfg = tomllib.loads(path.read_text())
    assert cfg == {"bucket": "mybucket", "prefix": "miniflow", "region": "us-east-1"}
    assert ".miniflow/" in (path.parent.parent / ".gitignore").read_text()


def test_cli_run_without_config_errors(clean_repo, monkeypatch):
    monkeypatch.chdir(clean_repo)  # own repo: must contain no .miniflow/config
    monkeypatch.delenv("MINIFLOW_BUCKET", raising=False)

    r = runner.invoke(cli.app, ["run", "examples.etl:ETLFlow"])  # CliRunner stdin is not a tty
    assert r.exit_code == 1
    assert "miniflow setup" in r.output
