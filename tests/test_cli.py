from typer.testing import CliRunner

from miniflow import cli, config

runner = CliRunner()


def test_cli_run_and_query(s3cfg, clean_repo, monkeypatch):
    monkeypatch.setattr(config.Config, "from_env", classmethod(lambda cls: s3cfg))

    r = runner.invoke(cli.app, ["run", "examples.etl:ETLFlow", "--repo", clean_repo])
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
