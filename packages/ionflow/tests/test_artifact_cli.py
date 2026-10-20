import json

from typer.testing import CliRunner

from ionflow import Artifacts, cli


def test_cli_import_run_query_edit_and_resume(cfg, clean_repo, monkeypatch, tmp_path):
    monkeypatch.chdir(clean_repo)
    monkeypatch.setattr(cli, "_config_or_setup", lambda: cfg)
    runner = CliRunner()
    path = tmp_path / "document.txt"
    path.write_text("one two three")
    imported = runner.invoke(cli.app, ["artifacts", "add", "examples.artifacts:TextFile", str(path)])
    assert imported.exit_code == 0, imported.output
    aid = imported.output.strip()
    run = runner.invoke(cli.app, ["run", "examples.artifacts:CountWords", "--threads", "-a", f"documents={aid}"])
    assert run.exit_code == 0, run.exception
    result = runner.invoke(cli.app, ["query", "examples.artifacts:CountWords", "SELECT words FROM report"])
    assert result.exit_code == 0 and "3" in result.output
    selected = runner.invoke(cli.app, ["artifacts", "query", "SELECT count(*) FROM artifact_bindings"])
    assert selected.exit_code == 0 and "1" in selected.output
    edited = runner.invoke(cli.app, ["artifacts", "update", aid, "--revision", "1", "--name", "reference"])
    assert edited.exit_code == 0 and json.loads(edited.output)["revision"] == 2
    assert runner.invoke(cli.app, ["artifacts", "update", aid, "--revision", "1", "--name", "stale"]).exit_code != 0
    resumed = runner.invoke(
        cli.app, ["run", "examples.artifacts:CountWords", "--threads", "--resume", run.output.strip()]
    )
    assert resumed.exit_code == 0, resumed.exception
    registry = Artifacts(cfg)
    assert registry.parents(aid) == [] and registry.producers(aid) == []
    assert len(registry.records()) == 2
