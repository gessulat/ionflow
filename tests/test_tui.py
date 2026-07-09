from textual.widgets import TabPane

from miniflow.tui import FlowTable, MiniflowTUI, OverviewScreen, RunDetailScreen, SetupScreen


async def test_setup_screen_when_unconfigured(tmp_path, monkeypatch):
    monkeypatch.delenv("MINIFLOW_BUCKET", raising=False)
    monkeypatch.chdir(tmp_path)
    app = MiniflowTUI()  # config=None -> Config.load() raises ConfigMissing
    async with app.run_test() as pilot:
        await pilot.pause()
        assert isinstance(app.screen, SetupScreen)


async def test_overview_tabs_and_drilldown(s3cfg, clean_repo, monkeypatch):
    monkeypatch.setattr("miniflow.storage.aws_credentials_available", lambda: True)
    from tui_flows.pipeline import Alpha, Beta

    Alpha.run(s3cfg, repo=clean_repo, use_processes=False)
    Beta.run(s3cfg, repo=clean_repo, use_processes=False)
    s3cfg.flows = "tui_flows"

    app = MiniflowTUI(config=s3cfg)
    async with app.run_test() as pilot:
        await pilot.pause()
        assert isinstance(app.screen, OverviewScreen)

        assert {p.id for p in app.screen.query(TabPane)} == {"flow-Alpha", "flow-Beta"}

        table = app.screen.query_one("#table-Alpha", FlowTable)
        labels = [str(c.label) for c in table.columns.values()]
        assert "run_id" in labels and "note" in labels  # std + custom ledger column
        assert table.row_count == 1

        await pilot.press("enter")  # drill into the highlighted run
        await pilot.pause()
        assert isinstance(app.screen, RunDetailScreen)

        await pilot.press("escape")  # back to overview
        await pilot.pause()
        assert isinstance(app.screen, OverviewScreen)

        # Tab cycles to the next flow
        await pilot.press("tab")
        await pilot.pause()
        from textual.widgets import TabbedContent

        assert app.screen.query_one(TabbedContent).active == "flow-Beta"
