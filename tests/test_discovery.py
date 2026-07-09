import textwrap

from miniflow import discovery


def _make_pkg(root, name, flows):
    pkg = root / name
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    for mod, cls in flows:
        (pkg / f"{mod}.py").write_text(
            textwrap.dedent(f"""
            from miniflow import Flow, Table
            class {cls}Rows(Table):
                x: int
            class {cls}(Flow):
                def data(self) -> {cls}Rows: ...
            """)
        )
    return pkg


def test_discover_flows_walks_submodules(tmp_path, monkeypatch):
    _make_pkg(tmp_path, "mypkg", [("a", "FlowA"), ("b", "FlowB")])
    monkeypatch.syspath_prepend(str(tmp_path))
    flows = discovery.discover_flows("mypkg")
    assert set(flows) == {"FlowA", "FlowB"}
    assert flows["FlowA"].__name__ == "FlowA"


def test_discover_flows_isolated_by_package(tmp_path, monkeypatch):
    _make_pkg(tmp_path, "pkgx", [("m", "OnlyX")])
    monkeypatch.syspath_prepend(str(tmp_path))
    flows = discovery.discover_flows("pkgx")
    assert set(flows) == {"OnlyX"}  # other packages' flows are filtered out


def test_flows_with_data(s3cfg, clean_repo):
    from tui_flows.pipeline import Alpha

    assert discovery.flows_with_data(s3cfg) == set()
    Alpha.run(s3cfg, repo=clean_repo, use_processes=False)
    assert "Alpha" in discovery.flows_with_data(s3cfg)


def test_schema_drift_detects_change(s3cfg, clean_repo, monkeypatch):
    from miniflow import storage
    from tui_flows.pipeline import Alpha

    Alpha.run(s3cfg, repo=clean_repo, use_processes=False)
    con = storage.connect(s3cfg)
    _, samples = discovery.scan_runs(s3cfg)
    assert discovery.schema_drift(Alpha, con, s3cfg, samples["Alpha"]) == []

    # pretend the code now declares `x` as text instead of the stored bigint
    monkeypatch.setattr(Alpha._dag.steps["data"].output_table.columns()["x"], "dtype", str)
    issues = discovery.schema_drift(Alpha, con, s3cfg, samples["Alpha"])
    assert any("data" in i and "x" in i for i in issues)
