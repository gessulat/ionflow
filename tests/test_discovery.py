import textwrap

from ionflow import discovery


def _make_pkg(root, name, flows):
    pkg = root / name
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    for mod, cls in flows:
        (pkg / f"{mod}.py").write_text(
            textwrap.dedent(f"""
            from ionflow import Flow, Table
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
    assert set(flows) == {"mypkg.a.FlowA", "mypkg.b.FlowB"}  # keys are flow_ids
    assert flows["mypkg.a.FlowA"].flow_id() == "mypkg.a.FlowA"


def test_discover_flows_isolated_by_package(tmp_path, monkeypatch):
    _make_pkg(tmp_path, "pkgx", [("m", "OnlyX")])
    monkeypatch.syspath_prepend(str(tmp_path))
    flows = discovery.discover_flows("pkgx")
    assert set(flows) == {"pkgx.m.OnlyX"}  # other packages' flows are filtered out


def test_flows_with_data(s3cfg):
    from ionflow import storage

    assert discovery.flows_with_data(s3cfg) == set()
    # key presence is all that matters here; no need for a full flow run
    storage.s3_client(s3cfg).put_object(Bucket=s3cfg.bucket, Key=f"{s3cfg.prefix}/Alpha/ledger.parquet", Body=b"")
    assert discovery.flows_with_data(s3cfg) == {"Alpha"}


def test_schema_drift_detects_change(s3cfg, repo, monkeypatch):
    from tui_flows.pipeline import Alpha

    from ionflow import storage

    Alpha.run(s3cfg, repo=repo, use_processes=False)
    con = storage.connect(s3cfg)
    _, samples, _ = discovery.scan_runs(s3cfg)
    assert discovery.schema_drift(Alpha, con, s3cfg, samples[Alpha.flow_id()]) == []

    # pretend the code now declares `x` as text instead of the stored bigint
    monkeypatch.setattr(Alpha._dag.steps["data"].output_table.columns()["x"], "dtype", str)
    issues = discovery.schema_drift(Alpha, con, s3cfg, samples[Alpha.flow_id()])
    assert any("data" in i and "x" in i for i in issues)
