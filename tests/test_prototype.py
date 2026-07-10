import datetime as dt
from pathlib import Path

import pyarrow as pa
import pytest
from flows import ProtoFlow

from microflow import Flow, ledger, storage


def test_untyped_step_rejected_outside_prototype():
    with pytest.raises(ValueError, match="output table annotation"):

        class Sloppy(Flow):
            def data(self): ...

    with pytest.raises(ValueError, match="output table annotation"):

        class SloppyNone(Flow):
            def data(self) -> None: ...  # `-> None` counts as untyped too


def test_prototype_attribute_form():
    class AttrProto(Flow):
        prototype = True

        def data(self) -> None: ...

    assert AttrProto.prototype
    assert AttrProto._dag.steps["data"].output_table is None


def test_untyped_empty_view_is_run_id_only():
    from microflow.query import _empty_view_sql

    assert _empty_view_sql(None) == "SELECT NULL::VARCHAR AS run_id WHERE false"


def test_clean_cli_refuses_stable_flows():
    from typer.testing import CliRunner

    from microflow import cli

    r = CliRunner().invoke(cli.app, ["clean", "flows:FlakyFlow"])
    assert r.exit_code == 1
    assert "not a prototype" in r.output


def test_startup_warnings_skip_prototype_drift(s3cfg):
    from microflow import discovery

    # ProtoFlow has an untyped step: an un-skipped drift check would fail on it
    assert discovery.startup_warnings({"ProtoFlow": ProtoFlow}, s3cfg, {"ProtoFlow"}, {}) == []


def test_scan_runs_keeps_namespaces_apart(s3cfg):
    from microflow import discovery

    client = storage.s3_client(s3cfg)
    client.put_object(Bucket=s3cfg.bucket, Key=f"{s3cfg.prefix}/Alpha/ledger.parquet", Body=b"")
    client.put_object(
        Bucket=s3cfg.bucket, Key=f"{s3cfg.prefix}/_proto/Beta/ledger.parquet", Body=b""
    )
    with_data, samples, proto_with_data = discovery.scan_runs(s3cfg)
    assert with_data == {"Alpha"}  # scratch is invisible to the stable scan
    assert proto_with_data == {"Beta"}
    assert samples == {}


def test_prototype_runs_in_scratch_and_allows_dirty_git(s3cfg, clean_repo):
    (Path(clean_repo) / "wip.txt").write_text("x")  # dirty tree: stable flows would refuse

    run_id = ProtoFlow.run(s3cfg, repo=clean_repo, use_processes=False)

    client = storage.s3_client(s3cfg)
    scratch = storage.list_keys(client, s3cfg.bucket, f"{s3cfg.prefix}/_proto/ProtoFlow/")
    assert any("numbers/part_0.parquet" in k for k in scratch)  # untyped step persisted
    assert not storage.list_keys(client, s3cfg.bucket, f"{s3cfg.prefix}/ProtoFlow/")

    con = ProtoFlow.duck(s3cfg)
    assert con.sql(f"SELECT x FROM numbers WHERE run_id = '{run_id}' ORDER BY x").fetchall() == [
        (1,), (2,), (3,)
    ]
    assert con.sql(f"SELECT n, total FROM total WHERE run_id = '{run_id}'").fetchone() == (3, 6)
    commit = con.sql(f"SELECT commit FROM ledger WHERE run_id = '{run_id}'").fetchone()[0]
    assert commit.startswith("dirty:")


def test_clean_wipes_scratch_only(s3cfg, repo):
    ProtoFlow.run(s3cfg, repo=repo, use_processes=False)
    client = storage.s3_client(s3cfg)
    client.put_object(Bucket=s3cfg.bucket, Key=f"{s3cfg.prefix}/Other/ledger.parquet", Body=b"")

    root = f"{s3cfg.prefix}/_proto/ProtoFlow/"
    assert storage.delete_prefix(client, s3cfg.bucket, root) > 0
    assert not storage.list_keys(client, s3cfg.bucket, root)
    # stable data outside the scratch prefix is untouched
    assert storage.list_keys(client, s3cfg.bucket, f"{s3cfg.prefix}/Other/")


def test_ledger_reset_on_conflict(s3cfg):
    client = storage.s3_client(s3cfg)
    key = "mf/F/ledger.parquet"
    started = dt.datetime(2024, 1, 1)
    ledger.upsert_run(
        client, s3cfg.bucket, key,
        {"run_id": "a", "status": "complete", "started": started, "x": 1},
        [pa.field("x", pa.int64())],
    )
    row = {"run_id": "b", "status": "complete", "started": started, "x": "now-a-string"}
    fields = [pa.field("x", pa.string())]
    with pytest.raises(Exception):  # noqa: B017 — any error is right; the reset path must not run
        ledger.upsert_run(client, s3cfg.bucket, key, row, fields)
    ledger.upsert_run(client, s3cfg.bucket, key, row, fields, reset_on_conflict=True)
    rows = ledger.read_all(client, s3cfg.bucket, key)
    assert [r["run_id"] for r in rows] == ["b"]  # fresh ledger, old history dropped
