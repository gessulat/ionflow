"""publish: copy a step's output to a stable S3 path with a provenance sidecar."""

import json

import pytest
from flows import EmptyFanFlow, FlakyFlow

from ionflow.storage import get_bytes_or_none, get_table, s3_client


def test_publish_copies_filters_and_guards(s3cfg, repo):
    run_id = EmptyFanFlow.run(s3cfg, repo=repo, use_processes=False)
    client = s3_client(s3cfg)

    m = EmptyFanFlow.publish(run_id, "agg", "stable/agg.parquet", config=s3cfg)
    t = get_table(client, s3cfg.bucket, "stable/agg.parquet")
    assert t.to_pylist() == [{"n": 0, "total": 0}]
    side = json.loads(get_bytes_or_none(client, s3cfg.bucket, "stable/agg.parquet.meta.json"))
    assert side["run_id"] == run_id and side["step"] == "agg" and side["commit"]
    assert m["flow"] == EmptyFanFlow.flow_id()

    # refuse to overwrite without force; force replaces
    with pytest.raises(FileExistsError, match="--force"):
        EmptyFanFlow.publish(run_id, "agg", "stable/agg.parquet", config=s3cfg)
    EmptyFanFlow.publish(run_id, "agg", "stable/agg.parquet", config=s3cfg, force=True)

    # WHERE filters the published rows
    EmptyFanFlow.publish(run_id, "agg", "stable/none.parquet", config=s3cfg, where="n > 0")
    assert get_table(client, s3cfg.bucket, "stable/none.parquet").num_rows == 0

    # a step with no parts (empty fan-out) publishes a typed, zero-row file
    EmptyFanFlow.publish(run_id, "work", "stable/work.parquet", config=s3cfg)
    t = get_table(client, s3cfg.bucket, "stable/work.parquet")
    assert t.num_rows == 0 and t.schema.names == ["g", "doubled"]


def test_publish_refuses_incomplete_and_unknown_runs(s3cfg, repo, tmp_path, monkeypatch):
    monkeypatch.setenv("FLAKY_DIR", str(tmp_path))
    monkeypatch.setenv("FLAKY_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom"):
        FlakyFlow.run(s3cfg, repo=repo, use_processes=False)
    failed = FlakyFlow.duck(s3cfg).sql("SELECT run_id FROM ledger").fetchone()[0]

    with pytest.raises(RuntimeError, match="did not complete"):
        FlakyFlow.publish(failed, "work", "stable/work.parquet", config=s3cfg)
    with pytest.raises(RuntimeError, match="run not found"):
        FlakyFlow.publish("nope", "work", "stable/work.parquet", config=s3cfg)
    with pytest.raises(ValueError, match="unknown step"):
        FlakyFlow.publish(failed, "nostep", "stable/work.parquet", config=s3cfg)
