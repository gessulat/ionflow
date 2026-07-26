"""Publish: bless a step's output into the stable publish DuckLake.

Atomic replace, snapshot history, queryable provenance — see decision 0009.
"""

import duckdb
import pytest
from flows import EmptyFanFlow, FlakyFlow
from schrein.ducklake import load_ducklake

from ionflow.publish import publish_lake
from ionflow.storage import s3_client, secret_sql


def _vanilla(cfg, dest=None):
    """A stock-DuckDB reader attached to the publish lake — no ionflow."""
    lake, bucket = publish_lake(cfg, dest)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(secret_sql(cfg))
    load_ducklake(con)
    con.execute(f"ATTACH 'ducklake:s3://{bucket}/{lake.register.key}' AS lake (READ_ONLY)")
    return con


def test_publish_blesses_replaces_and_records_provenance(s3cfg, repo):
    run_id = EmptyFanFlow.run(s3cfg, repo=repo, use_processes=False)

    m = EmptyFanFlow.publish(run_id, "agg", "blessed_agg", config=s3cfg)
    assert m["run_id"] == run_id and m["commit"]
    con = _vanilla(s3cfg)
    assert con.sql("SELECT n, total FROM lake.blessed_agg").fetchall() == [(0, 0)]
    prov = con.sql("SELECT table_name, flow, run_id, step FROM lake.publications").fetchall()
    assert prov == [("blessed_agg", EmptyFanFlow.flow_id(), run_id, "agg")]
    con.close()

    # republish = atomic replace; provenance appends; history is in snapshots
    EmptyFanFlow.publish(run_id, "agg", "blessed_agg", config=s3cfg, where="n > 0")
    con = _vanilla(s3cfg)
    assert con.sql("SELECT count(*) FROM lake.blessed_agg").fetchone()[0] == 0  # filter removed the row
    assert con.sql("SELECT count(*) FROM lake.publications").fetchone()[0] == 2
    assert con.sql("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'").fetchone()[0] == 2
    con.close()

    # a step with no parts (empty fan-out) publishes a typed, zero-row table
    EmptyFanFlow.publish(run_id, "work", "blessed_work", config=s3cfg)
    con = _vanilla(s3cfg)
    assert con.sql("SELECT count(*) FROM lake.blessed_work").fetchone()[0] == 0
    assert [r[0] for r in con.sql("DESCRIBE lake.blessed_work").fetchall()] == ["g", "doubled"]
    con.close()


def test_publish_to_another_bucket(s3cfg, repo):
    run_id = EmptyFanFlow.run(s3cfg, repo=repo, use_processes=False)
    other = f"{s3cfg.bucket}-pub"
    s3_client(s3cfg).create_bucket(Bucket=other)
    dest = f"s3://{other}/consumers/stable"
    EmptyFanFlow.publish(run_id, "agg", "agg", config=s3cfg, dest=dest)
    con = _vanilla(s3cfg, dest)
    assert con.sql("SELECT total FROM lake.agg").fetchone()[0] == 0
    con.close()


def test_publish_refuses_incomplete_unknown_and_bad_names(s3cfg, repo, tmp_path, monkeypatch):
    monkeypatch.setenv("FLAKY_DIR", str(tmp_path))
    monkeypatch.setenv("FLAKY_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom"):
        FlakyFlow.run(s3cfg, repo=repo, use_processes=False)
    failed = FlakyFlow.duck(s3cfg).sql("SELECT run_id FROM ledger").fetchone()[0]
    with pytest.raises(RuntimeError, match="did not complete"):
        FlakyFlow.publish(failed, "work", "blessed", config=s3cfg)
    with pytest.raises(RuntimeError, match="run not found"):
        FlakyFlow.publish("nope", "work", "blessed", config=s3cfg)
    with pytest.raises(ValueError, match="unknown step"):
        FlakyFlow.publish(failed, "nostep", "blessed", config=s3cfg)
    with pytest.raises(ValueError, match="invalid published table name"):
        FlakyFlow.publish(failed, "work", "no-dashes", config=s3cfg)
    with pytest.raises(ValueError, match="invalid published table name"):
        FlakyFlow.publish(failed, "work", "publications", config=s3cfg)
