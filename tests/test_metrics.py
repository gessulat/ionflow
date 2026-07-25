import datetime as dt

import pytest

from microflow import ledger, metrics, query, storage
from microflow.dag import build_dag


def test_mark_labels_samples():
    s = metrics.Sampler()  # not started: no timer thread, samples only via mark/stop
    s.mark("load")
    s.mark("score", 3)
    rows = s.stop()
    assert [(r[1], r[2]) for r in rows] == [("load", None), ("score", 3), ("score", 3)]
    assert all(r[3] > 0 and r[4] > 0 for r in rows)  # rss, threads


def test_fold_preserves_peaks_and_doubles_interval():
    s = metrics.Sampler()
    s._rows = [(float(i), "a", None, 100 + (1000 if i == 3 else 0), 2, 0.1 * i, 0.0) for i in range(8)]
    s._fold()
    assert len(s._rows) == 4
    assert max(r[3] for r in s._rows) == 1100  # the rss spike survives folding
    assert s._interval == metrics.BASE_INTERVAL * 2
    assert s._rows[-1][0] == 7.0  # latest timestamp kept


def test_fold_never_merges_across_step_boundaries():
    s = metrics.Sampler()
    s._rows = [
        (0.0, "a", None, 10, 1, 0.0, 0.0),
        (1.0, "b", None, 20, 1, 0.0, 0.0),
        (2.0, "b", None, 30, 1, 0.0, 0.0),
        (3.0, "b", None, 5, 1, 0.0, 0.0),
    ]
    s._fold()
    # 'a' can't pair with 'b' so it passes through; the two middle b's merge
    assert [(r[1], r[3]) for r in s._rows] == [("a", 10), ("b", 30), ("b", 5)]


def test_cap_triggers_fold(monkeypatch):
    monkeypatch.setattr(metrics, "MAX_SAMPLES", 4)
    s = metrics.Sampler()
    for _ in range(4):
        s._sample()
    assert len(s._rows) == 2  # folded once at the cap
    assert s._interval == metrics.BASE_INTERVAL * 2


def test_tick_runs_duties_on_their_own_cadences():
    flushed = []
    s = metrics.Sampler(flush=flushed.append)
    s._label = ("a", None)
    s._interval = 10.0
    s._last_sample = s._last_flush = 100.0

    s._tick(105.0)  # nothing due
    assert not s._rows and not flushed
    s._tick(111.0)  # sample due (10s), flush not (60s)
    assert len(s._rows) == 1 and not flushed
    s._tick(161.0)  # flush due
    assert flushed == [s.rows()]


def test_flush_errors_do_not_kill_sampling():
    def boom(rows):
        raise RuntimeError("s3 down")

    s = metrics.Sampler(flush=boom)
    s._label = ("a", None)
    s._last_sample = s._last_flush = 0.0
    s._tick(100.0)  # both duties due; the raising flush is swallowed
    assert len(s._rows) == 1


def test_stop_joins_the_thread():
    s = metrics.Sampler().start()
    s.stop()
    assert not s._thread.is_alive()


def test_stale_running_detection(s3cfg):
    client = storage.s3_client(s3cfg)
    layout = storage.Layout(s3cfg.prefix, "F")
    old = dt.datetime(2020, 1, 1)
    for run_id in ("dead", "alive"):
        ledger.upsert_run(
            client,
            s3cfg.bucket,
            layout.ledger,
            {"run_id": run_id, "status": "running", "started": old},
        )
    # a fresh metrics flush is the heartbeat; "dead" never wrote one
    metrics.write_parquet(client, s3cfg.bucket, layout.metrics("alive"), [(1.0, "a", None, 1, 1, 0.0, 0.0)])

    con = storage.connect(s3cfg)
    con.execute(f"CREATE VIEW ledger AS SELECT * FROM read_parquet('{storage.uri(s3cfg, layout.ledger)}')")
    stub = type("F", (), {"flow_id": classmethod(lambda cls: "F")})  # matches the layout above
    rel = query.ledger_with_liveness(stub, s3cfg, con)
    statuses = dict(rel.project("run_id, status").fetchall())
    assert statuses == {"dead": "running (stale?)", "alive": "running"}


def test_write_parquet_roundtrip(s3cfg):
    rows = [(1000.0, "load", None, 512, 4, 0.5, 0.1), (1001.0, "score", 2, 1024, 8, 1.5, 0.2)]
    client = storage.s3_client(s3cfg)
    metrics.write_parquet(client, s3cfg.bucket, "mf/F/runs/run_id=r1/_metrics.parquet", rows)
    table = storage.get_table(client, s3cfg.bucket, "mf/F/runs/run_id=r1/_metrics.parquet")
    assert table.schema.equals(metrics.SCHEMA)
    assert table.column("rss").to_pylist() == [512, 1024]
    assert table.column("branch").to_pylist() == [None, 2]


def test_reserved_step_names_rejected():
    class Bad:
        def metrics(self) -> None: ...

    with pytest.raises(ValueError, match="reserved"):
        build_dag(Bad)
