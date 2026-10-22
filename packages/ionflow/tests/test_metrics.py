import pytest

from ionflow import metrics, query
from ionflow.dag import build_dag
from ionflow.store import Store


def test_fold_preserves_peaks_and_doubles_interval():
    s = metrics.Sampler()
    s._rows = [(float(i), "a", None, 100 + (1000 if i == 3 else 0), 2, 0.1 * i, 0.0, 123) for i in range(8)]
    s._fold()
    assert len(s._rows) == 4
    assert max(r[3] for r in s._rows) == 1100  # the rss spike survives folding
    assert s._interval == metrics.BASE_INTERVAL * 2
    assert s._rows[-1][0] == 7.0  # latest timestamp kept
    assert {r[7] for r in s._rows} == {123}


def test_flush_errors_do_not_kill_sampling():
    def boom(rows):
        raise RuntimeError("s3 down")

    s = metrics.Sampler(flush=boom)
    s._label = ("a", None)
    s._last_sample = s._last_flush = 0.0
    s._tick(100.0)  # both duties due; the raising flush is swallowed
    assert len(s._rows) == 1


def test_stale_running_detection(cfg):
    store = Store.of(cfg)
    space = store.space("F")
    # a fresh metrics flush is the heartbeat; "dead" never wrote one
    metrics.write_parquet(space, space.metrics("alive"), [(1.0, "a", None, 1, 1, 0.0, 0.0, 123)])

    con = store.connect()
    con.execute(
        "CREATE VIEW ledger AS SELECT *, true AS local FROM (VALUES "
        "('dead', 'running', TIMESTAMP '2020-01-01'), "
        "('alive', 'running', TIMESTAMP '2020-01-01')) t(run_id, status, started)"
    )
    stub = type("F", (), {"flow_id": classmethod(lambda cls: "F")})  # matches the space above
    rel = query.ledger_with_liveness(stub, cfg, con)
    statuses = dict(rel.project("run_id, status").fetchall())
    assert statuses == {"dead": "running (stale?)", "alive": "running"}


def test_reserved_step_names_rejected():
    class Bad:
        def metrics(self) -> None: ...

    with pytest.raises(ValueError, match="reserved"):
        build_dag(Bad)
