import datetime as dt
import os
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pyarrow as pa
import pytest
from browser_flows import Observe

from ionflow import ArtifactInputs, Flow, FlowParams, LedgerColumns, metrics
from ionflow.phases import Phase, Phases
from ionflow.query import phase_stats


@pytest.mark.parametrize("name", ["phase", "_phases"])
@pytest.mark.parametrize(
    "declaration,base", [("Ledger", LedgerColumns), ("Params", FlowParams), ("Inputs", ArtifactInputs)]
)
def test_phase_runtime_names_cannot_be_shadowed(name, declaration, base):
    from browser_flows import Document

    schema = type("Schema", (base,), {"__annotations__": {name: Document if base is ArtifactInputs else str}})
    with pytest.raises((TypeError, ValueError), match="reserved|collide"):
        type("Invalid", (Flow,), {declaration: schema})


def test_phase_contexts_are_thread_local_and_failures_close_spans():
    recorder = Phases()

    def run(name: str) -> None:
        with recorder.step(name, name):
            with recorder.phase("outer"):
                with pytest.raises(ValueError, match="failure"):
                    with recorder.phase("inner"):
                        raise ValueError("failure")

    with ThreadPoolExecutor(2) as pool:
        list(pool.map(run, ["one", "two"]))
    for name in ("one", "two"):
        outer, inner = [r for r in recorder._rows if r["step"] == name]
        assert inner["parent_id"] == outer["span_id"]
        assert outer["status"] == "complete" and inner["status"] == "failed"
        assert outer["branch"] == inner["branch"] == name
        assert outer["process_id"] == inner["process_id"] == os.getpid()
        assert outer["started"] <= inner["started"] <= inner["ended"] <= outer["ended"]
    with pytest.raises(RuntimeError, match="running step"):
        with recorder.phase("outside"):
            pass


def test_phase_memory_uses_its_process_and_shares_thread_samples():
    start = dt.datetime(2026, 1, 1)
    ts = start.replace(tzinfo=dt.UTC).timestamp() + 1
    mib = 1024**2
    phases = pa.Table.from_pylist(
        [
            {
                "span_id": name,
                "parent_id": None,
                "step": "work",
                "branch": branch,
                "phase": name,
                "started": start,
                "ended": start + dt.timedelta(seconds=2),
                "status": "complete",
                "rss_start": 10 * mib,
                "rss_end": 20 * mib,
                "process_id": pid,
            }
            for name, branch, pid in [
                ("whole", None, 101),
                ("branch", "a", 102),
                ("thread", "b", 102),
                ("short", None, 103),
            ]
        ],
        schema=Phase.arrow_schema(),
    ).append_column("run_id", pa.array(["run"] * 4))
    samples = metrics.to_table(
        [
            (ts, "work", None, 30 * mib, 1, 0.0, 0.0, 101),
            (ts, "work", None, 40 * mib, 1, 0.0, 0.0, 102),
            (ts, "other", None, 900 * mib, 1, 0.0, 0.0, 999),
            (ts, None, None, 800 * mib, 1, 0.0, 0.0, 100),
            (ts + 2, "work", None, 700 * mib, 1, 0.0, 0.0, 101),
            (ts, "other", None, 600 * mib, 1, 0.0, 0.0, 101),
        ]
    ).append_column("run_id", pa.array(["run"] * 5 + ["other-run"]))
    with duckdb.connect() as con:
        con.register("phases", phases)
        con.register("metrics", samples)
        assert dict(phase_stats(con, "run").project("phase, peak_rss_mb").fetchall()) == {
            "whole": 30.0,
            "branch": 40.0,
            "thread": 40.0,
            "short": 20.0,
        }


@pytest.mark.slow
@pytest.mark.parametrize("processes", [False, True])
def test_failed_step_keeps_native_phase_telemetry(observed, repo, processes):
    with pytest.raises(ValueError, match="deliberate failure"):
        Observe.run(
            observed.config,
            repo=repo,
            use_processes=processes,
            artifacts={"document": observed.document.id},
            params={"fail": True},
        )
    with Observe.duck(observed.config) as con:
        rid = con.sql("SELECT run_id FROM ledger WHERE status = 'failed'").fetchone()[0]
        rows = phase_stats(con, rid).fetchall()
        assert len(rows) == 3
        pids = con.execute("SELECT DISTINCT process_id FROM phases WHERE run_id = ?", [rid]).fetchall()
        assert all((pid == os.getpid()) != processes for (pid,) in pids)
        assert con.execute("SELECT status FROM phases WHERE run_id = ? AND phase = 'failing'", [rid]).fetchone() == (
            "failed",
        )
