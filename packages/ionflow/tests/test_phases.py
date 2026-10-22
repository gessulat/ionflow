from concurrent.futures import ThreadPoolExecutor

import pytest
from browser_flows import Observe

from ionflow import ArtifactInputs, Flow, FlowParams, LedgerColumns
from ionflow.phases import Phases


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
        assert outer["started"] <= inner["started"] <= inner["ended"] <= outer["ended"]
    with pytest.raises(RuntimeError, match="running step"):
        with recorder.phase("outside"):
            pass


@pytest.mark.slow
@pytest.mark.parametrize("processes", [False, True])
def test_failed_step_keeps_native_phase_telemetry(observed, repo, processes):
    from ionflow.query import phase_stats

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
        assert con.execute("SELECT status FROM phases WHERE run_id = ? AND phase = 'failing'", [rid]).fetchone() == (
            "failed",
        )
