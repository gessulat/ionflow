import pytest
import typer
from flows import EmptyFanFlow, NoteFlow, ParamFlow, Src

from ionflow import Flow, FlowParams, LedgerColumns, cli
from ionflow.space import Space
from ionflow.store import Store


def test_params_bind_and_land_in_ledger(cfg, repo):
    run_id = ParamFlow.run(cfg, repo=repo, use_processes=False, params={"mult": 3})
    con = ParamFlow.duck(cfg)
    # branch workers read self.mult: g in (0, 1), each tripled
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (2, 3)
    # params are ledger columns, present from the first (running) upsert
    assert con.sql(f"SELECT mult FROM ledger WHERE run_id = '{run_id}'").fetchone()[0] == 3


def test_resume_reuses_origin_params(cfg, repo):
    origin = ParamFlow.run(cfg, repo=repo, use_processes=False, params={"mult": 3})
    resumed = ParamFlow.run(cfg, repo=repo, resume=origin, use_processes=False)
    con = ParamFlow.duck(cfg)
    assert con.sql(f"SELECT mult FROM ledger WHERE run_id = '{resumed}'").fetchone()[0] == 3
    with pytest.raises(ValueError, match="origin run's params"):
        ParamFlow.run(cfg, repo=repo, resume=origin, params={"mult": 5})


def test_resume_ignores_dropped_origin_params(cfg, repo):
    origin = EmptyFanFlow.run(cfg, repo=repo, use_processes=False)
    # simulate a param recorded by older code, since removed from the declaration
    space = Space.for_flow(Store.of(cfg), EmptyFanFlow)
    meta = space.read_meta(origin)
    space.write_meta(origin, {**meta, "params": {"gone": 1}})

    with pytest.warns(UserWarning, match="no longer declared"):
        EmptyFanFlow.run(cfg, repo=repo, resume=origin, use_processes=False)


def test_ledger_columns_land_on_completion_never_on_failure(cfg, repo, monkeypatch):
    """A failed run's ledger row keeps its params but never claims custom columns
    — even when the flow had already set them before the failing step."""
    monkeypatch.setenv("NOTE_FAIL", "1")
    with pytest.raises(RuntimeError, match="boom in doubled"):
        NoteFlow.run(cfg, repo=repo, use_processes=False)
    monkeypatch.delenv("NOTE_FAIL")
    done = NoteFlow.run(cfg, repo=repo, use_processes=False)

    con = NoteFlow.duck(cfg)
    rows = dict(con.sql("SELECT run_id, note FROM ledger").fetchall())
    assert rows.pop(done) == "set during the run"
    assert list(rows.values()) == [None]  # the failed run's row


def test_unset_ledger_column_fails_the_run(cfg, repo, monkeypatch):
    monkeypatch.setenv("NOTE_SKIP", "1")
    with pytest.raises(RuntimeError, match="not populated.*note"):
        NoteFlow.run(cfg, repo=repo, use_processes=False)


def test_resume_reuses_origin_ledger_columns(cfg, repo):
    """A fully-reused resume never runs the step that sets the column; the value
    must come forward from the origin's meta."""
    origin = NoteFlow.run(cfg, repo=repo, use_processes=False)
    resumed = NoteFlow.run(cfg, repo=repo, resume=origin, use_processes=False)
    con = NoteFlow.duck(cfg)
    assert con.sql(f"SELECT note FROM ledger WHERE run_id = '{resumed}'").fetchone()[0] == "set during the run"


def test_param_name_collision_rejected():
    with pytest.raises(TypeError, match="collide"):

        class Bad(Flow):
            class Params(FlowParams):
                data: int = 1

            def data(self) -> Src: ...

    with pytest.raises(TypeError, match="collide"):

        class BadLedger(Flow):
            class Params(FlowParams):
                note: int = 1

            class Ledger(LedgerColumns):
                note: str

            def data(self) -> Src: ...


def test_flow_without_params_refuses_params():
    with pytest.raises(ValueError, match="declares no Params"):
        EmptyFanFlow.resolve_params({"x": 1})


def test_cli_parse_params():
    class P(FlowParams):
        n: int
        lr: float
        flag: bool
        name: str

    parsed = cli._parse_params(P.columns(), ["n=3", "lr=0.5", "flag=true", "name=x"])
    assert parsed == {"n": 3, "lr": 0.5, "flag": True, "name": "x"}
    for bad in ["nope", "z=1", "n=abc", "flag=maybe"]:
        with pytest.raises(typer.BadParameter):
            cli._parse_params(P.columns(), [bad])
