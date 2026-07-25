import pytest
import typer
from flows import EmptyFanFlow, ParamFlow, Src

from microflow import Flow, FlowParams, cli, ledger, storage


def test_params_bind_and_land_in_ledger(s3cfg, repo):
    run_id = ParamFlow.run(s3cfg, repo=repo, use_processes=False, params={"mult": 3})
    con = ParamFlow.duck(s3cfg)
    # branch workers read self.mult: g in (0, 1), each tripled
    assert con.sql(f"SELECT n, total FROM agg WHERE run_id = '{run_id}'").fetchone() == (2, 3)
    # params are ledger columns, present from the first (running) upsert
    assert con.sql(f"SELECT mult FROM ledger WHERE run_id = '{run_id}'").fetchone()[0] == 3


def test_resume_reuses_origin_params(s3cfg, repo):
    origin = ParamFlow.run(s3cfg, repo=repo, use_processes=False, params={"mult": 3})
    resumed = ParamFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)
    con = ParamFlow.duck(s3cfg)
    assert con.sql(f"SELECT mult FROM ledger WHERE run_id = '{resumed}'").fetchone()[0] == 3
    with pytest.raises(ValueError, match="origin run's params"):
        ParamFlow.run(s3cfg, repo=repo, resume=origin, params={"mult": 5})


def test_resume_ignores_dropped_origin_params(s3cfg, repo):
    origin = EmptyFanFlow.run(s3cfg, repo=repo, use_processes=False)
    # simulate a param recorded by older code, since removed from the declaration
    client = storage.s3_client(s3cfg)
    key = storage.Layout.for_flow(s3cfg, EmptyFanFlow).meta(origin)
    meta = ledger.read_meta(client, s3cfg.bucket, key)
    ledger.write_meta(client, s3cfg.bucket, key, {**meta, "params": {"gone": 1}})

    with pytest.warns(UserWarning, match="no longer declared"):
        EmptyFanFlow.run(s3cfg, repo=repo, resume=origin, use_processes=False)


def test_resolve_validates_types():
    class P(FlowParams):
        lr: float = 0.1
        name: str | None = None

    assert P.resolve({"lr": 1}) == {"lr": 1.0, "name": None}  # int coerced for a float param
    with pytest.raises(TypeError, match="expected float"):
        P.resolve({"lr": "fast"})
    with pytest.raises(TypeError, match="may not be None"):
        P.resolve({"lr": None})


def test_param_types_restricted():
    import datetime as dt

    with pytest.raises(TypeError, match="int/float/str/bool"):

        class P(FlowParams):
            ts: dt.datetime


def test_param_name_collision_rejected():
    with pytest.raises(TypeError, match="collide"):

        class Bad(Flow):
            class Params(FlowParams):
                data: int = 1

            def data(self) -> Src: ...


def test_params_on_flow_without_params_rejected():
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
