import datetime as dt
import importlib.util
import inspect
import uuid
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import asdict

from . import ledger
from .config import Config
from .git import require_clean
from .storage import (
    Layout,
    compute_connection,
    load_relation,
    list_keys,
    s3_client,
    write_part,
)


def _now():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)  # naive UTC


def _new_run_id(now):
    return f"{int(now.timestamp() * 1000):013d}-{uuid.uuid4().hex[:8]}"


def _key(step, branch):
    return f"{step}/{branch}"


def _split(key):
    step, branch = key.rsplit("/", 1)
    return step, int(branch)


def _count_of(completed, step):
    return sum(1 for k in completed if _split(k)[0] == step)


def _input_relation(con, client, cfg, layout, run_id, dep, kind, branch):
    if kind == "COLLECT":
        keys = sorted(list_keys(client, cfg.bucket, layout.step_dir(run_id, dep) + "/"))
    else:
        keys = [layout.part(run_id, dep, branch if kind == "BRANCH" else 0)]
    return load_relation(con, client, cfg.bucket, keys)


def _call(con, client, cfg, layout, flow, step, run_id, branch):
    kwargs = {
        dep: _input_relation(con, client, cfg, layout, run_id, dep, kind, branch)
        for dep, kind in step.param_kinds.items()
    }
    return step.method(flow, **kwargs)


def _import(file, name):
    spec = importlib.util.spec_from_file_location("_mf_flow", file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)


def _branch_worker(payload):
    cfg = Config(**payload["config"])
    flow_cls = _import(payload["file"], payload["flow"])
    layout = Layout(cfg.prefix, flow_cls.__name__)
    con = compute_connection()
    client = s3_client(cfg)
    flow = flow_cls()
    step = flow_cls._dag.steps[payload["step"]]
    run_id, branch = payload["run_id"], payload["branch"]
    result = _call(con, client, cfg, layout, flow, step, run_id, branch)
    write_part(con, client, cfg.bucket, layout.part(run_id, step.name, branch), result, step.output_table)
    return branch


def _run_branches(flow_cls, cfg, run_id, step, todo, max_workers, use_processes):
    payload = {
        "config": asdict(cfg),
        "file": inspect.getfile(flow_cls),
        "flow": flow_cls.__name__,
        "step": step.name,
        "run_id": run_id,
    }
    pool = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    with pool(max_workers=max_workers) as ex:
        list(ex.map(_branch_worker, [{**payload, "branch": i} for i in todo]))


def run(flow_cls, config, repo=".", origin_run_id=None, max_workers=None, use_processes=True):
    commit = require_clean(repo)
    started = _now()
    run_id = _new_run_id(started)
    layout = Layout(config.prefix, flow_cls.__name__)
    client = s3_client(config)
    con = compute_connection()
    flow = flow_cls()
    dag = flow_cls._dag
    lc = getattr(flow_cls, "Ledger", None)
    custom_fields = lc.arrow_fields() if lc else []

    completed = set()
    if origin_run_id:
        origin = ledger.read_meta(client, config.bucket, layout.meta(origin_run_id))
        if origin is None:
            raise RuntimeError(f"origin run not found: {origin_run_id}")
        if origin.get("commit") != commit:
            warnings.warn(f"resuming {origin_run_id} at a different commit; reused steps keep old code")
        for k in origin.get("steps_complete", []):
            name, br = _split(k)
            client.copy_object(
                Bucket=config.bucket,
                CopySource={"Bucket": config.bucket, "Key": layout.part(origin_run_id, name, br)},
                Key=layout.part(run_id, name, br),
            )
            completed.add(k)
        for k, v in (origin.get("custom") or {}).items():  # reused steps won't reset these
            setattr(flow, k, v)

    def flush(finished=None, custom=None):
        meta = {
            "run_id": run_id,
            "started": started.isoformat(),
            "finished": finished.isoformat() if finished else None,
            "commit": commit,
            "origin_run_id": origin_run_id,
            "steps_complete": sorted(completed),
            "custom": custom or {},
        }
        ledger.write_meta(client, config.bucket, layout.meta(run_id), meta)

    flush()
    ledger.upsert_run(
        client, config.bucket, layout.ledger,
        {"run_id": run_id, "started": started, "commit": commit, "origin_run_id": origin_run_id},
        custom_fields,
    )

    for name in dag.order:
        step = dag.steps[name]
        branch_dep = next((d for d, k in step.param_kinds.items() if k == "BRANCH"), None)
        if branch_dep:
            n = _count_of(completed, branch_dep)
            todo = [i for i in range(n) if _key(name, i) not in completed]
            if todo:
                _run_branches(flow_cls, config, run_id, step, todo, max_workers, use_processes)
            completed.update(_key(name, i) for i in range(n))
        elif step.is_fanout:
            if not _count_of(completed, name):  # skip if a prior run already produced it
                items = list(_call(con, client, config, layout, flow, step, run_id, None))
                for i, item in enumerate(items):
                    write_part(con, client, config.bucket, layout.part(run_id, name, i), item, step.output_table)
                    completed.add(_key(name, i))
        elif _key(name, 0) not in completed:
            result = _call(con, client, config, layout, flow, step, run_id, None)
            write_part(con, client, config.bucket, layout.part(run_id, name, 0), result, step.output_table)
            completed.add(_key(name, 0))
        flush()

    custom = {}
    if lc:
        missing = [c for c in lc.columns() if not hasattr(flow, c)]
        if missing:
            raise RuntimeError(f"ledger columns not populated during run: {missing}")
        custom = {c: getattr(flow, c) for c in lc.columns()}
    finished = _now()
    flush(finished, custom)
    ledger.upsert_run(
        client, config.bucket, layout.ledger,
        {
            "run_id": run_id, "started": started, "finished": finished,
            "commit": commit, "origin_run_id": origin_run_id,
            "steps_complete": sorted(completed), **custom,
        },
        custom_fields,
    )
    return run_id
