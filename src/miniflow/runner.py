import datetime as dt
import importlib.util
import inspect
import uuid
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict

from . import ledger
from .config import Config
from .git import require_clean
from .storage import (
    Layout,
    compute_connection,
    empty_relation,
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


def _input_relation(con, client, cfg, layout, run_id, dep, kind, branch, dep_table):
    if kind == "COLLECT":
        keys = sorted(list_keys(client, cfg.bucket, layout.step_dir(run_id, dep) + "/"))
        if not keys:  # empty fan-out: hand the reducer a typed empty table
            return empty_relation(con, dep_table)
    else:
        keys = [layout.part(run_id, dep, branch if kind == "BRANCH" else 0)]
    return load_relation(con, client, cfg.bucket, keys)


def _call(con, client, cfg, layout, flow, step, run_id, branch):
    dag = flow.__class__._dag
    kwargs = {
        dep: _input_relation(
            con, client, cfg, layout, run_id, dep, kind, branch,
            dag.steps[dep].output_table,
        )
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
    """Run branches in parallel; return (indices that succeeded, first error or None).

    Failures don't cancel siblings, so a partial fan-out leaves the completed
    branches' parts durably on S3 for a later resume to reuse.
    """
    payload = {
        "config": asdict(cfg),
        "file": inspect.getfile(flow_cls),
        "flow": flow_cls.__name__,
        "step": step.name,
        "run_id": run_id,
    }
    pool = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    done, error = [], None
    with pool(max_workers=max_workers) as ex:
        futures = [ex.submit(_branch_worker, {**payload, "branch": i}) for i in todo]
        for fut in as_completed(futures):
            try:
                done.append(fut.result())
            except Exception as e:  # noqa: BLE001 — surface the first, keep the rest
                error = error or e
    return done, error


def run(flow_cls, config, repo=".", origin_run_id=None, max_workers=None, use_processes=True):
    commit = require_clean(repo)
    started = _now()
    run_id = _new_run_id(started)
    layout = Layout(config.prefix, flow_cls.__name__)
    client = s3_client(config)
    con = compute_connection()
    flow = flow_cls()
    dag = flow_cls._dag
    ledger_cols = flow_cls.ledger_columns()
    custom_fields = flow_cls.ledger_fields()

    completed = set()
    if origin_run_id:
        origin = ledger.read_meta(client, config.bucket, layout.meta(origin_run_id))
        if origin is None:
            raise RuntimeError(f"origin run not found: {origin_run_id}")
        if origin.get("commit") != commit:
            warnings.warn(f"resuming {origin_run_id} at a different commit; reused steps keep old code")
        def copy_part(k):
            name, br = _split(k)
            client.copy_object(
                Bucket=config.bucket,
                CopySource={"Bucket": config.bucket, "Key": layout.part(origin_run_id, name, br)},
                Key=layout.part(run_id, name, br),
            )
            return k

        with ThreadPoolExecutor(max_workers=16) as ex:  # server-side copies, IO-bound
            completed.update(ex.map(copy_part, origin.get("steps_complete", [])))
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

    def upsert(status, finished=None, custom=None):
        ledger.upsert_run(
            client, config.bucket, layout.ledger,
            {
                "run_id": run_id, "status": status, "started": started,
                "finished": finished, "commit": commit,
                "origin_run_id": origin_run_id, "steps_complete": sorted(completed),
                **(custom or {}),
            },
            custom_fields,
        )

    flush()
    upsert("running")

    try:
        for name in dag.order:
            step = dag.steps[name]
            branch_dep = next((d for d, k in step.param_kinds.items() if k == "BRANCH"), None)
            if branch_dep:
                n = _count_of(completed, branch_dep)
                todo = [i for i in range(n) if _key(name, i) not in completed]
                if todo:
                    done, error = _run_branches(
                        flow_cls, config, run_id, step, todo, max_workers, use_processes
                    )
                    completed.update(_key(name, i) for i in done)  # only branches that succeeded
                    if error:
                        raise error
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

        missing = [c for c in ledger_cols if not hasattr(flow, c)]
        if missing:
            raise RuntimeError(f"ledger columns not populated during run: {missing}")
        custom = {c: getattr(flow, c) for c in ledger_cols}
        finished = _now()
        flush(finished, custom)
        upsert("complete", finished, custom)
    except Exception:
        flush()  # persist partial progress so resume can reuse completed branches
        upsert("failed", finished=_now())
        raise

    return run_id
