import graphlib
import importlib.util
import inspect
import os
import threading
import uuid
import warnings
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from dataclasses import asdict
from multiprocessing import get_context

import psutil

from . import catalog, ledger, metrics
from .config import Config
from .git import require_clean
from .storage import (
    Layout,
    StepInput,
    connect,
    empty_relation,
    input_relation,
    part_keys,
    s3_client,
    utc_now,
    write_part,
)


def _new_run_id(now):
    return f"{int(now.timestamp() * 1000):013d}-{uuid.uuid4().hex[:8]}"


def _key(step, branch):
    return f"{step}/{branch}"


def _split(key):
    step, branch = key.rsplit("/", 1)
    return step, int(branch)


def _count_of(completed, step):
    return sum(1 for k in completed if _split(k)[0] == step)


def _keyed_fanouts(dag):
    """Fan-out step name -> its output table's fanout_key column, keyed steps only."""
    out = {}
    for name, step in dag.steps.items():
        if step.is_fanout and step.output_table.fanout_key_column():
            out[name] = step.output_table.fanout_key_column()
    return out


def _key_roots(dag, keyed):
    """Per-item step -> the keyed fan-out whose item keys name its branches.

    Branch identity travels down a chain of branch steps: branch i of a step
    branching over a branch step is still the fan-out's i-th item, so a chained
    step's parts are reusable by item key exactly like the first one's. Steps
    of an unkeyed fan-out's group have no such name and stay out.
    """
    return {m: g.fanout for g in dag.groups.values() if g.fanout in keyed for m in g.order}


def _downstream(dag, root):
    """`root` plus every step reachable from it through dependency edges."""
    out = {root}
    for name in dag.order:  # topo order: a step appears after all its deps
        if any(d in out for d in dag.steps[name].deps):
            out.add(name)
    return out


def _latest_complete(client, config, layout):
    """run_id of the newest complete run, or None (ledger rows sort by start time).

    Complete only — resuming must never chain onto a failed or running origin.
    `stats`' default "latest" is the looser newest-by-start-time of any status.
    """
    rows = ledger.read_all(client, config.bucket, layout.ledger)
    return next((r["run_id"] for r in reversed(rows) if r["status"] == "complete"), None)


def _fair_share(workers):
    """Per-worker DuckDB caps: workers x per-worker share ~ the machine (cores, 80% RAM)."""
    cores = os.cpu_count() or 1
    memory_mb = int(psutil.virtual_memory().total * 0.8) // workers // 2**20
    return max(1, cores // workers), memory_mb


def _step_input(con, client, cfg, layout, run_id, dep, kind, branch, dep_table):
    if kind == "COLLECT":
        keys = part_keys(client, cfg.bucket, layout, run_id, dep)
        if not keys:  # empty fan-out: hand the reducer a typed empty table
            return empty_relation(con, dep_table)
    else:
        keys = [layout.part(run_id, dep, branch if kind == "BRANCH" else 0)]
    return input_relation(con, cfg, keys)


def _call(con, client, cfg, layout, flow, step, run_id, branch):
    dag = flow.__class__._dag
    kwargs = {
        dep: StepInput(
            _step_input(
                con,
                client,
                cfg,
                layout,
                run_id,
                dep,
                kind,
                branch,
                dag.steps[dep].output_table,
            )
        )
        for dep, kind in step.param_kinds.items()
    }
    return step.method(flow, **kwargs)


def _import(file, name):
    spec = importlib.util.spec_from_file_location("_mf_flow", file)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)


def _branch_worker(payload):
    threads, memory_mb = _fair_share(payload["workers"])
    if payload["sample"]:  # own process: cap BLAS/OpenMP before the flow's imports
        # spawn imports numpy/torch fresh in this process, so the caps take effect;
        # setdefault keeps any explicit user setting. Never in thread mode — that
        # would mutate the orchestrator's environment.
        for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
            os.environ.setdefault(var, str(threads))
    cfg = Config(**payload["config"])
    flow_cls = _import(payload["file"], payload["flow"])
    # identity travels in the payload, never derived here: the re-imported
    # class's __module__ is the loader's ("_mf_flow"), so its own flow_id()
    # is wrong
    layout = Layout(cfg.prefix, payload["flow_id"])
    con = connect(cfg, threads=threads, memory_mb=memory_mb)
    client = s3_client(cfg)
    flow = flow_cls()
    flow.config = cfg
    for k, v in payload["params"].items():
        setattr(flow, k, v)
    group = flow_cls._dag.groups[payload["group"]]
    run_id, branch, pending = payload["run_id"], payload["branch"], payload["steps"]
    # own sampler only in a separate process; in thread mode the orchestrator's
    # sampler already covers this process (a second one would double-count CPU).
    # Each worker flushes to its OWN metrics key (one writer per object, no CAS),
    # so branch samples survive a run killed mid-branch up to the last flush.
    sampler, active, label_lock = None, set(), threading.Lock()

    def relabel():
        # concurrent members share one process, so they share one sample label
        if sampler:
            sampler.mark("+".join(sorted(active)) or None, branch)

    if payload["sample"]:
        metrics_key = layout.metrics_branch(run_id, group.fanout, branch)

        def flush_metrics(rows):
            metrics.write_parquet(client, cfg.bucket, metrics_key, rows)

        sampler = metrics.Sampler(flush=flush_metrics).start()
    retries = payload["retries"]

    def run_member(name):
        """One sub-step for this item, retried on its own — a flaky upload is not a re-search."""
        step = flow_cls._dag.steps[name]
        cur = con.cursor()  # own cursor: members of one item may run concurrently
        with label_lock:
            active.add(name)
            relabel()
        try:
            for attempt in range(retries + 1):  # retries the whole step, not just the S3 write:
                # transient step errors count too, and boto3 already retries at the HTTP layer
                try:
                    result = _call(cur, client, cfg, layout, flow, step, run_id, branch)
                    write_part(
                        client,
                        cfg.bucket,
                        layout.part(run_id, name, branch),
                        result,
                        step.output_table,
                        threads=threads,
                        memory_mb=memory_mb,
                    )
                    return
                except Exception:
                    if attempt == retries:
                        raise
        finally:
            with label_lock:
                active.discard(name)
                relabel()
            cur.close()

    done, error = [], None
    try:
        if len(pending) == 1:  # the common single-step branch: no pool, no extra thread
            try:
                run_member(pending[0])
                done.append(pending[0])
            except Exception as e:  # noqa: BLE001 — reported to the orchestrator with the progress
                error = e
        else:
            done, error = _run_sub_dag(group, pending, run_member)
    finally:
        if sampler:  # final superset write, on failure too — that's where perf data matters most
            try:
                flush_metrics(sampler.stop())
            except Exception:  # noqa: BLE001 — metrics must never mask the branch's outcome
                pass
    # progress travels with the failure: resume restarts this item at its first
    # incomplete sub-step, not at the top
    return {"branch": branch, "done": done, "error": error}


def _run_sub_dag(group, pending, run_member):
    """Run one item's sub-steps, independent ones concurrently; return (done, first error).

    Members already complete for this item are absent from `pending` — their
    parts are on S3 and dependents read them from there.
    """
    ts = graphlib.TopologicalSorter({m: [d for d in group.deps[m] if d in pending] for m in pending})
    ts.prepare()
    done, error, running = [], None, {}
    with ThreadPoolExecutor(max_workers=len(pending)) as ex:
        while ts.is_active() and error is None:
            for m in ts.get_ready():
                running[ex.submit(run_member, m)] = m
            if not running:
                break
            finished, _ = wait(list(running), return_when=FIRST_COMPLETED)
            for fut in finished:
                m = running.pop(fut)
                try:
                    fut.result()
                except Exception as e:  # noqa: BLE001 — its dependents are unreachable, siblings aren't
                    error = error if error is not None else e
                    continue
                done.append(m)
                ts.done(m)
        for fut, m in running.items():  # siblings already in flight still count when they land
            try:
                fut.result()
                done.append(m)
            except Exception as e:  # noqa: BLE001
                error = error if error is not None else e
    return done, error


def _run_items(flow_cls, cfg, run_id, group, items, max_workers, use_processes, retries, params):
    """Run each item's sub-DAG in its own worker; return ({item: sub-steps done}, first error).

    Failures don't cancel siblings, so a partial fan-out leaves the completed
    branches' parts durably on S3 for a later resume to reuse. Process-mode
    workers write their own per-branch metrics objects; the `metrics` view
    unions them with the orchestrator's.
    """
    cores = os.cpu_count() or 1
    workers = min(max_workers or cores, len(items))
    payload = {
        "config": asdict(cfg),
        "file": inspect.getfile(flow_cls),
        "flow": flow_cls.__name__,  # for getattr after the file import
        "flow_id": flow_cls.flow_id(),  # for the Layout; see _branch_worker
        "group": group.fanout,
        "run_id": run_id,
        "sample": use_processes,
        "retries": retries,
        "params": params,
        "workers": workers,  # workers derive their own caps: same machine, one formula
    }
    if use_processes:
        # spawn, never fork: the orchestrator is multi-threaded here (sampler,
        # DuckDB workers), and non-branch steps ran user code in this process,
        # so forked children inherit OpenMP/BLAS/CUDA state mid-flight (hangs,
        # "cannot re-initialize CUDA"). Spawn on 3.12/3.13 Linux too, where the
        # stdlib default is still fork.
        pool = ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn"))
    else:
        pool = ThreadPoolExecutor(max_workers=workers)
    done, error = {}, None
    with pool as ex:
        futures = [ex.submit(_branch_worker, {**payload, "branch": i, "steps": ms}) for i, ms in items.items()]
        for fut in as_completed(futures):
            try:
                result = fut.result()
            except Exception as e:  # noqa: BLE001 — a worker that died without reporting
                error = error if error is not None else e
                continue
            done[result["branch"]] = result["done"]
            if error is None:  # surface the first, keep the rest
                error = result["error"]
    return done, error


class CommitDriftError(RuntimeError):
    """Resume refused: HEAD differs from the origin run's commit."""


def run(
    flow_cls,
    config,
    repo=".",
    resume=None,
    max_workers=None,
    use_processes=True,
    branch_retries=0,
    params=None,
    allow_commit_drift=False,
):
    """Execute `flow_cls`; returns the new run_id.

    `resume` is the one resume spelling: an origin run_id reuses that run's
    completed steps; the string "latest" resumes the newest complete run and
    falls through to a fresh full run when there is none (so cron jobs can
    always pass it). Resume always produces a NEW, self-contained run.
    """
    if allow_commit_drift and not resume:
        raise ValueError("allow_commit_drift only applies to a resumed run; pass resume= as well")
    if resume and params:
        raise ValueError("resume reuses the origin run's params; do not pass params")
    commit = require_clean(repo)
    started = utc_now()
    run_id = _new_run_id(started)
    layout = Layout.for_flow(config, flow_cls)
    client = s3_client(config)
    origin_run_id = _latest_complete(client, config, layout) if resume == "latest" else resume
    # connecting here also installs httpfs once, before any worker pool spawns
    # (N cold workers would otherwise race the extension download)
    con = connect(config)
    cat = catalog.Catalog(config, flow_cls)
    flow = flow_cls()
    flow.config = config
    dag = flow_cls._dag
    ledger_cols = flow_cls.ledger_columns()
    custom_fields = flow_cls.custom_ledger_fields()

    completed = set()
    keyed = _keyed_fanouts(dag)
    fanout_keys = {}  # keyed fan-out -> [item key per branch index]; recorded in meta
    origin_completed, origin_fanout_keys = set(), {}
    # Steps downstream of a keyed fan-out can't be reused upfront: the fan-out
    # re-runs first (the work-list may have changed), branch parts are then
    # matched by item key, and everything else downstream is reused only when
    # the key set is unchanged.
    keyed_downstream = {f: _downstream(dag, f) for f in keyed} if origin_run_id else {}
    deferred = set().union(*keyed_downstream.values()) if keyed_downstream else set()
    key_roots = _key_roots(dag, keyed) if origin_run_id else {}
    # Positional reuse is only safe where branch position carries no meaning:
    # everything the fan-out's item keys name is matched by key instead, however
    # deep in a branch chain it sits (positions shift when the work-list does).
    lazy_reuse = {s for s in deferred if s not in keyed and s not in key_roots}

    def copy_part(name, src_branch, dst_branch):
        client.copy_object(
            Bucket=config.bucket,
            CopySource={"Bucket": config.bucket, "Key": layout.part(origin_run_id, name, src_branch)},
            Key=layout.part(run_id, name, dst_branch),
        )
        return _key(name, dst_branch)

    def copy_same(k):
        name, br = _split(k)
        return copy_part(name, br, br)

    def work_lists_unchanged(name, excluding=None):
        """True when every keyed fan-out upstream of `name` (bar `excluding`) kept its key set."""
        return all(
            set(fanout_keys.get(f) or []) == set(origin_fanout_keys.get(f) or [])
            for f, ds in keyed_downstream.items()
            if f != excluding and name in ds
        )

    def reuse_step(name):
        """Copy all of the origin's parts for `name` (positional reuse, deferred)."""
        if parts := [k for k in origin_completed if _split(k)[0] == name]:
            with ThreadPoolExecutor(max_workers=16) as ex:
                completed.update(ex.map(copy_same, parts))

    def reuse_matching_branches(name, root, todo):
        """Copy origin branch parts whose item key matches; return what's left to compute.

        `root` is the keyed fan-out naming the branches — the step itself for a
        direct branch step, an ancestor for a chained one (0014).
        """
        src = {k: j for j, k in enumerate(origin_fanout_keys.get(root) or [])}
        if not src:
            warnings.warn(
                f"origin run has no fanout keys for '{root}' (predates fanout_key?); recomputing every '{name}' branch",
                stacklevel=3,
            )
        hits = [
            (i, src[k]) for i in todo if (k := fanout_keys[root][i]) in src and _key(name, src[k]) in origin_completed
        ]
        if hits:
            with ThreadPoolExecutor(max_workers=16) as ex:
                completed.update(ex.map(lambda ij: copy_part(name, ij[1], ij[0]), hits))
        return [i for i in todo if _key(name, i) not in completed]

    if origin_run_id:
        origin = ledger.read_meta(client, config.bucket, layout.meta(origin_run_id))
        if origin is None:
            raise RuntimeError(f"origin run not found: {origin_run_id}")
        if origin.get("commit") != commit:
            # a mixed-commit run must be a visible, deliberate choice
            if not allow_commit_drift:
                raise CommitDriftError(
                    f"origin run {origin_run_id} is at commit {origin.get('commit')}, HEAD is "
                    f"{commit}; reused steps would keep old-code outputs. Pass "
                    "allow_commit_drift=True (CLI: --allow-commit-drift) to resume anyway."
                )
            warnings.warn(f"resuming {origin_run_id} at a different commit; reused steps keep old code", stacklevel=2)
        origin_completed = set(origin.get("steps_complete", []))
        origin_fanout_keys = origin.get("fanout_keys") or {}
        with ThreadPoolExecutor(max_workers=16) as ex:  # server-side copies, IO-bound
            completed.update(
                ex.map(
                    copy_same,
                    [k for k in origin_completed if _split(k)[0] not in deferred],
                )
            )
        for k, v in (origin.get("custom") or {}).items():  # reused steps won't reset these
            setattr(flow, k, v)
        params = origin.get("params") or {}
        if dropped := set(params) - set(flow_cls.params_columns()):
            warnings.warn(f"origin params no longer declared, ignored: {sorted(dropped)}", stacklevel=2)
            params = {k: v for k, v in params.items() if k not in dropped}

    params = flow_cls.resolve_params(params)
    for k, v in params.items():
        setattr(flow, k, v)

    def flush(finished=None, custom=None):
        meta = {
            "run_id": run_id,
            "started": started.isoformat(),
            "finished": finished.isoformat() if finished else None,
            "commit": commit,
            "origin_run_id": origin_run_id,
            "steps_complete": sorted(completed),
            "fanout_keys": fanout_keys,
            "params": params,
            "custom": custom or {},
        }
        ledger.write_meta(client, config.bucket, layout.meta(run_id), meta)

    def upsert(status, finished=None, custom=None):
        ledger.upsert_run(
            client,
            config.bucket,
            layout.ledger,
            {
                "run_id": run_id,
                "status": status,
                "started": started,
                "finished": finished,
                "commit": commit,
                "origin_run_id": origin_run_id,
                **params,
                **(custom or {}),
            },
            custom_fields,
        )

    cat.ensure_tables(flow_cls)  # before any run object exists: a run implies a catalog
    flush()
    upsert("running")

    def flush_metrics(rows):  # periodic (heartbeat) and final metrics write
        metrics.write_parquet(client, config.bucket, layout.metrics(run_id), rows)

    sampler = metrics.Sampler(flush=flush_metrics).start()

    def write_metrics():
        try:
            flush_metrics(sampler.stop())
        except Exception:  # noqa: BLE001 — metrics must never mask the run's outcome
            pass

    def register(name):
        cat.register_step(name, [layout.part(run_id, name, i) for i in range(_count_of(completed, name))])

    def run_group(group):
        """Execute the fan-out's sub-DAG per item — one worker owns one item end to end.

        No barrier between sub-steps: item A can be in the last one while item B
        is still in the first, so only `workers` items are ever in flight.
        """
        n = _count_of(completed, group.fanout)
        for member in group.order:  # key-matched copies first: they shrink every item's work
            left = [i for i in range(n) if _key(member, i) not in completed]
            # key-matched reuse also needs the step's OTHER inputs identical
            # to the origin's — any other keyed ancestor's change taints them
            root = key_roots.get(member)
            if left and root in fanout_keys and work_lists_unchanged(member, excluding=root):
                reuse_matching_branches(member, root, left)
        items = {}
        for i in range(n):
            if pending := [m for m in group.order if _key(m, i) not in completed]:
                items[i] = pending
        error = None
        if items:
            # process mode: workers measure the sub-steps; the orchestrator's pool-wait
            # must not be attributed to them (step stays NULL). In thread mode one
            # process runs interleaved items, so the whole group answers to one label.
            label = group.order[0] if len(group.order) == 1 else group.fanout
            sampler.mark(None if use_processes else label)
            done, error = _run_items(
                flow_cls,
                config,
                run_id,
                group,
                items,
                max_workers,
                use_processes,
                branch_retries,
                params,
            )
            for i, members in done.items():  # only the sub-steps that succeeded
                completed.update(_key(m, i) for m in members)
        for member in group.order:
            if _count_of(completed, member) == n:  # sub-steps done for every item stay
                register(member)  # queryable even when a later one killed the run
        if error:
            raise error

    try:
        for name in dag.plan:  # a fan-out node covers its group; members never stand alone
            step = dag.steps[name]
            if name in lazy_reuse and work_lists_unchanged(name):
                reuse_step(name)
            group = dag.groups.get(name)
            if step.is_fanout:
                if not _count_of(completed, name):  # skip if a prior run already produced it
                    sampler.mark(name)
                    key_col = keyed.get(name)
                    keys, item_keys, seen = [], [], set()
                    for i, item in enumerate(_call(con, client, config, layout, flow, step, run_id, None)):
                        value = write_part(
                            client,
                            config.bucket,
                            layout.part(run_id, name, i),
                            item,
                            step.output_table,
                            fanout_key=key_col,
                        )
                        if key_col:
                            if value in seen:
                                raise RuntimeError(
                                    f"fan-out '{name}': duplicate fanout key {value!r}; the "
                                    f"'{key_col}' column must identify each item uniquely"
                                )
                            seen.add(value)
                            item_keys.append(value)
                        keys.append(_key(name, i))
                    if key_col:
                        fanout_keys[name] = item_keys
                    # all-or-nothing: a partial fan-out must not look resumable,
                    # or resume would silently run with fewer branches
                    completed.update(keys)
                register(name)  # before its items run: a failing item leaves the fan-out queryable
                if group:
                    run_group(group)  # registers its own members, complete ones even on failure
            else:
                if _key(name, 0) not in completed:
                    sampler.mark(name)
                    result = _call(con, client, config, layout, flow, step, run_id, None)
                    write_part(client, config.bucket, layout.part(run_id, name, 0), result, step.output_table)
                    completed.add(_key(name, 0))
                register(name)
            flush()

        missing = [c for c in ledger_cols if not hasattr(flow, c)]
        if missing:
            raise RuntimeError(f"ledger columns not populated during run: {missing}")
        custom = {c: getattr(flow, c) for c in ledger_cols}
        finished = utc_now()
        flush(finished, custom)
        upsert("complete", finished, custom)
    except Exception:
        flush()  # persist partial progress so resume can reuse completed branches
        upsert("failed", finished=utc_now())
        raise
    finally:
        write_metrics()  # success AND failure — failed runs need perf data most

    return run_id
