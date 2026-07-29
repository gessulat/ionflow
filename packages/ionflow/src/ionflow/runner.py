import graphlib
import importlib.util
import inspect
import os
import threading
import typing
import uuid
import warnings
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from dataclasses import asdict
from multiprocessing import get_context

import psutil

from . import lake, metrics
from .arrow import StepInput
from .config import Config
from .git import require_clean
from .space import Space, key_slug, utc_now
from .state import Manifest, RunRecord, Slot
from .store import Store


def _new_run_id(now):
    return f"{int(now.timestamp() * 1000):013d}-{uuid.uuid4().hex[:8]}"


def _keyed_fanouts(dag):
    """Fan-out step name -> its output table's fanout_key column, keyed steps only."""
    out = {}
    for name, step in dag.steps.items():
        if step.is_fanout and step.output_table.fanout_key_column():
            out[name] = step.output_table.fanout_key_column()
    return out


def _latest_complete(space):
    """run_id of the newest complete run, or None (ledger rows sort by start time).

    Complete only — resuming must never chain onto a failed or running origin.
    `stats`' default "latest" is the looser newest-by-start-time of any status.
    """
    rows = space.read_rows(space.ledger)
    return next((r["run_id"] for r in reversed(rows) if r["status"] == "complete"), None)


def _fair_share(workers):
    """Per-worker DuckDB caps: workers x per-worker share ~ the machine (cores, 80% RAM)."""
    cores = os.cpu_count() or 1
    memory_mb = int(psutil.virtual_memory().total * 0.8) // workers // 2**20
    return max(1, cores // workers), memory_mb


class _Ctx(typing.NamedTuple):
    """What calling a step needs, on either side of the process boundary.

    Never pickled — each side builds its own, and they differ only in `resolve`:
    the orchestrator resolves inputs from the run's live Manifest, a worker from
    the paths its payload carries (plus what it wrote itself).
    """

    con: typing.Any
    space: Space
    flow: typing.Any
    run_id: str
    resolve: typing.Callable  # (con, dep, kind, item_key, dep_table) -> relation
    threads: int | None = None
    memory_mb: int | None = None


def _call(ctx, step, key):
    dag = ctx.flow.__class__._dag
    kwargs = {
        dep: StepInput(ctx.resolve(ctx.con, dep, kind, key, dag.steps[dep].output_table))
        for dep, kind in step.param_kinds.items()
    }
    return step.method(ctx.flow, **kwargs)


def _emit(ctx, step, key=None):
    """Call `step` and write what it returned; yield (Slot, part path) per part.

    The one call -> write -> report path, shared by the orchestrator and the item
    workers. A generator because that is the only difference between the three
    shapes: a plain step and a branch sub-step yield one part (named by `key`),
    a fan-out one per item — keyed items named by their extracted fanout_key
    value, positional ones by index. Recording completion stays with the caller —
    a fan-out is all-or-nothing, a sub-step reports through its worker.
    """
    result = _call(ctx, step, key)
    table = step.output_table
    if not step.is_fanout:
        k = 0 if key is None else key
        dest = ctx.space.part(ctx.run_id, step.name, k)
        ctx.space.write_part(dest, result, table, threads=ctx.threads, memory_mb=ctx.memory_mb)
        yield Slot(step.name, k), dest
        return
    key_col = table.fanout_key_column()
    seen = set()

    def dest_for(value):  # keyed: the part's name derives from its extracted key
        if value in seen:
            raise RuntimeError(
                f"fan-out '{step.name}': duplicate fanout key {value!r}; the "
                f"'{key_col}' column must identify each item uniquely"
            )
        seen.add(value)
        return ctx.space.part(ctx.run_id, step.name, value)

    for i, item in enumerate(result):
        dest = dest_for if key_col else ctx.space.part(ctx.run_id, step.name, i)
        value = ctx.space.write_part(
            dest, item, table, threads=ctx.threads, memory_mb=ctx.memory_mb, fanout_key=key_col
        )
        k = value if key_col else i
        yield Slot(step.name, k), ctx.space.part(ctx.run_id, step.name, k)


def _import(file, name):
    spec = importlib.util.spec_from_file_location("_mf_flow", file)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)


class _Bound(typing.NamedTuple):
    """What a worker process builds once per group and reuses for every item it gets.

    Deliberately NOT the DuckDB connection. Re-executing the flow module per item
    is pure waste, but an item's *database* is not: cursors of one connection share
    a catalog, so a step that materializes a named object (CREATE TABLE/VIEW/macro)
    would collide with the other items this worker handles. One instance per item
    also keeps `_fair_share`'s caps meaning what they did — a per-worker share of
    the machine — instead of one capped instance serving every item.
    """

    cfg: Config
    flow_cls: typing.Any
    space: Space
    threads: int | None
    memory_mb: int | None


_SHARED: dict[tuple, dict] = {}  # (run_id, fan-out) -> the pool's shared payload
_BOUND: dict[tuple, _Bound] = {}  # (run_id, fan-out) -> this process's binding
_BOUND_LOCK = threading.Lock()  # thread mode submits items concurrently into one process


def _init_group(shared):
    """Pool initializer: the group's shared payload lands once per process.

    Item payloads stay small this way — a wide Collect input's path list is
    pickled once per worker (spawn pickles initargs once), not once per item.
    """
    _SHARED[(shared["run_id"], shared["group"])] = shared


def _bound(shared):
    """This process's binding for one group, built on first use.

    The pool reuses its processes, so binding per item would re-execute the flow
    module — the user's own import-time code — once per item. Keyed on
    (run_id, group) rather than cached outright: in thread mode this process
    outlives the run, and a later run (or a later group of the same run) must
    never see the first one's flow class, Space or caps. `_run_items` drops the
    entry when the pool closes.
    """
    with _BOUND_LOCK:
        group_key = (shared["run_id"], shared["group"])
        if (bound := _BOUND.get(group_key)) is not None:
            return bound
        threads, memory_mb = _fair_share(shared["workers"])
        if shared["sample"]:  # own process: cap BLAS/OpenMP before the flow's imports
            # spawn imports numpy/torch fresh in this process, so the caps take effect;
            # setdefault keeps any explicit user setting. Never in thread mode — that
            # would mutate the orchestrator's environment.
            for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
                os.environ.setdefault(var, str(threads))
        cfg = Config(**shared["config"])
        flow_cls = _import(shared["file"], shared["flow"])
        # identity travels in the payload, never derived here: the re-imported
        # class's __module__ is the loader's ("_mf_flow"), so its own flow_id() is wrong
        space = Store.of(cfg).space(shared["flow_id"])
        _BOUND[group_key] = bound = _Bound(cfg, flow_cls, space, threads, memory_mb)
        return bound


def _branch_worker(item):
    shared = _SHARED[(item["run_id"], item["group"])]
    bound = _bound(shared)
    flow_cls, space = bound.flow_cls, bound.space
    flow = flow_cls()  # per item: thread-mode items share this process, not their `self`
    flow.config = bound.cfg
    for k, v in shared["params"].items():
        setattr(flow, k, v)
    run_id, key, pending = item["run_id"], item["key"], item["steps"]
    written = {}  # member -> part path, as this worker completes them

    def resolve(rcon, dep, kind, _key, table):
        """Payload-backed input resolution: `written` covers members this worker
        produced, `inputs` every branch input that already existed, and the
        shared `deps` map the group's whole-step (non-branch) inputs."""
        if kind == "BRANCH":
            return space.relation(rcon, [written.get(dep) or item["inputs"][dep]])
        paths = shared["deps"].get(dep) or []
        return space.relation(rcon, paths) if paths else table.empty(rcon)

    # this item's OWN DuckDB instance: private catalog and spill dir, and the
    # caps are a per-worker share of the machine (see _Bound)
    con = space.store.connect(threads=bound.threads, memory_mb=bound.memory_mb)
    ctx = _Ctx(con, space, flow, run_id, resolve, bound.threads, bound.memory_mb)
    group = flow_cls._dag.groups[item["group"]]
    # own sampler only in a separate process; in thread mode the orchestrator's
    # sampler already covers this process (a second one would double-count CPU).
    # Each worker flushes to its OWN metrics key (one writer per object, no CAS),
    # so branch samples survive a run killed mid-branch up to the last flush.
    sampler, active, label_lock = None, set(), threading.Lock()

    def relabel():
        # concurrent members share one process, so they share one sample label
        if sampler:
            sampler.mark("+".join(sorted(active)) or None, str(key))

    if shared["sample"]:
        metrics_key = space.metrics_branch(run_id, group.fanout, key_slug(key))

        def flush_metrics(rows):
            metrics.write_parquet(space, metrics_key, rows)

        sampler = metrics.Sampler(flush=flush_metrics).start()
    retries = shared["retries"]

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
                    for _slot, path in _emit(ctx._replace(con=cur), step, key):
                        written[name] = path
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
    return {"key": key, "done": done, "error": error}


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


def _run_items(flow_cls, cfg, run_id, group, items, deps, max_workers, use_processes, retries, params):
    """Run each item's sub-DAG in its own worker; return ({item key: sub-steps done}, first error).

    `items` maps item key -> (pending sub-steps, that item's resolved branch-input
    paths); `deps` are the group's whole-step inputs, shared by every item.
    Failures don't cancel siblings, so a partial fan-out leaves the completed
    items' parts durably on S3 for a later resume to reuse. Process-mode
    workers write their own per-item metrics objects; the `metrics` view
    unions them with the orchestrator's.
    """
    cores = os.cpu_count() or 1
    workers = min(max_workers or cores, len(items))
    shared = {
        "config": asdict(cfg),
        "file": inspect.getfile(flow_cls),
        "flow": flow_cls.__name__,  # for getattr after the file import
        "flow_id": flow_cls.flow_id(),  # for the Space; see _bound
        "group": group.fanout,
        "run_id": run_id,
        "sample": use_processes,
        "retries": retries,
        "params": params,
        "deps": deps,  # non-branch dep -> its part paths, so no worker LISTs for them
        "workers": workers,  # workers derive their own caps: same machine, one formula
    }
    if use_processes:
        # spawn, never fork: the orchestrator is multi-threaded here (sampler,
        # DuckDB workers), and non-branch steps ran user code in this process,
        # so forked children inherit OpenMP/BLAS/CUDA state mid-flight (hangs,
        # "cannot re-initialize CUDA"). Spawn on 3.12/3.13 Linux too, where the
        # stdlib default is still fork.
        pool = ProcessPoolExecutor(
            max_workers=workers, mp_context=get_context("spawn"), initializer=_init_group, initargs=(shared,)
        )
    else:
        pool = ThreadPoolExecutor(max_workers=workers, initializer=_init_group, initargs=(shared,))
    done, error = {}, None
    try:
        with pool as ex:
            futures = [
                ex.submit(
                    _branch_worker,
                    {"run_id": run_id, "group": group.fanout, "key": k, "steps": steps, "inputs": inputs},
                )
                for k, (steps, inputs) in items.items()
            ]
            for fut in as_completed(futures):
                try:
                    result = fut.result()
                except Exception as e:  # noqa: BLE001 — a worker that died without reporting
                    error = error if error is not None else e
                    continue
                done[result["key"]] = result["done"]
                if error is None:  # surface the first, keep the rest
                    error = result["error"]
    finally:
        # the pool has joined, so nothing can still hold the binding: drop it and
        # its connection's spill dir. A no-op in the parent under process mode,
        # where only the (now dead) children ever bound anything.
        group_key = (run_id, group.fanout)
        _BOUND.pop(group_key, None)
        _SHARED.pop(group_key, None)
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
    store = Store.of(config)
    space = Space.for_flow(store, flow_cls)
    origin_run_id = _latest_complete(space) if resume == "latest" else resume
    # connecting here also installs httpfs once, before any worker pool spawns
    # (N cold workers would otherwise race the extension download)
    con = store.connect()
    cat = lake.Catalog(space)
    flow = flow_cls()
    flow.config = config
    dag = flow_cls._dag
    ledger_cols = flow_cls.ledger_columns()
    custom_fields = flow_cls.custom_ledger_fields()

    manifest = Manifest()
    origin = Manifest()  # the origin run's parts, empty on a fresh run
    keyed = _keyed_fanouts(dag)
    # Steps downstream of a keyed fan-out can't be reused upfront: the fan-out
    # re-runs first (the work-list may have changed), branch parts are then
    # matched by item key, and everything else downstream is reused only when
    # the key set is unchanged.
    keyed_downstream = {f: dag.downstream(f) for f in keyed} if origin_run_id else {}
    deferred = set().union(*keyed_downstream.values()) if keyed_downstream else set()
    # Branch identity travels down a chain (0014), so every step of a keyed
    # fan-out's group is named by that fan-out's item keys, however deep in a
    # branch chain it sits — reuse is a key lookup, never positional (0018).
    key_roots = {m: r for m, r in dag.roots.items() if r in keyed} if origin_run_id else {}
    lazy_reuse = {s for s in deferred if s not in keyed and s not in key_roots}

    def copy_slots(slots):
        """Server-side copy origin parts into this run, recorded as each copy
        lands — a failure mid-iteration keeps the prefix as real progress."""
        if not slots:
            return
        triples = [(origin.get(s.step, s.key), space.part(run_id, s.step, s.key), s) for s in slots]
        for slot in space.copy_parts(triples):
            manifest.add(slot, space.part(run_id, slot.step, slot.key))

    def work_lists_unchanged(name, excluding=None):
        """True when every keyed fan-out upstream of `name` (bar `excluding`) kept its key set."""
        return all(
            set(manifest.keys(f)) == set(origin.keys(f))
            for f, ds in keyed_downstream.items()
            if f != excluding and name in ds
        )

    if origin_run_id:
        origin_meta = space.read_meta(origin_run_id)
        if origin_meta is None:
            raise RuntimeError(f"origin run not found: {origin_run_id}")
        if origin_meta.get("commit") != commit:
            # a mixed-commit run must be a visible, deliberate choice
            if not allow_commit_drift:
                raise CommitDriftError(
                    f"origin run {origin_run_id} is at commit {origin_meta.get('commit')}, HEAD is "
                    f"{commit}; reused steps would keep old-code outputs. Pass "
                    "allow_commit_drift=True (CLI: --allow-commit-drift) to resume anyway."
                )
            warnings.warn(f"resuming {origin_run_id} at a different commit; reused steps keep old code", stacklevel=2)
        origin = Manifest.from_meta(origin_meta.get("manifest"))
        # server-side copies, IO-bound: one batch, one pool
        copy_slots([s for s, _ in origin.slots() if s.step not in deferred])
        for k, v in (origin_meta.get("custom") or {}).items():  # reused steps won't reset these
            setattr(flow, k, v)
        params = origin_meta.get("params") or {}
        if dropped := set(params) - set(flow_cls.params_columns()):
            warnings.warn(f"origin params no longer declared, ignored: {sorted(dropped)}", stacklevel=2)
            params = {k: v for k, v in params.items() if k not in dropped}

    params = flow_cls.resolve_params(params)
    for k, v in params.items():
        setattr(flow, k, v)

    record = RunRecord(
        run_id=run_id,
        status="running",
        started=started,
        finished=None,
        commit=commit,
        origin_run_id=origin_run_id,
        params=params,
        custom={},
        custom_fields=custom_fields,
    )

    cat.ensure_tables(flow_cls)  # before any run object exists: a run implies a catalog
    record.persist(space, manifest, "running")

    def flush_metrics(rows):  # periodic (heartbeat) and final metrics write
        metrics.write_parquet(space, space.metrics(run_id), rows)

    sampler = metrics.Sampler(flush=flush_metrics).start()

    def write_metrics():
        try:
            flush_metrics(sampler.stop())
        except Exception:  # noqa: BLE001 — metrics must never mask the run's outcome
            pass

    def register(name):
        cat.register_parts({name: manifest.paths(name)})

    def resolve(rcon, dep, kind, key, table):
        """Manifest-backed input resolution (BRANCH = this item's part, else all parts)."""
        if kind == "BRANCH":
            return space.relation(rcon, [manifest.get(dep, key)])
        paths = manifest.paths(dep)
        return space.relation(rcon, paths) if paths else table.empty(rcon)

    ctx = _Ctx(con, space, flow, run_id, resolve)  # uncapped: the orchestrator owns the machine

    def group_deps(group):
        """The group's whole-step inputs: {non-branch dep: part paths} — external
        by construction (a per-item step reads its siblings via Branch only)."""
        return {
            dep: manifest.paths(dep)
            for m in group.order
            for dep, kind in dag.steps[m].param_kinds.items()
            if kind != "BRANCH"
        }

    def item_inputs(pending, key):
        """One item's already-written branch inputs — the fan-out part and any
        member finished earlier; the worker derives only the paths it writes."""
        pend = set(pending)
        return {
            dep: manifest.get(dep, key)
            for m in pending
            for dep, kind in dag.steps[m].param_kinds.items()
            if kind == "BRANCH" and dep not in pend
        }

    def run_group(group):
        """Execute the fan-out's sub-DAG per item — one worker owns one item end to end.

        No barrier between sub-steps: item A can be in the last one while item B
        is still in the first, so only `workers` items are ever in flight.
        """
        keys = manifest.keys(group.fanout)
        for member in group.order:  # key-matched reuse first: it shrinks every item's work
            root = key_roots.get(member)
            # key-matched reuse also needs the step's OTHER inputs identical
            # to the origin's — any other keyed ancestor's change taints them
            if root is None or not work_lists_unchanged(member, excluding=root):
                continue
            copy_slots(
                [
                    Slot(member, k)
                    for k in keys
                    if Slot(member, k) not in manifest and origin.get(member, k) is not None
                ]
            )
        items = {}
        for k in keys:
            if pending := [m for m in group.order if Slot(m, k) not in manifest]:
                items[k] = (pending, item_inputs(pending, k))
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
                group_deps(group),
                max_workers,
                use_processes,
                branch_retries,
                params,
            )
            for k, members in done.items():  # only the sub-steps that succeeded
                for m in members:
                    manifest.add(Slot(m, k), space.part(run_id, m, k))
        for member in group.order:
            if manifest.width(member) == len(keys):  # sub-steps done for every item stay
                register(member)  # queryable even when a later one killed the run
        if error:
            raise error

    try:
        for name in dag.plan:  # a fan-out node covers its group; members never stand alone
            step = dag.steps[name]
            if name in lazy_reuse and work_lists_unchanged(name):
                copy_slots([Slot(name, k) for k in origin.keys(name) if Slot(name, k) not in manifest])
            group = dag.groups.get(name)
            if step.is_fanout:
                if not manifest.width(name):  # skip if a prior run already produced it
                    sampler.mark(name)
                    # all-or-nothing: a partial fan-out must not look resumable,
                    # or resume would silently run with fewer branches
                    for slot, path in list(_emit(ctx, step)):
                        manifest.add(slot, path)
                register(name)  # before its items run: a failing item leaves the fan-out queryable
                if group:
                    run_group(group)  # registers its own members, complete ones even on failure
            else:
                if Slot(name, 0) not in manifest:
                    sampler.mark(name)
                    for slot, path in _emit(ctx, step):
                        manifest.add(slot, path)
                register(name)
            space.write_meta(run_id, record.meta(manifest))

        missing = [c for c in ledger_cols if not hasattr(flow, c)]
        if missing:
            raise RuntimeError(f"ledger columns not populated during run: {missing}")
        record.custom = {c: getattr(flow, c) for c in ledger_cols}
        record.finished = utc_now()
        record.persist(space, manifest, "complete")
    except Exception:
        # persist partial progress so resume can reuse completed items
        record.finished = utc_now()
        record.persist(space, manifest, "failed")
        raise
    finally:
        write_metrics()  # success AND failure — failed runs need perf data most

    return run_id
