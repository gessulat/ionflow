import graphlib
import importlib.util
import inspect
import os
import sys
import threading
import typing
import uuid
import warnings
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import asdict
from multiprocessing import get_context

import psutil

from . import lake, metrics
from .arrow import StepInput
from .artifact import Artifact, Artifacts
from .config import Config
from .git import require_clean
from .phases import Phases
from .plan import Planner
from .provenance import ArtifactRuntime, activity_key, bindings, reused_activity
from .space import Space, key_slug, utc_now
from .state import Manifest, RunRecord, Slot
from .store import Store


def _new_run_id(now):
    return f"{int(now.timestamp() * 1000):013d}-{uuid.uuid4().hex[:8]}"


def _fair_share(workers):
    """Per-worker DuckDB caps: workers x per-worker share ~ the machine (cores, 80% RAM)."""
    cores = os.cpu_count() or 1
    memory_mb = int(psutil.virtual_memory().total * 0.8) // workers // 2**20
    return max(1, cores // workers), memory_mb


class _Ctx(typing.NamedTuple):
    """What calling a step needs inside a worker (never pickled — each worker
    builds its own; `resolve` maps a dependency to its input relation)."""

    con: typing.Any
    space: Space
    flow: typing.Any
    run_id: str
    resolve: typing.Callable  # (con, dep, kind, item_key, dep_table) -> relation
    threads: int | None = None
    memory_mb: int | None = None


def _call(ctx, step, key):
    dag = ctx.flow.__class__._dag
    kwargs = {}
    for dep, kind in step.param_kinds.items():
        if kind == "INPUT":
            value = ctx.flow._artifact_inputs[dep]
            for ref in value if isinstance(value, list) else [value]:
                ctx.flow.artifacts.use(ref, role=dep)
            kwargs[dep] = value
        else:
            value = ctx.resolve(ctx.con, dep, kind, key, dag.steps[dep].output_table)
            kwargs[dep] = value if dag.steps[dep].is_artifact else StepInput(value)
    return step.method(ctx.flow, **kwargs)


def _emit(ctx, step, key=None):
    """Call `step` and write what it returned; yield (Slot, part path) per part.

    The one call -> write -> report path for every task shape: a plain step and
    a branch sub-step yield one part (named by `key`), a fan-out one per item —
    keyed items named by their extracted fanout_key value, positional ones by
    index. Recording completion stays with the caller — a fan-out is
    all-or-nothing, a sub-step reports through its worker.
    """
    result = _call(ctx, step, key)
    table = step.output_table
    runtime = ctx.flow.artifacts
    if step.is_artifact:
        seen = set()
        baseline = runtime.snapshot()
        for value in result if step.is_fanout else [result]:
            if not isinstance(value, Artifact) or value._kind != table._kind:
                raise TypeError(f"step '{step.name}' must return {table._kind} artifacts")
            ref = runtime.publish(value)
            k = ref.id if step.is_fanout else 0 if key is None else key
            if k in seen:
                raise ValueError(f"fan-out '{step.name}': duplicate artifact {k}")
            seen.add(k)
            yield Slot(step.name, k), ref.id, runtime.snapshot(key=k, narrow=step.is_fanout)
            runtime.restore(baseline)
        return
    if not step.is_fanout:
        k = 0 if key is None else key
        dest = ctx.space.part(ctx.run_id, step.name, k)
        ctx.space.write_part(dest, result, table, threads=ctx.threads, memory_mb=ctx.memory_mb)
        yield Slot(step.name, k), dest, runtime.snapshot(key=k)
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
        yield Slot(step.name, k), ctx.space.part(ctx.run_id, step.name, k), runtime.snapshot(key=k)


def _import(file, name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, file)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    return getattr(mod, name)


class _Bound(typing.NamedTuple):
    """What a worker process builds once per run and reuses for every task it gets.

    Deliberately NOT the DuckDB connection. Re-executing the flow module per task
    is pure waste, but a task's *database* is not: cursors of one connection share
    a catalog, so a step that materializes a named object (CREATE TABLE/VIEW/macro)
    would collide with the other tasks this worker handles.
    """

    cfg: Config
    flow_cls: typing.Any
    space: Space


_RUN: dict[str, dict] = {}  # run_id -> the run's shared payload (pool initializer)
_BOUND: dict[str, _Bound] = {}  # run_id -> this process's binding
_DEPS: dict[tuple, dict] = {}  # (run_id, node, phase) -> manifest and provenance snapshot
_BOUND_LOCK = threading.Lock()  # thread mode submits tasks concurrently into one process


def _init_run(shared):
    """Pool initializer: the run's constants land once per process (spawn
    pickles initargs once), so task payloads stay a few keys."""
    _RUN[shared["run_id"]] = shared


def _bound(shared, threads):
    """This process's binding for the run, built on first use.

    Keyed on run_id rather than cached outright: in thread mode this process
    outlives the run, and a later run must never see the first one's flow
    class or Space; `run()` drops the entry when the pool closes. `threads`
    (the first task's fair share) caps BLAS before the flow's imports.
    """
    with _BOUND_LOCK:
        if (bound := _BOUND.get(shared["run_id"])) is not None:
            return bound
        if shared["sample"]:  # own process: cap BLAS/OpenMP before the flow's imports
            # spawn imports numpy/torch fresh in this process, so the caps take effect;
            # setdefault keeps any explicit user setting. Never in thread mode — that
            # would mutate the orchestrator's environment.
            for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
                os.environ.setdefault(var, str(threads))
        cfg = Config(**shared["config"])
        flow_cls = _import(shared["file"], shared["flow"], "_ionflow_" + shared["run_id"].replace("-", "_"))
        # identity travels in the payload, never derived here: the re-imported
        # The isolated loader's module name is not the flow's durable identity.
        space = Store.of(cfg).space(shared["flow_id"])
        _BOUND[shared["run_id"]] = bound = _Bound(cfg, flow_cls, space)
        return bound


def _node_manifest(space, run_id, node, phase):
    """The run's manifest as this node's tasks may read it, off `_meta.json` —
    the orchestrator flushes before scheduling a node, so every input a task
    resolves is in it. One GET per (process, node, phase); a group's items
    share one snapshot, taken separately from their fan-out's (whose own
    snapshot predates the fan-out's parts).
    """
    if (key := (run_id, node, phase)) not in _DEPS:
        _DEPS[key] = space.read_meta(run_id) or {}
    return _DEPS[key]


def _task_worker(task):
    """One scheduler task: a whole plain/fan-out step (`key` None), or one
    item's pending sub-steps. Returns the parts written, the first error (with
    the parts that still completed), and any declared Ledger columns a
    whole-step task set on `self`.
    """
    shared = _RUN[task["run_id"]]
    bound = _bound(shared, task["threads"])
    flow_cls, space = bound.flow_cls, bound.space
    flow = flow_cls()  # per task: thread-mode tasks share this process, not their `self`
    flow.config = bound.cfg
    registry = Artifacts(space.store)
    flow.artifacts = ArtifactRuntime(registry, shared["flow_id"], shared["run_id"])
    flow._phases = Phases()
    declarations = flow_cls.Inputs.declarations() if flow_cls.Inputs else {}
    flow._artifact_inputs = bindings(registry, shared["artifacts"], declarations)
    for name, value in flow._artifact_inputs.items():
        setattr(flow, name, value)
    for k, v in {**shared["params"], **task["custom"]}.items():
        setattr(flow, k, v)
    run_id, node, key, pending = task["run_id"], task["node"], task["key"], task["steps"]
    meta = _node_manifest(space, run_id, node, "step" if key is None else "items")
    manifest = Manifest.from_meta(meta.get("manifest"))
    activities = dict(meta.get("activities") or {})
    reported = {}
    written = {}  # step -> part path, as this task completes them

    def resolve(rcon, dep, kind, k, table):
        """Inputs come from the node's manifest snapshot; `written` overlays the
        members this task produced itself (BRANCH = this item's part only)."""
        keys = [k] if kind == "BRANCH" else manifest.keys(dep)
        paths = [written.get(dep) or manifest.get(dep, k)] if kind == "BRANCH" else manifest.paths(dep)
        refs = []
        for dep_key, path in zip(keys, paths, strict=True):
            activity = activities.get(activity_key(dep, dep_key), {})
            flow.artifacts.inherit(activity.get("ancestors", []))
            if issubclass(table, Artifact):
                output = next(o for o in activity["outputs"] if o["role"] == "output" and o["artifact"]["id"] == path)
                refs.append(flow.artifacts.use(registry.get(path, table, snapshot=output["artifact"]), role=dep))
        if issubclass(table, Artifact):
            return refs if kind == "COLLECT" or flow_cls._dag.steps[dep].is_fanout and kind != "BRANCH" else refs[0]
        if kind == "BRANCH":
            return space.relation(rcon, paths)
        return space.relation(rcon, paths) if paths else table.empty(rcon)

    # this task's OWN DuckDB instance: private catalog and spill dir, capped to
    # the fair share the orchestrator computed from the tasks in flight
    con = space.store.connect(threads=task["threads"], memory_mb=task["memory_mb"])
    ctx = _Ctx(con, space, flow, run_id, resolve, task["threads"], task["memory_mb"])
    # own sampler only in a separate process; in thread mode the orchestrator's
    # sampler already covers this process (a second one would double-count CPU).
    # Each task flushes to its OWN metrics key (one writer per object, no CAS),
    # so its samples survive a run killed mid-task up to the last flush.
    sampler, active, label_lock = None, set(), threading.Lock()
    metrics_key = (
        space.metrics_node(run_id, node) if key is None else space.metrics_branch(run_id, node, key_slug(key))
    )
    phases_key = metrics_key.replace("/_metrics", "/_phases")

    def relabel():
        # concurrent members share one process, so they share one sample label
        if sampler:
            sampler.mark("+".join(sorted(active)) or None, None if key is None else str(key))

    if shared["sample"]:
        def flush_metrics(rows):
            metrics.write_parquet(space, metrics_key, rows)
            flow._phases.write(space, phases_key)

        sampler = metrics.Sampler(flush=flush_metrics).start()
    retries = shared["retries"]
    parts = []  # (step, key, path) triples, reported even when a later member fails

    def run_member(name):
        """One step of this task. Group sub-steps retry on their own — a flaky
        upload is not a re-search; whole-step tasks never retry."""
        step = flow_cls._dag.steps[name]
        tries = 1 if name == node else retries + 1  # retries the whole sub-step, not just
        # the S3 write: transient step errors count too, and boto3 retries at the HTTP layer
        cur = con.cursor()  # own cursor: members of one item may run concurrently
        with label_lock:
            active.add(name)
            relabel()
        try:
            for attempt in range(tries):
                flow.artifacts.begin(name, 0 if key is None else key)
                try:
                    with flow._phases.step(name, key):
                        for slot, path, activity in _emit(ctx._replace(con=cur), step, key):
                            written[name] = path
                            parts.append((slot.step, slot.key, path))
                            ak = activity_key(slot.step, slot.key)
                            if activity["ancestors"] or activity["outputs"]:
                                activities[ak] = reported[ak] = activity
                    return
                except Exception:
                    if attempt == tries - 1:
                        raise
        finally:
            if not step.is_fanout or not parts:
                ak = activity_key(name, 0 if key is None else key)
                if ak not in reported:
                    activity = flow.artifacts.snapshot()
                    if activity["ancestors"] or activity["outputs"]:
                        activities[ak] = reported[ak] = activity
            with label_lock:
                active.discard(name)
                relabel()
            cur.close()

    error = None
    try:
        if len(pending) == 1:  # the common single-step task: no pool, no extra thread
            try:
                run_member(pending[0])
            except Exception as e:  # noqa: BLE001 — reported to the orchestrator with the progress
                error = e
        else:
            _, error = _run_sub_dag(flow_cls._dag.groups[node], pending, run_member)
    finally:
        if sampler:  # final superset write, on failure too — that's where perf data matters most
            try:
                flush_metrics(sampler.stop())
            except Exception:  # noqa: BLE001 — metrics must never mask the task's outcome
                pass
        else:
            try:
                flow._phases.write(space, phases_key)
            except Exception:  # noqa: BLE001 — telemetry must never mask the task's outcome
                pass
    custom = {c: getattr(flow, c) for c in flow_cls.ledger_columns() if hasattr(flow, c)} if key is None else {}
    # progress travels with the failure: resume restarts an item at its first
    # incomplete sub-step, not at the top
    return {"parts": parts, "error": error, "custom": custom, "activities": reported}


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


class CommitDriftError(RuntimeError):
    """Resume refused: HEAD differs from the origin run's commit."""


def preview(flow_cls, config, resume):
    """(origin run_id, {step: (reused, computed)}) for a would-be `resume`,
    assuming unchanged keyed work-lists — see `Planner.preview`. Runs nothing."""
    cat = lake.Catalog(Space.for_flow(Store.of(config), flow_cls))
    row, rows = cat.run_state(resume)
    if row is None:
        raise RuntimeError("no complete run to resume" if resume == "latest" else f"origin run not found: {resume}")
    return row["run_id"], Planner(flow_cls._dag, Manifest.from_rows(rows)).preview()


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
    artifacts=None,
):
    """Execute `flow_cls`; returns the new run_id.

    Every step body runs in a worker (0021); the orchestrator only schedules —
    independent chains of the DAG execute concurrently, sharing one pool.
    `resume` is the one resume spelling: an origin run_id reuses that run's
    completed steps; the string "latest" resumes the newest complete run and
    falls through to a fresh full run when there is none (so cron jobs can
    always pass it). Resume always produces a NEW run — its manifest
    references the origin's completed parts, and it computes only the rest.
    """
    if not config.local_dir:
        raise ValueError("runs require a local store; configure local_dir and optional remote")
    if allow_commit_drift and not resume:
        raise ValueError("allow_commit_drift only applies to a resumed run; pass resume= as well")
    if resume and (params or artifacts):
        raise ValueError("resume reuses the origin run's params and artifacts; do not pass params or artifacts")
    commit = require_clean(repo)
    started = utc_now()
    run_id = _new_run_id(started)
    store = Store.of(config)
    space = Space.for_flow(store, flow_cls)
    # connecting here installs httpfs once, before any worker pool spawns
    # (N cold workers would otherwise race the extension download)
    store.connect()
    cat = lake.Catalog(space)
    dag = flow_cls._dag
    ledger_cols = flow_cls.ledger_columns()

    # one snapshot answers everything resume needs: the origin's row + manifest
    origin_row, origin_rows = cat.run_state(resume) if resume else (None, [])
    if resume and resume != "latest" and origin_row is None:
        raise RuntimeError(f"origin run not found: {resume}")
    origin_run_id = origin_row["run_id"] if origin_row else None  # "latest" with no origin: fresh
    from .artifact import MissingPayload

    if origin_row and not cat.availability()[origin_row["run_id"]]["local"]:
        raise MissingPayload(f"run {origin_run_id}: download this run before resuming")

    manifest = Manifest()
    origin = Manifest.from_rows(origin_rows)  # the origin run's parts, empty on a fresh run
    planner = Planner(dag, origin)  # ALL reuse decisions; inert on a fresh run
    pending = []  # manifest triples written since the last catalog commit
    custom = {}  # declared Ledger columns, merged from whole-step task results
    activities, pending_activities = {}, {}

    def note(slot, path):
        """Record one part: in the live manifest and in the next catalog commit."""
        manifest.add(slot, path)
        pending.append((slot.step, slot.key, path))

    def reuse_slots(slots):
        """Reference origin parts in this run's manifest — no copies (0020):
        a part is immutable, written and registered once by the run that
        computed it; reuse is manifest rows pointing at the origin's objects."""
        for s in slots:
            note(s, origin.get(s.step, s.key))
            ak = activity_key(s.step, s.key)
            if origin_row and ak in origin_row.get("activities", {}):
                activities[ak] = pending_activities[ak] = reused_activity(origin_row["activities"][ak])

    if origin_row:
        if origin_row["commit"] != commit:
            # a mixed-commit run must be a visible, deliberate choice
            if not allow_commit_drift:
                raise CommitDriftError(
                    f"origin run {origin_run_id} is at commit {origin_row['commit']}, HEAD is "
                    f"{commit}; reused steps would keep old-code outputs. Pass "
                    "allow_commit_drift=True (CLI: --allow-commit-drift) to resume anyway."
                )
            warnings.warn(f"resuming {origin_run_id} at a different commit; reused steps keep old code", stacklevel=2)
        custom = dict(origin_row["custom"])  # reused steps won't reset these
        params = origin_row["params"]
        if dropped := set(params) - set(flow_cls.params_columns()):
            warnings.warn(f"origin params no longer declared, ignored: {sorted(dropped)}", stacklevel=2)
            params = {k: v for k, v in params.items() if k not in dropped}
    reuse_slots(planner.upfront())
    params = flow_cls.resolve_params(params)
    declarations = flow_cls.Inputs.declarations() if flow_cls.Inputs else {}
    artifact_inputs = origin_row["artifacts"] if origin_row else Artifacts(store).bind(declarations, artifacts)
    for value in artifact_inputs.values():
        for ref in value if isinstance(value, list) else [value]:
            if any(
                not store.exists(f"{config.prefix}/artifacts/_blobs/sha256/{f['sha256']}")
                for f in ref["content"]["files"]
            ):
                raise MissingPayload(f"{ref['id']}: download this artifact before starting a run")
    bindings(Artifacts(store), artifact_inputs, declarations)

    record = RunRecord(
        run_id=run_id,
        status="running",
        started=started,
        finished=None,
        commit=commit,
        origin_run_id=origin_run_id,
        params=params,
        custom={},
        artifacts=artifact_inputs,
    )

    def flush(row=False, genesis=None):
        """One catalog commit + the meta recovery export; no-op when unchanged."""
        if not (pending or pending_activities or row or genesis):
            return
        committed = cat.commit_run(
            run_id,
            row=record.row() if row else None,
            rows=list(pending),
            genesis=genesis,
            activities=list(pending_activities.values()),
        )
        pending.clear()
        pending_activities.clear()
        space.write_meta(
            run_id, record.meta(manifest, activities) | {"version": committed["version"], "files": committed["files"]}
        )
        from .storage import request_refresh

        request_refresh(config)

    # genesis + the running row + upfront reuse, atomically, before any part
    # object exists: a run implies a catalog
    flush(row=True, genesis=flow_cls)

    def flush_metrics(rows):  # periodic (heartbeat) and final metrics write
        metrics.write_parquet(space, space.metrics(run_id), rows)

    sampler = metrics.Sampler(flush=flush_metrics).start()

    workers = max_workers or os.cpu_count() or 1
    shared = {
        "config": asdict(config) | {"remote": None},
        "file": inspect.getfile(flow_cls),
        "flow": flow_cls.__name__,  # for getattr after the file import
        "flow_id": flow_cls.flow_id(),  # for the Space; see _bound
        "run_id": run_id,
        "sample": use_processes,
        "retries": branch_retries,
        "params": params,
        "artifacts": artifact_inputs,
    }
    if use_processes:
        # spawn, never fork: this process is multi-threaded (the sampler, DuckDB
        # inside the catalog commits), and forked children would inherit that
        # state mid-flight. Spawn on 3.12/3.13 Linux too, where the stdlib
        # default is still fork. Workers spawn on demand, up to the cap.
        pool = ProcessPoolExecutor(
            max_workers=workers, mp_context=get_context("spawn"), initializer=_init_run, initargs=(shared,)
        )
    else:
        pool = ThreadPoolExecutor(max_workers=workers, initializer=_init_run, initargs=(shared,))

    ts = graphlib.TopologicalSorter(dag.schedule_graph())
    ts.prepare()
    running = {}  # future -> its scheduler node
    left = {}  # node -> outstanding task count
    labels = Counter()  # thread mode: the sampler's active node labels
    error = None

    def mark(nk, delta):
        # process mode: workers measure their own steps, and the orchestrator's
        # pool-wait must not be attributed to any of them (its label stays NULL)
        if use_processes:
            return
        group = dag.groups.get(nk[1]) if isinstance(nk, tuple) else None
        label = nk if group is None else group.order[0] if len(group.order) == 1 else group.fanout
        labels[label] += delta
        sampler.mark("+".join(sorted(k for k, v in labels.items() if v > 0)) or None)

    def launch(nk):
        """Reuse first; then one task per remaining unit of work — a whole
        plain/fan-out step, or one item's pending sub-steps (one worker owns
        one item end to end, no barrier between sub-steps). A node with
        nothing left completes on the spot."""
        if group := (dag.groups[nk[1]] if isinstance(nk, tuple) else None):
            reuse_slots(planner.member_reuse(manifest, group))  # reuse shrinks every item's work
            tasks = [
                {"node": group.fanout, "key": k, "steps": todo}
                for k in manifest.keys(group.fanout)
                if (todo := [m for m in group.order if Slot(m, k) not in manifest])
            ]
        else:
            reuse_slots(planner.node_reuse(manifest, nk))
            have = manifest.width(nk) if dag.steps[nk].is_fanout else Slot(nk, 0) in manifest
            tasks = [] if have else [{"node": nk, "key": None, "steps": [nk]}]
        flush()  # reuse refs + upstream parts land before any worker reads meta
        if not tasks:
            ts.done(nk)
            return
        left[nk] = len(tasks)
        # a fair share of the machine per task, sized by what is actually in
        # flight — a solo step gets the whole machine, a wide wave splits it
        threads, memory_mb = _fair_share(min(workers, len(running) + len(tasks)))
        for t in tasks:
            t |= {"run_id": run_id, "custom": dict(custom), "threads": threads, "memory_mb": memory_mb}
            running[pool.submit(_task_worker, t)] = nk
            mark(nk, +1)

    def settle(fut):
        """Fold one finished task into the run; completes its node when last."""
        nonlocal error
        nk = running.pop(fut)
        mark(nk, -1)
        try:
            result = fut.result()
            parts, err = result["parts"], result["error"]
            custom.update(result["custom"])
            for ak, activity in result["activities"].items():
                activities[ak] = pending_activities[ak] = activity
        except Exception as e:  # noqa: BLE001 — a worker that died without reporting
            parts, err = [], e
        # all-or-nothing: a partial fan-out must not look resumable, or resume
        # would silently run with fewer branches; items keep partial progress
        if not (not isinstance(nk, tuple) and dag.steps[nk].is_fanout and err):
            for s, k, p in parts:
                note(Slot(s, k), p)
        if error is None:  # surface the first, keep the rest
            error = err
        left[nk] -= 1
        if not left[nk]:
            del left[nk]
            flush()  # the node's parts + manifest rows land in one commit
            if error is None:
                ts.done(nk)

    try:
        with pool:
            while True:
                while error is None and (ready := [*ts.get_ready()]):
                    for nk in ready:
                        launch(nk)
                if not running:  # done — or failed, with every in-flight task settled
                    break
                finished, _ = wait(list(running), return_when=FIRST_COMPLETED)
                for fut in finished:
                    settle(fut)
        if error:
            raise error
        if missing := [c for c in ledger_cols if c not in custom]:
            raise RuntimeError(f"ledger columns not populated during run: {missing}")
        record.custom = custom
        record.finished = utc_now()
        record.status = "complete"
        flush(row=True)
    except Exception:
        # commit partial progress so resume can reuse completed items
        record.finished = utc_now()
        record.status = "failed"
        flush(row=True)
        raise
    finally:
        # thread mode: the pool has joined, so nothing still holds the binding
        # (a no-op in process mode, where only the dead children ever bound)
        _RUN.pop(run_id, None)
        bound = _BOUND.pop(run_id, None)
        if bound is not None:
            sys.modules.pop(bound.flow_cls.__module__, None)
            flow_cls._registry.pop(bound.flow_cls.flow_id(), None)
        for k in [k for k in _DEPS if k[0] == run_id]:
            del _DEPS[k]
        try:
            flush_metrics(sampler.stop())
        except Exception:  # noqa: BLE001 — metrics must never mask the run's outcome
            pass

    return run_id
