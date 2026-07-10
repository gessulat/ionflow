import importlib
import pkgutil

from . import storage
from .flow import Flow


def discover_flows(package):
    """Import `package` and all its submodules, returning its {name: Flow subclass}.

    Registration is a side effect of importing (see Flow.__init_subclass__), so
    this walks the whole package to catch flows split across modules, then keeps
    only the flows defined under `package`. A TUI launch is a fresh process, so
    the registry reflects the current code with no stale entries.
    """
    mod = importlib.import_module(package)
    for info in pkgutil.walk_packages(getattr(mod, "__path__", []), mod.__name__ + "."):
        importlib.import_module(info.name)
    return {
        name: cls
        for name, cls in Flow._registry.items()
        if cls.__module__ == package or cls.__module__.startswith(package + ".")
    }


def scan_runs(config):
    """One LIST of the bucket prefix: stable flows with a ledger, one sample part
    key per step, and prototype flows with a scratch ledger.

    Returns (with_data, samples, proto_with_data). `samples` is {flow: {step:
    key}}, stable namespace only — the two namespaces must never mix, or a
    stable flow's drift check would DESCRIBE leftover scratch parquet.
    """
    client = storage.s3_client(config)
    prefix = config.prefix + "/"
    proto_prefix = prefix + "_proto/"
    with_data, samples, proto_with_data = set(), {}, set()
    for key in storage.list_keys(client, config.bucket, prefix):
        if key.startswith(proto_prefix):
            parsed = storage.parse_key(proto_prefix, key)
            if parsed and parsed[1] is None:
                proto_with_data.add(parsed[0])
            continue
        parsed = storage.parse_key(prefix, key)
        if parsed is None:
            continue
        flow, step = parsed
        if step is None:
            with_data.add(flow)
        else:
            samples.setdefault(flow, {}).setdefault(step, key)
    return with_data, samples, proto_with_data


def flows_with_data(config):
    """Flow names that have a ledger on S3 (i.e. at least one recorded run)."""
    return scan_runs(config)[0]


def schema_drift(flow_cls, con, config, samples):
    """Messages where a step's declared schema differs from what's on S3.

    Reads one sample part's parquet footer per step (via httpfs), instead of
    binding a whole-glob view.
    """
    issues = []
    for name, step in flow_cls._dag.steps.items():
        if name not in samples:
            continue
        declared = {n: c.persisted_type() for n, c in step.output_table.columns().items()}
        persisted = {
            r[0]: r[1]
            for r in con.sql(
                f"DESCRIBE SELECT * FROM read_parquet('{storage.uri(config, samples[name])}', "
                "hive_partitioning=false)"
            ).fetchall()
        }
        for col, dtype in declared.items():
            if col not in persisted:
                issues.append(f"{flow_cls.__name__}.{name}: missing column '{col}'")
            elif persisted[col] != dtype:
                issues.append(f"{flow_cls.__name__}.{name}: '{col}' {persisted[col]}→{dtype}")
        for col in persisted:
            if col not in declared:
                issues.append(f"{flow_cls.__name__}.{name}: unexpected column '{col}'")
    return issues


def startup_warnings(flows, config, with_data, samples):
    """Health check: orphaned S3 data + schema drift, as human-readable lines."""
    warnings = []
    for name in sorted(with_data - set(flows)):
        warnings.append(f"{name}: has runs on S3 but no matching Flow in code")
    con = storage.connect(config)  # one connection serves every flow's drift check
    for name, cls in flows.items():
        if name not in with_data or cls.prototype:  # scratch churns by design
            continue
        try:
            warnings += schema_drift(cls, con, config, samples.get(name, {}))
        except Exception as e:  # noqa: BLE001
            warnings.append(f"{name}: schema check failed: {e}")
    return warnings
