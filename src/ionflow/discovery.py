import importlib
import pkgutil

from . import catalog, storage
from .flow import Flow


def discover_flows(package):
    """Import `package` and all its submodules, returning its {flow_id: Flow subclass}.

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
    """Flow names with a ledger on S3 — one LIST of the bucket prefix."""
    client = storage.s3_client(config)
    prefix = config.prefix + "/"
    suffix = "/ledger.parquet"
    return {
        key[len(prefix) : -len(suffix)]
        for key in storage.list_keys(client, config.bucket, prefix)
        if key.endswith(suffix) and "/" not in key[len(prefix) : -len(suffix)]
    }


def schema_drift(flow_cls, config):
    """Messages where a step's declared schema differs from its catalog table.

    One catalog read covers every step. No catalog (flow never ran) means no
    drift; a step without a table yet was added since genesis and gets one on
    the next run — also not drift.
    """
    issues = []
    tables = catalog.Catalog(config, flow_cls).tables()
    for name, step in flow_cls._dag.steps.items():
        if name not in tables:
            continue
        declared = {n: c.persisted_type() for n, c in step.output_table.columns().items()}
        persisted = {c: t for c, t in tables[name].items() if c != "run_id"}
        for col, dtype in declared.items():
            if col not in persisted:
                issues.append(f"{flow_cls.flow_id()}.{name}: missing column '{col}'")
            elif persisted[col] != dtype:
                issues.append(f"{flow_cls.flow_id()}.{name}: '{col}' {persisted[col]}→{dtype}")
        for col in persisted:
            if col not in declared:
                issues.append(f"{flow_cls.flow_id()}.{name}: unexpected column '{col}'")
    return issues


def startup_warnings(flows, config, with_data):
    """Health check: orphaned S3 data + schema drift, as human-readable lines."""
    warnings = []
    for name in sorted(with_data - set(flows)):
        warnings.append(f"{name}: has runs on S3 but no matching Flow in code")
    for name, cls in flows.items():
        if name not in with_data:
            continue
        try:
            warnings += schema_drift(cls, config)
        except Exception as e:  # noqa: BLE001
            warnings.append(f"{name}: schema check failed: {e}")
    return warnings
