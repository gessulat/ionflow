import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

CONFIG_DIR = ".ionflow"
CONFIG_FILE = "config"

# The setup wizard's fields — one spec drives both the CLI prompts and the TUI
# form: (config key, prompt label, default). A None default means required.
SETUP_FIELDS = [
    ("bucket", "S3 bucket", None),
    ("prefix", "Key prefix", "ionflow"),
    ("endpoint", "S3 endpoint URL (blank for AWS)", ""),
    ("region", "Region", "us-east-1"),
    ("flows", "Flows package for the TUI (e.g. myproject.flows, blank to skip)", ""),
]


def config_from_setup(vals):
    """Config from setup answers keyed like SETUP_FIELDS (blank optionals -> None)."""
    return Config(
        bucket=vals["bucket"],
        prefix=vals["prefix"],
        endpoint_url=vals["endpoint"] or None,
        region=vals["region"],
        flows=vals["flows"] or None,
    )


class ConfigMissing(RuntimeError):
    """Raised when no `.ionflow/config` (with a bucket or local_dir) can be found."""


def find_config(start="."):
    """Nearest `.ionflow/config`, walking up from `start` like git finds `.git`."""
    here = Path(start).resolve()
    for d in [here, *here.parents]:
        path = d / CONFIG_DIR / CONFIG_FILE
        if path.is_file():
            return path
    return None


def read_config(start="."):
    """Raw settings dict from the nearest `.ionflow/config` (empty if none)."""
    path = find_config(start)
    return tomllib.loads(path.read_text()) if path else {}


def resolve_local_dir(base, value):
    """Absolute store root from a config value (relative paths anchor at `base`)."""
    return str((Path(base) / Path(value).expanduser()).resolve())


def write_config(root, cfg):
    """Serialize `cfg` to `<root>/.ionflow/config` (TOML); return the path written."""
    d = Path(root) / CONFIG_DIR
    d.mkdir(exist_ok=True)
    # json.dumps writes each value: the stdlib has a TOML reader but no writer,
    # and JSON's string escapes are a subset of TOML basic-string escapes, so
    # quotes/backslashes/control chars in a value can't corrupt the file
    lines = []
    if cfg.local_dir:
        lines.append(f"local_dir = {json.dumps(cfg.local_dir)}")
    else:
        lines.append(f"bucket = {json.dumps(cfg.bucket)}")
    lines.append(f"prefix = {json.dumps(cfg.prefix)}")
    if cfg.endpoint_url:
        lines.append(f"endpoint = {json.dumps(cfg.endpoint_url)}")
    if not cfg.local_dir:
        lines.append(f"region = {json.dumps(cfg.region)}")
    if cfg.flows:
        lines.append(f"flows = {json.dumps(cfg.flows)}")
    path = d / CONFIG_FILE
    path.write_text("\n".join(lines) + "\n")
    return path


@dataclass
class Config:
    """Store connection + layout config. The only thing ionflow needs to set up.

    Exactly one of `bucket` (S3 mode) and `local_dir` (local mode — the whole
    store is a plain directory, no bucket, no credentials) is set.

    >>> Config(bucket="b").prefix
    'ionflow'
    >>> Config(local_dir="/tmp/store").bucket is None
    True
    """

    bucket: str | None = None
    prefix: str = "ionflow"
    endpoint_url: str | None = None
    region: str = "us-east-1"
    flows: str | None = None  # package the TUI imports to discover Flow classes
    access_key: str | None = None
    secret_key: str | None = None
    local_dir: str | None = None  # local mode: absolute store root once loaded

    @classmethod
    def load(cls, start="."):
        """Read `.ionflow/config` (TOML), with IONFLOW_* env vars overriding.

        A relative `local_dir` resolves against the directory holding
        `.ionflow/` (the repo root), so the store travels with the checkout.
        Credentials are never stored here; they come from the standard AWS chain.
        """
        path = find_config(start)
        data = read_config(start)
        # the store mode is picked per layer: any IONFLOW_BUCKET/LOCAL_DIR env
        # var replaces the file's choice wholesale, never mixes with it
        if os.environ.get("IONFLOW_BUCKET") or os.environ.get("IONFLOW_LOCAL_DIR"):
            bucket, local_dir = os.environ.get("IONFLOW_BUCKET"), os.environ.get("IONFLOW_LOCAL_DIR")
        else:
            bucket, local_dir = data.get("bucket"), data.get("local_dir")
        if not bucket and not local_dir:
            raise ConfigMissing("no ionflow config found; run `ionflow setup`")
        if bucket and local_dir:
            raise ValueError("config sets both `bucket` and `local_dir`; keep exactly one (see `ionflow sync`)")
        if local_dir:
            local_dir = resolve_local_dir(path.parent.parent if path else Path.cwd(), local_dir)
        return cls(
            bucket=bucket,
            prefix=os.environ.get("IONFLOW_PREFIX") or data.get("prefix", "ionflow"),
            endpoint_url=os.environ.get("IONFLOW_ENDPOINT") or data.get("endpoint"),
            region=os.environ.get("AWS_DEFAULT_REGION") or data.get("region", "us-east-1"),
            flows=data.get("flows"),
            local_dir=local_dir,
        )
