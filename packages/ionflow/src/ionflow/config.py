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
    """Raised when no `.ionflow/config` (with a bucket) can be found."""


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


def write_config(root, cfg):
    """Serialize `cfg` to `<root>/.ionflow/config` (TOML); return the path written."""
    d = Path(root) / CONFIG_DIR
    d.mkdir(exist_ok=True)
    # json.dumps writes each value: the stdlib has a TOML reader but no writer,
    # and JSON's string escapes are a subset of TOML basic-string escapes, so
    # quotes/backslashes/control chars in a value can't corrupt the file
    lines = [f"bucket = {json.dumps(cfg.bucket)}", f"prefix = {json.dumps(cfg.prefix)}"]
    if cfg.endpoint_url:
        lines.append(f"endpoint = {json.dumps(cfg.endpoint_url)}")
    lines.append(f"region = {json.dumps(cfg.region)}")
    if cfg.flows:
        lines.append(f"flows = {json.dumps(cfg.flows)}")
    path = d / CONFIG_FILE
    path.write_text("\n".join(lines) + "\n")
    return path


@dataclass
class Config:
    """S3 connection + layout config. The only thing ionflow needs to set up.

    >>> Config(bucket="b").prefix
    'ionflow'
    """

    bucket: str
    prefix: str = "ionflow"
    endpoint_url: str | None = None
    region: str = "us-east-1"
    flows: str | None = None  # package the TUI imports to discover Flow classes
    access_key: str | None = None
    secret_key: str | None = None

    @classmethod
    def load(cls, start="."):
        """Read `.ionflow/config` (TOML), with IONFLOW_* env vars overriding.

        Credentials are never stored here; they come from the standard AWS chain.
        """
        data = read_config(start)
        bucket = os.environ.get("IONFLOW_BUCKET") or data.get("bucket")
        if not bucket:
            raise ConfigMissing("no ionflow config found; run `ionflow setup`")
        return cls(
            bucket=bucket,
            prefix=os.environ.get("IONFLOW_PREFIX") or data.get("prefix", "ionflow"),
            endpoint_url=os.environ.get("IONFLOW_ENDPOINT") or data.get("endpoint"),
            region=os.environ.get("AWS_DEFAULT_REGION") or data.get("region", "us-east-1"),
            flows=data.get("flows"),
        )
