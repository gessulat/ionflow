import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

CONFIG_DIR = ".miniflow"
CONFIG_FILE = "config"


class ConfigMissing(RuntimeError):
    """Raised when no `.miniflow/config` (with a bucket) can be found."""


def find_config(start="."):
    """Nearest `.miniflow/config`, walking up from `start` like git finds `.git`."""
    here = Path(start).resolve()
    for d in [here, *here.parents]:
        path = d / CONFIG_DIR / CONFIG_FILE
        if path.is_file():
            return path
    return None


def read_config(start="."):
    """Raw settings dict from the nearest `.miniflow/config` (empty if none)."""
    path = find_config(start)
    return tomllib.loads(path.read_text()) if path else {}


def write_config(root, cfg):
    """Serialize `cfg` to `<root>/.miniflow/config` (TOML); return the path written."""
    d = Path(root) / CONFIG_DIR
    d.mkdir(exist_ok=True)
    lines = [f'bucket = "{cfg.bucket}"', f'prefix = "{cfg.prefix}"']
    if cfg.endpoint_url:
        lines.append(f'endpoint = "{cfg.endpoint_url}"')
    lines.append(f'region = "{cfg.region}"')
    path = d / CONFIG_FILE
    path.write_text("\n".join(lines) + "\n")
    return path


@dataclass
class Config:
    """S3 connection + layout config. The only thing miniflow needs to set up.

    >>> Config(bucket="b").prefix
    'miniflow'
    """

    bucket: str
    prefix: str = "miniflow"
    endpoint_url: str | None = None
    region: str = "us-east-1"
    access_key: str | None = None
    secret_key: str | None = None

    @classmethod
    def load(cls, start="."):
        """Read `.miniflow/config` (TOML), with MINIFLOW_* env vars overriding.

        Credentials are never stored here; they come from the standard AWS chain.
        """
        data = read_config(start)
        bucket = os.environ.get("MINIFLOW_BUCKET") or data.get("bucket")
        if not bucket:
            raise ConfigMissing("no miniflow config found; run `miniflow setup`")
        return cls(
            bucket=bucket,
            prefix=os.environ.get("MINIFLOW_PREFIX") or data.get("prefix", "miniflow"),
            endpoint_url=os.environ.get("MINIFLOW_ENDPOINT") or data.get("endpoint"),
            region=os.environ.get("AWS_DEFAULT_REGION") or data.get("region", "us-east-1"),
        )
