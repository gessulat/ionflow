import os
from dataclasses import dataclass


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
    def from_env(cls):
        try:
            bucket = os.environ["MINIFLOW_BUCKET"]
        except KeyError:
            raise RuntimeError("MINIFLOW_BUCKET is not set")
        return cls(
            bucket=bucket,
            prefix=os.environ.get("MINIFLOW_PREFIX", "miniflow"),
            endpoint_url=os.environ.get("MINIFLOW_ENDPOINT"),
            region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
