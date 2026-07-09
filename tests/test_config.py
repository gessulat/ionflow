import pytest

from miniflow.config import Config, ConfigMissing, find_config

_ENV = ["MINIFLOW_BUCKET", "MINIFLOW_PREFIX", "MINIFLOW_ENDPOINT", "AWS_DEFAULT_REGION"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for v in _ENV:
        monkeypatch.delenv(v, raising=False)


def _write(root, text):
    (root / ".miniflow").mkdir()
    (root / ".miniflow" / "config").write_text(text)


def test_load_walks_up(tmp_path):
    _write(tmp_path, 'bucket = "b"\nprefix = "p"\nregion = "eu-west-1"\n')
    sub = tmp_path / "a" / "b"
    sub.mkdir(parents=True)
    cfg = Config.load(str(sub))
    assert (cfg.bucket, cfg.prefix, cfg.region) == ("b", "p", "eu-west-1")
    assert cfg.access_key is None  # creds never come from the file


def test_endpoint_optional(tmp_path):
    _write(tmp_path, 'bucket = "b"\n')
    cfg = Config.load(str(tmp_path))
    assert cfg.endpoint_url is None and cfg.prefix == "miniflow" and cfg.flows is None


def test_flows_roundtrips(tmp_path):
    from miniflow.config import write_config

    write_config(tmp_path, Config(bucket="b", flows="myproj.flows"))
    assert Config.load(str(tmp_path)).flows == "myproj.flows"


def test_missing_raises(tmp_path):
    assert find_config(str(tmp_path)) is None
    with pytest.raises(ConfigMissing):
        Config.load(str(tmp_path))


def test_env_overrides_file(tmp_path, monkeypatch):
    _write(tmp_path, 'bucket = "filebucket"\n')
    monkeypatch.setenv("MINIFLOW_BUCKET", "envbucket")
    assert Config.load(str(tmp_path)).bucket == "envbucket"
