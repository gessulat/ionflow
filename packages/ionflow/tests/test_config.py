import pytest

from ionflow.config import Config, ConfigMissing, find_config

_ENV = ["IONFLOW_BUCKET", "IONFLOW_PREFIX", "IONFLOW_ENDPOINT", "IONFLOW_LOCAL_DIR", "AWS_DEFAULT_REGION"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for v in _ENV:
        monkeypatch.delenv(v, raising=False)


def _write(root, text):
    (root / ".ionflow").mkdir()
    (root / ".ionflow" / "config").write_text(text)


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
    assert cfg.endpoint_url is None and cfg.prefix == "ionflow" and cfg.flows is None


def test_flows_roundtrips(tmp_path):
    from ionflow.config import write_config

    write_config(tmp_path, Config(bucket="b", flows="myproj.flows"))
    assert Config.load(str(tmp_path)).flows == "myproj.flows"


def test_write_escapes_special_characters(tmp_path):
    from ionflow.config import write_config

    write_config(tmp_path, Config(bucket="b", prefix='pre"fix\\path'))
    assert Config.load(str(tmp_path)).prefix == 'pre"fix\\path'


def test_missing_raises(tmp_path):
    assert find_config(str(tmp_path)) is None
    with pytest.raises(ConfigMissing):
        Config.load(str(tmp_path))


def test_env_overrides_file(tmp_path, monkeypatch):
    _write(tmp_path, 'bucket = "filebucket"\n')
    monkeypatch.setenv("IONFLOW_BUCKET", "envbucket")
    assert Config.load(str(tmp_path)).bucket == "envbucket"


def test_local_dir_resolves_against_config_root(tmp_path):
    _write(tmp_path, 'local_dir = ".ionflow/data"\n')
    sub = tmp_path / "deep" / "er"
    sub.mkdir(parents=True)
    cfg = Config.load(str(sub))  # found by walking up; path anchors at the root
    assert cfg.bucket is None
    assert cfg.local_dir == str(tmp_path / ".ionflow" / "data")


def test_local_dir_roundtrips_and_absolute_stays(tmp_path):
    from ionflow.config import write_config

    write_config(tmp_path, Config(local_dir=str(tmp_path / "elsewhere")))
    cfg = Config.load(str(tmp_path))
    assert cfg.local_dir == str(tmp_path / "elsewhere") and cfg.bucket is None


def test_bucket_and_local_dir_together_refused(tmp_path):
    _write(tmp_path, 'bucket = "b"\nlocal_dir = "data"\n')
    with pytest.raises(ValueError, match="both"):
        Config.load(str(tmp_path))


def test_local_dir_env_var(tmp_path, monkeypatch):
    monkeypatch.setenv("IONFLOW_LOCAL_DIR", str(tmp_path / "envstore"))
    assert Config.load(str(tmp_path)).local_dir == str(tmp_path / "envstore")


def test_env_var_switches_mode_over_file(tmp_path, monkeypatch):
    _write(tmp_path, 'local_dir = "data"\n')  # file says local ...
    monkeypatch.setenv("IONFLOW_BUCKET", "envbucket")  # ... env says bucket: env wins
    cfg = Config.load(str(tmp_path))
    assert cfg.bucket == "envbucket" and cfg.local_dir is None

    monkeypatch.delenv("IONFLOW_BUCKET")
    monkeypatch.setenv("IONFLOW_LOCAL_DIR", str(tmp_path / "envstore"))
    (tmp_path / ".ionflow" / "config").write_text('bucket = "filebucket"\n')  # and vice versa
    cfg = Config.load(str(tmp_path))
    assert cfg.local_dir == str(tmp_path / "envstore") and cfg.bucket is None
