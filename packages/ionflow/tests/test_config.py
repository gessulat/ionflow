import pytest

from ionflow.config import Config, ConfigMissing, find_config, write_config

_ENV = ["IONFLOW_BUCKET", "IONFLOW_PREFIX", "IONFLOW_ENDPOINT", "IONFLOW_LOCAL_DIR", "AWS_DEFAULT_REGION"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for v in _ENV:
        monkeypatch.delenv(v, raising=False)


def _write(root, text):
    (root / ".ionflow").mkdir()
    (root / ".ionflow" / "config").write_text(text)


# (file text or None, env {var: fn(root)}, expected {attr: value or fn(root)})
LOADS = [
    (
        "file_wins_when_no_env",
        'bucket = "b"\nprefix = "p"\nregion = "eu-west-1"\n',
        {},
        {"bucket": "b", "prefix": "p", "region": "eu-west-1", "access_key": None},
    ),  # creds never from the file
    ("defaults", 'bucket = "b"\n', {}, {"endpoint_url": None, "prefix": "ionflow", "flows": None}),
    ("env_beats_file", 'bucket = "filebucket"\n', {"IONFLOW_BUCKET": lambda r: "envbucket"}, {"bucket": "envbucket"}),
    (
        "local_dir_anchors_at_the_config_root",
        'local_dir = ".ionflow/data"\n',
        {},
        {"bucket": None, "local_dir": lambda r: str(r / ".ionflow" / "data")},
    ),
    (
        "local_dir_from_env_alone",
        None,
        {"IONFLOW_LOCAL_DIR": lambda r: str(r / "envstore")},
        {"local_dir": lambda r: str(r / "envstore")},
    ),
    (
        "env_bucket_switches_a_local_file",
        'local_dir = "data"\n',
        {"IONFLOW_BUCKET": lambda r: "envbucket"},
        {"bucket": "envbucket", "local_dir": None},
    ),
    (
        "env_local_dir_switches_a_bucket_file",
        'bucket = "filebucket"\n',
        {"IONFLOW_LOCAL_DIR": lambda r: str(r / "envstore")},
        {"local_dir": lambda r: str(r / "envstore"), "bucket": None},
    ),
]


@pytest.mark.parametrize(("text", "env", "expected"), [c[1:] for c in LOADS], ids=[c[0] for c in LOADS])
def test_load(tmp_path, monkeypatch, text, env, expected):
    if text is not None:
        _write(tmp_path, text)
    for name, val in env.items():
        monkeypatch.setenv(name, val(tmp_path))
    sub = tmp_path / "a" / "b"  # always load from a subdir: pins the walk-up too
    sub.mkdir(parents=True)
    cfg = Config.load(str(sub))
    assert {k: getattr(cfg, k) for k in expected} == {k: v(tmp_path) if callable(v) else v for k, v in expected.items()}


@pytest.mark.parametrize(
    "make",
    [
        lambda r: Config(bucket="b", flows="myproj.flows"),
        lambda r: Config(bucket="b", prefix='pre"fix\\path'),  # JSON-as-TOML escaping
        lambda r: Config(local_dir=str(r / "elsewhere")),  # an absolute local_dir stays absolute
    ],
    ids=["flows", "escapes", "absolute_local_dir"],
)
def test_write_config_round_trips(tmp_path, make):
    cfg = make(tmp_path)
    write_config(tmp_path, cfg)
    loaded = Config.load(str(tmp_path))
    assert (loaded.bucket, loaded.prefix, loaded.flows, loaded.local_dir) == (
        cfg.bucket,
        cfg.prefix,
        cfg.flows,
        cfg.local_dir,
    )


def test_missing_raises(tmp_path):
    assert find_config(str(tmp_path)) is None
    with pytest.raises(ConfigMissing):
        Config.load(str(tmp_path))


def test_bucket_and_local_dir_together_refused(tmp_path):
    _write(tmp_path, 'bucket = "b"\nlocal_dir = "data"\n')
    with pytest.raises(ValueError, match="both"):
        Config.load(str(tmp_path))
