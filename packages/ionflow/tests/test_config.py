import pytest

from ionflow.config import Config, ConfigMissing, find_config, write_config

_ENV = [
    "IONFLOW_REMOTE",
    "IONFLOW_BUCKET",
    "IONFLOW_PREFIX",
    "IONFLOW_ENDPOINT",
    "IONFLOW_LOCAL_DIR",
    "AWS_DEFAULT_REGION",
]


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
        'remote = "s3://b/p"\nprefix = "p"\nregion = "eu-west-1"\n',
        {},
        {"remote": "s3://b/p", "prefix": "p", "region": "eu-west-1", "access_key": None},
    ),  # creds never from the file
    ("defaults", 'remote = "s3://b"\n', {}, {"endpoint_url": None, "prefix": "ionflow", "flows": None}),
    (
        "env_beats_file",
        'remote = "s3://filebucket"\n',
        {"IONFLOW_REMOTE": lambda r: "s3://envbucket"},
        {"remote": "s3://envbucket"},
    ),
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
        "env_remote_preserves_local_file",
        'local_dir = "data"\n',
        {"IONFLOW_REMOTE": lambda r: "s3://envbucket"},
        {"remote": "s3://envbucket", "local_dir": lambda r: str(r / "data")},
    ),
    (
        "env_local_dir_preserves_remote",
        'remote = "s3://filebucket"\n',
        {"IONFLOW_LOCAL_DIR": lambda r: str(r / "envstore")},
        {"local_dir": lambda r: str(r / "envstore"), "remote": "s3://filebucket"},
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
        lambda r: Config(local_dir=str(r / "store"), remote="s3://b/p", flows="myproj.flows"),
        lambda r: Config(local_dir=str(r / "store"), prefix='pre"fix\\path'),  # JSON-as-TOML escaping
        lambda r: Config(local_dir=str(r / "elsewhere")),  # an absolute local_dir stays absolute
    ],
    ids=["flows", "escapes", "absolute_local_dir"],
)
def test_write_config_round_trips(tmp_path, make):
    cfg = make(tmp_path)
    write_config(tmp_path, cfg)
    loaded = Config.load(str(tmp_path))
    assert (loaded.remote, loaded.prefix, loaded.flows, loaded.local_dir) == (
        cfg.remote,
        cfg.prefix,
        cfg.flows,
        cfg.local_dir,
    )


def test_missing_raises(tmp_path):
    assert find_config(str(tmp_path)) is None
    with pytest.raises(ConfigMissing):
        Config.load(str(tmp_path))


def test_legacy_bucket_config_is_refused(tmp_path):
    _write(tmp_path, 'bucket = "b"\nlocal_dir = "data"\n')
    with pytest.raises(ValueError, match="optional remote"):
        Config.load(str(tmp_path))
