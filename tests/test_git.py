import subprocess

import pytest

from miniflow.git import DirtyRepoError, require_clean


def _run(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path):
    _run(tmp_path, "init")
    _run(tmp_path, "config", "user.email", "t@t.com")
    _run(tmp_path, "config", "user.name", "t")
    (tmp_path / "a.txt").write_text("hi")
    _run(tmp_path, "add", "a.txt")
    _run(tmp_path, "commit", "-m", "init")
    return tmp_path


def test_clean_returns_hash(repo):
    h = require_clean(repo)
    assert len(h) == 40


def test_modified_is_dirty(repo):
    (repo / "a.txt").write_text("changed")
    with pytest.raises(DirtyRepoError):
        require_clean(repo)


def test_untracked_is_dirty(repo):
    (repo / "b.txt").write_text("new")
    with pytest.raises(DirtyRepoError):
        require_clean(repo)
