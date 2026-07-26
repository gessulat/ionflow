from pathlib import Path

import pytest

from ionflow.git import DirtyRepoError, repo_root, require_clean


def test_clean_returns_hash(clean_repo):
    h = require_clean(clean_repo)
    assert len(h) == 40


def test_modified_is_dirty(clean_repo):
    (Path(clean_repo) / "flow.py").write_text("changed")
    with pytest.raises(DirtyRepoError):
        require_clean(clean_repo)


def test_untracked_is_dirty(clean_repo):
    (Path(clean_repo) / "b.txt").write_text("new")
    with pytest.raises(DirtyRepoError):
        require_clean(clean_repo)


def test_repo_root(clean_repo, tmp_path):
    assert repo_root(clean_repo) is not None
    outside = tmp_path / "not-a-repo"
    outside.mkdir(exist_ok=True)
    assert repo_root(str(outside)) is None
