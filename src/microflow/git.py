import subprocess


class DirtyRepoError(RuntimeError):
    """Raised when a flow is started with uncommitted changes."""


def _git(repo, *args):
    r = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True
    )
    if r.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout.strip()


def commit_hash(repo="."):
    return _git(repo, "rev-parse", "HEAD")


def repo_root(start="."):
    """Work-tree root, or None if `start` isn't in a git repo (or git is absent)."""
    try:
        return _git(start, "rev-parse", "--show-toplevel")
    except (RuntimeError, FileNotFoundError):
        return None


def require_clean(repo="."):
    """Return HEAD commit hash, or raise if the working tree is dirty."""
    status = _git(repo, "status", "--porcelain")
    if status:
        raise DirtyRepoError(
            "refusing to run with a dirty working tree:\n" + status
        )
    return commit_hash(repo)


def commit_or_dirty(repo="."):
    """HEAD commit hash, prefixed 'dirty:' when the tree has changes (prototype runs)."""
    try:
        return require_clean(repo)  # one definition of "dirty" for both modes
    except DirtyRepoError:
        return f"dirty:{commit_hash(repo)}"
