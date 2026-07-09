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


def require_clean(repo="."):
    """Return HEAD commit hash, or raise if the working tree is dirty."""
    status = _git(repo, "status", "--porcelain")
    if status:
        raise DirtyRepoError(
            "refusing to run with a dirty working tree:\n" + status
        )
    return commit_hash(repo)
