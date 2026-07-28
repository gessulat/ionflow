"""M0: register basics, zombie writer, replay, and the lost-update hammer."""

import pytest
from botocore.exceptions import ClientError
from helpers import error_code, make_client, run_threads

from schrein import CasRegister, CommitConflict
from schrein.register import RETRYABLE

KEY = "reg/state"


def register_for(s3):
    client, bucket, _ = s3
    return CasRegister(client, bucket, KEY)


def test_register_basics(s3):
    """One timeline: absent read, genesis, read-your-write, then a second
    genesis and a stale If-Match both refused as retryable."""
    reg = register_for(s3)
    assert reg.read() == (None, None)
    etag1 = reg.write(b"v1", None)
    assert reg.read() == (b"v1", etag1)
    with pytest.raises(ClientError) as e:  # second genesis
        reg.write(b"v2", None)
    assert error_code(e) in RETRYABLE
    etag2 = reg.write(b"v2", etag1)
    with pytest.raises(ClientError) as e:  # stale If-Match
        reg.write(b"v3", etag1)
    assert error_code(e) in RETRYABLE
    assert reg.read() == (b"v2", etag2)  # the refused writes changed nothing


def test_zombie_writer_rebases(s3):
    """A writer resuming with a stale etag fails harmlessly, then lands via
    transact — and the intervening commit survives."""
    reg = register_for(s3)
    stale = reg.write(b"base", None)
    reg.write(b"intervening", stale)  # another writer commits meanwhile
    with pytest.raises(ClientError):
        reg.write(b"zombie", stale)
    reg.transact(lambda old: old + b"+zombie")
    assert reg.read()[0] == b"intervening+zombie"


def test_transact_replays_on_conflict(s3):
    """A transact forced into one lost race calls fn exactly twice, the second
    time against the winner's state."""
    reg = register_for(s3)
    other = CasRegister(reg.client, reg.bucket, reg.key)
    calls = []

    def fn(old):
        calls.append(old)
        if len(calls) == 1:
            other.write(b"competitor", None)  # lands between our read and write
        return b"mine"

    reg.transact(fn, base_delay=0.01)
    assert calls == [None, b"competitor"]
    assert reg.read()[0] == b"mine"


def test_transact_gives_up_with_commit_conflict(s3, monkeypatch):
    reg = register_for(s3)
    err = ClientError({"Error": {"Code": "PreconditionFailed", "Message": ""}}, "PutObject")

    def always_lose(body, expect_etag):
        raise err

    monkeypatch.setattr(reg, "write", always_lose)
    with pytest.raises(CommitConflict):
        reg.transact(lambda old: b"x", max_attempts=3, base_delay=0.001)


def test_non_retryable_error_propagates(s3):
    client, _, _ = s3
    reg = CasRegister(client, "no-such-bucket-schrein", KEY)
    with pytest.raises(ClientError):
        reg.transact(lambda old: b"x")


def test_lost_update_hammer(s3):
    """The crown jewel: N threads x M commits, each appending one unique row.

    Final state must hold exactly N*M rows — none lost, no duplicates.
    """
    _, bucket, endpoint = s3
    n_threads, n_commits = 8, 25

    def worker(i):
        reg = CasRegister(make_client(endpoint), bucket, KEY)  # own client per thread
        for j in range(n_commits):
            row = f"{i}:{j}".encode()
            reg.transact(
                lambda old, row=row: old + b"\n" + row if old else row,
                max_attempts=64,
                base_delay=0.01,
            )

    run_threads(worker, n_threads)
    final, _ = register_for(s3).read()
    rows = final.split(b"\n")
    assert len(rows) == n_threads * n_commits
    assert len(set(rows)) == n_threads * n_commits
