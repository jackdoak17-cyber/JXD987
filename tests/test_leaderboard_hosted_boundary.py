"""Hosted mode must require trusted, pre-existing lock and credential pins."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile

import pytest

from scripts import leaderboard_finite_trial_supervisor as supervisor
from scripts import leaderboard_hosted_boundary as hosted


@pytest.fixture
def lock_fixture(monkeypatch):
    with tempfile.TemporaryDirectory(prefix=".leaderboard-lock-test-", dir=Path.home()) as root:
        directory = Path(root).resolve()
        records = {}
        for kind in ("worker", "heavy"):
            file = directory / f"{kind}.lock"
            file.touch(mode=0o600)
            info = file.stat()
            records[kind] = {"path": str(file), "directory": str(directory),
                             "device": info.st_dev, "inode": info.st_ino}
        pin_file = directory / "pin.json"
        pin_file.write_text(json.dumps(records))
        pin_file.chmod(0o600)
        monkeypatch.setenv("LEADERBOARD_RESTRICTED_HOST_PIN_FILE", str(pin_file))
        yield pin_file, records


def test_reviewed_lock_is_nonblocking_and_refuses_replacement(lock_fixture):
    _, pins = lock_fixture
    path = Path(pins["worker"]["path"])
    with hosted.acquire_hosted_lock(str(path), "worker"):
        assert hosted.acquire_hosted_lock(str(path), "worker") is None
    retired = path.with_suffix(".retired")
    path.rename(retired)
    path.touch(mode=0o600)
    with pytest.raises(RuntimeError, match="lock identity"):
        hosted.acquire_hosted_lock(str(path), "worker")
    path.unlink()
    path.symlink_to(retired)
    with pytest.raises(OSError):
        hosted.acquire_hosted_lock(str(path), "worker")


def test_hosted_pins_refuse_aliases_and_unprotected_parent(lock_fixture):
    pin_file, pins = lock_fixture
    alias = dict(pins, heavy=pins["worker"])
    pin_file.write_text(json.dumps(alias))
    with pytest.raises(RuntimeError, match="identities alias"):
        hosted.read_lock_pins()
    pin_file.parent.chmod(0o770)
    try:
        with pytest.raises(RuntimeError, match="directory is not protected"):
            hosted.read_lock_pins()
    finally:
        pin_file.parent.chmod(0o700)


def test_hosted_child_receives_only_worker_dsn_and_public_pins(monkeypatch):
    for key in hosted.TARGET_ENV:
        monkeypatch.setenv(key, key)
    monkeypatch.setenv("LEADERBOARD_WORKER_DB_URL", "worker")
    monkeypatch.setenv("LEADERBOARD_DRAIN_DB_URL", "drain-secret")
    monkeypatch.setenv("LEADERBOARD_OPERATOR_DB_URL", "operator-secret")
    monkeypatch.setenv("SUPABASE_DB_URL", "owner-secret")
    env = supervisor.child_environment("a"*16, (3, 4), "/worker", "/heavy", hosted_worker_url="worker")
    assert env["SUPABASE_DB_URL_SESSION"] == "worker"
    assert all(env[key] == key for key in hosted.TARGET_ENV)
    assert all(secret not in env.values() for secret in ("drain-secret", "operator-secret", "owner-secret"))
    assert all(key not in env for key in ("LEADERBOARD_WORKER_DB_URL", "LEADERBOARD_DRAIN_DB_URL",
                                         "LEADERBOARD_OPERATOR_DB_URL", "SUPABASE_DB_URL"))
