import os
from pathlib import Path

import pytest

from scripts.leaderboard_shadow_capture import capture_fixture_ids
from scripts.leaderboard_shadow_worker import DirtyClaim, process_dirty_claim


ROOT = Path(__file__).resolve().parents[1]


class FakeCursor:
    def __init__(self, delete_count=1):
        self.executed = []
        self.rowcount = 0
        self.delete_count = delete_count

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        self.executed.append((normalized, params))
        if normalized.startswith("delete from leaderboard_shadow_v1.fixture_capture_queue"):
            self.rowcount = self.delete_count
        elif normalized.startswith("update leaderboard_shadow_v1.fixture_capture_queue"):
            self.rowcount = 1

    def fetchone(self):
        return ({"ok": True},)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self._cursor


def test_capture_hook_is_inert_by_default(monkeypatch):
    monkeypatch.delenv("LEADERBOARD_SHADOW_CAPTURE_ENABLED", raising=False)
    cursor = FakeCursor()
    capture_fixture_ids(cursor, [3, 2, 2])
    assert cursor.executed == []


def test_capture_hook_is_sorted_and_fails_with_authoritative_transaction(monkeypatch):
    monkeypatch.setenv("LEADERBOARD_SHADOW_CAPTURE_ENABLED", "true")
    cursor = FakeCursor()
    capture_fixture_ids(cursor, [3, 2, 2])
    assert len(cursor.executed) == 1
    assert "capture_fixture_source_changes" in cursor.executed[0][0]
    assert cursor.executed[0][1] == ([2, 3],)

    class FailingCursor(FakeCursor):
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("capture failed")

    with pytest.raises(RuntimeError, match="capture failed"):
        capture_fixture_ids(FailingCursor(), [2])


def test_dirty_acknowledgement_is_fenced_to_claimed_version():
    claim = DirtyClaim(1001, 4, "worker:token", 7)
    acknowledged = FakeCursor(delete_count=1)
    assert process_dirty_claim(FakeConnection(acknowledged), claim) is True
    assert not any(query.startswith("update leaderboard_shadow_v1.fixture_capture_queue") for query, _ in acknowledged.executed)

    redirtied = FakeCursor(delete_count=0)
    assert process_dirty_claim(FakeConnection(redirtied), claim) is False
    assert any(query.startswith("update leaderboard_shadow_v1.fixture_capture_queue") for query, _ in redirtied.executed)
    delete_params = next(params for query, params in redirtied.executed if query.startswith("delete from"))
    assert delete_params == (1001, 4, "worker:token", 7)


def test_worker_and_capture_hooks_are_pinned_in_the_vps_runtime_manifest():
    runtime_files = {
        line.strip()
        for line in (ROOT / "scripts/vps/runtime_files.txt").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert {
        "scripts/leaderboard_shadow_capture.py",
        "scripts/leaderboard_shadow_worker.py",
        "scripts/vps/run_leaderboard_shadow_worker.sh",
        "scripts/export_to_supabase.py",
        "scripts/postmatch_fixture_detail_delivery.py",
    } <= runtime_files
    wrapper = (ROOT / "scripts/vps/run_leaderboard_shadow_worker.sh").read_text()
    assert "verify_runtime_manifest_or_exit" in wrapper
    assert "require_runtime_manifest_entries_or_exit" in wrapper
