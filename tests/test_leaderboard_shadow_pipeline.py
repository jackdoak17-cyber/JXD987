import os
import json
from pathlib import Path

import pytest

import scripts.leaderboard_shadow_worker as worker_module
from scripts.leaderboard_shadow_capture import capture_fixture_ids
from scripts.leaderboard_shadow_worker import (
    MAX_FIXTURES_PER_RUN,
    MAX_SECONDS_PER_RUN,
    DirtyClaim,
    RunBudget,
    configure_temp_file_limit_if_permitted,
    load_external_resource_evidence,
    pending_cohorts,
    process_dirty_claim,
)


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


class CohortCursor(FakeCursor):
    def fetchall(self):
        return [(271, 27897)]


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


def test_pending_cohorts_defer_both_recorded_and_current_dirty_cohorts():
    cursor = CohortCursor()
    assert pending_cohorts(FakeConnection(cursor), 3) == [(271, 27897)]
    query, params = cursor.executed[0]
    assert params == (3,)
    assert "fixture_capture_queue queue" in query
    assert "fixture_source_versions recorded" in query
    assert "public.fixtures current_fixture" in query
    assert (
        "(recorded.league_id, recorded.season_id) = "
        "(cursor.league_id, cursor.season_id)"
    ) in query
    assert (
        "(current_fixture.league_id, current_fixture.season_id) = "
        "(cursor.league_id, cursor.season_id)"
    ) in query


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
    assert "--fixture-limit 250" in wrapper
    assert "--max-seconds 30" in wrapper
    assert "--isolated-test-override" not in wrapper


def test_run_budget_rejects_unbounded_fixture_and_time_limits():
    assert RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN).available()
    with pytest.raises(ValueError, match="fixture limit"):
        RunBudget.create(MAX_FIXTURES_PER_RUN + 1, MAX_SECONDS_PER_RUN)
    with pytest.raises(ValueError, match="max seconds"):
        RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN + 0.01)


def test_run_budget_stops_at_250_fixture_operations():
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    for _ in range(MAX_FIXTURES_PER_RUN):
        assert budget.available()
        budget.consume()
    assert budget.processed == 250
    assert not budget.available()


def test_temp_file_limit_is_applied_only_with_parameter_privilege():
    class CapabilityCursor(FakeCursor):
        def __init__(self, permitted):
            super().__init__()
            self.permitted = permitted

        def fetchone(self):
            return (self.permitted,)

    denied = CapabilityCursor(False)
    assert configure_temp_file_limit_if_permitted(FakeConnection(denied)) is False
    assert sum("temp_file_limit" in query for query, _ in denied.executed) == 1

    permitted = CapabilityCursor(True)
    assert configure_temp_file_limit_if_permitted(FakeConnection(permitted)) is True
    statements = "\n".join(statement for statement, _ in permitted.executed)
    assert "leaderboard_shadow_v1.pipeline_state" in statements
    assert sum("temp_file_limit" in query for query, _ in permitted.executed) == 2


def test_run_budget_stops_when_30_second_deadline_is_reached(monkeypatch):
    clock = iter([100.0, 129.9, 130.0])
    monkeypatch.setattr(worker_module.time, "monotonic", lambda: next(clock))
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    assert budget.available()
    assert not budget.available()


def test_external_resource_evidence_is_exact_and_protected(tmp_path):
    path = tmp_path / "resource.json"
    path.write_text(json.dumps({
        "disk_available_bytes": 30 * 1024**3,
        "source_ingestion_baseline_p95_ms": 10,
        "source_ingestion_current_p95_ms": 10.5,
        "observed_at": "2026-10-28T18:00:00Z",
    }))
    path.chmod(0o600)
    cursor = FakeCursor()
    evidence = load_external_resource_evidence(FakeConnection(cursor), str(path))
    assert evidence["disk_available_bytes"] == 30 * 1024**3
    assert "set_external_resource_evidence" in cursor.executed[0][0]

    path.chmod(0o622)
    with pytest.raises(RuntimeError, match="non-group/world-writable"):
        load_external_resource_evidence(FakeConnection(FakeCursor()), str(path))


def test_external_resource_evidence_rejects_unknown_fields(tmp_path):
    path = tmp_path / "resource.json"
    path.write_text(json.dumps({
        "disk_available_bytes": 30 * 1024**3,
        "source_ingestion_baseline_p95_ms": 10,
        "source_ingestion_current_p95_ms": 10.5,
        "observed_at": "2026-10-28T18:00:00Z",
        "unreviewed_override": True,
    }))
    path.chmod(0o600)
    with pytest.raises(RuntimeError, match="unexpected or missing"):
        load_external_resource_evidence(FakeConnection(FakeCursor()), str(path))
