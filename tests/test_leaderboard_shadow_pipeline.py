import os
import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest

import scripts.leaderboard_shadow_worker as worker_module
from scripts.leaderboard_shadow_capture import capture_fixture_ids
from scripts.leaderboard_shadow_worker import (
    MAX_FIXTURES_PER_RUN,
    MAX_SECONDS_PER_RUN,
    RESOURCE_GUARD_FIXTURE_INTERVAL,
    SAFETY_CHECK_RESERVE_SECONDS,
    DirtyClaim,
    RunBudget,
    configure_temp_file_limit_if_permitted,
    claim_dirty,
    finish_generation,
    finish_worker_run_with_resource_check,
    load_external_resource_evidence,
    pending_cohorts,
    process_generation_job,
    process_dirty_claim,
    process_capture_queue,
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


class ClaimCursor(FakeCursor):
    def fetchone(self):
        return {
            "fixture_id": 1001,
            "dirty_version": 4,
            "claim_token": "worker:token",
            "claim_fence": 7,
        }


class GenerationFinishCursor(FakeCursor):
    def __init__(self, row):
        super().__init__()
        self.row = row

    def fetchone(self):
        return self.row


class GenerationJobCursor(FakeCursor):
    def __init__(self, staged, snapshot_hash="fresh-hash"):
        super().__init__()
        rows = [
            {
                "fixture_id": 19729028,
                "expected_source_hash": "stale-hash",
                "change_kind": "upsert",
                "fencing_number": 4,
            },
            {"snapshot": {
                "source_hash": snapshot_hash,
                "facts": [{"player_id": 1}],
            }},
        ]
        if snapshot_hash != "stale-hash":
            rows.append({"capture_fixture_source_changes": [{
                "fixture_id": 19729028,
                "source_version": 2,
            }]})
        rows.append({"stage_fixture_result": staged})
        self.rows = iter(rows)

    def fetchone(self):
        return next(self.rows)


class GenerationJobConnection(FakeConnection):
    def __init__(self, cursor):
        super().__init__(cursor)
        self.isolation_levels = []

    def set_session(self, isolation_level):
        self.isolation_levels.append(isolation_level)


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


def test_dirty_claim_can_be_bounded_to_recorded_or_current_cohort():
    cursor = ClaimCursor()
    claim = claim_dirty(
        FakeConnection(cursor),
        "worker",
        league_id=27,
        season_id=25654,
    )
    assert claim == DirtyClaim(1001, 4, "worker:token", 7)
    query, params = cursor.executed[0]
    assert "fixture_source_versions recorded" in query
    assert "public.fixtures current_fixture" in query
    assert "for update of queue skip locked" in query
    assert params[:5] == (27, 27, 25654, 27, 25654)


def test_unscoped_dirty_claim_preserves_global_queue_ordering():
    cursor = ClaimCursor()
    claim_dirty(FakeConnection(cursor), "worker")
    query, params = cursor.executed[0]
    assert "order by queue.last_dirty_at, queue.fixture_id" in query
    assert params[:5] == (None, None, None, None, None)


def test_capture_resource_guard_is_checked_in_bounded_groups(monkeypatch):
    claims = iter([
        DirtyClaim(fixture_id, 1, f"worker:{fixture_id}", 1)
        for fixture_id in range(1, RESOURCE_GUARD_FIXTURE_INTERVAL * 2 + 2)
    ] + [None])
    guard_calls = []
    monkeypatch.setattr(worker_module, "claim_dirty", lambda *_args, **_kwargs: next(claims))
    monkeypatch.setattr(worker_module, "process_dirty_claim", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        worker_module,
        "enforce_resource_guard",
        lambda *_args, **_kwargs: guard_calls.append(True) or {"ok": True},
    )
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    result = process_capture_queue(
        FakeConnection(FakeCursor()),
        "worker",
        MAX_FIXTURES_PER_RUN,
        budget,
        True,
    )
    assert result == {
        "claimed": RESOURCE_GUARD_FIXTURE_INTERVAL * 2 + 1,
        "acknowledged": RESOURCE_GUARD_FIXTURE_INTERVAL * 2 + 1,
        "redirtied": 0,
        "failed": 0,
    }
    assert len(guard_calls) == 3


def test_worker_finish_is_included_in_final_resource_evidence(monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_module,
        "finish_worker_run",
        lambda *_args, **_kwargs: calls.append("finish"),
    )
    monkeypatch.setattr(
        worker_module,
        "apply_recovery_timeouts",
        lambda *_args, **_kwargs: calls.append("timeouts"),
    )
    monkeypatch.setattr(
        worker_module,
        "enforce_resource_guard",
        lambda *_args, **_kwargs: calls.append("guard") or {"ok": True},
    )

    evidence = finish_worker_run_with_resource_check(
        object(), 7, 11, 11, 0, True,
    )

    assert evidence == {"ok": True}
    assert calls == ["finish", "timeouts", "guard"]


def test_superseded_generation_defers_to_a_replacement_without_worker_failure():
    cursor = GenerationFinishCursor(("superseded", 138, 1))

    assert finish_generation(FakeConnection(cursor), 1000016) is False
    assert len(cursor.executed) == 1
    assert "cohort_pending_publications generation" in cursor.executed[0][0]


def test_source_drift_is_recaptured_before_replacement_generation():
    cursor = GenerationJobCursor(staged=False)
    connection = GenerationJobConnection(cursor)

    assert process_generation_job(connection, 1000023, "worker") is False

    queries = [query for query, _params in cursor.executed]
    assert "claim_generation_job" in queries[0]
    assert "fixture_source_snapshot" in queries[1]
    assert "capture_fixture_source_changes" in queries[2]
    assert "stage_fixture_result" in queries[3]
    assert cursor.executed[2][1] == ([19729028],)
    assert connection.isolation_levels == ["REPEATABLE READ", "READ COMMITTED"]


def test_successful_staging_does_not_create_a_duplicate_source_event():
    cursor = GenerationJobCursor(staged=True, snapshot_hash="stale-hash")
    connection = GenerationJobConnection(cursor)

    assert process_generation_job(connection, 1000023, "worker") is True

    queries = [query for query, _params in cursor.executed]
    assert not any("capture_fixture_source_changes" in query for query in queries)


def test_failed_active_generation_remains_fatal():
    cursor = GenerationFinishCursor(("processing", 0, 1))

    with pytest.raises(RuntimeError, match="generation 1000016 has 1 failed jobs"):
        finish_generation(FakeConnection(cursor), 1000016)


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
    assert "run_with_database_heavy_lock" in wrapper
    assert "LEADERBOARD_SHADOW_DB_HEAVY_LOCK_WAIT_SECONDS:-0" in wrapper


def test_database_heavy_lock_defers_overlap_then_releases(tmp_path):
    if shutil.which("flock") is None:
        pytest.skip("Linux flock is unavailable on this host")

    lock_file = tmp_path / "database-heavy.lock"
    ready_file = tmp_path / "holder-ready"
    env = {
        **os.environ,
        "ODDSSEARCH_DB_HEAVY_LOCK_FILE": str(lock_file),
        "COMMON_SH": str(ROOT / "scripts/vps/common.sh"),
        "READY_FILE": str(ready_file),
    }
    holder = subprocess.Popen(
        [
            "bash",
            "-lc",
            'source "$COMMON_SH"; run_with_database_heavy_lock 0 '
            'bash -c \'printf ready > "$READY_FILE"; sleep 1\'',
        ],
        env=env,
    )
    try:
        deadline = time.monotonic() + 3
        while not ready_file.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready_file.exists(), "lock holder did not start"

        deferred = subprocess.run(
            [
                "bash",
                "-lc",
                'source "$COMMON_SH"; run_with_database_heavy_lock 0 true',
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        assert deferred.returncode == 75
        assert "database-heavy maintenance slot unavailable" in deferred.stdout

        waited = subprocess.run(
            [
                "bash",
                "-lc",
                'source "$COMMON_SH"; run_with_database_heavy_lock 2 true',
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=4,
        )
        assert waited.returncode == 0, waited.stderr
        assert holder.wait(timeout=2) == 0

        released = subprocess.run(
            [
                "bash",
                "-lc",
                'source "$COMMON_SH"; run_with_database_heavy_lock 0 true',
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        assert released.returncode == 0
    finally:
        if holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=2)


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
    clock = iter([
        100.0,
        100.0 + MAX_SECONDS_PER_RUN - SAFETY_CHECK_RESERVE_SECONDS - 0.01,
        100.0 + MAX_SECONDS_PER_RUN - SAFETY_CHECK_RESERVE_SECONDS,
    ])
    monkeypatch.setattr(worker_module.time, "monotonic", lambda: next(clock))
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    assert budget.available()
    assert not budget.available()


def test_run_budget_reserves_time_for_mandatory_safety_checks(monkeypatch):
    near_deadline = 200.0 + MAX_SECONDS_PER_RUN - 0.1
    clock = iter([200.0, near_deadline, near_deadline])
    monkeypatch.setattr(worker_module.time, "monotonic", lambda: next(clock))
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    assert budget.remaining_seconds() == pytest.approx(0.1)
    assert not budget.available()


def test_external_resource_evidence_is_exact_and_protected(tmp_path):
    path = tmp_path / "resource.json"
    path.write_text(json.dumps({
        "disk_available_bytes": 30 * 1024**3,
        "observed_at": "2026-10-28T18:00:00Z",
    }))
    path.chmod(0o600)
    cursor = FakeCursor()
    evidence = load_external_resource_evidence(FakeConnection(cursor), str(path))
    assert evidence["disk_available_bytes"] == 30 * 1024**3
    assert "refresh_external_resource_evidence_v2" in cursor.executed[0][0]
    assert cursor.executed[0][1] == (
        30 * 1024**3, "2026-10-28T18:00:00Z",
    )

    path.chmod(0o622)
    with pytest.raises(RuntimeError, match="non-group/world-writable"):
        load_external_resource_evidence(FakeConnection(FakeCursor()), str(path))


def test_external_resource_evidence_rejects_unknown_fields(tmp_path):
    path = tmp_path / "resource.json"
    path.write_text(json.dumps({
        "disk_available_bytes": 30 * 1024**3,
        "observed_at": "2026-10-28T18:00:00Z",
        "unreviewed_override": True,
    }))
    path.chmod(0o600)
    with pytest.raises(RuntimeError, match="unexpected or missing"):
        load_external_resource_evidence(FakeConnection(FakeCursor()), str(path))
