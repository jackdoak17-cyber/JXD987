import datetime as dt

import pytest

from scripts.leaderboard_dedicated_lane_check import P95, validate_before


NOW = dt.datetime.now(dt.timezone.utc)
RELEASE = "a" * 40


def snapshot():
    return {
        "status": {
            "release_sha": RELEASE,
            "worker_enabled": True,
            "capture_enabled": True,
            "backfill_enabled": False,
            "resource_paused": True,
            "resource_pause_reason": P95,
            "resource_guard": {"ok": False, "breaches": [P95]},
        },
        "database": {
            "database_name": "postgres",
            "database_user": "postgres",
            "network_connection": True,
            "max_connections": 90,
            "total_connections": 40,
            "active_connections": 3,
            "idle_in_transaction": 0,
            "lock_waiters": 0,
            "open_runs": 0,
            "open_windows": 0,
            "open_trial_reservations": 0,
            "open_rolling_reservations": 0,
        },
        "source": {
            "latest_status": "success",
            "latest_release_id": "source-a",
            "release_id": "source-a",
            "latest_work_units": 6,
            "work_units": 20,
            "sample_count": 40,
            "latest_observed_at": NOW - dt.timedelta(minutes=2),
            "processing_p95_ms": 141_000,
        },
    }


def test_preflight_accepts_only_the_reviewed_p95_lane_envelope():
    result = validate_before(snapshot(), RELEASE)
    assert result["ok"] is True
    assert result["mode"] == "before"


def test_preflight_accepts_only_the_exact_reviewed_previous_release_for_first_run():
    value = snapshot()
    value["status"]["release_sha"] = "b" * 40
    assert validate_before(value, RELEASE, "b" * 40)["ok"] is True
    with pytest.raises(RuntimeError, match="release differs"):
        validate_before(value, RELEASE, "c" * 40)


@pytest.mark.parametrize(("section", "field", "value", "message"), [
    ("status", "resource_pause_reason", "rolling_wal_bytes_limit", "p95-only"),
    ("status", "worker_enabled", False, "p95-only"),
    ("database", "total_connections", 68, "75 percent"),
    ("database", "active_connections", 11, "active database"),
    ("database", "lock_waiters", 1, "lock_waiters"),
    ("source", "latest_status", "failure", "latest source cycle"),
])
def test_preflight_fails_closed_on_each_changed_condition(section, field, value, message):
    value_snapshot = snapshot()
    value_snapshot[section][field] = value
    with pytest.raises(RuntimeError, match=message):
        validate_before(value_snapshot, RELEASE)


def test_preflight_rejects_stale_source_evidence():
    value = snapshot()
    value["source"]["latest_observed_at"] = NOW - dt.timedelta(minutes=16)
    with pytest.raises(RuntimeError, match="stale"):
        validate_before(value, RELEASE)
