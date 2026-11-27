import datetime as dt
import json
import os
from pathlib import Path

import pytest

from scripts.leaderboard_dedicated_lane_check import (
    MAX_RUN_DURATION_SECONDS,
    MAX_SOURCE_EVIDENCE_AGE_SECONDS,
    P95,
    validate_before,
    validate_resource_evidence,
)


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
    assert result["resource_envelope"] == "source_p95_yellow"


def test_preflight_accepts_the_fully_green_resource_envelope():
    value = snapshot()
    value["status"].update({"resource_paused": False, "resource_pause_reason": None})
    value["status"]["resource_guard"] = {"ok": True, "breaches": []}
    result = validate_before(value, RELEASE)
    assert result["resource_envelope"] == "green"


def test_preflight_accepts_only_the_exact_reviewed_previous_release_for_first_run():
    value = snapshot()
    value["status"]["release_sha"] = "b" * 40
    assert validate_before(value, RELEASE, "b" * 40)["ok"] is True
    with pytest.raises(RuntimeError, match="release differs"):
        validate_before(value, RELEASE, "c" * 40)


@pytest.mark.parametrize(("section", "field", "value", "message"), [
    ("status", "resource_pause_reason", "rolling_wal_bytes_limit", "green-or-p95"),
    ("status", "worker_enabled", False, "green-or-p95"),
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
    value["source"]["latest_observed_at"] = NOW - dt.timedelta(
        seconds=MAX_SOURCE_EVIDENCE_AGE_SECONDS + 1
    )
    with pytest.raises(RuntimeError, match="stale"):
        validate_before(value, RELEASE)


def test_preflight_reserves_the_lane_for_a_just_completed_source_cycle():
    assert MAX_SOURCE_EVIDENCE_AGE_SECONDS == 5 * 60
    value = snapshot()
    value["source"]["latest_observed_at"] = NOW - dt.timedelta(
        seconds=MAX_SOURCE_EVIDENCE_AGE_SECONDS - 1
    )
    assert validate_before(value, RELEASE)["ok"] is True


def write_resource_evidence(tmp_path, observed_at):
    path = tmp_path / "resource.json"
    path.write_text(json.dumps({
        "disk_available_bytes": 80 * 1024**3,
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
    }))
    os.chmod(path, 0o600)
    return path


def test_resource_evidence_preflight_accepts_a_fresh_private_observation(tmp_path):
    path = write_resource_evidence(tmp_path, NOW - dt.timedelta(seconds=239))
    result = validate_resource_evidence(path, NOW)
    assert result["disk_available_bytes"] == 80 * 1024**3


def test_resource_evidence_preflight_defers_before_the_database_ttl_gap(tmp_path):
    path = write_resource_evidence(tmp_path, NOW - dt.timedelta(seconds=241))
    with pytest.raises(RuntimeError, match="not fresh enough"):
        validate_resource_evidence(path, NOW)


def test_resource_evidence_preflight_rejects_an_untrusted_file(tmp_path):
    path = write_resource_evidence(tmp_path, NOW)
    os.chmod(path, 0o644)
    with pytest.raises(RuntimeError, match="untrusted"):
        validate_resource_evidence(path, NOW)


def test_dedicated_wrapper_uses_the_reviewed_three_minute_budget():
    wrapper = (Path(__file__).parents[1] / "scripts/vps/run_leaderboard_dedicated_lane.sh").read_text(
        encoding="utf-8"
    )
    assert "PUBLISHER_WORK_RESERVE_SECONDS=195" in wrapper
    assert "PUBLISHER_WINDOW_END_SECONDS=900" in wrapper
    assert "--fixture-limit 250 --max-seconds 180" in wrapper
    assert "LEADERBOARD_DEDICATED_LANE_COHORT" in wrapper
    assert '--cohort-limit 1' in wrapper
    assert '"${scope_args[@]}"' in wrapper
    assert MAX_RUN_DURATION_SECONDS == 185
