from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.vps.select_stats_reconciliation_leagues import (
    finish_attempt,
    groups_for,
    next_group,
    parse_leagues,
)

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/vps/select_stats_reconciliation_leagues.py"


def report_for(tmp_path: Path, pending: dict, *, batches: int, selected: int,
               accepted: int = 0, status: str = "success") -> Path:
    report = tmp_path / "report.json"
    report.write_text(json.dumps({
        "release_id": pending["release_id"], "scope_attempt_id": pending["attempt_id"],
        "scope_epoch": pending["epoch"], "leagues": pending["leagues"],
        "status": status, "batches": batches,
        "fixtures_selected": selected, "fixtures_accepted": accepted,
    }))
    return report


def finish(tmp_path: Path, pending: dict, *, batches: int = 0, selected: int = 0,
           accepted: int = 0, status: int = 0, report: bool = True,
           leagues: list[int] = [8, 9, 12], group_size: int = 1) -> dict:
    path = report_for(tmp_path, pending, batches=batches, selected=selected, accepted=accepted) if report else tmp_path / "missing.json"
    return finish_attempt(leagues, group_size, tmp_path / "cursor.json", "test-release",
                          pending["attempt_id"], status, path)


def test_rotation_covers_each_league_and_survives_restart(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    leagues = [8, 9, 12]
    assert groups_for(leagues, 1) == [[8], [9], [12]]
    for expected in ([8], [9], [12], [8]):
        pending = next_group(leagues, 1, cursor, "test-release")
        assert pending["leagues"] == expected
        finish(tmp_path, pending)
    assert json.loads(cursor.read_text())["next_group"] == 1
    assert cursor.stat().st_mode & 0o777 == 0o600


def test_full_empty_sweep_stays_valid_after_restart_and_another_empty_scope(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    for _ in range(3):
        pending = next_group([8, 9, 12], 1, cursor, "test-release")
        result = finish(tmp_path, pending)
    assert result["aggregate"]["empty_sweep"] is True
    restarted = next_group([8, 9, 12], 1, cursor, "test-release")
    again = finish(tmp_path, restarted)
    assert again["aggregate"]["empty_sweep"] is True
    assert json.loads(cursor.read_text())["empty_streak"] == 3
    assert next_group([8, 9, 12], 1, cursor, "test-release")["leagues"] == [9]


def test_malformed_completion_cannot_count_as_healthy_coverage(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    for _ in range(3):
        pending = next_group([8, 9, 12], 1, cursor, "test-release")
        finish(tmp_path, pending)
    state = json.loads(cursor.read_text())
    state["last_completed"]["8"] = {}
    cursor.write_text(json.dumps(state))
    with pytest.raises(ValueError, match="completion time"):
        next_group([8, 9, 12], 1, cursor, "test-release")


def test_empty_first_scope_does_not_end_sweep_before_work_later(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    a = next_group([8, 9, 12], 1, cursor, "test-release")
    assert finish(tmp_path, a)["aggregate"]["empty_sweep"] is False
    b = next_group([8, 9, 12], 1, cursor, "test-release")
    result = finish(tmp_path, b, batches=1, selected=4, accepted=0)
    assert result["outcome"] == "processed"
    assert result["accepted"] == 0
    assert result["aggregate"]["state"] == "unknown"
    c = next_group([8, 9, 12], 1, cursor, "test-release")
    assert finish(tmp_path, c)["aggregate"]["empty_sweep"] is False
    assert next_group([8, 9, 12], 1, cursor, "test-release")["leagues"] == [8]


def test_failed_a_empty_b_and_restart_retains_failure_until_a_recovers(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    a = next_group([8, 9, 12], 1, cursor, "test-release")
    failed = finish(tmp_path, a, status=1, report=False)
    assert failed["aggregate"]["state"] == "degraded"
    assert failed["aggregate"]["unresolved_leagues"] == [8]
    b = next_group([8, 9, 12], 1, cursor, "test-release")
    assert finish(tmp_path, b)["aggregate"]["unresolved_leagues"] == [8]
    c = next_group([8, 9, 12], 1, cursor, "test-release")
    assert finish(tmp_path, c)["aggregate"]["unresolved_leagues"] == [8]
    # Simulate restart by loading the persisted file through another call.
    a_retry = next_group([8, 9, 12], 1, cursor, "test-release")
    recovered = finish(tmp_path, a_retry)
    assert recovered["aggregate"]["state"] == "healthy"
    assert recovered["aggregate"]["unresolved_leagues"] == []
    assert "8" in json.loads(cursor.read_text())["last_failure"]


def test_crash_after_claim_marks_interrupted_and_other_leagues_continue(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    a = next_group([8, 9, 12], 1, cursor, "test-release")
    b = next_group([8, 9, 12], 1, cursor, "test-release")
    assert b["leagues"] == [9]
    state = json.loads(cursor.read_text())
    assert state["unresolved"]["8"]["outcome"] == "interrupted"
    finish(tmp_path, b)
    c = next_group([8, 9, 12], 1, cursor, "test-release")
    finish(tmp_path, c)
    a_retry = next_group([8, 9, 12], 1, cursor, "test-release")
    assert a_retry["leagues"] == [8]
    assert finish(tmp_path, a_retry)["aggregate"]["unresolved_leagues"] == []
    assert a_retry["attempt_id"] != a["attempt_id"]


def test_wrong_report_identity_is_failure_and_cannot_clear_previous_failure(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    pending = next_group([8, 9, 12], 1, cursor, "test-release")
    path = report_for(tmp_path, pending, batches=1, selected=1)
    payload = json.loads(path.read_text())
    payload["scope_attempt_id"] = "stale"
    path.write_text(json.dumps(payload))
    result = finish_attempt([8, 9, 12], 1, cursor, "test-release", pending["attempt_id"], 0, path)
    assert result["outcome"] == "invalid_report"
    assert result["aggregate"]["unresolved_leagues"] == [8]


def test_mismatched_completion_cannot_rewrite_pending_scope(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    pending = next_group([8, 9, 12], 1, cursor, "test-release")
    with pytest.raises(ValueError, match="identity mismatch"):
        finish_attempt([8, 9, 12], 1, cursor, "test-release", "wrong", 0, tmp_path / "missing")
    assert json.loads(cursor.read_text())["pending"]["attempt_id"] == pending["attempt_id"]


def test_changed_config_keeps_unresolved_league_and_resets_coverage(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    a = next_group([8, 9], 1, cursor, "test-release")
    finish_attempt([8, 9], 1, cursor, "test-release", a["attempt_id"], 1, tmp_path / "missing")
    changed = next_group([12, 8, 9], 2, cursor, "test-release")
    assert changed["leagues"] == [12, 8]
    state = json.loads(cursor.read_text())
    assert state["unresolved"]["8"]["outcome"] == "failed"
    assert state["last_completed"] == {}
    assert changed["epoch"] != a["epoch"]


def test_release_change_starts_unknown_epoch_without_losing_unresolved_failure(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    a = next_group([8, 9], 1, cursor, "old")
    finish_attempt([8, 9], 1, cursor, "old", a["attempt_id"], 1, tmp_path / "missing")
    newer = next_group([8, 9], 1, cursor, "new")
    state = json.loads(cursor.read_text())
    assert newer["epoch"] != a["epoch"]
    assert state["last_completed"] == {}
    assert "8" in state["unresolved"]


@pytest.mark.parametrize("raw", ["", "8,", "8,,9", "8,8", "0,8", "-8,9", "8,abc"])
def test_invalid_league_configuration_is_rejected(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_leagues(raw)


def test_corrupt_cursor_is_rejected_without_advancing(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    cursor.write_text('{"version":2,"signature":')
    with pytest.raises(json.JSONDecodeError):
        next_group([8, 9], 1, cursor, "release")


def test_count_does_not_advance_cursor(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    common = ["--leagues", "8,9,12", "--group-size", "1"]
    count = subprocess.run([sys.executable, str(HELPER), "count", *common],
                           text=True, capture_output=True, check=True)
    assert count.stdout.strip() == "3"
    assert not cursor.exists()
    next_run = subprocess.run([sys.executable, str(HELPER), "next", *common,
                               "--cursor-file", str(cursor), "--release-id", "test-release"],
                              text=True, capture_output=True, check=True)
    assert json.loads(next_run.stdout)["leagues"] == [8]


def test_supervisor_passes_selected_scope_to_worker() -> None:
    source = (ROOT / "scripts/vps/run_stats_reconciliation.sh").read_text()
    chain = source.split("RECONCILIATION_COMMAND=$(cat <<'CHAIN'", 1)[1].split("\nCHAIN", 1)[0]
    assert 'select_stats_reconciliation_leagues.py" next' in chain
    assert '--leagues "${selected_leagues}"' in chain
