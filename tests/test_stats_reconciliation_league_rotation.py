from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.vps.select_stats_reconciliation_leagues import (
    groups_for,
    next_group,
    parse_leagues,
)


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/vps/select_stats_reconciliation_leagues.py"


def test_rotation_covers_each_league_and_survives_restart(tmp_path: Path) -> None:
    cursor = tmp_path / "state/cursor.json"
    leagues = [8, 9, 12, 14, 24]
    assert groups_for(leagues, 2) == [[8, 9], [12, 14], [24]]
    assert next_group(leagues, 2, cursor) == [8, 9]
    assert next_group(leagues, 2, cursor) == [12, 14]
    assert json.loads(cursor.read_text())["next_group"] == 2
    assert next_group(leagues, 2, cursor) == [24]
    assert next_group(leagues, 2, cursor) == [8, 9]
    assert cursor.stat().st_mode & 0o777 == 0o600


def test_changed_scope_resets_cursor_without_losing_configured_leagues(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    assert next_group([8, 9, 12, 14], 2, cursor) == [8, 9]
    assert next_group([8, 9, 12, 14, 2], 1, cursor) == [8]
    assert [next_group([8, 9, 12, 14, 2], 1, cursor) for _ in range(4)] == [
        [9], [12], [14], [2]
    ]


@pytest.mark.parametrize("raw", ["", "8,", "8,,9", "8,8", "0,8", "-8,9", "8,abc"])
def test_invalid_league_configuration_is_rejected(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_leagues(raw)


def test_corrupt_cursor_is_rejected_without_advancing(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    cursor.write_text('{"version":1,"signature":"bad","next_group":-1}')
    # An old signature is a different configuration and safely restarts.
    assert next_group([8, 9], 1, cursor) == [8]
    cursor.write_text('{"version":1,"signature":')
    with pytest.raises(json.JSONDecodeError):
        next_group([8, 9], 1, cursor)


def test_count_does_not_advance_cursor_and_next_is_group_scoped(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor.json"
    common = ["--leagues", "8,9,12,14,24", "--group-size", "2"]
    count = subprocess.run(
        [sys.executable, str(HELPER), "count", *common],
        text=True, capture_output=True, check=True,
    )
    assert count.stdout.strip() == "3"
    assert not cursor.exists()
    next_run = subprocess.run(
        [sys.executable, str(HELPER), "next", *common, "--cursor-file", str(cursor)],
        text=True, capture_output=True, check=True,
    )
    assert next_run.stdout.strip() == "8,9"


def test_supervisor_passes_selected_scope_to_worker() -> None:
    source = (ROOT / "scripts/vps/run_stats_reconciliation.sh").read_text()
    chain = source.split("RECONCILIATION_COMMAND=$(cat <<'CHAIN'", 1)[1].split("\nCHAIN", 1)[0]
    assert "select_stats_reconciliation_leagues.py\" next" in chain
    assert '--leagues "${selected_leagues}"' in chain
    assert '--leagues "${STATS_RECONCILE_LEAGUES}" \\' not in chain.split("exec ", 1)[-1]
    assert "empty_group_count >= STATS_RECONCILE_GROUP_COUNT" in source
