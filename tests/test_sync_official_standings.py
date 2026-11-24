from __future__ import annotations

import pytest

from scripts.sync_official_standings import (
    SeasonTarget,
    load_targets,
    normalize_corrections,
    normalize_standings,
    parse_expectations,
    selected_stage,
    verify_expectations,
)


class RecordingCursor:
    def __init__(self) -> None:
        self.query = ""
        self.params = ()

    def execute(self, query, params) -> None:
        self.query = query
        self.params = params

    def fetchall(self):
        return [(9, 27903, "2026-08-01")]


def details(*, played: int, wins: int, draws: int, losses: int, goals_for: int, goals_against: int):
    return [
        {"type_id": 129, "value": played},
        {"type_id": 130, "value": wins},
        {"type_id": 131, "value": draws},
        {"type_id": 132, "value": losses},
        {"type_id": 133, "value": goals_for},
        {"type_id": 134, "value": goals_against},
        {"type_id": 179, "value": goals_for - goals_against},
    ]


def standing(team_id: int, position: int, points: int, **record: int):
    return {
        "id": 900000 + team_id,
        "participant_id": team_id,
        "league_id": 9,
        "season_id": 27903,
        "stage_id": 77480001,
        "group_id": None,
        "round_id": 340000,
        "standing_rule_id": 180,
        "position": position,
        "result": "equal",
        "points": points,
        "details": details(**record),
        "participant": {
            "id": team_id,
            "name": "Southampton" if team_id == 65 else f"Team {team_id}",
            "short_code": "SOU" if team_id == 65 else None,
            "image_path": f"https://cdn.example/{team_id}.png",
        },
    }


def test_southampton_official_points_and_rank_are_not_recomputed_from_record() -> None:
    target = SeasonTarget(league_id=9, season_id=27903, start_date="2026-08-01")
    rows = [
        standing(
            65,
            6,
            27,
            played=16,
            wins=9,
            draws=4,
            losses=3,
            goals_for=31,
            goals_against=15,
        ),
        standing(
            66,
            5,
            28,
            played=16,
            wins=8,
            draws=4,
            losses=4,
            goals_for=25,
            goals_against=18,
        ),
    ]

    normalized = normalize_standings(target, 77480001, rows)
    southampton = next(row for row in normalized if row["team_id"] == 65)

    assert southampton["position"] == 6
    assert southampton["points"] == 27
    assert southampton["played"] == 16
    assert southampton["wins"] == 9
    assert southampton["draws"] == 4
    assert southampton["losses"] == 3


def test_active_correction_is_preserved_as_audit_evidence() -> None:
    target = SeasonTarget(league_id=9, season_id=27903, start_date="2026-08-01")
    rows = [
        {
            "id": 7001,
            "season_id": 27903,
            "stage_id": 77480001,
            "group_id": None,
            "participant_id": 65,
            "type_id": 190,
            "value": 4,
            "calc_type": "-",
            "active": True,
        }
    ]

    corrections = normalize_corrections(target, rows)

    assert corrections == [
        {
            "provider_correction_id": 7001,
            "league_id": 9,
            "season_id": 27903,
            "stage_id": 77480001,
            "group_id": None,
            "team_id": 65,
            "type_id": 190,
            "value": 4,
            "calc_type": "-",
            "active": True,
        }
    ]


def test_current_table_stage_wins_over_later_pending_stage() -> None:
    stages = [
        {"id": 10, "sort_order": 1, "is_current": True, "pending": False},
        {"id": 20, "sort_order": 2, "is_current": False, "pending": True},
    ]
    standings = [{"stage_id": 10}, {"stage_id": 20}]

    assert selected_stage(stages, standings) == 10


def test_latest_finished_table_is_used_when_current_knockout_has_no_table() -> None:
    stages = [
        {"id": 10, "sort_order": 1, "is_current": False, "pending": False},
        {"id": 20, "sort_order": 2, "is_current": True, "pending": False},
    ]
    standings = [{"stage_id": 10}]

    assert selected_stage(stages, standings) == 10


def test_incomplete_standing_is_rejected_instead_of_publishing_partial_data() -> None:
    target = SeasonTarget(league_id=9, season_id=27903, start_date="2026-08-01")
    rows = [
        standing(
            65,
            6,
            27,
            played=16,
            wins=9,
            draws=4,
            losses=3,
            goals_for=31,
            goals_against=15,
        ),
        standing(
            66,
            5,
            28,
            played=16,
            wins=8,
            draws=4,
            losses=4,
            goals_for=25,
            goals_against=18,
        ),
    ]
    rows[0]["details"] = [detail for detail in rows[0]["details"] if detail["type_id"] != 134]

    with pytest.raises(ValueError, match="missing details"):
        normalize_standings(target, 77480001, rows)


def test_duplicate_team_is_rejected() -> None:
    target = SeasonTarget(league_id=9, season_id=27903, start_date="2026-08-01")
    row = standing(
        65,
        6,
        27,
        played=16,
        wins=9,
        draws=4,
        losses=3,
        goals_for=31,
        goals_against=15,
    )
    duplicate = {**row, "id": 900999, "position": 7}

    with pytest.raises(ValueError, match="duplicate team"):
        normalize_standings(target, 77480001, [row, duplicate])


def test_exact_provider_expectation_guards_southampton_contract() -> None:
    target = SeasonTarget(league_id=9, season_id=27903, start_date="2026-08-01")
    standings = [{"team_id": 65, "position": 6, "points": 27}]
    expectations = parse_expectations(["27903:65:6:27"])

    verify_expectations(target, standings, expectations)

    with pytest.raises(ValueError, match="standing expectation failed"):
        verify_expectations(target, standings, parse_expectations(["27903:65:3:31"]))


def test_routine_target_scope_is_current_only_not_implicit_historical_backfill() -> None:
    cursor = RecordingCursor()

    targets = load_targets(cursor, [9], [])

    assert targets == [SeasonTarget(league_id=9, season_id=27903, start_date="2026-08-01")]
    assert "season_rank = 1" in cursor.query
    assert "official_team_standings" not in cursor.query
    assert cursor.params == ([9], False)
