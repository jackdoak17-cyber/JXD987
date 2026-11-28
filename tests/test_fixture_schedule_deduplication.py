from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scripts.refresh_fixture_delivery import reconcile_duplicate_fixtures


UTC = timezone.utc


def row(fixture_id: int, updated_at: datetime, **changes):
    value = {
        "id": fixture_id,
        "league_id": 779,
        "season_id": 26720,
        "starting_at": datetime(2026, 12, 3, 0, 0, tzinfo=UTC),
        "status": "NS",
        "status_code": "NS",
        "home_team_id": 641,
        "away_team_id": 275,
        "home_score": None,
        "away_score": None,
        "home_ht_score": None,
        "away_ht_score": None,
        "source_updated_at": updated_at,
    }
    value.update(changes)
    return value


def test_exact_duplicate_keeps_newest_provider_record() -> None:
    old = row(19904936, datetime(2026, 11, 9, tzinfo=UTC))
    new = row(19908343, datetime(2026, 11, 23, tzinfo=UTC))
    reconciled, suppressions = reconcile_duplicate_fixtures([old, new], scope="schedule")
    assert [item["id"] for item in reconciled] == [19908343]
    assert suppressions == [
        {
            "scope": "schedule",
            "kept_fixture_id": 19908343,
            "suppressed_fixture_id": 19904936,
            "league_id": 779,
            "starting_at": "2026-12-03T00:00:00+00:00",
            "home_team_id": 641,
            "away_team_id": 275,
        }
    ]


def test_conflicting_duplicate_fails_closed() -> None:
    pending = row(19904936, datetime(2026, 11, 9, tzinfo=UTC))
    conflicting = row(
        19908343,
        datetime(2026, 11, 23, tzinfo=UTC),
        status="FT",
        status_code="FT",
        home_score=1,
        away_score=0,
    )
    with pytest.raises(RuntimeError, match="conflicting duplicate fixtures"):
        reconcile_duplicate_fixtures([pending, conflicting], scope="schedule")
