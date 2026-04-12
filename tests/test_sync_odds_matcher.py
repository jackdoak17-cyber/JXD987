from __future__ import annotations

import unittest
from datetime import datetime

from scripts.sync_odds import match_event_to_fixture, team_aliases


def build_fixture(
    fixture_id: int,
    home_name: str,
    away_name: str,
    starting_at: datetime,
) -> dict[str, object]:
    return {
        "fixture_id": fixture_id,
        "league_id": 0,
        "starting_at": starting_at,
        "home_alias": team_aliases(home_name),
        "away_alias": team_aliases(away_name),
        "has_moneyline_odds": False,
    }


def build_event(home_name: str, away_name: str, date_iso: str) -> dict[str, object]:
    return {
        "home": home_name,
        "away": away_name,
        "date": date_iso,
    }


class MatchEventToFixtureRegressionTests(unittest.TestCase):
    def test_matches_midnight_placeholder_when_names_are_exact(self) -> None:
        event = build_event(
            "Racing Club De Lens",
            "Toulouse FC",
            "2026-04-17T18:45:00Z",
        )
        fixture = build_fixture(
            19467900,
            "Lens",
            "Toulouse",
            datetime(2026, 4, 19, 0, 0, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19467900)

    def test_matches_provider_city_suffixes_against_midnight_placeholder(self) -> None:
        event = build_event(
            "Goztepe Izmir",
            "Kasimpasa Istanbul",
            "2026-04-12T14:00:00Z",
        )
        fixture = build_fixture(
            19443188,
            "Göztepe",
            "Kasımpaşa",
            datetime(2026, 4, 12, 0, 0, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19443188)

    def test_prefers_precise_kickoff_over_placeholder_candidate(self) -> None:
        event = build_event(
            "Istanbul Basaksehir",
            "Genclerbirligi SK",
            "2026-04-11T11:30:00Z",
        )
        placeholder = build_fixture(
            19443183,
            "İstanbul Başakşehir",
            "Gençlerbirliği",
            datetime(2026, 4, 12, 0, 0, 0),
        )
        precise = build_fixture(
            19443179,
            "İstanbul Başakşehir",
            "Gençlerbirliği",
            datetime(2026, 4, 11, 11, 30, 0),
        )

        matched = match_event_to_fixture(event, [placeholder, precise])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19443179)

    def test_rejects_placeholder_fixture_when_date_gap_is_too_large(self) -> None:
        event = build_event(
            "Paris Saint-Germain",
            "FC Nantes",
            "2026-04-22T17:00:00Z",
        )
        fixture = build_fixture(
            19467899,
            "Paris Saint Germain",
            "Nantes",
            datetime(2026, 4, 19, 0, 0, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNone(matched)


if __name__ == "__main__":
    unittest.main()
