from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scripts.sync_odds import (
    DEFAULT_BOOKMAKERS,
    canonicalize_bookmakers,
    fixture_window_bounds,
    fetch_league_odds_payload,
    inspect_upstream_moneyline,
    load_fixture_moneyline_completeness,
    load_default_bookmakers,
    match_event_to_fixture,
    team_aliases,
)


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
    def test_default_bookmakers_match_the_user_facing_contract(self) -> None:
        self.assertEqual(DEFAULT_BOOKMAKERS, ["Bet365", "Paddy Power", "Unibet", "BetMGM"])
        self.assertEqual(load_default_bookmakers(), DEFAULT_BOOKMAKERS)

    def test_settled_history_window_uses_complete_utc_calendar_days(self) -> None:
        start, end = fixture_window_bounds(2, 0, calendar_history=True)

        self.assertEqual(end.tzinfo, None)
        self.assertEqual(end.hour, 0)
        self.assertEqual(end.minute, 0)
        self.assertEqual(end.second, 0)
        self.assertEqual((end - start).days, 2)
        self.assertEqual(end.date(), datetime.now(timezone.utc).date())

    def test_historical_fetch_is_required_until_all_moneyline_sides_exist(self) -> None:
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [
            (1, 100, 200, "team", 100, "home", 2.1),
            (1, 100, 200, "team", 200, "away", 3.2),
            (1, 100, 200, None, None, "draw", 3.4),
            (2, 100, 200, "team", 100, "home", 2.1),
        ]

        self.assertEqual(load_fixture_moneyline_completeness(session, [1, 2]), {1})

    def test_inspects_only_usable_configured_moneyline_markets(self) -> None:
        sides = inspect_upstream_moneyline(
            {
                "Unibet": [
                    {"name": "ML", "odds": [{"home": "2.1", "draw": "3.4", "away": "3.2"}]},
                    {"name": "ML HT", "odds": [{"home": "2.0", "draw": "3.0", "away": "4.0"}]},
                ],
                "Unknown": [{"name": "ML", "odds": [{"home": "2.0"}]}],
            },
            {"unibet"},
        )

        self.assertEqual(sides, {"Unibet": ["home", "draw", "away"]})

    def test_does_not_treat_invalid_or_non_moneyline_prices_as_provider_support(self) -> None:
        sides = inspect_upstream_moneyline(
            {
                "Bet365": [
                    {"name": "ML", "odds": [{"home": "1", "draw": "501", "away": "not-a-price"}]},
                    {"name": "Totals", "odds": [{"home": "2.0"}]},
                ]
            },
            {"bet365"},
        )

        self.assertEqual(sides, {})

    @patch("scripts.sync_odds.OddsApiClient")
    def test_fetch_emits_per_fixture_evidence_for_matched_provider_response(self, client_type) -> None:
        client = MagicMock()
        client.request.side_effect = [
            [
                {
                    "id": 7001,
                    "home": "Home FC",
                    "away": "Away FC",
                    "date": "2026-09-04T16:00:00Z",
                    "status": "pending",
                }
            ],
            [
                {
                    "id": 7001,
                    "bookmakers": {
                        "Unibet": [
                            {"name": "ML", "odds": [{"home": "2.1", "draw": "3.4", "away": "3.2"}]}
                        ]
                    },
                }
            ],
        ]
        client.stats = SimpleNamespace(
            total_calls=2,
            calls_by_endpoint={"events": 1, "odds/multi": 1},
            api_time_seconds=0.1,
            rate_limit_hits=0,
            rate_limit_sleeps=0,
            last_rate_limit=None,
        )
        client_type.return_value = client

        result = fetch_league_odds_payload(
            444,
            "test-league",
            [build_fixture(19629715, "Home FC", "Away FC", datetime(2026, 9, 4, 16, 0))],
            "football",
            0,
            14,
            ["Unibet"],
            0,
        )

        self.assertIsNone(result.error)
        self.assertEqual(len(result.moneyline_coverage), 1)
        evidence = result.moneyline_coverage[0]
        self.assertEqual(evidence["matching_status"], "matched")
        self.assertEqual(evidence["odds_response_status"], "received")
        self.assertEqual(evidence["supported_moneyline_bookmakers"], ["Unibet"])
        self.assertEqual(
            evidence["moneyline_sides_by_bookmaker"],
            {"Unibet": ["home", "draw", "away"]},
        )

    @patch("scripts.sync_odds.OddsApiClient")
    def test_settled_history_uses_historical_endpoint_and_emits_evidence(self, client_type) -> None:
        events_client = MagicMock()
        historical_client = MagicMock()
        events_client.request.return_value = [
            {
                "id": 8001,
                "home": "Home FC",
                "away": "Away FC",
                "date": "2026-08-31T16:00:00Z",
                "status": "finished",
            }
        ]
        historical_client.request.return_value = {
            "bookmakers": {
                "Unibet": [
                    {"name": "ML", "odds": [{"home": "2.1", "draw": "3.4", "away": "3.2"}]}
                ]
            }
        }
        stats = SimpleNamespace(
            total_calls=1,
            calls_by_endpoint={"events": 1},
            api_time_seconds=0.1,
            rate_limit_hits=0,
            rate_limit_sleeps=0,
            last_rate_limit=None,
        )
        historical_stats = SimpleNamespace(
            total_calls=1,
            calls_by_endpoint={"historical/odds": 1},
            api_time_seconds=0.1,
            rate_limit_hits=0,
            rate_limit_sleeps=0,
            last_rate_limit=None,
        )
        events_client.stats = stats
        historical_client.stats = historical_stats
        client_type.side_effect = [events_client, historical_client]

        result = fetch_league_odds_payload(
            444,
            "test-league",
            [build_fixture(19629715, "Home FC", "Away FC", datetime(2026, 8, 31, 16, 0))],
            "football",
            2,
            0,
            ["Unibet"],
            0,
            calendar_history=True,
        )

        self.assertIsNone(result.error)
        historical_client.request.assert_called_once_with(
            "historical/odds",
            params={"eventId": "8001", "bookmakers": "Unibet"},
        )
        self.assertEqual(len(result.odds_records), 1)
        self.assertEqual(result.moneyline_coverage[0]["odds_response_status"], "received")

    def test_accepts_all_supported_odds_bookmakers(self) -> None:
        bookmakers, unknown = canonicalize_bookmakers(
            ["Unibet", "BetMGM", "Betfair Exchange", "Paddy Power", "Bet365"]
        )

        self.assertEqual(
            bookmakers,
            ["Unibet", "BetMGM", "Betfair Exchange", "Paddy Power", "Bet365"],
        )
        self.assertEqual(unknown, [])

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

    def test_matches_formal_provider_name_to_short_local_name(self) -> None:
        event = build_event(
            "Kocaelispor",
            "Amed Sportif Faaliyetler",
            "2026-08-24T18:30:00Z",
        )
        fixture = build_fixture(
            19746638,
            "Kocaelispor",
            "Amed SK",
            datetime(2026, 8, 24, 18, 30, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19746638)

    def test_does_not_match_unrelated_teams_sharing_the_al_prefix(self) -> None:
        event = build_event(
            "Al-Fayha FC",
            "Al-Kholood",
            "2026-04-17T15:55:00Z",
        )
        fixture = build_fixture(
            19467901,
            "Al Draih",
            "Al-Qadsiah",
            datetime(2026, 4, 17, 15, 55, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNone(matched)

    def test_matches_exact_teams_that_use_the_al_prefix(self) -> None:
        event = build_event(
            "Al Draih",
            "Al-Qadsiah",
            "2026-04-17T15:55:00Z",
        )
        fixture = build_fixture(
            19467902,
            "Al Draih",
            "Al-Qadsiah",
            datetime(2026, 4, 17, 15, 55, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19467902)

    def test_matches_provider_renamed_saudi_club(self) -> None:
        event = build_event(
            "Diriyah Club",
            "Al-Kholood",
            "2026-08-26T18:00:00Z",
        )
        fixture = build_fixture(
            19777692,
            "Al Draih",
            "Al Kholood",
            datetime(2026, 8, 26, 18, 0, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19777692)

    def test_matches_brazilian_provider_state_qualifiers(self) -> None:
        event = build_event(
            "Botafogo FR RJ",
            "CA Paranaense PR",
            "2026-08-24T23:00:00Z",
        )
        fixture = build_fixture(
            19621848,
            "Botafogo",
            "Athletico PR",
            datetime(2026, 8, 24, 23, 0, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19621848)

    def test_matches_brazilian_provider_long_names_to_short_names(self) -> None:
        event = build_event(
            "Athletic Club Sjdr MG",
            "Gremio Novorizontino SP",
            "2026-08-24T22:30:00Z",
        )
        fixture = build_fixture(
            19667205,
            "Athletic Club",
            "Novorizontino",
            datetime(2026, 8, 24, 22, 30, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19667205)

    def test_matches_brazilian_state_qualifiers_with_schedule_drift(self) -> None:
        event = build_event(
            "Botafogo FC SP",
            "Goias EC GO",
            "2026-09-12T18:00:00Z",
        )
        fixture = build_fixture(
            19667167,
            "Botafogo SP",
            "Goiás",
            datetime(2026, 9, 14, 22, 30, 0),
        )

        matched = match_event_to_fixture(event, [fixture])

        self.assertIsNotNone(matched)
        self.assertEqual(matched["fixture_id"], 19667167)


if __name__ == "__main__":
    unittest.main()
