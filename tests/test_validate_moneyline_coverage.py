import unittest

from scripts.validate_moneyline_coverage import (
    evaluate_failures,
    evaluate_provider_aware_failures,
)


class ValidateMoneylineCoverageTests(unittest.TestCase):
    def test_passes_when_league_meets_threshold(self) -> None:
        failures = evaluate_failures(
            [
                {
                    "league_id": 444,
                    "fixtures_in_window": 8,
                    "fixtures_with_complete_moneyline": 8,
                    "coverage_pct": 100.0,
                    "missing_fixture_ids": [],
                    "first_missing_starting_at": None,
                }
            ],
            fail_below_pct=100.0,
        )
        self.assertEqual(failures, [])

    def test_fails_when_league_drops_below_threshold(self) -> None:
        failures = evaluate_failures(
            [
                {
                    "league_id": 444,
                    "fixtures_in_window": 8,
                    "fixtures_with_complete_moneyline": 7,
                    "coverage_pct": 87.5,
                    "missing_fixture_ids": [19629715],
                    "first_missing_starting_at": "2026-04-11T12:00:00+00:00",
                }
            ],
            fail_below_pct=100.0,
        )
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["league_id"], 444)
        self.assertEqual(failures[0]["missing_fixture_ids"], [19629715])

    def test_ignores_leagues_without_upcoming_fixtures(self) -> None:
        failures = evaluate_failures(
            [
                {
                    "league_id": 600,
                    "fixtures_in_window": 0,
                    "fixtures_with_complete_moneyline": 0,
                    "coverage_pct": 0.0,
                    "missing_fixture_ids": [],
                    "first_missing_starting_at": None,
                }
            ],
            fail_below_pct=100.0,
        )
        self.assertEqual(failures, [])

    def test_accepts_only_explicit_empty_provider_market_as_provider_gap(self) -> None:
        failures, provider_gaps, pipeline_failures, unresolved = evaluate_provider_aware_failures(
            [
                {
                    "league_id": 444,
                    "fixtures_in_window": 2,
                    "fixtures_with_complete_moneyline": 1,
                    "coverage_pct": 50.0,
                    "missing_fixture_ids": [19629715],
                }
            ],
            {
                19629715: {
                    "fixture_id": 19629715,
                    "event_id": 7001,
                    "matching_status": "matched",
                    "odds_response_status": "received",
                    "supported_moneyline_bookmakers": [],
                }
            },
            fail_below_pct=100.0,
        )

        self.assertEqual(failures, [])
        self.assertEqual([row["fixture_id"] for row in provider_gaps], [19629715])
        self.assertEqual(pipeline_failures, [])
        self.assertEqual(unresolved, [])

    def test_keeps_usable_upstream_market_as_pipeline_failure(self) -> None:
        failures, provider_gaps, pipeline_failures, unresolved = evaluate_provider_aware_failures(
            [
                {
                    "league_id": 444,
                    "fixtures_in_window": 2,
                    "fixtures_with_complete_moneyline": 1,
                    "coverage_pct": 50.0,
                    "missing_fixture_ids": [19629715],
                }
            ],
            {
                19629715: {
                    "fixture_id": 19629715,
                    "event_id": 7001,
                    "matching_status": "matched",
                    "odds_response_status": "received",
                    "supported_moneyline_bookmakers": ["Unibet"],
                }
            },
            fail_below_pct=100.0,
        )

        self.assertEqual(len(failures), 1)
        self.assertEqual([row["fixture_id"] for row in pipeline_failures], [19629715])
        self.assertEqual(provider_gaps, [])
        self.assertEqual(unresolved, [])

    def test_accepts_valid_empty_odds_response_as_provider_gap(self) -> None:
        failures, provider_gaps, pipeline_failures, unresolved = evaluate_provider_aware_failures(
            [
                {
                    "league_id": 444,
                    "fixtures_in_window": 2,
                    "fixtures_with_complete_moneyline": 1,
                    "coverage_pct": 50.0,
                    "missing_fixture_ids": [19629715],
                }
            ],
            {
                19629715: {
                    "fixture_id": 19629715,
                    "event_id": 7001,
                    "matching_status": "matched",
                    "odds_response_status": "empty",
                    "supported_moneyline_bookmakers": [],
                }
            },
            fail_below_pct=100.0,
        )

        self.assertEqual(failures, [])
        self.assertEqual([row["fixture_id"] for row in provider_gaps], [19629715])
        self.assertEqual(pipeline_failures, [])
        self.assertEqual(unresolved, [])

    def test_accepts_only_a_valid_empty_date_scoped_event_probe_as_provider_gap(self) -> None:
        failures, provider_gaps, pipeline_failures, unresolved = evaluate_provider_aware_failures(
            [
                {
                    "league_id": 989,
                    "fixtures_in_window": 2,
                    "fixtures_with_complete_moneyline": 1,
                    "coverage_pct": 50.0,
                    "missing_fixture_ids": [19674675],
                }
            ],
            {
                19674675: {
                    "fixture_id": 19674675,
                    "odds_api_league": "china-chinese-super-league",
                    "matching_status": "provider_gap",
                    "provider_gap_reason": "no_events_for_fixture_date",
                    "provider_event_feed_status": "empty",
                    "provider_event_probe": {
                        "endpoint": "events",
                        "response_status": "ok",
                        "from": "2026-09-12T00:00:00Z",
                        "to": "2026-09-12T23:59:59Z",
                        "events_returned": 0,
                    },
                    "event_id": None,
                    "odds_response_status": "not_applicable",
                    "supported_moneyline_bookmakers": [],
                }
            },
            fail_below_pct=100.0,
        )

        self.assertEqual(failures, [])
        self.assertEqual([row["fixture_id"] for row in provider_gaps], [19674675])
        self.assertEqual(pipeline_failures, [])
        self.assertEqual(unresolved, [])

    def test_rejects_unproven_provider_gap_evidence(self) -> None:
        failures, provider_gaps, pipeline_failures, unresolved = evaluate_provider_aware_failures(
            [
                {
                    "league_id": 989,
                    "fixtures_in_window": 1,
                    "fixtures_with_complete_moneyline": 0,
                    "coverage_pct": 0.0,
                    "missing_fixture_ids": [19674675],
                }
            ],
            {
                19674675: {
                    "fixture_id": 19674675,
                    "matching_status": "provider_gap",
                    "provider_gap_reason": "no_events_for_fixture_date",
                    "provider_event_feed_status": "empty",
                    "provider_event_probe": {
                        "endpoint": "events",
                        "response_status": "ok",
                        "from": "2026-09-12T00:00:00Z",
                        "to": "2026-09-12T23:59:59Z",
                        "events_returned": 1,
                    },
                }
            },
            fail_below_pct=100.0,
        )

        self.assertEqual(len(failures), 1)
        self.assertEqual(provider_gaps, [])
        self.assertEqual(pipeline_failures, [])
        self.assertEqual([row["fixture_id"] for row in unresolved], [19674675])

    def test_missing_provider_evidence_fails_closed(self) -> None:
        failures, provider_gaps, pipeline_failures, unresolved = evaluate_provider_aware_failures(
            [
                {
                    "league_id": 444,
                    "fixtures_in_window": 2,
                    "fixtures_with_complete_moneyline": 1,
                    "coverage_pct": 50.0,
                    "missing_fixture_ids": [19629715],
                }
            ],
            {},
            fail_below_pct=100.0,
        )

        self.assertEqual(len(failures), 1)
        self.assertEqual(provider_gaps, [])
        self.assertEqual(pipeline_failures, [])
        self.assertEqual([row["fixture_id"] for row in unresolved], [19629715])


if __name__ == "__main__":
    unittest.main()
