import unittest

from scripts.validate_moneyline_coverage import evaluate_failures


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


if __name__ == "__main__":
    unittest.main()
