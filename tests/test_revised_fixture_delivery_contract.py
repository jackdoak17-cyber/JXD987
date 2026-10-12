from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.refresh_fixture_delivery import (
    calculate_metrics,
    london_today,
    order_history_rows,
    validate_metric_identity_set,
    validate_standings_rows,
)


UTC = timezone.utc


class RevisedFixtureDeliveryContractTests(unittest.TestCase):
    def test_history_order_has_a_total_latest_first_key(self) -> None:
        rows = [
            {"id": 1, "starting_at": datetime(2026, 8, 22, tzinfo=UTC)},
            {"id": 2, "starting_at": datetime(2026, 8, 22, tzinfo=UTC)},
        ]
        self.assertEqual([row["id"] for row in order_history_rows(rows)], [1, 2])

    def test_london_today_is_derived_from_an_explicit_clock_zone(self) -> None:
        self.assertEqual(london_today(datetime(2026, 8, 22, 23, 30, tzinfo=UTC)), "2026-08-23")

    def test_metric_identity_validation_rejects_count_balanced_wrong_rows(self) -> None:
        expected = [
            {"fixture_id": 10, "side": side, "metrics_window": window, "metrics_mode": mode}
            for side in ("home", "away")
            for window in range(5, 16)
            for mode in ("overall", "venue")
        ]
        wrong = [*expected[:-1], {**expected[-1], "fixture_id": 11}]
        with self.assertRaisesRegex(RuntimeError, "identity"):
            validate_metric_identity_set([10], wrong)

    def test_metric_identity_validation_rejects_a_wrong_team_for_a_valid_side(self) -> None:
        expected = [
            {
                "fixture_id": 10,
                "team_id": 100 if side == "home" else 200,
                "side": side,
                "metrics_window": window,
                "metrics_mode": mode,
            }
            for side in ("home", "away")
            for window in range(5, 16)
            for mode in ("overall", "venue")
        ]
        wrong = [*expected[:-1], {**expected[-1], "team_id": 999}]
        with self.assertRaisesRegex(RuntimeError, "identity"):
            validate_metric_identity_set([10], wrong, {10: {"home": 100, "away": 200}})

    def test_metric_identity_validation_requires_both_history_scopes(self) -> None:
        expected = [
            {
                "fixture_id": 10,
                "team_id": 100 if side == "home" else 200,
                "side": side,
                "metrics_window": window,
                "metrics_mode": mode,
                "season_scope": season_scope,
            }
            for season_scope in ("all", "current")
            for side in ("home", "away")
            for window in range(5, 16)
            for mode in ("overall", "venue")
        ]

        validate_metric_identity_set([10], expected, {10: {"home": 100, "away": 200}})

        with self.assertRaisesRegex(RuntimeError, "identity"):
            validate_metric_identity_set([10], expected[:-1], {10: {"home": 100, "away": 200}})

    def test_goal_aggregates_match_the_source_values(self) -> None:
        metrics, _ = calculate_metrics(
            [{
                "id": 1,
                "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                "home_team_id": 10,
                "away_team_id": 20,
                "home_score": 3,
                "away_score": 1,
            }],
            10,
            8,
            None,
        )
        self.assertEqual(metrics["goalsScored"], 3)
        self.assertEqual(metrics["avgGoalsScored"], 3)
        self.assertEqual(metrics["avgTotalGoals"], 4)

    def test_standings_validation_rejects_count_balanced_wrong_identity(self) -> None:
        expected = {
            (8, 28083): {
                100: {
                    "rank": 1,
                    "points": 3,
                    "played": 1,
                    "goals_for": 2,
                    "goals_against": 0,
                    "goal_diff": 2,
                },
            },
        }
        wrong = [{
            "league_id": 8,
            "season_id": 28083,
            "team_id": 999,
            "rank": 1,
            "points": 3,
            "played": 1,
            "goals_for": 2,
            "goals_against": 0,
            "goal_diff": 2,
        }]
        with self.assertRaisesRegex(RuntimeError, "standings identity"):
            validate_standings_rows(expected, wrong)


if __name__ == "__main__":
    unittest.main()
