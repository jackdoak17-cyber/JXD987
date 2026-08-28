from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.refresh_fixture_delivery import (
    add_metrics_provenance,
    build_season_scoped_history,
    calculate_metrics,
    compute_standings,
)


UTC = timezone.utc


class FixtureDeliveryMetricsTests(unittest.TestCase):
    def test_history_is_scoped_to_league_and_season(self) -> None:
        fixture_time = datetime(2026, 8, 28, tzinfo=UTC)
        history = build_season_scoped_history(
            [
                {
                    "id": 300,
                    "starting_at": datetime(2026, 8, 1, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": 28083,
                    "home_team_id": 10,
                    "away_team_id": 20,
                    "home_score": 1,
                    "away_score": 0,
                },
                {
                    "id": 200,
                    "starting_at": datetime(2026, 8, 15, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": 25583,
                    "home_team_id": 10,
                    "away_team_id": 30,
                    "home_score": 4,
                    "away_score": 0,
                },
                {
                    "id": 100,
                    "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                    "league_id": 384,
                    "season_id": 28083,
                    "home_team_id": 10,
                    "away_team_id": 40,
                    "home_score": 5,
                    "away_score": 0,
                },
            ]
        )

        current_season_history = [
            row
            for row in history[(8, 28083, 10)]
            if row["starting_at"] < fixture_time
        ]
        metrics, _ = calculate_metrics(current_season_history, 10, 8, None)

        self.assertEqual([row["id"] for row in current_season_history], [300])
        self.assertEqual(metrics["sample"], 1)
        self.assertEqual(metrics["goalsScored"], 1)

    def test_empty_bucket_is_explicitly_marked_none(self) -> None:
        metrics, source = calculate_metrics([], 10, 8, "home")
        self.assertIsNone(source)
        add_metrics_provenance(metrics, "venue")
        self.assertEqual(metrics["sample"], 0)
        self.assertEqual(metrics["metricsSource"], "venue")
        self.assertEqual(metrics["sampleStatus"], "none")

    def test_partial_venue_and_overall_buckets_keep_distinct_provenance(self) -> None:
        history = [
            {
                "id": 2,
                "starting_at": datetime(2026, 8, 15, tzinfo=UTC),
                "home_team_id": 10,
                "away_team_id": 20,
                "home_score": 1,
                "away_score": 0,
            },
            {
                "id": 1,
                "starting_at": datetime(2026, 8, 8, tzinfo=UTC),
                "home_team_id": 30,
                "away_team_id": 10,
                "home_score": 2,
                "away_score": 2,
            },
        ]

        overall, _ = calculate_metrics(history, 10, 8, None)
        venue, _ = calculate_metrics(history, 10, 8, "home")
        add_metrics_provenance(overall, "overall")
        add_metrics_provenance(venue, "venue")

        self.assertEqual(overall["sample"], 2)
        self.assertEqual(overall["sampleStatus"], "partial")
        self.assertEqual(venue["sample"], 1)
        self.assertEqual(venue["sampleStatus"], "partial")
        self.assertEqual(overall["metricsSource"], "overall")
        self.assertEqual(venue["metricsSource"], "venue")

    def test_standings_equal_totals_preserve_strict_row_order_tie_break(self) -> None:
        standings = compute_standings(
            [
                {
                    "id": 2,
                    "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": 28083,
                    "home_team_id": 20,
                    "away_team_id": 21,
                    "home_score": 1,
                    "away_score": 1,
                },
                {
                    "id": 1,
                    "starting_at": datetime(2026, 8, 15, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": 28083,
                    "home_team_id": 10,
                    "away_team_id": 11,
                    "home_score": 1,
                    "away_score": 1,
                },
            ]
        )

        ranked = standings[(8, 28083)]

        self.assertEqual(ranked[20]["rank"], 1)
        self.assertEqual(ranked[21]["rank"], 2)
        self.assertEqual(ranked[10]["rank"], 3)
        self.assertEqual(ranked[11]["rank"], 4)


if __name__ == "__main__":
    unittest.main()
