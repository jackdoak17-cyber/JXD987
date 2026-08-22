from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.refresh_fixture_delivery import add_metrics_provenance, calculate_metrics


UTC = timezone.utc


class FixtureDeliveryMetricsTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
