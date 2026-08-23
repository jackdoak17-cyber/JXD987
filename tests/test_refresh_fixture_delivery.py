from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.refresh_fixture_delivery import (
    EXCLUDED_CUPS,
    add_metrics_provenance,
    calculate_metrics,
    classify_source_fixtures,
)


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


class FixtureDeliveryCompletenessTests(unittest.TestCase):
    def _row(self, **overrides: object) -> dict[str, object]:
        row = {
            "id": 123,
            "starting_at": datetime(2026, 8, 25, tzinfo=UTC),
            "league_id": 8,
            "status": "NS",
            "status_code": None,
            "home_team_id": 10,
            "away_team_id": 20,
            "league_name": "Premier League",
            "home_team_name": "Home",
            "away_team_name": "Away",
        }
        row.update(overrides)
        return row

    def test_incomplete_non_cup_rows_are_reported_instead_of_dropped(self) -> None:
        valid, cups, hidden, incomplete = classify_source_fixtures(
            [self._row(), self._row(id=124, home_team_name=None)]
        )
        self.assertEqual([row["id"] for row in valid], [123])
        self.assertEqual(cups, 0)
        self.assertEqual(hidden, 0)
        self.assertEqual(incomplete, [{"fixture_id": 124, "missing": ["home_team_name"]}])

    def test_cups_and_hidden_statuses_are_not_completeness_failures(self) -> None:
        cup = self._row(id=125, league_id=next(iter(EXCLUDED_CUPS)))
        postponed = self._row(id=126, status="POSTPONED")
        valid, cups, hidden, incomplete = classify_source_fixtures([cup, postponed])
        self.assertEqual(valid, [])
        self.assertEqual(cups, 1)
        self.assertEqual(hidden, 1)
        self.assertEqual(incomplete, [])


if __name__ == "__main__":
    unittest.main()
