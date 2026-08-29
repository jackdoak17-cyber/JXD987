from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from scripts.refresh_fixture_delivery import (
    build_season_scoped_history,
    compute_standings,
    iso_date,
    publish_release,
)


UTC = timezone.utc


class FixtureDeliveryReleaseContractTests(unittest.TestCase):
    def test_publish_release_switches_pointer_only_after_build_is_marked_published(self) -> None:
        cursor = Mock()
        cursor.rowcount = 1

        publish_release(
            cursor,
            "00000000-0000-4000-8000-000000000001",
            {"schedule": 1, "standings": 0, "metrics": 44, "odds": 0},
            datetime(2026, 8, 22, tzinfo=UTC),
        )

        self.assertEqual(cursor.execute.call_count, 2)
        queries = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertIn("fixture_delivery_releases", queries[0])
        self.assertIn("fixture_delivery_current_publication", queries[1])

    def test_fixture_date_is_derived_in_europe_london(self) -> None:
        # 23:30 UTC is already the next calendar day in London during BST.
        self.assertEqual(
            iso_date(datetime(2026, 8, 22, 23, 30, tzinfo=UTC)),
            "2026-08-23",
        )

    def test_null_season_rows_are_not_used_as_season_scoped_history(self) -> None:
        history = build_season_scoped_history(
            [
                {
                    "id": 1,
                    "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": None,
                    "home_team_id": 10,
                    "away_team_id": 20,
                    "home_score": 1,
                    "away_score": 0,
                }
            ]
        )
        self.assertEqual(history, {})

    def test_null_season_rows_do_not_crash_standings_or_create_a_rank(self) -> None:
        standings = compute_standings(
            [
                {
                    "id": 1,
                    "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": None,
                    "home_team_id": 10,
                    "away_team_id": 20,
                    "home_score": 1,
                    "away_score": 0,
                }
            ]
        )
        self.assertEqual(standings, {})


if __name__ == "__main__":
    unittest.main()
