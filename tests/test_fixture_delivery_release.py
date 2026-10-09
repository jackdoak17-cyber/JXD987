from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from scripts.refresh_fixture_delivery import (
    build_season_scoped_history,
    compute_standings,
    finalize_release,
    iso_date,
    publish_release,
    record_verified_noop,
)


UTC = timezone.utc


class FixtureDeliveryReleaseContractTests(unittest.TestCase):
    def test_verified_noop_writes_health_evidence_but_no_projection_rows(self) -> None:
        cursor = Mock()
        cursor.fetchone.return_value = ("run-id",)
        cursor.rowcount = 1
        report = {"components": {}}
        fingerprint = {
            "version": 1,
            "algorithm_version": "fixture-delivery-v1",
            "sha256": "abc",
            "requested_start": "2026-10-07",
            "requested_end": "2026-11-21",
        }

        record_verified_noop(
            cursor,
            "00000000-0000-4000-8000-000000000001",
            datetime(2026, 10, 7, tzinfo=UTC).date(),
            datetime(2026, 11, 21, tzinfo=UTC).date(),
            1232,
            19000,
            fingerprint,
            report,
        )

        queries = "\n".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        self.assertNotIn("insert into public.fixture_delivery_schedule", queries)
        self.assertNotIn("insert into public.fixture_delivery_standings", queries)
        self.assertNotIn("insert into public.fixture_delivery_metrics", queries)
        self.assertNotIn("insert into public.fixture_delivery_odds", queries)
        self.assertIn("fixture_delivery_refresh_runs", queries)
        self.assertIn("health_checked_at = now()", queries)
        self.assertTrue(report["verified_noop"])
        self.assertEqual(report["projection_rows_written"], 0)
        self.assertTrue(all(component["rows_written"] == 0 for component in report["components"].values()))

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

    def test_retention_failure_does_not_roll_back_published_release(self) -> None:
        connection = Mock()
        cursor = Mock()
        cursor.rowcount = 1

        def execute(query, params=None):
            if "fixture_delivery_gc" in query:
                raise TimeoutError("retention timeout")

        cursor.execute.side_effect = execute
        report = {}

        finalize_release(
            connection,
            cursor,
            "00000000-0000-4000-8000-000000000001",
            {"schedule": 1, "standings": 0, "metrics": 44, "odds": 0},
            datetime(2026, 8, 22, tzinfo=UTC),
            report,
        )

        self.assertEqual(connection.commit.call_count, 1)
        connection.rollback.assert_called_once_with()
        self.assertTrue(report["published"])
        self.assertEqual(report["garbage_collection_status"], "degraded")
        self.assertEqual(report["garbage_collection_error_class"], "TimeoutError")

    def test_successful_retention_runs_after_publication_commit(self) -> None:
        connection = Mock()
        cursor = Mock()
        cursor.rowcount = 1
        cursor.fetchone.return_value = (3,)
        report = {}

        finalize_release(
            connection,
            cursor,
            "00000000-0000-4000-8000-000000000001",
            {"schedule": 1, "standings": 0, "metrics": 44, "odds": 0},
            datetime(2026, 8, 22, tzinfo=UTC),
            report,
        )

        self.assertEqual(connection.commit.call_count, 2)
        connection.rollback.assert_not_called()
        self.assertTrue(report["published"])
        self.assertEqual(report["garbage_collected_releases"], 3)
        self.assertEqual(report["garbage_collection_status"], "succeeded")

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
