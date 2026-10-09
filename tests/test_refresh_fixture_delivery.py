from __future__ import annotations

import unittest
from copy import deepcopy
from datetime import date, datetime, timezone
import inspect

from scripts.refresh_fixture_delivery import (
    COMPLETED_SEMANTIC_FIELDS,
    SCHEDULE_SEMANTIC_FIELDS,
    add_metrics_provenance,
    all_completed_fixtures,
    build_season_scoped_history,
    calculate_metrics,
    compute_standings,
    fingerprints_match,
    history_rows_for_fixture,
    semantic_projection_fingerprint,
    strict_current_season_rank,
    validate_delivery_schema,
    validate_source_fixture_identity,
)


UTC = timezone.utc


class SchemaCursor:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.params = None

    def execute(self, query, params=None):
        self.query = query
        self.params = params

    def fetchall(self):
        return self.rows


class FixtureDeliveryMetricsTests(unittest.TestCase):
    @staticmethod
    def changed_value(value):
        if isinstance(value, datetime):
            return value.replace(minute=(value.minute + 1) % 60)
        if isinstance(value, int):
            return value + 1
        if value is None:
            return "now-present"
        return f"{value}-changed"

    def semantic_fixture(self, fixture_id: int = 1) -> dict:
        return {
            "id": fixture_id,
            "starting_at": datetime(2026, 10, 9, 14, 0, tzinfo=UTC),
            "status": "NS",
            "status_code": "NS",
            "league_id": 8,
            "season_id": 28083,
            "home_team_id": 10,
            "away_team_id": 20,
            "home_score": None,
            "away_score": None,
            "home_ht_score": None,
            "away_ht_score": None,
            "league_name": "Premier League",
            "league_logo": "league.png",
            "home_team_name": "Home",
            "home_team_short_code": "HOM",
            "home_team_image_path": "home.png",
            "away_team_name": "Away",
            "away_team_short_code": "AWY",
            "away_team_image_path": "away.png",
        }

    def completed_fixture(self, fixture_id: int = 99) -> dict:
        row = self.semantic_fixture(fixture_id)
        row.update({
            "starting_at": datetime(2026, 10, 1, 14, 0, tzinfo=UTC),
            "status": "FT",
            "status_code": "FT",
            "home_score": 2,
            "away_score": 1,
            "home_ht_score": 1,
            "away_ht_score": 0,
        })
        return row

    def fingerprint(self, schedule=None, completed=None, **kwargs):
        return semantic_projection_fingerprint(
            schedule if schedule is not None else [self.semantic_fixture()],
            completed if completed is not None else [self.completed_fixture()],
            kwargs.pop("start", date(2026, 10, 7)),
            kwargs.pop("end", date(2026, 11, 21)),
            kwargs.pop("leagues", [8]),
            **kwargs,
        )

    def test_identical_semantic_snapshots_reuse_projection(self) -> None:
        first = self.fingerprint()
        second = self.fingerprint()
        self.assertEqual(first, second)
        self.assertTrue(fingerprints_match(first, second))

    def test_real_score_and_historical_correction_force_publication(self) -> None:
        baseline = self.fingerprint()
        changed_schedule = self.semantic_fixture()
        changed_schedule.update({"status": "FT", "status_code": "FT", "home_score": 1, "away_score": 0})
        corrected_history = self.completed_fixture()
        corrected_history["away_score"] = 2

        self.assertNotEqual(baseline["sha256"], self.fingerprint(schedule=[changed_schedule])["sha256"])
        self.assertNotEqual(baseline["sha256"], self.fingerprint(completed=[corrected_history])["sha256"])

    def test_cancelled_postponed_or_rescheduled_fixture_forces_publication(self) -> None:
        baseline = self.fingerprint()
        for status in ("CANCELLED", "POSTPONED"):
            changed = self.semantic_fixture()
            changed["status"] = status
            self.assertNotEqual(baseline["sha256"], self.fingerprint(schedule=[changed])["sha256"])
        rescheduled = self.semantic_fixture()
        rescheduled["starting_at"] = datetime(2026, 10, 10, 14, 0, tzinfo=UTC)
        self.assertNotEqual(baseline["sha256"], self.fingerprint(schedule=[rescheduled])["sha256"])

    def test_midnight_window_and_algorithm_changes_force_publication(self) -> None:
        baseline = self.fingerprint()
        next_window = self.fingerprint(start=date(2026, 10, 8), end=date(2026, 11, 22))
        next_algorithm = self.fingerprint(algorithm_version="fixture-delivery-v2")

        self.assertFalse(fingerprints_match(baseline, next_window))
        self.assertFalse(fingerprints_match(baseline, next_algorithm))

    def test_missing_or_incompatible_fingerprint_forces_publication(self) -> None:
        candidate = self.fingerprint()
        self.assertFalse(fingerprints_match(None, candidate))
        incompatible = deepcopy(candidate)
        incompatible["version"] += 1
        self.assertFalse(fingerprints_match(incompatible, candidate))

    def test_every_schedule_semantic_field_is_fingerprinted(self) -> None:
        baseline = self.fingerprint()
        for field in SCHEDULE_SEMANTIC_FIELDS:
            changed = self.semantic_fixture()
            changed[field] = self.changed_value(changed[field])
            with self.subTest(field=field):
                self.assertNotEqual(
                    baseline["sha256"],
                    self.fingerprint(schedule=[changed])["sha256"],
                )

    def test_every_completed_semantic_field_is_fingerprinted(self) -> None:
        baseline = self.fingerprint()
        for field in COMPLETED_SEMANTIC_FIELDS:
            changed = self.completed_fixture()
            changed[field] = self.changed_value(changed[field])
            with self.subTest(field=field):
                self.assertNotEqual(
                    baseline["sha256"],
                    self.fingerprint(completed=[changed])["sha256"],
                )

    def test_delivery_schema_contract_accepts_live_primary_keys(self) -> None:
        cursor = SchemaCursor([
            ("fixture_delivery_schedule", ["release_id", "fixture_id"]),
            ("fixture_delivery_standings", ["release_id", "league_id", "season_id", "team_id"]),
            ("fixture_delivery_metrics", [
                "release_id", "fixture_id", "team_id", "side", "metrics_window",
                "metrics_mode", "season_scope",
            ]),
            ("fixture_delivery_odds", [
                "release_id", "fixture_id", "bookmaker_id", "market_key", "selection_key",
                "participant_type", "participant_id", "line_key",
            ]),
        ])

        validate_delivery_schema(cursor)

    def test_delivery_schema_contract_rejects_old_metrics_key(self) -> None:
        cursor = SchemaCursor([
            ("fixture_delivery_schedule", ["release_id", "fixture_id"]),
            ("fixture_delivery_standings", ["release_id", "league_id", "season_id", "team_id"]),
            ("fixture_delivery_metrics", [
                "release_id", "fixture_id", "team_id", "side", "metrics_window", "metrics_mode",
            ]),
            ("fixture_delivery_odds", [
                "release_id", "fixture_id", "bookmaker_id", "market_key", "selection_key",
                "participant_type", "participant_id", "line_key",
            ]),
        ])

        with self.assertRaisesRegex(RuntimeError, "fixture_delivery_metrics"):
            validate_delivery_schema(cursor)

    def test_duplicate_source_fixture_ids_fail_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "duplicate fixture ids: 42"):
            validate_source_fixture_identity([{"id": 42}, {"id": 42}])

    def test_goal_aggregates_are_counted_once(self) -> None:
        metrics, _ = calculate_metrics(
            [
                {
                    "id": 1,
                    "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                    "home_team_id": 10,
                    "away_team_id": 20,
                    "home_score": 3,
                    "away_score": 1,
                }
            ],
            10,
            8,
            None,
        )

        self.assertEqual(metrics["goalsScored"], 3)
        self.assertEqual(metrics["goalsConceded"], 1)
        self.assertEqual(metrics["avgGoalsScored"], 3)
        self.assertEqual(metrics["avgGoalsConceded"], 1)
        self.assertEqual(metrics["avgTotalGoals"], 4)

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

    def test_strict_current_season_rank_does_not_fallback_to_prior_season(self) -> None:
        standings = compute_standings(
            [
                {
                    "id": 1,
                    "starting_at": datetime(2026, 8, 15, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": 28083,
                    "home_team_id": 20,
                    "away_team_id": 21,
                    "home_score": 1,
                    "away_score": 0,
                }
            ]
        )

        self.assertEqual(strict_current_season_rank(standings, 8, 28083, 20), 1)
        self.assertIsNone(strict_current_season_rank(standings, 8, 28083, 10))

    def test_completed_fixture_query_matches_strict_oracle_order(self) -> None:
        source = inspect.getsource(all_completed_fixtures)

        self.assertIn("order by starting_at desc", source)
        self.assertNotIn("order by starting_at desc, id desc", source)

    def test_history_preserves_source_order_for_equal_kickoff_times(self) -> None:
        history = build_season_scoped_history(
            [
                {
                    "id": 1,
                    "starting_at": datetime(2026, 8, 15, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": 28083,
                    "home_team_id": 10,
                    "away_team_id": 20,
                    "home_score": 1,
                    "away_score": 0,
                },
                {
                    "id": 2,
                    "starting_at": datetime(2026, 8, 15, tzinfo=UTC),
                    "league_id": 8,
                    "season_id": 28083,
                    "home_team_id": 10,
                    "away_team_id": 30,
                    "home_score": 0,
                    "away_score": 1,
                },
            ]
        )

        self.assertEqual([row["id"] for row in history[(8, 28083, 10)]], [1, 2])

    def test_finished_fixture_history_includes_target_but_upcoming_history_does_not(self) -> None:
        history = [
            {
                "id": 2,
                "starting_at": datetime(2026, 8, 28, tzinfo=UTC),
                "home_team_id": 10,
                "away_team_id": 20,
                "home_score": 2,
                "away_score": 0,
            },
            {
                "id": 1,
                "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                "home_team_id": 10,
                "away_team_id": 30,
                "home_score": 1,
                "away_score": 0,
            },
        ]
        finished_fixture = {
            "id": 2,
            "starting_at": datetime(2026, 8, 28, tzinfo=UTC),
            "status": "FT",
            "status_code": "FT",
            "home_score": 2,
            "away_score": 0,
        }
        upcoming_fixture = {
            "id": 2,
            "starting_at": datetime(2026, 8, 28, tzinfo=UTC),
            "status": "NS",
            "status_code": "NS",
            "home_score": None,
            "away_score": None,
        }

        self.assertEqual(
            [row["id"] for row in history_rows_for_fixture(history, finished_fixture)],
            [2, 1],
        )
        self.assertEqual(
            [row["id"] for row in history_rows_for_fixture(history[1:], upcoming_fixture)],
            [1],
        )


if __name__ == "__main__":
    unittest.main()
