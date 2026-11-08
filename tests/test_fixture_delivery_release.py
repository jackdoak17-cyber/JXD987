from __future__ import annotations

import json
import unittest
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch

from scripts import refresh_fixture_delivery as publisher
from scripts.refresh_fixture_delivery import (
    bounded_release_pin_expires_at,
    build_season_scoped_history,
    compute_standings,
    current_resolved_publication_state,
    finalize_release,
    iso_date,
    publish_release,
    publication_skip_reason,
    record_verified_noop,
    validate_session_capable_dsn,
    verify_superseded_core_skip,
)


UTC = timezone.utc


class FixtureDeliveryReleaseContractTests(unittest.TestCase):
    def test_resolver_and_release_evidence_share_one_statement(self) -> None:
        cursor = Mock()
        now = datetime(2026, 11, 8, 16, 0, tzinfo=UTC)
        cursor.fetchone.return_value = (
            "release-id", now, now, now + timedelta(hours=2), {}, "release-id", "current",
        )
        state = current_resolved_publication_state(cursor)
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args.args[0]
        self.assertIn("fixture_delivery_release_resolve(null::uuid)", sql)
        self.assertIn("fixture_delivery_releases", sql)
        self.assertEqual(state["resolver_state"], "current")

    def test_superseded_primary_skip_requires_current_supported_release(self) -> None:
        now = datetime(2026, 11, 8, 16, 0, tzinfo=UTC)
        current = {
            "release_id": "current-release",
            "resolver_release_id": "current-release",
            "resolver_state": "current",
            "published_at": now - timedelta(hours=2),
            "health_checked_at": now,
            "pin_expires_at": now + timedelta(hours=2),
            "metadata": {"last_source_freshness_evidence": {
                "verified_at": (now - timedelta(minutes=5)).isoformat(),
                "max_age_minutes": 540,
            }},
        }
        report = {
            "skipped": True,
            "published": False,
            "skip_reason": "newer_current_source",
            "current_release_id": "current-release",
            "pin_expires_at": (now + timedelta(minutes=90)).isoformat(),
            "source_freshness": {"verified_at": (now - timedelta(hours=7)).isoformat()},
        }
        verify_superseded_core_skip(report, current, now=now)
        for malformed in (
            {**report, "skip_reason": "publication_active"},
            {**report, "skip_reason": "unknown"},
            {**report, "skipped": False},
            {**report, "published": True},
            {**report, "current_release_id": "other-release"},
            {**report, "source_freshness": {}},
        ):
            with self.subTest(malformed=malformed):
                with self.assertRaises((RuntimeError, KeyError)):
                    verify_superseded_core_skip(malformed, current, now=now)
        current["pin_expires_at"] = now + timedelta(minutes=10)
        with self.assertRaisesRegex(RuntimeError, "runway"):
            verify_superseded_core_skip(report, current, now=now)
        current["pin_expires_at"] = now + timedelta(hours=2)
        current["health_checked_at"] = None
        current["resolver_state"] = "stale"
        with self.assertRaisesRegex(RuntimeError, "resolver"):
            verify_superseded_core_skip(report, current, now=now)

    def test_session_lock_rejects_supabase_transaction_pooler(self) -> None:
        validate_session_capable_dsn("postgresql://user:pass@aws-0.pooler.supabase.com:5432/postgres")
        validate_session_capable_dsn("postgresql://user:pass@db.example.com:5432/postgres")
        with self.assertRaisesRegex(RuntimeError, "session-pooler"):
            validate_session_capable_dsn("postgresql://user:pass@aws-0.pooler.supabase.com:6543/postgres")
        with self.assertRaisesRegex(RuntimeError, "session-pooler"):
            validate_session_capable_dsn("postgresql://user:pass@custom-pooler.example.com:6543/postgres")
        for dsn in (
            "postgresql://user:pass@aws-0.pooler.supabase.com:5432/postgres?port=6543",
            "postgresql:///postgres?host=aws-0.pooler.supabase.com&port=6543",
            "host=aws-0.pooler.supabase.com port=6543 dbname=postgres",
        ):
            with self.subTest(dsn=dsn):
                with self.assertRaisesRegex(RuntimeError, "session-pooler"):
                    validate_session_capable_dsn(dsn)
        with patch.dict("os.environ", {"PGPORT": "6543"}):
            with self.assertRaisesRegex(RuntimeError, "session-pooler"):
                validate_session_capable_dsn("postgresql://user:pass@aws-0.pooler.supabase.com/postgres")

    def test_older_fallback_cannot_shorten_primary_noop_before_heartbeat(self) -> None:
        now = datetime(2026, 11, 8, 16, 0, tzinfo=UTC)
        current = {
            "release_id": "primary-release",
            "published_at": now - timedelta(hours=2),
            "health_checked_at": now,
            "pin_expires_at": now + timedelta(hours=2),
            "metadata": {"last_source_freshness_evidence": {
                "verified_at": (now - timedelta(minutes=27)).isoformat(),
            }},
        }
        fallback = {"verified_at": (now - timedelta(minutes=450)).isoformat()}
        self.assertEqual(
            publication_skip_reason(
                current, fallback, now + timedelta(minutes=90),
                guarded=True, min_interval_seconds=3600, now=now,
            ),
            "newer_current_source",
        )

    def test_recent_publication_with_short_pin_allows_fresh_recovery(self) -> None:
        now = datetime(2026, 11, 8, 16, 0, tzinfo=UTC)
        current = {
            "release_id": "short-pin-release",
            "published_at": now - timedelta(minutes=20),
            "health_checked_at": now - timedelta(minutes=20),
            "pin_expires_at": now + timedelta(minutes=10),
            "metadata": {"last_source_freshness_evidence": {
                "verified_at": (now - timedelta(hours=8)).isoformat(),
            }},
        }
        fresh = {"verified_at": (now - timedelta(minutes=2)).isoformat()}
        self.assertIsNone(publication_skip_reason(
            current, fresh, now + timedelta(hours=2),
            guarded=True, min_interval_seconds=3600, now=now,
        ))
        current["pin_expires_at"] = now + timedelta(hours=2)
        self.assertEqual(publication_skip_reason(
            current, fresh, now + timedelta(hours=2),
            guarded=True, min_interval_seconds=3600, now=now,
        ), "recent_publication_satisfies_guard")

    def test_current_full_publication_evidence_blocks_older_candidate(self) -> None:
        now = datetime(2026, 11, 8, 16, 0, tzinfo=UTC)
        current = {
            "release_id": "new-full-release",
            "published_at": now - timedelta(hours=2),
            "health_checked_at": now - timedelta(hours=2),
            "pin_expires_at": now + timedelta(hours=2),
            "metadata": {"last_source_freshness_evidence": {
                "verified_at": (now - timedelta(minutes=27)).isoformat(),
            }},
        }
        self.assertEqual(publication_skip_reason(
            current, {"verified_at": (now - timedelta(hours=7)).isoformat()},
            now + timedelta(minutes=90), guarded=True,
            min_interval_seconds=3600, now=now,
        ), "newer_current_source")

    def test_guard_reads_committed_state_after_session_lock(self) -> None:
        now = datetime.now(UTC)
        conn = MagicMock()
        cursor = Mock()
        conn.cursor.return_value.__enter__.return_value = cursor
        stale_after_lock = {
            "release_id": "just-renewed",
            "published_at": now - timedelta(hours=2),
            "health_checked_at": now,
            "pin_expires_at": now + timedelta(hours=2),
            "metadata": {"last_source_freshness_evidence": {
                "verified_at": (now - timedelta(minutes=5)).isoformat(),
            }},
        }
        def committed_state(_cur):
            self.assertEqual(conn.commit.call_count, 1)
            return stale_after_lock
        with (
            patch.object(publisher, "connection", return_value=conn),
            patch.object(publisher, "try_lock_refresh_publication", return_value=True),
            patch.object(publisher, "validate_delivery_schema"),
            patch.object(publisher, "verify_fixture_source_freshness", return_value={
                "verified_at": (now - timedelta(hours=7)).isoformat(),
            }),
            patch.object(publisher, "bounded_release_pin_expires_at", return_value=now + timedelta(minutes=90)),
            patch.object(publisher, "current_publication_guard_state", side_effect=committed_state),
            patch.object(publisher, "source_fixtures") as source_rows,
        ):
            args = publisher.build_parser().parse_args(["--skip-if-publication-active"])
            report = publisher.run(args)
        self.assertEqual(report["skip_reason"], "newer_current_source")
        source_rows.assert_not_called()
        conn.close.assert_called_once_with()

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
        source_freshness = {
            "kind": "fixture_core_pipeline_job",
            "job_id": "run_p3_fixture_core_export",
            "verified_at": "2026-10-07T12:00:00+00:00",
            "age_seconds": 60,
            "max_age_minutes": 540,
        }
        pin_expires_at = datetime(2026, 10, 7, 14, 1, tzinfo=UTC)

        record_verified_noop(
            cursor,
            "00000000-0000-4000-8000-000000000001",
            datetime(2026, 10, 7, tzinfo=UTC).date(),
            datetime(2026, 11, 21, tzinfo=UTC).date(),
            1232,
            19000,
            fingerprint,
            source_freshness,
            {"schedule": 10, "standings": 20, "metrics": 880, "odds": 30},
            pin_expires_at,
            report,
        )

        queries = "\n".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        self.assertNotIn("insert into public.fixture_delivery_schedule", queries)
        self.assertNotIn("insert into public.fixture_delivery_standings", queries)
        self.assertNotIn("insert into public.fixture_delivery_metrics", queries)
        self.assertNotIn("insert into public.fixture_delivery_odds", queries)
        self.assertIn("fixture_delivery_refresh_runs", queries)
        self.assertIn("health_checked_at = now()", queries)
        self.assertIn("pin_expires_at = %s", queries)
        self.assertTrue(report["verified_noop"])
        self.assertEqual(report["pin_expires_at"], pin_expires_at.isoformat())
        self.assertEqual(report["projection_rows_written"], 0)
        self.assertEqual(report["projection_rows_verified"]["schedule"], 10)
        self.assertTrue(all(component["rows_written"] == 0 for component in report["components"].values()))

    def test_release_lease_is_bounded_by_source_freshness(self) -> None:
        freshness = {
            "verified_at": "2026-10-07T00:00:00+00:00",
            "max_age_minutes": 540,
        }

        self.assertEqual(
            bounded_release_pin_expires_at(
                freshness,
                now=datetime(2026, 10, 7, 8, 30, tzinfo=UTC),
            ),
            datetime(2026, 10, 7, 9, 0, tzinfo=UTC),
        )

    def test_release_lease_uses_two_hours_when_source_window_is_longer(self) -> None:
        freshness = {
            "verified_at": "2026-10-07T00:00:00+00:00",
            "max_age_minutes": 540,
        }

        self.assertEqual(
            bounded_release_pin_expires_at(
                freshness,
                now=datetime(2026, 10, 7, 1, 0, tzinfo=UTC),
            ),
            datetime(2026, 10, 7, 3, 0, tzinfo=UTC),
        )

    def test_expired_source_cannot_bound_a_release_lease(self) -> None:
        freshness = {
            "verified_at": "2026-10-07T00:00:00+00:00",
            "max_age_minutes": 540,
        }

        with self.assertRaisesRegex(RuntimeError, "expired"):
            bounded_release_pin_expires_at(
                freshness,
                now=datetime(2026, 10, 7, 9, 0, tzinfo=UTC),
            )

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

    def test_new_release_persists_selected_source_provenance(self) -> None:
        cursor = Mock()
        cursor.rowcount = 1
        evidence = {
            "kind": "fixture_core_pipeline_job",
            "job_id": "run_fixture_core_fallback",
            "verified_at": "2026-11-08T07:56:25+00:00",
            "age_seconds": 18900,
            "max_age_minutes": 540,
        }
        publish_release(
            cursor,
            "00000000-0000-4000-8000-000000000001",
            {"schedule": 1, "standings": 0, "metrics": 44, "odds": 0},
            datetime(2026, 11, 8, tzinfo=UTC),
            semantic_fingerprint={"sha256": "abc"},
            source_freshness=evidence,
        )
        metadata = json.loads(cursor.execute.call_args_list[0].args[1][-2])
        self.assertEqual(metadata["last_source_freshness_evidence"], evidence)
        self.assertEqual(metadata["semantic_projection_fingerprint"], {"sha256": "abc"})

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
