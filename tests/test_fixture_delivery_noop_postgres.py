from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import Json, execute_values

from scripts.refresh_fixture_delivery import (
    METRIC_MODES,
    METRIC_SEASON_SCOPES,
    METRIC_WINDOWS,
    record_verified_noop,
    validate_release_components,
    validate_schedule_projection,
    verify_fixture_source_freshness,
)


DATABASE_URL = os.environ.get("AUDIT04_FIXTURE_POSTGRES_URL")
UTC = timezone.utc
RELEASE_ID = "00000000-0000-4000-8000-000000000051"


@unittest.skipUnless(
    DATABASE_URL,
    "AUDIT04_FIXTURE_POSTGRES_URL is required for the isolated PostgreSQL replay",
)
class FixtureDeliveryNoopPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = psycopg2.connect(DATABASE_URL)
        cls.conn.autocommit = True
        with cls.conn.cursor() as cur:
            cur.execute("select current_database()")
            database = str(cur.fetchone()[0])
            if not database.startswith("audit04_fixture_"):
                raise RuntimeError(
                    "refusing destructive fixture replay outside an audit04_fixture_ database"
                )
            cur.execute(
                """
                create schema operations;
                create table operations.pipeline_job_runs (
                  job_id text not null,
                  status text not null,
                  finished_at timestamptz not null
                );
                create table public.fixture_delivery_releases (
                  id uuid primary key,
                  status text not null,
                  health_checked_at timestamptz,
                  pin_expires_at timestamptz not null,
                  metadata jsonb not null default '{}'::jsonb
                );
                create table public.fixture_delivery_current_publication (
                  publication_key text primary key,
                  release_id uuid not null
                );
                create table public.fixture_delivery_refresh_runs (
                  id bigint generated always as identity primary key,
                  component text not null,
                  requested_start date not null,
                  requested_end date not null,
                  release_id uuid,
                  completed_at timestamptz,
                  status text,
                  rows_read integer,
                  rows_written integer,
                  rows_rejected integer,
                  rows_missing integer,
                  rejected_cup_rows integer,
                  source_watermark timestamptz,
                  error_message text,
                  metadata jsonb
                );
                create table public.fixture_delivery_schedule (
                  release_id uuid not null,
                  fixture_id bigint not null,
                  home_team_id bigint not null,
                  away_team_id bigint not null,
                  status text,
                  status_code text,
                  home_score integer,
                  away_score integer,
                  primary key (release_id, fixture_id)
                );
                create table public.fixture_delivery_standings (
                  release_id uuid not null,
                  league_id bigint not null,
                  season_id bigint not null,
                  team_id bigint not null,
                  rank integer not null,
                  points integer not null,
                  played integer not null,
                  goals_for integer not null,
                  goals_against integer not null,
                  goal_diff integer not null,
                  primary key (release_id, league_id, season_id, team_id)
                );
                create table public.fixture_delivery_metrics (
                  release_id uuid not null,
                  fixture_id bigint not null,
                  team_id bigint not null,
                  side text not null,
                  metrics_window integer not null,
                  metrics_mode text not null,
                  season_scope text not null,
                  primary key (
                    release_id, fixture_id, team_id, side,
                    metrics_window, metrics_mode, season_scope
                  )
                );
                create table public.fixture_delivery_odds (
                  release_id uuid not null,
                  fixture_id bigint not null
                );
                """
            )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.conn.close()

    def setUp(self) -> None:
        fingerprint = {
            "version": 1,
            "algorithm_version": "fixture-delivery-v1",
            "sha256": "verified-semantic-snapshot",
            "requested_start": "2026-10-20",
            "requested_end": "2026-12-02",
        }
        with self.conn.cursor() as cur:
            cur.execute(
                """
                truncate operations.pipeline_job_runs,
                         public.fixture_delivery_refresh_runs,
                         public.fixture_delivery_odds,
                         public.fixture_delivery_metrics,
                         public.fixture_delivery_standings,
                         public.fixture_delivery_schedule,
                         public.fixture_delivery_current_publication,
                         public.fixture_delivery_releases restart identity;
                insert into operations.pipeline_job_runs values
                  ('run_p3_fixture_core_export', 'success', now() - interval '5 minutes');
                insert into public.fixture_delivery_releases
                  (id, status, health_checked_at, pin_expires_at, metadata)
                values (%s, 'published', now() - interval '3 hours',
                        now() - interval '1 hour',
                        jsonb_build_object('semantic_projection_fingerprint', %s::jsonb));
                insert into public.fixture_delivery_current_publication
                values ('fixtures', %s);
                insert into public.fixture_delivery_schedule values
                  (%s, 1001, 10, 20, 'NS', 'NS', null, null);
                insert into public.fixture_delivery_standings values
                  (%s, 8, 28083, 10, 1, 3, 1, 2, 0, 2),
                  (%s, 8, 28083, 20, 2, 0, 1, 0, 2, -2);
                """,
                (RELEASE_ID, Json(fingerprint), RELEASE_ID, RELEASE_ID, RELEASE_ID, RELEASE_ID),
            )
            metric_rows = [
                (RELEASE_ID, 1001, 10 if side == "home" else 20, side, window, mode, scope)
                for side in ("home", "away")
                for window in METRIC_WINDOWS
                for mode in METRIC_MODES
                for scope in METRIC_SEASON_SCOPES
            ]
            execute_values(
                cur,
                "insert into public.fixture_delivery_metrics values %s",
                metric_rows,
            )

    def test_verified_noop_renews_same_complete_release_without_projection_writes(self) -> None:
        source = [{"id": 1001, "status": "NS", "status_code": "NS", "home_score": None, "away_score": None}]
        expected_standings = {
            (8, 28083): {
                10: {"rank": 1, "points": 3, "played": 1, "goals_for": 2, "goals_against": 0, "goal_diff": 2},
                20: {"rank": 2, "points": 0, "played": 1, "goals_for": 0, "goals_against": 2, "goal_diff": -2},
            }
        }
        report = {"components": {}}
        fingerprint = {
            "version": 1,
            "algorithm_version": "fixture-delivery-v1",
            "sha256": "verified-semantic-snapshot",
            "requested_start": "2026-10-20",
            "requested_end": "2026-12-02",
        }
        with self.conn.cursor() as cur:
            freshness = verify_fixture_source_freshness(cur, 540)
            validate_schedule_projection(cur, source, RELEASE_ID)
            counts = validate_release_components(
                cur,
                RELEASE_ID,
                [1001],
                expected_standings,
            )
            record_verified_noop(
                cur,
                RELEASE_ID,
                date(2026, 10, 20),
                date(2026, 12, 2),
                1,
                1,
                fingerprint,
                freshness,
                counts,
                report,
            )
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select p.release_id::text, r.pin_expires_at > now(),
                       r.metadata->'semantic_projection_fingerprint',
                       count(*) filter (where rr.rows_written = 0),
                       count(*)
                  from public.fixture_delivery_current_publication p
                  join public.fixture_delivery_releases r on r.id = p.release_id
                  join public.fixture_delivery_refresh_runs rr on rr.release_id = r.id
                 where p.publication_key = 'fixtures'
                 group by p.release_id, r.pin_expires_at, r.metadata
                """
            )
            release_id, lease_current, stored_fingerprint, zero_writes, run_count = cur.fetchone()
        self.assertEqual(release_id, RELEASE_ID)
        self.assertTrue(lease_current)
        self.assertEqual(stored_fingerprint, fingerprint)
        self.assertEqual((zero_writes, run_count), (4, 4))
        self.assertTrue(report["verified_noop"])
        self.assertEqual(report["projection_rows_written"], 0)
        self.assertEqual(report["projection_rows_verified"]["metrics"], 88)

    def test_stale_source_cannot_renew_the_release(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "update operations.pipeline_job_runs set finished_at = now() - interval '541 minutes'"
            )
            cur.execute(
                "select pin_expires_at from public.fixture_delivery_releases where id = %s",
                (RELEASE_ID,),
            )
            before = cur.fetchone()[0]
            with self.assertRaisesRegex(RuntimeError, "stale"):
                verify_fixture_source_freshness(cur, 540)
            cur.execute(
                "select pin_expires_at from public.fixture_delivery_releases where id = %s",
                (RELEASE_ID,),
            )
            after = cur.fetchone()[0]
        self.assertEqual(after, before)

    def test_incomplete_projection_is_not_eligible_for_renewal(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "delete from public.fixture_delivery_metrics where ctid in "
                "(select ctid from public.fixture_delivery_metrics limit 1)"
            )
            with self.assertRaisesRegex(RuntimeError, "metrics identity mismatch"):
                validate_release_components(cur, RELEASE_ID, [1001])


if __name__ == "__main__":
    unittest.main()
