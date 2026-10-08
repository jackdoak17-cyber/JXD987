from __future__ import annotations

import os
import unittest

import psycopg2

from scripts.hydrate_referee_history import upsert_assignments
from scripts.sync_fixture_referee_stats import (
    fetch_fixture_referee_metrics,
    upsert_fixture_referee_stats,
)


DATABASE_URL = os.environ.get("ROOT2_POSTGRES_URL")


@unittest.skipUnless(DATABASE_URL, "ROOT2_POSTGRES_URL is required for PostgreSQL replay tests")
class RefereeRefreshPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = psycopg2.connect(DATABASE_URL)
        cls.conn.autocommit = True
        with cls.conn.cursor() as cur:
            cur.execute(
                """
                drop table if exists public.fixture_referee_stats;
                drop table if exists public.fixture_statistics;
                drop table if exists public.fixture_referees;
                drop table if exists public.fixtures;
                drop table if exists public.referees;

                create table public.referees (
                  id bigint primary key,
                  name text not null
                );
                create table public.fixtures (
                  id bigint primary key,
                  starting_at timestamptz not null,
                  status text,
                  home_score integer,
                  away_score integer
                );
                create table public.fixture_referees (
                  fixture_id bigint not null,
                  referee_id bigint not null,
                  role text not null,
                  is_primary boolean not null default false,
                  source text,
                  extra jsonb,
                  updated_at timestamptz not null default now(),
                  last_synced_at timestamptz not null default now(),
                  unique (fixture_id, referee_id, role)
                );
                create table public.fixture_statistics (
                  fixture_id bigint not null,
                  type_id integer not null,
                  value numeric
                );
                create table public.fixture_referee_stats (
                  fixture_id bigint primary key,
                  referee_id bigint not null,
                  referee_name text not null,
                  avg_yellow_cards float8,
                  avg_fouls float8,
                  games_with_3plus_cards_pct float8,
                  games_with_red_card_pct float8,
                  avg_corners float8,
                  sample integer,
                  source text,
                  avg_total_cards float8,
                  games_with_4plus_cards_pct float8,
                  games_with_5plus_cards_pct float8,
                  sample_5 integer,
                  sample_10 integer,
                  sample_20 integer,
                  history_through timestamptz,
                  calculation_version text,
                  data_status text,
                  windows jsonb,
                  updated_at timestamptz not null default now()
                );
                """
            )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.conn.close()

    def setUp(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                drop trigger if exists fail_referee_stats_write on public.fixture_referee_stats;
                drop function if exists public.fail_referee_stats_write();
                truncate public.fixture_referee_stats, public.fixture_statistics,
                         public.fixture_referees, public.fixtures, public.referees;
                insert into public.referees values (1, 'Ref One'), (2, 'Ref Two'), (3, 'Ref Three');
                """
            )
            for referee_id in (1, 2, 3):
                for index in range(1, 21):
                    fixture_id = referee_id * 1000 + index
                    cur.execute(
                        """
                        insert into public.fixtures
                          (id, starting_at, status, home_score, away_score)
                        values (%s, now() - make_interval(days => %s), 'FT', 1, 0);
                        insert into public.fixture_referees
                          (fixture_id, referee_id, role, is_primary, source, extra)
                        values (%s, %s, 'main', true, 'sportmonks', %s::jsonb);
                        insert into public.fixture_statistics values
                          (%s, 84, %s), (%s, 83, %s);
                        """,
                        (
                            fixture_id,
                            30 - index,
                            fixture_id,
                            referee_id,
                            f'{{"fixture_id": {fixture_id}, "referee_id": {referee_id}, "type_id": 6}}',
                            fixture_id,
                            (index % 5) + 1,
                            fixture_id,
                            1 if index % 7 == 0 else 0,
                        ),
                    )
            cur.execute(
                """
                insert into public.fixtures (id, starting_at, status) values
                  (100, now() + interval '1 day', 'NS'),
                  (101, now() + interval '2 days', 'NS'),
                  (102, now() + interval '3 days', 'NS');
                insert into public.fixture_referees
                  (fixture_id, referee_id, role, is_primary, source, extra)
                values
                  (100, 1, 'main', true, 'sportmonks', '{}'),
                  (101, 1, 'main', true, 'sportmonks', '{}'),
                  (102, 3, 'main', true, 'sportmonks', '{}');
                """
            )

    def fetch(self, mode: str):
        conn = psycopg2.connect(DATABASE_URL)
        try:
            return fetch_fixture_referee_metrics(
                conn,
                days_back=0,
                days_forward=14,
                fixture_id=0,
                limit_fixtures=0,
                refresh_mode=mode,
                recent_history_hours=48,
            )
        finally:
            conn.close()

    def write(self, rows):
        conn = psycopg2.connect(DATABASE_URL)
        try:
            return upsert_fixture_referee_stats(conn, rows)
        finally:
            conn.close()

    def projection(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select fixture_id, referee_id, referee_name, avg_yellow_cards,
                       games_with_3plus_cards_pct, games_with_red_card_pct,
                       sample, avg_total_cards, games_with_4plus_cards_pct,
                       games_with_5plus_cards_pct, sample_5, sample_10, sample_20,
                       history_through, calculation_version, data_status, windows
                  from public.fixture_referee_stats
                 order by fixture_id
                """
            )
            return cur.fetchall()

    def test_full_mode_is_semantically_idempotent_and_preserves_updated_at_on_noop(self) -> None:
        full_rows = self.fetch("full")
        self.assertEqual([row["fixture_id"] for row in full_rows], [100, 101, 102])
        self.assertEqual(next(row for row in full_rows if row["fixture_id"] == 102)["sample_20"], 20)
        first = self.write(full_rows)
        self.assertEqual((first.inserted, first.updated), (3, 0))
        baseline = self.projection()
        with self.conn.cursor() as cur:
            cur.execute("select fixture_id, updated_at from public.fixture_referee_stats order by fixture_id")
            timestamps = cur.fetchall()

        second = self.write(self.fetch("full"))
        self.assertEqual((second.inserted, second.updated), (0, 0))
        self.assertEqual(self.projection(), baseline)
        with self.conn.cursor() as cur:
            cur.execute("select fixture_id, updated_at from public.fixture_referee_stats order by fixture_id")
            self.assertEqual(cur.fetchall(), timestamps)

    def test_frequent_mode_covers_missing_reassignment_and_recent_completion(self) -> None:
        self.write(self.fetch("full"))
        self.assertEqual(self.fetch("frequent"), [])

        with self.conn.cursor() as cur:
            cur.execute("delete from public.fixture_referee_stats where fixture_id = 100")
        self.assertEqual([row["fixture_id"] for row in self.fetch("frequent")], [100])
        self.write(self.fetch("frequent"))

        with self.conn.cursor() as cur:
            cur.execute("delete from public.fixture_referees where fixture_id = 101")
            cur.execute(
                "insert into public.fixture_referees values (101, 2, 'main', true, 'sportmonks', '{}', now(), now())"
            )
        reassigned = self.fetch("frequent")
        self.assertEqual([row["fixture_id"] for row in reassigned], [101])
        self.assertEqual(reassigned[0]["referee_id"], 2)
        self.write(reassigned)

        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into public.fixtures values (9001, now() - interval '1 hour', 'FT', 2, 1);
                insert into public.fixture_referees values
                  (9001, 1, 'main', true, 'sportmonks', '{}', now(), now());
                insert into public.fixture_statistics values (9001, 84, 8), (9001, 83, 1);
                """
            )
        recent = self.fetch("frequent")
        self.assertEqual([row["fixture_id"] for row in recent], [100])
        self.write(recent)
        frequent_projection = self.projection()
        self.write(self.fetch("full"))
        self.assertEqual(self.projection(), frequent_projection)

    def test_old_historical_correction_is_caught_by_hourly_full_mode(self) -> None:
        self.write(self.fetch("full"))
        baseline = self.projection()
        with self.conn.cursor() as cur:
            cur.execute("update public.fixture_statistics set value = 12 where fixture_id = 1020 and type_id = 84")

        self.assertEqual(self.fetch("frequent"), [])
        changed = self.write(self.fetch("full"))
        self.assertGreater(changed.updated, 0)
        self.assertNotEqual(self.projection(), baseline)

    def test_history_hydration_only_mutates_semantic_changes(self) -> None:
        row = {
            "fixture_id": 9999,
            "referee_id": 1,
            "role": "main",
            "is_primary": True,
            "source": "sportmonks",
            "extra": {"fixture_id": 9999, "referee_id": 1, "type_id": 6},
        }
        with self.conn.cursor() as cur:
            cur.execute("insert into public.fixtures values (9999, now() - interval '3 days', 'FT', 1, 1)")
        conn = psycopg2.connect(DATABASE_URL)
        try:
            first = upsert_assignments(conn, [row])
            second = upsert_assignments(conn, [row])
            changed_row = {**row, "extra": {**row["extra"], "corrected": True}}
            third = upsert_assignments(conn, [changed_row])
        finally:
            conn.close()
        self.assertEqual((first.inserted, first.updated), (1, 0))
        self.assertEqual((second.inserted, second.updated), (0, 0))
        self.assertEqual((third.inserted, third.updated), (0, 1))

    def test_failed_stats_transaction_rolls_back_and_retry_is_idempotent(self) -> None:
        self.write(self.fetch("full"))
        baseline = self.projection()
        changed_rows = self.fetch("full")
        changed_rows[0]["referee_name"] = "Corrected Referee Name"
        with self.conn.cursor() as cur:
            cur.execute(
                """
                create function public.fail_referee_stats_write() returns trigger language plpgsql as $$
                begin raise exception 'referee replay failure'; end $$;
                create trigger fail_referee_stats_write before update on public.fixture_referee_stats
                for each row execute function public.fail_referee_stats_write();
                """
            )
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with self.assertRaisesRegex(Exception, "referee replay failure"):
                upsert_fixture_referee_stats(conn, changed_rows)
            conn.rollback()
        finally:
            conn.close()
        self.assertEqual(self.projection(), baseline)
        with self.conn.cursor() as cur:
            cur.execute("drop trigger fail_referee_stats_write on public.fixture_referee_stats")
            cur.execute("drop function public.fail_referee_stats_write()")
        retry = self.write(changed_rows)
        self.assertEqual(retry.updated, 1)
        self.assertEqual(self.write(changed_rows).total, 0)


if __name__ == "__main__":
    unittest.main()
