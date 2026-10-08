from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2

from scripts.reconcile_fixture_delivery_odds import LOCK_NAME, reconcile


DATABASE_URL = os.environ.get("ROOT1A_POSTGRES_URL")
UTC = timezone.utc
CURRENT_RELEASE = "00000000-0000-4000-8000-000000000001"
PINNED_RELEASE = "00000000-0000-4000-8000-000000000002"


@unittest.skipUnless(DATABASE_URL, "ROOT1A_POSTGRES_URL is required for PostgreSQL replay tests")
class FixtureDeliveryOddsPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = psycopg2.connect(DATABASE_URL)
        cls.conn.autocommit = True
        with cls.conn.cursor() as cur:
            cur.execute(
                """
                drop table if exists public.fixture_delivery_odds;
                drop table if exists public.odds_outcomes;
                drop table if exists public.fixture_delivery_schedule;
                drop table if exists public.fixture_delivery_current_publication;
                drop table if exists public.fixture_delivery_releases;
                create table public.fixture_delivery_releases (
                  id uuid primary key, status text not null, pin_expires_at timestamptz not null,
                  odds_rows integer not null default 0
                );
                create table public.fixture_delivery_current_publication (
                  publication_key text primary key, release_id uuid not null
                );
                create table public.fixture_delivery_schedule (
                  release_id uuid not null, fixture_id bigint not null, league_id integer not null,
                  status text, primary key (release_id, fixture_id)
                );
                create table public.odds_outcomes (
                  fixture_id bigint not null, bookmaker_id integer not null,
                  market_key text not null, selection_key text not null,
                  line numeric, price_decimal numeric not null, price_american integer,
                  participant_type text, participant_id bigint, last_updated_at timestamptz,
                  unique (fixture_id, bookmaker_id, market_key, selection_key, line)
                );
                create table public.fixture_delivery_odds (
                  release_id uuid not null, fixture_id bigint not null, bookmaker_id integer not null,
                  market_key text not null, selection_key text not null,
                  participant_type text not null, participant_id bigint not null,
                  line numeric, line_key numeric not null, price_decimal numeric not null,
                  price_american integer, source_last_updated_at timestamptz, observed_at timestamptz not null,
                  primary key (release_id, fixture_id, bookmaker_id, market_key, selection_key,
                               participant_type, participant_id, line_key)
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
                drop trigger if exists fail_fixture_odds_insert on public.fixture_delivery_odds;
                drop function if exists public.fail_fixture_odds_insert();
                truncate public.fixture_delivery_odds, public.odds_outcomes,
                         public.fixture_delivery_schedule, public.fixture_delivery_current_publication,
                         public.fixture_delivery_releases;
                insert into public.fixture_delivery_releases (id, status, pin_expires_at) values
                  (%s, 'published', now() + interval '2 hours'),
                  (%s, 'published', now() + interval '1 hour');
                insert into public.fixture_delivery_current_publication values ('fixtures', %s);
                """,
                (CURRENT_RELEASE, PINNED_RELEASE, CURRENT_RELEASE),
            )

    def add_fixture(self, fixture_id: int, *, league_id: int = 8, status: str = "NS") -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into public.fixture_delivery_schedule
                select id, %s, %s, %s from public.fixture_delivery_releases
                """,
                (fixture_id, league_id, status),
            )

    def add_canonical(
        self,
        fixture_id: int,
        selection: str,
        price: str,
        *,
        bookmaker: int = 2,
        market: str = "moneyline",
        line: str | None = None,
        participant_type: str | None = "team",
        participant_id: int | None = 10,
        updated_at: datetime | None = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into public.odds_outcomes
                  (fixture_id, bookmaker_id, market_key, selection_key, line, price_decimal,
                   price_american, participant_type, participant_id, last_updated_at)
                values (%s, %s, %s, %s, %s, %s, 120, %s, %s, %s)
                """,
                (
                    fixture_id,
                    bookmaker,
                    market,
                    selection,
                    Decimal(line) if line is not None else None,
                    Decimal(price),
                    participant_type,
                    participant_id,
                    updated_at or datetime(2026, 10, 7, 20, 49, tzinfo=UTC),
                ),
            )

    def run_reconcile(self):
        conn = psycopg2.connect(DATABASE_URL)
        try:
            return reconcile(conn)
        finally:
            conn.close()

    def projection(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select release_id::text, fixture_id, bookmaker_id, market_key, selection_key,
                       participant_type, participant_id, line, price_decimal, price_american,
                       source_last_updated_at
                  from public.fixture_delivery_odds
                 order by release_id, fixture_id, bookmaker_id, market_key, selection_key,
                          participant_type, participant_id, line_key
                """
            )
            return cur.fetchall()

    def current_price_mismatch_count(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                  from public.odds_outcomes o
                  join public.fixture_delivery_odds d
                    on d.release_id = %s
                   and d.fixture_id = o.fixture_id
                   and d.bookmaker_id = o.bookmaker_id
                   and d.market_key = o.market_key
                   and d.selection_key = o.selection_key
                   and d.participant_type = coalesce(o.participant_type, 'team')
                   and d.participant_id = coalesce(o.participant_id, 0)
                   and d.line_key = coalesce(o.line, -9999::numeric)
                 where d.price_decimal is distinct from o.price_decimal
                """,
                (CURRENT_RELEASE,),
            )
            return int(cur.fetchone()[0])

    def release_counts_match(self) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select bool_and(r.odds_rows = coalesce(o.actual_rows, 0))
                  from public.fixture_delivery_releases r
                  left join (
                    select release_id, count(*)::integer actual_rows
                      from public.fixture_delivery_odds group by release_id
                  ) o on o.release_id = r.id
                """
            )
            return bool(cur.fetchone()[0])

    def test_insert_exact_match_and_timestamp_only_change(self) -> None:
        self.add_fixture(100)
        self.add_canonical(100, "home", "2.10")
        inserted = self.run_reconcile()
        self.assertEqual(inserted["inserted_rows"], 2)
        self.assertEqual(inserted["release_metadata_updated"], 2)
        self.assertTrue(self.release_counts_match())
        self.assertEqual(self.run_reconcile()["status"], "noop")

        changed = datetime(2026, 10, 7, 21, 4, tzinfo=UTC)
        with self.conn.cursor() as cur:
            cur.execute("update public.odds_outcomes set last_updated_at = %s", (changed,))
        refreshed = self.run_reconcile()
        self.assertEqual(refreshed["affected_release_fixture_pairs"], 2)
        self.assertTrue(all(row[-1] == changed for row in self.projection()))

    def test_price_line_participant_insert_and_removal(self) -> None:
        self.add_fixture(101)
        self.add_canonical(101, "home", "2.10", participant_id=10)
        self.add_canonical(101, "draw", "3.20", participant_type=None, participant_id=None)
        self.run_reconcile()
        with self.conn.cursor() as cur:
            cur.execute("update public.odds_outcomes set price_decimal = 2.20 where selection_key = 'home'")
            cur.execute(
                """
                update public.odds_outcomes
                   set line = 0.5, participant_type = 'player', participant_id = 99
                 where selection_key = 'draw'
                """
            )
            cur.execute("delete from public.odds_outcomes where selection_key = 'home'")
        self.add_canonical(101, "away", "4.10", participant_id=20)
        result = self.run_reconcile()
        self.assertEqual(result["affected_release_fixture_pairs"], 2)
        rows = self.projection()
        self.assertFalse(any(row[4] == "home" for row in rows))
        self.assertTrue(any(row[4] == "draw" and row[5:8] == ("player", 99, Decimal("0.5")) for row in rows))
        self.assertTrue(any(row[4] == "away" for row in rows))

    def test_price_change_and_new_eligible_market_refresh_both_releases(self) -> None:
        self.add_fixture(108)
        self.add_canonical(108, "home", "2.10")
        self.run_reconcile()
        with self.conn.cursor() as cur:
            cur.execute("update public.odds_outcomes set price_decimal = 2.20 where selection_key = 'home'")
        self.add_canonical(108, "away", "3.40", market="h2h", participant_id=20)
        result = self.run_reconcile()
        self.assertEqual(result["affected_release_fixture_pairs"], 2)
        rows = self.projection()
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(any(row[0] == release for row in rows) for release in (CURRENT_RELEASE, PINNED_RELEASE)))
        self.assertEqual(sum(1 for row in rows if row[4] == "home" and row[8] == Decimal("2.20")), 2)
        self.assertEqual(sum(1 for row in rows if row[3] == "h2h" and row[4] == "away"), 2)

    def test_participant_mapping_to_null_uses_full_publisher_normalization(self) -> None:
        self.add_fixture(109)
        self.add_canonical(109, "home", "2.10", participant_type="player", participant_id=77)
        self.run_reconcile()
        with self.conn.cursor() as cur:
            cur.execute(
                "update public.odds_outcomes set participant_type = null, participant_id = null"
            )
        self.run_reconcile()
        rows = self.projection()
        self.assertTrue(all(row[5] == "team" and row[6] == 0 for row in rows))

    def test_wholly_absent_scope_and_non_card_changes_are_noops(self) -> None:
        self.add_fixture(102)
        self.add_canonical(102, "home", "2.10")
        self.run_reconcile()
        # Root #1 conservatively retains a wholly omitted market/bookmaker in
        # canonical odds_outcomes, so the card projection remains unchanged.
        baseline = self.projection()
        self.add_canonical(102, "over", "1.90", market="total_goals", participant_id=0)
        self.add_canonical(102, "home", "1.95", bookmaker=99, participant_id=10)
        self.assertEqual(self.run_reconcile()["status"], "noop")
        self.assertEqual(self.projection(), baseline)

    def test_invalid_price_and_excluded_fixture_do_not_project(self) -> None:
        self.add_fixture(103)
        self.add_fixture(104, league_id=24)
        self.add_canonical(103, "home", "1.00")
        self.add_canonical(104, "home", "2.10")
        result = self.run_reconcile()
        self.assertEqual(result["source_rows"], 0)
        self.assertEqual(result["status"], "noop")

    def test_settled_fixture_is_not_changed_without_a_canonical_change(self) -> None:
        self.add_fixture(105, status="FT")
        self.add_canonical(105, "home", "2.10")
        self.run_reconcile()
        baseline = self.projection()
        self.assertEqual(self.run_reconcile()["status"], "noop")
        self.assertEqual(self.projection(), baseline)

    def test_publisher_lock_collision_defers_without_writes(self) -> None:
        self.add_fixture(106)
        self.add_canonical(106, "home", "2.10")
        blocker = psycopg2.connect(DATABASE_URL)
        try:
            with blocker.cursor() as cur:
                cur.execute("select pg_advisory_xact_lock(hashtextextended(%s, 0))", (LOCK_NAME,))
            result = self.run_reconcile()
            self.assertEqual(result["status"], "deferred_lock_busy")
            self.assertEqual(self.projection(), [])
        finally:
            blocker.rollback()
            blocker.close()
        self.assertEqual(self.run_reconcile()["status"], "succeeded")

    def test_transaction_failure_rolls_back_and_retry_is_idempotent(self) -> None:
        self.add_fixture(107)
        self.add_canonical(107, "home", "2.10")
        self.run_reconcile()
        baseline = self.projection()
        with self.conn.cursor() as cur:
            cur.execute("update public.odds_outcomes set price_decimal = 2.20")
            cur.execute(
                """
                create function public.fail_fixture_odds_insert() returns trigger language plpgsql as $$
                begin raise exception 'replay insert failure'; end $$;
                create trigger fail_fixture_odds_insert before insert on public.fixture_delivery_odds
                for each row execute function public.fail_fixture_odds_insert();
                """
            )
        with self.assertRaisesRegex(Exception, "replay insert failure"):
            self.run_reconcile()
        self.assertEqual(self.projection(), baseline)
        with self.conn.cursor() as cur:
            cur.execute("drop trigger fail_fixture_odds_insert on public.fixture_delivery_odds")
            cur.execute("drop function public.fail_fixture_odds_insert()")
        self.assertEqual(self.run_reconcile()["status"], "succeeded")
        self.assertEqual(self.run_reconcile()["status"], "noop")

    def test_observed_six_fixture_twelve_difference_replay_reaches_zero(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "update public.fixture_delivery_releases set pin_expires_at = now() - interval '1 minute' where id = %s",
                (PINNED_RELEASE,),
            )
        fixtures = [19621791, 19621794, 19621798, 19667118, 19667119, 19667125]
        canonical_prices = ["1.54", "4.00", "3.70", "2.88", "2.10", "2.95", "3.50", "3.00", "3.75", "1.83", "3.30", "2.45"]
        stale_prices = ["1.57", "3.95", "3.75", "2.90", "2.15", "3.00", "3.45", "3.10", "3.90", "1.80", "3.40", "2.35"]
        index = 0
        with self.conn.cursor() as cur:
            for fixture_id in fixtures:
                self.add_fixture(fixture_id)
                for side in ("home", "away"):
                    self.add_canonical(fixture_id, side, canonical_prices[index], participant_id=10 + index)
                    cur.execute(
                        """
                        insert into public.fixture_delivery_odds values
                          (%s, %s, 2, 'moneyline', %s, 'team', %s, null, -9999,
                           %s, 120, %s, now())
                        """,
                        (
                            CURRENT_RELEASE,
                            fixture_id,
                            side,
                            10 + index,
                            Decimal(stale_prices[index]),
                            datetime(2026, 10, 7, 20, 34, tzinfo=UTC),
                        ),
                    )
                    index += 1
        self.assertEqual(self.current_price_mismatch_count(), 12)
        result = self.run_reconcile()
        self.assertEqual(result["affected_release_fixture_pairs"], 6)
        self.assertEqual(self.current_price_mismatch_count(), 0)


if __name__ == "__main__":
    unittest.main()
