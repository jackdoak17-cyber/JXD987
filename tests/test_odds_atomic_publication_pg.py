"""Disposable-Postgres rollback test using the installed quote-record RPC.

Set ODDS_ATOMIC_TEST_DATABASE_URL to a local database whose name starts with
odds_atomic_test_. No production database can pass this guard.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn

from scripts.export_odds_to_supabase_psql import stage_and_upsert
from scripts.odds_atomic_publication import AtomicOddsPublication
from scripts.reconcile_fixture_delivery_odds import reconcile


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = """
create table public.fixtures(
  id bigint primary key, league_id int, starting_at timestamp,
  status text, status_code text, home_score int, away_score int,
  home_team_id bigint, away_team_id bigint
);
create table public.odds_outcomes(
  id bigserial primary key, fixture_id bigint, bookmaker_id int,
  market_key text, selection_key text, line numeric, price_decimal numeric,
  price_american int, participant_type text, participant_id bigint,
  last_updated_at timestamptz
);
create unique index odds_outcomes_identity on public.odds_outcomes(
  fixture_id, bookmaker_id, market_key, selection_key, (coalesce(line,-9999))
);
create table public.odds_quote_verifications(
  fixture_id bigint not null, bookmaker_id int not null,
  market_key text not null, selection_key text not null,
  participant_type text not null, participant_id bigint not null,
  line_key numeric not null, price_decimal numeric not null,
  source_last_updated_at timestamptz, last_verified_at timestamptz not null,
  source_run_id text not null,
  primary key(fixture_id,bookmaker_id,market_key,selection_key,
              participant_type,participant_id,line_key)
);
create table public.fixture_delivery_releases(
  id uuid primary key, status text, pin_expires_at timestamptz, odds_rows int
);
create table public.fixture_delivery_current_publication(
  publication_key text primary key, release_id uuid
);
create table public.fixture_delivery_schedule(
  release_id uuid, fixture_id bigint, league_id int,
  primary key(release_id,fixture_id)
);
create table public.fixture_delivery_odds(
  release_id uuid, fixture_id bigint, bookmaker_id int, market_key text,
  selection_key text, participant_type text, participant_id bigint,
  line numeric, line_key numeric, price_decimal numeric, price_american int,
  source_last_updated_at timestamptz, observed_at timestamptz
);
"""
RELEASE = "11111111-1111-1111-1111-111111111111"
SOURCE_OLD = "2026-11-21T21:00:00Z"
SOURCE_NEW = "2026-11-21T22:00:00Z"


@unittest.skipUnless(os.environ.get("ODDS_ATOMIC_TEST_DATABASE_URL"), "disposable Postgres URL not set")
class AtomicOddsPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.url = os.environ["ODDS_ATOMIC_TEST_DATABASE_URL"]
        database = parse_dsn(cls.url).get("dbname", "")
        if not database.startswith("odds_atomic_test_"):
            raise RuntimeError("atomic integration tests require a disposable odds_atomic_test_ database")
        with psycopg2.connect(cls.url) as conn:
            with conn.cursor() as cur:
                cur.execute("drop schema public cascade; create schema public;")
                cur.execute(SCHEMA)
                cur.execute((ROOT / "tests/fixtures/odds_quote_verifications_record.sql").read_text())

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.spool = self.root / "spool"
        (self.spool / "pending").mkdir(parents=True)
        self.sqlite_db = self.root / "source.sqlite"
        with sqlite3.connect(self.sqlite_db) as source:
            source.executescript("""
                create table fixtures(id integer primary key,league_id integer,starting_at text);
                create table odds_outcomes(
                  fixture_id integer,bookmaker_id integer,market_key text,
                  selection_key text,participant_type text,participant_id integer,
                  line numeric,price_decimal numeric,last_updated_at text
                );
            """)
            source.execute(
                "insert into fixtures values(1001,384,?)",
                ((datetime.now(timezone.utc) + timedelta(hours=12)).isoformat(),),
            )
            source.execute(
                "insert into odds_outcomes values(1001,2,'moneyline','home','team',11,null,2.5,?)",
                (SOURCE_NEW,),
            )
        self.csv_path = self.root / "stage.csv"
        with self.csv_path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "fixture_id", "bookmaker_id", "market_key", "selection_key",
                "line", "price_decimal", "price_american", "participant_type",
                "participant_id", "last_updated_at",
            ])
            writer.writerow([1001, 2, "moneyline", "home", "", 2.5, 150, "team", 11, SOURCE_NEW])
        with psycopg2.connect(self.url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    truncate public.odds_outcomes, public.odds_quote_verifications,
                      public.fixture_delivery_releases,
                      public.fixture_delivery_current_publication,
                      public.fixture_delivery_schedule,
                      public.fixture_delivery_odds restart identity;
                """)
                cur.execute(
                    "insert into public.fixtures values(1001,384,%s,'NS','NS',null,null,11,22) "
                    "on conflict(id) do update set starting_at=excluded.starting_at",
                    (datetime.now(timezone.utc) + timedelta(hours=12),),
                )
                cur.execute(
                    "insert into public.odds_outcomes(fixture_id,bookmaker_id,market_key,"
                    "selection_key,line,price_decimal,price_american,participant_type,"
                    "participant_id,last_updated_at) values(1001,2,'moneyline','home',"
                    "null,2.0,100,'team',11,%s)",
                    (SOURCE_OLD,),
                )
                cur.execute(
                    "insert into public.odds_quote_verifications values "
                    "(1001,2,'moneyline','home','team',11,-9999,2.0,%s,now()-interval '10 minutes','old-run-1001')",
                    (SOURCE_OLD,),
                )
                cur.execute(
                    "insert into public.fixture_delivery_releases values (%s,'published',now()+interval '1 day',1)",
                    (RELEASE,),
                )
                cur.execute(
                    "insert into public.fixture_delivery_current_publication values('fixtures',%s)",
                    (RELEASE,),
                )
                cur.execute(
                    "insert into public.fixture_delivery_schedule values(%s,1001,384)",
                    (RELEASE,),
                )
                cur.execute(
                    "insert into public.fixture_delivery_odds values "
                    "(%s,1001,2,'moneyline','home','team',11,null,-9999,2.0,100,%s,now())",
                    (RELEASE, SOURCE_OLD),
                )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def observation(self, observed_at: datetime | None = None) -> None:
        now = datetime.now(timezone.utc)
        document = {
            "schema_version": 1,
            "source": "odds_api_provider_response",
            "run_id": "new-run-1001",
            "lane": "p2",
            "generated_at": now.isoformat(),
            "quotes": [{
                "fixture_id": 1001,
                "bookmaker_id": 2,
                "market_key": "moneyline",
                "selection_key": "home",
                "participant_type": "team",
                "participant_id": 11,
                "line": None,
                "price_decimal": 2.5,
                "source_last_updated_at": SOURCE_NEW,
                "observed_at": (observed_at or now).isoformat(),
            }],
            "errors": [],
        }
        (self.spool / "pending" / "p2-test.json").write_text(json.dumps(document))

    def publish(self, inject_after_reconcile: bool = False) -> None:
        context = AtomicOddsPublication(
            "delivery", self.url, self.sqlite_db, [384], self.spool,
        )
        try:
            # The installed wrapper owns fd 9; this disposable DB test owns
            # only the database/spool transaction boundary.
            with patch.object(AtomicOddsPublication, "verify_writer_fd"):
                context.prepare()
            if inject_after_reconcile:
                def fail_after_reconcile() -> None:
                    context.release_report = reconcile(context.connection, commit=False)
                    raise RuntimeError("injected after release reconciliation")
                context.before_commit = fail_after_reconcile
            stage_and_upsert(
                self.url, self.csv_path, "test", [384], 0, 14,
                ["moneyline"], False, None, None, True, atomic_context=context,
            )
            context.mark_committed()
            context.finalize()
        finally:
            context.close()

    def identities(self) -> tuple[str, str, str]:
        with psycopg2.connect(self.url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    select o.price_decimal::text, d.price_decimal::text,
                           v.price_decimal::text
                      from public.odds_outcomes o
                      join public.fixture_delivery_odds d on d.fixture_id=o.fixture_id
                      join public.odds_quote_verifications v on v.fixture_id=o.fixture_id
                     where o.fixture_id=1001
                """)
                return cur.fetchone()

    def test_failure_after_release_reconciliation_rolls_back_all_three(self) -> None:
        self.observation()
        with self.assertRaisesRegex(RuntimeError, "injected after release reconciliation"):
            self.publish(inject_after_reconcile=True)
        self.assertEqual(self.identities(), ("2.0", "2.0", "2.0"))
        self.assertTrue((self.spool / "pending" / "p2-test.json").exists())

    def test_monotonic_older_observation_rolls_back_all_three(self) -> None:
        with psycopg2.connect(self.url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "update public.odds_quote_verifications set last_verified_at=now() "
                    "where fixture_id=1001"
                )
        self.observation(datetime.now(timezone.utc) - timedelta(hours=1))
        with self.assertRaisesRegex(RuntimeError, "exact quote evidence is incomplete"):
            self.publish()
        self.assertEqual(self.identities(), ("2.0", "2.0", "2.0"))
        self.assertTrue((self.spool / "pending" / "p2-test.json").exists())

    def test_p3_moneyline_guard_blocks_partial_coverage(self) -> None:
        self.observation()
        context = AtomicOddsPublication(
            "p3", self.url, self.sqlite_db, [384], self.spool,
        )
        try:
            with patch.object(AtomicOddsPublication, "verify_writer_fd"), patch.object(
                AtomicOddsPublication, "release_p3_writer_lock"
            ):
                context.prepare()
            with self.assertRaisesRegex(RuntimeError, "moneyline coverage validation failed"):
                stage_and_upsert(
                    self.url, self.csv_path, "test", [384], 0, 14,
                    ["moneyline"], False, None, None, True,
                    atomic_context=context,
                )
        finally:
            context.close()
        self.assertEqual(self.identities(), ("2.0", "2.0", "2.0"))
        self.assertTrue((self.spool / "pending" / "p2-test.json").exists())

    def test_other_player_same_fixture_line_does_not_poison_identity_check(self) -> None:
        """Exercise the defensive participant join even beyond today's raw key."""
        context = AtomicOddsPublication(
            "delivery", self.url, self.sqlite_db, [384], self.spool,
        )
        try:
            with context.connection.cursor() as cur:
                # The current raw index also prevents this duplicate key. Drop
                # it only inside this transaction to test the invariant if a
                # future source schema permits participant-aware raw keys.
                cur.execute("drop index public.odds_outcomes_identity")
                cur.execute("""
                    create temp table odds_pre_usable(
                      fixture_id bigint, bookmaker_id int, market_key text,
                      selection_key text, line_key numeric, price_decimal numeric,
                      last_updated_at timestamptz, starting_at timestamp,
                      participant_type text, participant_id bigint
                    ) on commit drop
                """)
                cur.execute("""
                    insert into odds_pre_usable values
                    (1001,2,'player_shots','over',4.5,2.0,%s,
                     now()+interval '12 hours','player',101)
                """, (SOURCE_OLD,))
                for player_id, price in ((101, 3.0), (202, 9.0)):
                    cur.execute("""
                        insert into public.odds_outcomes(
                          fixture_id,bookmaker_id,market_key,selection_key,line,
                          price_decimal,price_american,participant_type,
                          participant_id,last_updated_at
                        ) values(1001,2,'player_shots','over',4.5,%s,200,'player',%s,%s)
                    """, (price, player_id, SOURCE_NEW))
                cur.execute("""
                    insert into public.odds_quote_verifications values
                    (1001,2,'player_shots','over','player',101,4.5,3.0,%s,now(),'new-run-101')
                """, (SOURCE_NEW,))
            context._assert_prior_usable_remains_exact()
        finally:
            context.close()  # Rollback restores the production-shaped index.

    def test_success_publishes_all_three_then_archives(self) -> None:
        self.observation()
        self.publish()
        self.assertEqual(self.identities(), ("2.5", "2.5", "2.5"))
        self.assertTrue((self.spool / "processed" / "p2-test.json").exists())
        self.assertFalse((self.spool / "pending" / "p2-test.json").exists())


if __name__ == "__main__":
    unittest.main()
