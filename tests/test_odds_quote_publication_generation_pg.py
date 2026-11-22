"""Disposable PostgreSQL proof for private quote-publication generation.

Set ODDS_QUOTE_GENERATION_TEST_DATABASE_URL to a local database whose name
starts with odds_quote_generation_test_. The test destroys its schemas.
"""

from __future__ import annotations

import json
import os
import threading
import time
import unittest
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20261122053718_odds_quote_publication_generation.sql"
RECORDER = ROOT / "tests/fixtures/odds_quote_verifications_record.sql"
OLD_CLEANUP = (
    "  delete from public.odds_quote_verifications\n"
    "  where last_verified_at < now() - interval '30 days';"
)
NEW_CLEANUP = (
    "  update operations.quote_verification_maintenance\n"
    "     set last_cleaned_at = statement_timestamp()\n"
    "   where singleton is true\n"
    "     and last_cleaned_at < statement_timestamp() - interval '1 hour';\n"
    "  if found then\n"
    "    delete from public.odds_quote_verifications\n"
    "    where last_verified_at < now() - interval '30 days';\n"
    "  end if;"
)
SCHEMA = """
create schema operations;
create table operations.quote_verification_maintenance(
  singleton boolean primary key default true check(singleton),
  last_cleaned_at timestamptz not null
);
insert into operations.quote_verification_maintenance values(true,'-infinity');
create table public.odds_outcomes(
  id bigserial primary key,
  fixture_id bigint not null, bookmaker_id int not null,
  market_key text not null, selection_key text not null,
  line numeric, price_decimal numeric not null,
  participant_type text, participant_id bigint,
  last_updated_at timestamptz
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
"""
QUOTE = """
insert into public.odds_outcomes(
  fixture_id,bookmaker_id,market_key,selection_key,line,price_decimal,
  participant_type,participant_id,last_updated_at
) values (%s,2,'player_shots',%s,4.5,%s,'player',%s,now())
"""
OBSERVATION = {
    "fixture_id": 1001,
    "bookmaker_id": 2,
    "market_key": "player_shots",
    "selection_key": "test_player_over",
    "participant_type": "player",
    "participant_id": 10,
    "line": 4.5,
    "price_decimal": 2.2,
}
DENIED_ROLES = (
    "anon", "authenticated", "service_role",
    "statsweb_pool", "oddssearch_operations_reader",
)


@unittest.skipUnless(
    os.environ.get("ODDS_QUOTE_GENERATION_TEST_DATABASE_URL"),
    "disposable PostgreSQL URL not set",
)
class QuotePublicationGenerationPGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.url = os.environ["ODDS_QUOTE_GENERATION_TEST_DATABASE_URL"]
        db_name = parse_dsn(cls.url).get("dbname", "")
        if not db_name.startswith("odds_quote_generation_test_"):
            raise RuntimeError("quote generation tests require a disposable test database")
        recorder = RECORDER.read_text()
        assert recorder.count(OLD_CLEANUP) == 1
        recorder = recorder.replace(OLD_CLEANUP, NEW_CLEANUP)
        with psycopg2.connect(cls.url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("drop schema if exists operations cascade")
                cursor.execute("drop schema if exists public cascade")
                cursor.execute("create schema public")
                cursor.execute(SCHEMA)
                # Supabase can have explicit per-schema default grants. These
                # must not survive merely because PUBLIC was revoked.
                for role in DENIED_ROLES:
                    cursor.execute("select to_regrole(%s)", (role,))
                    if cursor.fetchone()[0] is None:
                        cursor.execute(f"create role {role} nologin")
                    cursor.execute(
                        f"alter default privileges in schema operations "
                        f"grant execute on functions to {role}"
                    )
                    cursor.execute(
                        f"alter default privileges in schema operations "
                        f"grant select on tables to {role}"
                    )
                cursor.execute(recorder)
                cursor.execute(
                    "select md5(prosrc) from pg_proc "
                    "where oid='public.odds_quote_verifications_record(text,jsonb)'::regprocedure"
                )
                actual_hash = cursor.fetchone()[0]
                assert actual_hash == "909133ec6a24a9e209390ac9914d2085", actual_hash
                cursor.execute(MIGRATION.read_text())

    def setUp(self) -> None:
        # Use a separate transaction for each scenario, then roll it back.
        self.connection = psycopg2.connect(self.url)
        self.connection.autocommit = False
        self.cursor = self.connection.cursor()
        self.cursor.execute("select operations.odds_quote_publication_generation()")
        self.start_generation = int(self.cursor.fetchone()[0])

    def tearDown(self) -> None:
        self.connection.rollback()
        self.cursor.close()
        self.connection.close()

    def generation(self, cursor=None) -> int:
        target = cursor or self.cursor
        target.execute("select operations.odds_quote_publication_generation()")
        value = target.fetchone()[0]
        self.assertIsInstance(value, str)
        return int(value)

    def test_empty_new_deleted_and_zero_row_changes(self) -> None:
        self.cursor.execute("select count(*) from public.odds_outcomes where fixture_id=1001")
        self.assertEqual(self.cursor.fetchone()[0], 0)
        self.cursor.execute(QUOTE, (1001, "test_player_over", 2.2, 10))
        self.assertEqual(self.generation(), self.start_generation + 1)
        self.cursor.execute("update public.odds_outcomes set price_decimal=2.3 where fixture_id=-1")
        self.assertEqual(self.generation(), self.start_generation + 2)
        self.cursor.execute("delete from public.odds_outcomes where fixture_id=1001")
        self.assertEqual(self.generation(), self.start_generation + 3)
        self.cursor.execute("select count(*) from public.odds_outcomes where fixture_id=1001")
        self.assertEqual(self.cursor.fetchone()[0], 0)

    def test_uncoordinated_truncate_is_rejected_without_commit(self) -> None:
        self.cursor.execute(QUOTE, (1001, "test_player_over", 2.2, 10))
        self.cursor.execute(
            "insert into public.odds_quote_verifications values "
            "(1001,2,'player_shots','test_player_over','player',10,4.5,"
            "2.2,now(),now(),'existing-run')"
        )
        before = self.generation()
        for table in ("public.odds_outcomes", "public.odds_quote_verifications"):
            self.cursor.execute("savepoint before_truncate")
            with self.assertRaisesRegex(psycopg2.Error, "TRUNCATE is prohibited"):
                self.cursor.execute(f"truncate {table}")
            self.cursor.execute("rollback to savepoint before_truncate")
            self.cursor.execute(f"select count(*) from {table} where fixture_id=1001")
            self.assertEqual(self.cursor.fetchone()[0], 1)
            self.assertEqual(self.generation(), before)

    def test_missing_or_disabled_guard_fails_closed(self) -> None:
        self.cursor.execute("savepoint before_guard")
        self.cursor.execute(
            "alter table public.odds_outcomes disable trigger odds_quote_generation_dml"
        )
        with self.assertRaisesRegex(psycopg2.Error, "triggers unavailable"):
            self.generation()
        self.cursor.execute("rollback to savepoint before_guard")
        self.assertEqual(self.generation(), self.start_generation)
        self.cursor.execute("savepoint before_state")
        self.cursor.execute("delete from operations.odds_quote_publication_state")
        with self.assertRaisesRegex(psycopg2.Error, "state unavailable"):
            self.generation()
        self.cursor.execute("rollback to savepoint before_state")
        self.assertEqual(self.generation(), self.start_generation)

    def test_table_swap_and_large_decimal_generation_fail_safe(self) -> None:
        self.cursor.execute("savepoint before_large")
        self.cursor.execute(
            "update operations.odds_quote_publication_state "
            "set generation=9007199254740993 where id=1"
        )
        self.cursor.execute("select operations.odds_quote_publication_generation()")
        self.assertEqual(self.cursor.fetchone()[0], "9007199254740993")
        self.cursor.execute("rollback to savepoint before_large")
        self.cursor.execute("savepoint before_swap")
        self.cursor.execute("alter table public.odds_outcomes rename to odds_outcomes_old")
        self.cursor.execute("create table public.odds_outcomes as select * from public.odds_outcomes_old")
        with self.assertRaisesRegex(psycopg2.Error, "state unavailable"):
            self.generation()
        self.cursor.execute("rollback to savepoint before_swap")
        self.assertEqual(self.generation(), self.start_generation)

    def test_no_public_or_web_role_privileges(self) -> None:
        for role in ("public", *DENIED_ROLES):
            self.cursor.execute(
                "select has_function_privilege(%s,"
                "'operations.odds_quote_publication_generation()','EXECUTE'),"
                "has_function_privilege(%s,"
                "'operations.odds_quote_publication_bump()','EXECUTE'),"
                "has_function_privilege(%s,"
                "'operations.odds_quote_publication_reject_truncate()','EXECUTE'),"
                "has_table_privilege(%s,"
                "'operations.odds_quote_publication_state','SELECT')",
                (role, role, role, role),
            )
            self.assertEqual(self.cursor.fetchone(), (False, False, False, False), role)

    def test_same_name_limited_or_conditional_trigger_fails_closed(self) -> None:
        for replacement in (
            "before insert or update of price_decimal or delete",
            "before insert or update or delete",
        ):
            self.cursor.execute("savepoint before_replacement")
            self.cursor.execute(
                "drop trigger odds_quote_generation_dml on public.odds_outcomes"
            )
            when_clause = "when (false)" if "update of" not in replacement else ""
            self.cursor.execute(
                f"create trigger odds_quote_generation_dml {replacement} "
                f"on public.odds_outcomes for each statement {when_clause} "
                "execute function operations.odds_quote_publication_bump()"
            )
            self.cursor.execute(
                "alter table public.odds_outcomes "
                "enable always trigger odds_quote_generation_dml"
            )
            with self.assertRaisesRegex(psycopg2.Error, "triggers unavailable"):
                self.generation()
            self.cursor.execute("rollback to savepoint before_replacement")
        self.cursor.execute("savepoint before_arguments")
        self.cursor.execute(
            "drop trigger odds_quote_generation_dml on public.odds_outcomes"
        )
        self.cursor.execute(
            "create trigger odds_quote_generation_dml "
            "before insert or update or delete on public.odds_outcomes "
            "for each statement execute function "
            "operations.odds_quote_publication_bump('ignored')"
        )
        self.cursor.execute(
            "alter table public.odds_outcomes "
            "enable always trigger odds_quote_generation_dml"
        )
        with self.assertRaisesRegex(psycopg2.Error, "triggers unavailable"):
            self.generation()
        self.cursor.execute("rollback to savepoint before_arguments")
        self.assertEqual(self.generation(), self.start_generation)

    def test_truncate_does_not_wait_on_writer_singleton_before_rejection(self) -> None:
        self.cursor.execute(QUOTE, (1001, "test_player_over", 2.2, 10))
        done = threading.Event()
        failure: list[BaseException] = []
        result_codes: list[str] = []

        def attempted_truncate() -> None:
            try:
                with psycopg2.connect(self.url) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("set local statement_timeout='2s'")
                        cursor.execute("truncate public.odds_quote_verifications")
            except psycopg2.Error as exc:
                result_codes.append(exc.pgcode)
                if exc.pgcode != "55000" or "TRUNCATE is prohibited" not in str(exc):
                    failure.append(exc)
            finally:
                done.set()

        thread = threading.Thread(target=attempted_truncate, daemon=True)
        thread.start()
        self.assertTrue(done.wait(1), "TRUNCATE waited on the writer's singleton")
        thread.join(timeout=1)
        self.assertFalse(failure, failure)
        self.assertEqual(result_codes, ["55000"])
        # This acquires the ledger lock after the rejected TRUNCATE has left.
        self.cursor.execute(
            "update public.odds_quote_verifications "
            "set price_decimal=price_decimal where fixture_id=-1"
        )
        self.assertGreater(self.generation(), self.start_generation)

    def test_actual_recorder_maintenance_first_and_monotonic_replay(self) -> None:
        self.cursor.execute(QUOTE, (1001, "test_player_over", 2.2, 10))
        after_raw = self.generation()
        observation = dict(OBSERVATION)
        self.cursor.execute(
            "select last_updated_at from public.odds_outcomes where fixture_id=1001"
        )
        observation["source_last_updated_at"] = self.cursor.fetchone()[0].isoformat()
        self.cursor.execute("select now()")
        observation["observed_at"] = self.cursor.fetchone()[0].isoformat()
        self.cursor.execute(
            "select * from public.odds_quote_verifications_record(%s,%s::jsonb)",
            ("test-run-1001", json.dumps([observation])),
        )
        self.assertEqual(self.cursor.fetchone(), (1, 1, 1))
        after_first = self.generation()
        self.assertGreater(after_first, after_raw)
        self.cursor.execute(
            "select * from public.odds_quote_verifications_record(%s,%s::jsonb)",
            ("test-run-1001", json.dumps([observation])),
        )
        self.assertEqual(self.cursor.fetchone(), (1, 1, 0))
        self.assertGreater(self.generation(), after_first)
        self.cursor.execute(
            "select price_decimal::text from public.odds_quote_verifications "
            "where fixture_id=1001"
        )
        self.assertEqual(self.cursor.fetchone()[0], "2.2")

    def test_version_read_does_not_wait_for_uncommitted_writer(self) -> None:
        self.cursor.execute(QUOTE, (1001, "test_player_over", 2.2, 10))
        # This connection owns the singleton update but has not committed.
        with psycopg2.connect(self.url) as reader:
            with reader.cursor() as other:
                other.execute("set local statement_timeout='500ms'")
                before = time.monotonic()
                self.assertEqual(self.generation(other), self.start_generation)
                self.assertLess(time.monotonic() - before, 0.5)

    def test_concurrent_writers_obey_maintenance_then_quote_lock_order(self) -> None:
        self.cursor.execute(
            "update operations.quote_verification_maintenance "
            "set last_cleaned_at=now() where singleton"
        )
        blocked = threading.Event()
        finished = threading.Event()
        failure: list[BaseException] = []

        def second_writer() -> None:
            try:
                with psycopg2.connect(self.url) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("set local statement_timeout='4s'")
                        blocked.set()
                        cursor.execute(QUOTE, (1002, "second_player_over", 3.0, 20))
                        cursor.execute(
                            "update operations.quote_verification_maintenance "
                            "set last_cleaned_at=now() where singleton"
                        )
            except BaseException as exc:
                failure.append(exc)
            finally:
                finished.set()

        thread = threading.Thread(target=second_writer, daemon=True)
        thread.start()
        self.assertTrue(blocked.wait(1))
        self.assertFalse(finished.wait(0.2))
        self.connection.commit()
        self.assertTrue(finished.wait(4))
        thread.join(timeout=1)
        self.assertFalse(failure, failure)
        self.assertGreater(self.generation(), self.start_generation)

    def test_statement_level_cost_shape(self) -> None:
        self.cursor.execute("select pg_current_wal_insert_lsn()")
        start_lsn = self.cursor.fetchone()[0]
        started = time.monotonic()
        self.cursor.execute(
            "insert into public.odds_outcomes("
            "fixture_id,bookmaker_id,market_key,selection_key,line,"
            "price_decimal,participant_type,participant_id,last_updated_at) "
            "select 3000+g,2,'player_shots','player_'||g||'_over',4.5,"
            "2.2,'player',g,now() from generate_series(1,10000) g"
        )
        elapsed_ms = round((time.monotonic() - started) * 1000, 2)
        self.assertEqual(self.generation(), self.start_generation + 1)
        self.cursor.execute("select pg_wal_lsn_diff(pg_current_wal_insert_lsn(), %s)", (start_lsn,))
        wal_bytes = int(self.cursor.fetchone()[0])
        self.assertGreater(wal_bytes, 0)
        print(f"10k-row statement: {elapsed_ms} ms, WAL {wal_bytes} bytes, generation +1")


if __name__ == "__main__":
    unittest.main()
