"""Full-row parity of the installed capture method against its prior SQL.

Uses only a disposable local PostgreSQL cluster; no hosted URL is read.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2

from scripts.odds_atomic_publication import AtomicOddsPublication
from scripts.odds_delivery_quote_verification import CUSTOMER_MARKETS


ROOT = Path(__file__).resolve().parents[1]
PG_BIN = Path(os.environ.get("ODDS_TEST_PG_BIN", "/opt/homebrew/opt/postgresql@17/bin"))
ORIGINAL = (ROOT / "tests/fixtures/odds_preusable_capture_original.sql").read_text()
SCHEMA = """
create table public.fixtures(id bigint primary key, starting_at timestamptz not null);
create table public.odds_outcomes(
  id bigserial primary key, fixture_id bigint not null, bookmaker_id integer not null,
  market_key text not null, selection_key text not null, participant_type text,
  participant_id bigint, line numeric, price_decimal numeric not null,
  last_updated_at timestamptz);
create table public.odds_quote_verifications(
  fixture_id bigint not null, bookmaker_id integer not null, market_key text not null,
  selection_key text not null, participant_type text not null, participant_id bigint not null,
  line_key numeric not null, price_decimal numeric not null,
  source_last_updated_at timestamptz, last_verified_at timestamptz not null,
  primary key(fixture_id,bookmaker_id,market_key,selection_key,
    participant_type,participant_id,line_key));
"""


def seed(cur):
    now = datetime.now(timezone.utc)
    fixtures = {
        101: now + timedelta(hours=1, minutes=59),
        102: now + timedelta(hours=2, minutes=1),
        103: now + timedelta(hours=23, minutes=59),
        104: now + timedelta(days=1, minutes=1),
        105: now - timedelta(minutes=1),
    }
    for fixture_id, kickoff in fixtures.items():
        cur.execute("insert into public.fixtures values(%s,%s)", (fixture_id, kickoff))
    sequence = 0

    def quote(fixture_id, market, label, *, age_minutes=30, price=2,
              source="timestamp", ledger_price=None, ledger_source="same",
              participant_type="player", participant_id=7, ledger_type=None,
              ledger_id=None, line=0.5, ledger_line=None, write_ledger=True):
        nonlocal sequence
        sequence += 1
        selection = f"{label}_{sequence}"
        source_time = None if source == "null" else now - timedelta(minutes=10)
        ledger_time = (source_time if ledger_source == "same" else None if ledger_source == "null"
                       else now - timedelta(minutes=11))
        cur.execute("""insert into public.odds_outcomes
          (fixture_id,bookmaker_id,market_key,selection_key,participant_type,
           participant_id,line,price_decimal,last_updated_at)
          values(%s,2,%s,%s,%s,%s,%s,%s,%s)""",
          (fixture_id,market,selection,participant_type,participant_id,line,price,source_time))
        if write_ledger:
            cur.execute("""insert into public.odds_quote_verifications
              (fixture_id,bookmaker_id,market_key,selection_key,
               participant_type,participant_id,line_key,price_decimal,
               source_last_updated_at,last_verified_at)
              values(%s,2,%s,%s,%s,%s,%s,%s,%s,%s)""",
              (fixture_id,market,selection,
               ledger_type if ledger_type is not None else (participant_type or "team"),
               ledger_id if ledger_id is not None else (participant_id or 0),
               ledger_line if ledger_line is not None else (line if line is not None else -9999),
               price if ledger_price is None else ledger_price,ledger_time,
               now - timedelta(minutes=age_minutes)))

    for market in sorted(CUSTOMER_MARKETS):
        quote(101,market,"tier2h",age_minutes=119)
        quote(102,market,"tier3h_lower",age_minutes=179,source="null",
              participant_type=None,participant_id=None,line=None)
        quote(103,market,"tier3h_upper",age_minutes=179)
        quote(104,market,"tier8h",age_minutes=479)
    quote(101,"player_shots","stale2h",age_minutes=121)
    quote(102,"player_shots","stale3h",age_minutes=181)
    quote(104,"player_shots","stale8h",age_minutes=481)
    quote(104,"player_shots","future_skew_ok",age_minutes=-1)
    quote(104,"player_shots","future_skew_bad",age_minutes=-3)
    quote(104,"player_shots","wrong_price",ledger_price=3)
    quote(104,"player_shots","wrong_source",ledger_source="different")
    quote(104,"player_shots","wrong_type",ledger_type="team")
    quote(104,"player_shots","wrong_id",ledger_id=8)
    quote(104,"player_shots","wrong_line",ledger_line=1.5)
    quote(104,"player_shots","missing_ledger",write_ledger=False)
    quote(104,"player_shots","null_source_mismatch",source="null",ledger_source="different")
    quote(104,"player_shots","price1",price=1)
    quote(104,"player_shots","price500",price=500)
    quote(104,"player_shots","price_over500",price=500.01)
    quote(105,"player_shots","past_fixture")
    quote(104,"unsupported_alias","unsupported_market")
    return sequence


def canonical(rows):
    return sorted(json.dumps(row, default=str) for row in rows)


@unittest.skipUnless((PG_BIN / "initdb").exists(), "local PostgreSQL 17 is unavailable")
class PreUsableCapturePostgresTests(unittest.TestCase):
    def test_actual_capture_matches_original_full_rows(self):
        with tempfile.TemporaryDirectory(prefix="odds-preusable-") as directory:
            root = Path(directory)
            data = root / "data"
            subprocess.run([str(PG_BIN / "initdb"), "-D", str(data),
                            "--auth-local=trust", "--no-instructions"],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            started = False
            try:
                subprocess.run([str(PG_BIN / "pg_ctl"), "-D", str(data),
                                "-l", str(root / "postgres.log"),
                                "-o", f"-c listen_addresses='' -k {root} -p 55459", "start"],
                               check=True, stdout=subprocess.DEVNULL)
                started = True
                conn = psycopg2.connect(host=str(root), port=55459, dbname="postgres")
                try:
                    with conn.cursor() as cur:
                        cur.execute(SCHEMA)
                        seeded = seed(cur)
                    conn.commit()
                    with conn.cursor() as cur:
                        cur.execute("set transaction isolation level repeatable read")
                        cur.execute(ORIGINAL, (sorted(CUSTOMER_MARKETS),))
                        before = cur.fetchall()
                        publication = object.__new__(AtomicOddsPublication)
                        publication.connection = conn
                        publication.capture_pre_usable()
                        cur.execute("select * from odds_pre_usable")
                        after = cur.fetchall()
                    self.assertEqual(seeded, 101)
                    self.assertEqual(len(before), 86)
                    self.assertEqual(canonical(before), canonical(after))
                    conn.rollback()
                finally:
                    conn.close()
            finally:
                if started:
                    subprocess.run([str(PG_BIN / "pg_ctl"), "-D", str(data),
                                    "-m", "immediate", "stop"], check=True,
                                   stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
