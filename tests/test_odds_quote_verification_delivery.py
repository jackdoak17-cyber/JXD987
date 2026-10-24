from __future__ import annotations

import gzip
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.odds_quote_verification import (
    current_customer_quotes,
    load_document,
    record_spool,
    validate_document,
)


class OddsQuoteVerificationDeliveryTests(unittest.TestCase):
    @staticmethod
    def sqlite_database(directory: str, *, price: float = 5.0) -> Path:
        path = Path(directory) / "jxd.sqlite"
        with sqlite3.connect(path) as connection:
            connection.executescript("""
                create table fixtures (id integer primary key, league_id integer, starting_at text);
                create table odds_outcomes (
                    fixture_id integer, bookmaker_id integer, market_key text,
                    selection_key text, participant_type text, participant_id integer,
                    line numeric, price_decimal numeric, last_updated_at text
                );
            """)
            kickoff = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
            connection.execute("insert into fixtures values (19728194, 8, ?)", (kickoff,))
            connection.execute(
                "insert into odds_outcomes values (19728194,2,'moneyline','home','team',51,null,?,?)",
                (price, "2026-10-18T10:00:00Z"),
            )
        return path

    @staticmethod
    def document() -> dict:
        return {
            "schema_version": 1,
            "source": "odds_api_provider_response",
            "run_id": "p1-20261021T120000-123",
            "quotes": [{
                "fixture_id": 19728194,
                "bookmaker_id": 2,
                "market_key": "moneyline",
                "selection_key": "home",
                "participant_type": "team",
                "participant_id": 51,
                "line": None,
                "price_decimal": 5.0,
                "source_last_updated_at": "2026-10-18T10:00:00Z",
                "observed_at": datetime.now(timezone.utc).isoformat(),
            }],
        }

    def test_claimed_compressed_file_is_read_and_validated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".p1.json.gz.processing-host-1"
            with gzip.open(path, "wt", encoding="utf-8") as handle:
                json.dump(self.document(), handle)

            self.assertEqual(len(validate_document(load_document(path))), 1)

    def test_replay_older_than_seven_days_fails_closed(self) -> None:
        document = self.document()
        document["quotes"][0]["observed_at"] = (
            datetime.now(timezone.utc) - timedelta(days=8)
        ).isoformat()

        with self.assertRaisesRegex(ValueError, "invalid quote"):
            validate_document(document)

    def test_stale_worker_claim_is_restored_before_retry(self) -> None:
        from unittest.mock import patch
        import os

        with tempfile.TemporaryDirectory() as directory:
            sqlite_db = self.sqlite_database(directory)
            pending = Path(directory) / "pending"
            pending.mkdir()
            claimed = pending / ".p1.json.gz.processing-old-host-1"
            with gzip.open(claimed, "wt", encoding="utf-8") as handle:
                json.dump(self.document(), handle)
            old = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
            os.utime(claimed, (old, old))
            with patch("psycopg2.connect", side_effect=RuntimeError("database unavailable")):
                with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                    record_spool(Path(directory), "postgresql://invalid", sqlite_db, [8])

            self.assertTrue((pending / "p1.json.gz").exists())

    def test_only_current_local_quote_is_eligible_for_acknowledgement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sqlite_db = self.sqlite_database(directory)
            current = self.document()["quotes"][0]
            stale = {**current, "price_decimal": 4.5}

            self.assertEqual(current_customer_quotes(sqlite_db, [current, stale], [8]), [current])

    def test_wrong_league_quote_is_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sqlite_db = self.sqlite_database(directory)
            quote = self.document()["quotes"][0]

            self.assertEqual(current_customer_quotes(sqlite_db, [quote], [9]), [])

    def test_partial_database_match_keeps_evidence_pending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sqlite_db = self.sqlite_database(directory)
            pending = Path(directory) / "pending"
            pending.mkdir()
            evidence = pending / "p1.json"
            evidence.write_text(json.dumps(self.document()))

            cursor = MagicMock()
            cursor.__enter__.return_value = cursor
            cursor.fetchone.return_value = (1, 0, 0)
            connection = MagicMock()
            connection.__enter__.return_value = connection
            connection.cursor.return_value = cursor

            with patch("psycopg2.connect", return_value=connection):
                with self.assertRaisesRegex(RuntimeError, "matched 0 of 1"):
                    record_spool(Path(directory), "postgresql://test", sqlite_db, [8])

            self.assertTrue(evidence.exists())
            self.assertFalse((Path(directory) / "processed" / evidence.name).exists())

    def test_started_fixture_unmatched_frozen_quote_is_superseded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sqlite_db = self.sqlite_database(directory)
            with sqlite3.connect(sqlite_db) as connection:
                connection.execute(
                    "update fixtures set starting_at = ? where id = 19728194",
                    ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),),
                )
            pending = Path(directory) / "pending"
            pending.mkdir()
            evidence = pending / "p1.json"
            evidence.write_text(json.dumps(self.document()))

            cursor = MagicMock()
            cursor.__enter__.return_value = cursor
            cursor.fetchone.return_value = (1, 0, 0)
            connection = MagicMock()
            connection.__enter__.return_value = connection
            connection.cursor.return_value = cursor

            with patch("psycopg2.connect", return_value=connection):
                result = record_spool(Path(directory), "postgresql://test", sqlite_db, [8])

            self.assertEqual(result["superseded"], 1)
            self.assertFalse(evidence.exists())
            self.assertTrue((Path(directory) / "processed" / evidence.name).exists())


if __name__ == "__main__":
    unittest.main()
