from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.validate_odds_delivery_source import validate_source


class ValidateOddsDeliverySourceTests(unittest.TestCase):
    @staticmethod
    def database(directory: str, *, include_current: bool) -> Path:
        path = Path(directory) / "jxd.sqlite"
        kickoff = datetime.now(timezone.utc) + (timedelta(days=1) if include_current else -timedelta(days=60))
        with sqlite3.connect(path) as connection:
            connection.executescript("""
                create table fixtures (id integer primary key, league_id integer, starting_at text);
                create table odds_outcomes (
                    fixture_id integer, market_key text, last_updated_at text
                );
            """)
            connection.execute("insert into fixtures values (1,8,?)", (kickoff.isoformat(),))
            connection.execute("insert into odds_outcomes values (1,'moneyline',?)", (datetime.now(timezone.utc).isoformat(),))
        return path

    def test_current_populated_database_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = validate_source(self.database(directory, include_current=True), [8], 14)
            self.assertEqual(report["fixture_count"], 1)
            self.assertEqual(report["moneyline_count"], 1)

    def test_obsolete_database_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "zero fixtures"):
                validate_source(self.database(directory, include_current=False), [8], 14)

    def test_relative_database_path_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "absolute"):
            validate_source(Path("data/jxd.sqlite"), [8], 14)


if __name__ == "__main__":
    unittest.main()
