import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.export_to_supabase import choose_keep_seasons
from scripts.export_odds_to_supabase_psql import stage_and_upsert


class ExportSeasonRetentionTests(unittest.TestCase):
    def test_team_history_export_can_retain_all_provider_seasons(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            create table seasons (
                id integer primary key,
                league_id integer not null,
                is_current integer not null,
                end_date text
            )
            """
        )
        conn.executemany(
            "insert into seasons(id, league_id, is_current, end_date) values (?, ?, ?, ?)",
            [
                (100, 8, 1, "2027-05-31"),
                (101, 8, 0, "2026-05-31"),
                (102, 8, 0, "2025-05-31"),
            ],
        )
        conn.commit()

        self.assertEqual(choose_keep_seasons(conn, [8]), {100, 101})
        self.assertEqual(choose_keep_seasons(conn, [8], keep_all=True), {100, 101, 102})


class ExportOddsConflictContractTests(unittest.TestCase):
    def test_upsert_arbitrates_null_normalized_and_legacy_unique_keys(self):
        captured_sql = {}

        def fake_run_psql(*args, **kwargs):
            captured_sql["text"] = Path(args[1]).read_text(encoding="utf-8")
            return "1\t1\t0\n"

        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "odds.csv"
            csv_path.write_text(
                "fixture_id,bookmaker_id,market_key,selection_key,line,price_decimal,"
                "price_american,participant_type,participant_id,last_updated_at\n"
                "1,2,moneyline,home,,2.1,-110,team,10,2026-09-01T00:00:00Z\n",
                encoding="utf-8",
            )
            with patch(
                "scripts.export_odds_to_supabase_psql.shutil.which",
                return_value="/usr/bin/psql",
            ), patch(
                "scripts.export_odds_to_supabase_psql.run_psql",
                side_effect=fake_run_psql,
            ):
                counts = stage_and_upsert(
                    "postgresql://db",
                    csv_path,
                    "contract-test",
                    [8],
                    2,
                    14,
                    {"moneyline"},
                    False,
                    None,
                    None,
                    calendar_window=True,
                )

        self.assertEqual(counts["upserted_total"], 1)
        self.assertEqual(captured_sql["text"].count("on conflict ("), 1)
        self.assertIn("selection_key,", captured_sql["text"])
        self.assertIn("(coalesce(line, -9999))", captured_sql["text"])
        self.assertIn("coalesce(line, -9999)", captured_sql["text"])


if __name__ == "__main__":
    unittest.main()
