import sqlite3
import unittest

from scripts.export_to_supabase import choose_keep_seasons


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


if __name__ == "__main__":
    unittest.main()
