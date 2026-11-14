"""Disposable PostgreSQL checks for scoped restore transaction boundaries.

Set C476_DISPOSABLE_PARENT and C476_PG_BIN to run this integration test.
The parent must be on a different filesystem from the root volume.
"""

import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import export_fixture_scoped_psql as scoped_restore


TABLES = ("fixture_players", "fixture_statistics", "fixture_player_statistics", "odds_outcomes")


class ScopedRestoreTransactionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parent = os.environ.get("C476_DISPOSABLE_PARENT")
        pg_bin = Path(os.environ.get("C476_PG_BIN", ""))
        if not parent or not (pg_bin / "initdb").is_file() or not (pg_bin / "pg_ctl").is_file():
            raise unittest.SkipTest("requires explicit disposable PostgreSQL parent and binaries")
        parent = Path(parent).resolve(strict=True)
        if parent.stat().st_dev == Path("/").stat().st_dev:
            raise AssertionError("disposable PostgreSQL parent must be off the root volume")

        cls._data_tmp = tempfile.TemporaryDirectory(prefix="c476-restore-", dir=parent)
        cls.addClassCleanup(cls._data_tmp.cleanup)
        cls._socket_tmp = tempfile.TemporaryDirectory(prefix="c476-sock-", dir="/private/tmp")
        cls.addClassCleanup(cls._socket_tmp.cleanup)
        data = Path(cls._data_tmp.name) / "data"
        pg_env = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("PG", "SUPABASE_", "LEADERBOARD_", "M3_"))
            and key != "DATABASE_URL"
        }
        cls._pg_env = pg_env
        cls._pg_ctl = str(pg_bin / "pg_ctl")
        cls._data = data
        cls._port = 56000 + os.getpid() % 3000
        subprocess.run(
            [str(pg_bin / "initdb"), "-D", str(data), "-A", "trust", "-U", "postgres", "--no-instructions"],
            env=pg_env, check=True, capture_output=True, text=True,
        )
        subprocess.run(
            [cls._pg_ctl, "-D", str(data), "-l", str(Path(cls._data_tmp.name) / "postgres.log"),
             "-o", f"-F -c listen_addresses='' -k {cls._socket_tmp.name} -p {cls._port}", "start"],
            env=pg_env, check=True, capture_output=True, text=True,
        )
        cls.addClassCleanup(lambda: subprocess.run(
            [cls._pg_ctl, "-D", str(data), "stop", "-m", "immediate"],
            env=pg_env, check=True, capture_output=True, text=True,
        ))
        cls._dsn = f"host={cls._socket_tmp.name} port={cls._port} dbname=postgres user=postgres connect_timeout=3"
        with psycopg2.connect(cls._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("select current_database(), current_setting('data_directory'), inet_server_addr()")
                database, directory, server_address = cur.fetchone()
                assert database == "postgres" and directory == str(data) and server_address is None
                for table in TABLES:
                    cur.execute(f"create table public.{table} (fixture_id bigint not null, value text not null)")

    def setUp(self):
        self.writer = psycopg2.connect(self._dsn)
        self.reader = psycopg2.connect(self._dsn)
        self.addCleanup(self.writer.close)
        self.addCleanup(self.reader.close)
        with self.writer.cursor() as cur:
            for table in TABLES:
                cur.execute(f"truncate public.{table}")
                cur.execute(f"insert into public.{table} values (1, 'old')")
        self.writer.commit()

    def values(self):
        with self.reader.cursor() as cur:
            result = {}
            for table in TABLES:
                cur.execute(f"select value from public.{table} where fixture_id=1")
                result[table] = [row[0] for row in cur.fetchall()]
        self.reader.commit()
        return result

    def test_later_copy_failure_rolls_back_every_table(self):
        new = [{"fixture_id": 1, "value": "new"}]
        invalid = [{"fixture_id": "invalid", "value": "new"}]
        with self.assertRaises(psycopg2.Error):
            scoped_restore.restore_fixture_tables(self.writer, [1], new, new, new, invalid)
        self.assertEqual(self.values(), {table: ["old"] for table in TABLES})
        with self.reader.cursor() as cur:
            cur.execute("lock table public.fixture_players, public.fixture_player_statistics in share mode nowait")
        self.reader.commit()

    def test_intermediate_state_is_invisible_and_publication_lock_conflicts(self):
        new = [{"fixture_id": 1, "value": "new"}]
        original = scoped_restore.copy_table
        observed = []

        def probe(conn, table, rows, columns, fixture_ids):
            result = original(conn, table, rows, columns, fixture_ids)
            if table == "fixture_players":
                observed.append(self.values()["fixture_players"])
                with self.assertRaises(psycopg2.errors.LockNotAvailable):
                    with self.reader.cursor() as cur:
                        cur.execute("lock table public.fixture_players in share mode nowait")
                self.reader.rollback()
            return result

        with patch.object(scoped_restore, "copy_table", side_effect=probe):
            counts = scoped_restore.restore_fixture_tables(self.writer, [1], new, new, new, [])
        self.assertEqual(observed, [["old"]])
        self.assertEqual(counts, {
            "fixture_players": 1, "fixture_statistics": 1,
            "fixture_player_statistics": 1, "odds_outcomes": 0,
        })
        self.assertEqual(self.values(), {
            "fixture_players": ["new"], "fixture_statistics": ["new"],
            "fixture_player_statistics": ["new"], "odds_outcomes": [],
        })

    def test_importer_waits_for_publication_source_lock(self):
        new = [{"fixture_id": 1, "value": "new"}]
        with self.reader.cursor() as cur:
            cur.execute("lock table public.fixture_players, public.fixture_player_statistics in share mode nowait")

        finished = threading.Event()
        errors = []

        def restore():
            try:
                with psycopg2.connect(self._dsn + " application_name=c476_importer") as conn:
                    scoped_restore.restore_fixture_tables(conn, [1], new, new, new, new)
            except Exception as exc:
                errors.append(exc)
            finally:
                finished.set()

        worker = threading.Thread(target=restore, daemon=True)
        worker.start()
        self.addCleanup(lambda: (self.reader.rollback(), worker.join(timeout=5)))
        deadline = time.monotonic() + 5
        waiting = False
        while time.monotonic() < deadline:
            with self.writer.cursor() as cur:
                cur.execute("""select exists (
                    select 1 from pg_stat_activity
                    where application_name='c476_importer' and wait_event_type='Lock'
                )""")
                waiting = cur.fetchone()[0]
            self.writer.commit()
            if waiting:
                break
            time.sleep(0.02)
        self.assertTrue(waiting, "importer should wait behind the publication source lock")
        self.assertFalse(finished.is_set())
        self.reader.commit()
        worker.join(timeout=5)
        self.assertTrue(finished.is_set())
        self.assertEqual(errors, [])
        self.assertEqual(self.values(), {table: ["new"] for table in TABLES})


if __name__ == "__main__":
    unittest.main()
