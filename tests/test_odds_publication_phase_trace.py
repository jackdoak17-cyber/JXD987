"""The atomic publisher must report the failing execute and still roll back."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.export_odds_to_supabase_psql import stage_and_upsert
from scripts.odds_atomic_publication import AtomicOddsPublication, PublicationPhaseTrace


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, _params=None):
        self.last_sql = sql
        self.connection.statements.append(sql)
        if "insert into public.odds_outcomes as o" in sql:
            raise TimeoutError("simulated statement timeout")

    def copy_expert(self, sql, _source):
        self.connection.statements.append(sql)

    def fetchone(self):
        if "from odds_outcomes_stage" in self.last_sql and "count(*)" in self.last_sql:
            return (1,)
        if "deleted_existing" in self.last_sql:
            return ("deleted_existing", 0)
        if "classified as" in self.last_sql:
            return (0, 0, 0, 0, 0, 1)
        if "src_count" in self.last_sql:
            return ("src_count", 1)
        raise AssertionError("unexpected fetch")


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class PublicationPhaseTraceTests(unittest.TestCase):
    def test_broken_log_pipe_does_not_turn_committed_step_into_failure(self):
        trace = PublicationPhaseTrace()
        with patch("builtins.print", side_effect=BrokenPipeError("closed log pipe")):
            with trace.step("transaction_commit"):
                pass
        self.assertIsNone(trace.failed_phase)
        self.assertEqual(trace.timings[-1]["status"], "ok")

    def test_broken_log_pipe_does_not_mask_database_failure(self):
        trace = PublicationPhaseTrace()
        with patch("builtins.print", side_effect=BrokenPipeError("closed log pipe")):
            with self.assertRaisesRegex(TimeoutError, "database timeout"):
                with trace.step("canonical_upsert"):
                    raise TimeoutError("database timeout")
        self.assertEqual(trace.failed_phase, "canonical_upsert")
        self.assertEqual(trace.timings[-1]["status"], "failed")

    def test_failed_upsert_is_named_and_uncommitted_transaction_rolls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "odds.csv"
            csv_path.write_text("fixture_id,bookmaker_id\n", encoding="utf-8")
            publication = object.__new__(AtomicOddsPublication)
            publication.connection = FakeConnection()
            publication.phase_trace = PublicationPhaseTrace()
            publication.committed = False
            publication.claims = []
            publication.moneyline_report = None
            publication.release_report = None

            try:
                with self.assertRaisesRegex(TimeoutError, "simulated statement timeout"):
                    stage_and_upsert(
                        "unused", csv_path, "test", [384], 0, 14,
                        ["moneyline"], False, None, None, True,
                        atomic_context=publication,
                    )
            finally:
                publication.close()

            self.assertEqual(publication.phase_trace.failed_phase, "canonical_upsert")
            self.assertEqual(publication.phase_trace.timings[-1]["status"], "failed")
            self.assertEqual(publication.phase_trace.timings[-1]["phase"], "canonical_upsert")
            self.assertEqual(publication.connection.rollbacks, 1)
            self.assertTrue(publication.connection.closed)
            self.assertNotIn("commit;", publication.connection.statements)


if __name__ == "__main__":
    unittest.main()
