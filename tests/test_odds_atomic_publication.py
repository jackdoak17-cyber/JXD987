"""File and lock boundaries for one-commit odds publication."""

from __future__ import annotations

import fcntl
import os
import subprocess
import socket
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import export_odds_to_supabase_psql as exporter
from scripts import odds_delivery_quote_verification as quotes
from scripts.odds_atomic_publication import AtomicOddsPublication


class AtomicOddsPublicationTests(unittest.TestCase):
    def test_direct_export_cli_fails_before_opening_any_source(self) -> None:
        with patch.dict(os.environ, {"ODDS_ATOMIC_PUBLISH_MODE": ""}), patch.object(
            sys, "argv", ["export_odds_to_supabase_psql.py"]
        ):
            with self.assertRaisesRegex(SystemExit, "requires ODDS_ATOMIC_PUBLISH_MODE"):
                exporter.main()

    def test_pending_files_restore_after_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spool = Path(directory)
            pending = spool / "pending"
            pending.mkdir()
            for name in ("p1.json", "p2.json.gz"):
                (pending / name).write_text("evidence", encoding="utf-8")
            claims = quotes.claim_pending(spool)
            self.assertEqual(len(claims), 2)
            self.assertFalse((pending / "p1.json").exists())
            self.assertFalse((pending / "p2.json.gz").exists())
            quotes.restore_claimed(claims)
            self.assertEqual(
                sorted(path.name for path in pending.glob("*.json*")),
                ["p1.json", "p2.json.gz"],
            )

    def test_files_archive_only_on_explicit_finalization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spool = Path(directory)
            pending = spool / "pending"
            pending.mkdir()
            (pending / "p2.json").write_text("evidence", encoding="utf-8")
            claims = quotes.claim_pending(spool)
            self.assertFalse((spool / "processed" / "p2.json").exists())
            quotes.archive_claimed(claims, spool)
            self.assertTrue((spool / "processed" / "p2.json").exists())
            self.assertFalse((pending / "p2.json").exists())

    def test_dead_local_claim_is_recovered_without_waiting_thirty_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spool = Path(directory)
            pending = spool / "pending"
            pending.mkdir()
            dead = pending / f".p2.json.processing-{socket.gethostname()}-999999999"
            dead.write_text("evidence", encoding="utf-8")
            claims = quotes.claim_pending(spool)
            self.assertEqual([original.name for original, _claimed in claims], ["p2.json"])
            quotes.restore_claimed(claims)
            self.assertTrue((pending / "p2.json").exists())

    def test_monotonic_match_without_exact_ledger_is_rejected(self) -> None:
        document = {"run_id": "observed-run-1001", "quotes": [{"fixture_id": 1001}]}
        prepared = [(document, document["quotes"], [])]
        with patch.object(quotes, "record_document", return_value={
            "supplied": 1, "matched": 1, "written": 0,
        }), patch.object(
            quotes, "assert_current_verified",
            side_effect=RuntimeError("old ledger identity"),
        ) as exact:
            with self.assertRaisesRegex(RuntimeError, "old ledger identity"):
                quotes.record_claimed(object(), prepared)
            exact.assert_called_once()

    def test_postcommit_archive_error_returns_unarchived_file_to_pending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spool = Path(directory)
            pending = spool / "pending"
            pending.mkdir()
            (pending / "p2.json").write_text("evidence", encoding="utf-8")
            publication = object.__new__(AtomicOddsPublication)
            publication.mode = "delivery"
            publication.spool_dir = spool
            publication.claims = quotes.claim_pending(spool)
            publication.committed = True
            publication.release_report = None
            with patch(
                "scripts.odds_atomic_publication.archive_claimed",
                side_effect=OSError("archive unavailable"),
            ):
                with self.assertRaisesRegex(OSError, "archive unavailable"):
                    publication.finalize()
            self.assertTrue((pending / "p2.json").exists())

    def test_p3_writer_fd_is_released_while_parent_still_open(self) -> None:
        with tempfile.NamedTemporaryFile() as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)

            def pass_writer_fd() -> None:
                os.dup2(lock.fileno(), 9)

            child = subprocess.run(
                [sys.executable, "-c",
                 "from scripts.odds_atomic_publication import AtomicOddsPublication;"
                 "AtomicOddsPublication.release_p3_writer_lock()"],
                cwd=Path(__file__).resolve().parents[1],
                env={**os.environ, "ODDS_SYNC_LOCK_FILE": lock.name},
                preexec_fn=pass_writer_fd,
                close_fds=False,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(child.returncode, 0, child.stderr)
            with open(lock.name, "r+") as other:
                fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(other.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    unittest.main()
