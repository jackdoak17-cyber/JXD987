from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.odds_quote_verification import load_document, record_spool, validate_document


class OddsQuoteVerificationDeliveryTests(unittest.TestCase):
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
            pending = Path(directory) / "pending"
            pending.mkdir()
            claimed = pending / ".p1.json.gz.processing-old-host-1"
            with gzip.open(claimed, "wt", encoding="utf-8") as handle:
                json.dump(self.document(), handle)
            old = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
            os.utime(claimed, (old, old))
            with patch("psycopg2.connect", side_effect=RuntimeError("database unavailable")):
                with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                    record_spool(Path(directory), "postgresql://invalid")

            self.assertTrue((pending / "p1.json.gz").exists())


if __name__ == "__main__":
    unittest.main()
