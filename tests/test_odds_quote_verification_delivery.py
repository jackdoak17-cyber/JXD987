from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.odds_quote_verification import (load_document, record_spool, validate_document, write_observation_spool, append_run_metrics)


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

    def test_actual_provider_observations_spool_with_exact_quote_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            quote = dict(self.document()["quotes"][0])
            path = write_observation_spool(Path(directory), "p2", [quote, quote])
            self.assertTrue(path.name.startswith("p2-"))
            self.assertEqual(path.parent.name, "pending")
            self.assertEqual(load_document(path)["quotes"], [quote])
            self.assertEqual(len(validate_document(load_document(path))), 1)

    def test_zero_provider_quotes_are_reported_as_zero_not_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = write_observation_spool(Path(directory), "p1", [])
            document = load_document(path)
            self.assertEqual(document["quotes"], [])
            metrics_path = Path(directory) / "run-metrics.jsonl"
            append_run_metrics(metrics_path, document, {"supplied": 0, "matched": 0, "written": 0})
            event = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual(event["supplied"], 0)
            self.assertEqual(event["matched"], 0)
            self.assertEqual(event["unmatched"], 0)
            self.assertEqual(event["tier"], "p1")

    def test_run_metric_aggregates_report_unmatched_evidence_without_fake_freshness(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            document = self.document()
            metrics_path = Path(directory) / "run-metrics.jsonl"
            append_run_metrics(metrics_path, document, {"supplied": 10, "matched": 7, "written": 3})
            event = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual((event["supplied"], event["matched"], event["written"], event["unmatched"]), (10, 7, 3, 3))
            self.assertEqual(event["observed_last_at"], document["quotes"][0]["observed_at"])

    def test_no_publication_or_heartbeat_can_create_a_provider_quote_spool(self) -> None:
        from pathlib import Path as _Path
        source = _Path("scripts/sync_odds.py").read_text(encoding="utf-8")
        self.assertIn('provider_observed_at = datetime.now(timezone.utc).isoformat()', source)
        self.assertIn('odds_batch = client.request(', source)
        self.assertIn('observed_at = odds_record.get("provider_observed_at")', source)
        self.assertIn('source_at = row.get("last_updated_at")', source)
        self.assertNotIn('provider_observed_at = datetime.now(timezone.utc).isoformat()\n    session.commit()', source)

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
