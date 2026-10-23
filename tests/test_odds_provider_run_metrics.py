from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.odds_provider_run_metrics import (
    summarize_fetch, summarize_history, write_run_metric,
)


class ProviderRunMetricsTests(unittest.TestCase):
    @staticmethod
    def coverage() -> list[dict]:
        return [
            {"matching_status": "matched", "odds_response_status": "received",
             "moneyline_sides_by_bookmaker": {"Bet365": ["home", "away"], "Paddy Power": ["home", "draw", "away"]}},
            {"matching_status": "provider_gap", "odds_response_status": "not_applicable",
             "moneyline_sides_by_bookmaker": {}},
            {"matching_status": "matched", "odds_response_status": "missing",
             "moneyline_sides_by_bookmaker": {}},
        ]

    def test_counts_are_provider_observations_not_falsely_claimed_verified(self) -> None:
        data = summarize_fetch(
            run_id="p2-20261023T180000-1000", tier="p2",
            fixtures_in_scope=3, coverage=self.coverage(),
            observations=[{"fixture_id": 1}], api_calls=4, errors=[],
        )
        self.assertEqual(data["normalized_observations"], 1)
        self.assertEqual(data["fixtures_in_scope"], 3)
        self.assertEqual(data["matching_status_counts"], {"matched": 2, "provider_gap": 1})
        self.assertEqual(data["response_status_counts"]["missing"], 1)
        self.assertEqual(data["returned_moneyline_side_gaps"], 1)
        self.assertIsNone(data["canonical_quotes_matched"])
        self.assertIsNone(data["canonical_quotes_unmatched"])

    def test_partially_failed_run_does_not_appear_successful(self) -> None:
        data = summarize_fetch(
            run_id="p1-20261023T170000-10", tier="p1",
            fixtures_in_scope=6, coverage=self.coverage(), observations=[{}],
            api_calls=7, errors=["league 8 HTTP 503", "league 564 timed out"],
        )
        self.assertEqual(data["provider_fetch_status"], "partial_failure")
        self.assertEqual(data["error_count"], 2)
        self.assertEqual(data["normalized_observations"], 1)
        with tempfile.TemporaryDirectory() as d:
            write_run_metric(Path(d), data)
            summary = summarize_history(Path(d))
        self.assertEqual(summary["tiers"]["p1"]["partial_failures"], 1)

    def test_empty_success_is_not_evidence_of_fresh_quotes(self) -> None:
        data = summarize_fetch(
            run_id="p1-20261023T170000-11", tier="p1",
            fixtures_in_scope=0, coverage=[], observations=[], api_calls=0, errors=[],
        )
        self.assertEqual(data["normalized_observations"], 0)
        self.assertEqual(data["provider_fetch_status"], "completed")
        with tempfile.TemporaryDirectory() as d:
            write_run_metric(Path(d), data)
            self.assertEqual(summarize_history(Path(d))["tiers"]["p1"]["zero_observation_runs"], 1)

    def test_run_replay_is_idempotent_and_conflicting_replay_rejected(self) -> None:
        data = summarize_fetch(
            run_id="p3-20261023T170000-11", tier="p3",
            fixtures_in_scope=0, coverage=[], observations=[], api_calls=0, errors=[],
        )
        with tempfile.TemporaryDirectory() as d:
            path = write_run_metric(Path(d), data)
            self.assertEqual(write_run_metric(Path(d), data), path)
            with self.assertRaisesRegex(ValueError, "conflicting metrics"):
                write_run_metric(Path(d), {**data, "api_calls": 100})
            self.assertEqual(len(list(Path(d).glob("*.json"))), 1)

    def test_retention_deletes_only_metric_files_older_than_30_days(self) -> None:
        now = datetime.now(timezone.utc)
        data = summarize_fetch(
            run_id="p3-20261023T180000-11", tier="p3",
            fixtures_in_scope=1, coverage=[], observations=[], api_calls=0, errors=[],
        )
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            old = root / "p1-old-run.json"
            old.write_text(json.dumps(data), encoding="utf-8")
            stamp = (now - timedelta(days=31)).timestamp()
            os.utime(old, (stamp, stamp))
            unrelated = root / "readme.txt"
            unrelated.write_text("keep", encoding="utf-8")
            unrelated_json = root / "unrelated.json"
            unrelated_json.write_text("{}", encoding="utf-8")
            os.utime(unrelated_json, (stamp, stamp))
            write_run_metric(root, data, now=now)
            self.assertFalse(old.exists())
            self.assertTrue(unrelated.exists())
            self.assertTrue(unrelated_json.exists())
            self.assertEqual(summarize_history(root)["tiers"]["p3"]["runs"], 1)

    def test_history_summary_distinguishes_provider_and_canonical_matching(self) -> None:
        data = summarize_fetch(
            run_id="p2-20261023T160000-11", tier="p2",
            fixtures_in_scope=2, coverage=self.coverage(), observations=[{}, {}],
            api_calls=4, errors=[],
        )
        with tempfile.TemporaryDirectory() as d:
            write_run_metric(Path(d), data)
            summary = summarize_history(Path(d))
            self.assertEqual(summary["scope"], "provider_fetch_only_not_canonical_verification")
            self.assertEqual(summary["tiers"]["p2"]["normalized_observations"], 2)
            self.assertEqual(summary["tiers"]["p2"]["fixtures_missing_event"], 1)

    def test_reject_incorrect_tier_and_unsafe_run_id(self) -> None:
        with self.assertRaises(ValueError):
            summarize_fetch(run_id="p2-20261023", tier="p1", fixtures_in_scope=0,
                            coverage=[], observations=[], api_calls=0, errors=[])
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                write_run_metric(Path(d), {"run_id": "../escape"})

    def test_existing_priority_and_bookmaker_contract_stays_unchanged(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "scripts/sync_odds.py").read_text(encoding="utf-8")
        for wrapper in ("run_p1.sh", "run_p2.sh", "run_p3.sh"):
            text = (repo / "scripts/vps" / wrapper).read_text(encoding="utf-8")
            self.assertIn('$(odds_bookmaker_csv)', text)
            self.assertIn("ODDS_PROVIDER_METRICS_DIR", text)
        self.assertIn('write_observation_document(', source)
        self.assertIn('observation_from_row(row, str(odds_record["observed_at"]))', source)


if __name__ == "__main__":
    unittest.main()
