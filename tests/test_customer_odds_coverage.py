import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.report_customer_odds_coverage import provider_summary, summarize


class CustomerOddsCoverageTests(unittest.TestCase):
    def test_customer_coverage_does_not_count_provider_gaps_as_available(self) -> None:
        coverage = summarize([
            {"league_id": 651, "fixtures": 9, "with_stored_odds": 8,
             "with_any_verified_price": 7, "with_complete_verified_moneyline": 6,
             "without_provider_prices": 1, "with_prices_but_no_current_verification": 1},
            {"league_id": 944, "fixtures": 9, "with_stored_odds": 8,
             "with_any_verified_price": 8, "with_complete_verified_moneyline": 8,
             "without_provider_prices": 1, "with_prices_but_no_current_verification": 0},
        ])
        self.assertEqual(coverage["totals"]["fixtures"], 18)
        self.assertEqual(coverage["totals"]["without_provider_prices"], 2)
        self.assertEqual(coverage["customer_any_price_coverage_pct"], 83.33)
        self.assertEqual(coverage["customer_complete_moneyline_coverage_pct"], 77.78)
        self.assertEqual(coverage["published_price_coverage_pct"], 88.89)
        self.assertNotEqual(coverage["customer_complete_moneyline_coverage_pct"], 100)

    def test_empty_publication_reports_zero_availability_without_division_error(self) -> None:
        result = summarize([])
        self.assertEqual(result["totals"]["fixtures"], 0)
        self.assertEqual(result["customer_complete_moneyline_coverage_pct"], 0)

    def test_provider_gap_and_pipeline_health_report_remain_distinct(self) -> None:
        now = datetime(2026, 10, 24, 9, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            report.write_text(json.dumps({
                "generated_at": (now - timedelta(minutes=15)).isoformat(),
                "errors": [],
                "moneyline_coverage": [
                    {"matching_status": "matched", "odds_response_status": "empty"},
                    {"matching_status": "matched", "odds_response_status": "received"},
                ],
            }), encoding="utf-8")
            result = provider_summary(str(report), now)
            self.assertEqual(result["state"], "observed")
            self.assertEqual(result["by_matching_and_response"]["matched:empty"], 1)
            self.assertEqual(result["errors"], 0)

    def test_stale_provider_evidence_never_proves_current_coverage(self) -> None:
        now = datetime(2026, 10, 24, 9, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            report.write_text(json.dumps({
                "generated_at": (now - timedelta(hours=7)).isoformat(),
                "moneyline_coverage": [{"matching_status": "matched", "odds_response_status": "received"}],
            }), encoding="utf-8")
            self.assertEqual(provider_summary(str(report), now)["state"], "stale")


if __name__ == "__main__":
    unittest.main()
