from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from jxd.odds_api_client import OddsApiClient, OddsApiError

from scripts.validate_odds_api_market_catalog import (
    REQUIRED_MARKETS,
    catalog_allows_sync,
    validate_catalog,
    main,
)


class OddsApiMarketCatalogTests(unittest.TestCase):
    @staticmethod
    def valid_items() -> list[dict[str, object]]:
        return [
            {"name": name, "shape": shape, "prematch": True}
            for name, (shape, _market_key) in REQUIRED_MARKETS.items()
        ]

    def test_current_catalog_and_parser_contract_pass(self) -> None:
        result = validate_catalog(self.valid_items())

        self.assertTrue(result["ok"])
        self.assertFalse(result["provider_degraded"])
        self.assertEqual(result["missing_required_markets"], [])
        self.assertEqual(result["shape_mismatches"], [])
        self.assertEqual(result["parser_mismatches"], [])
        self.assertTrue(catalog_allows_sync(result, allow_provider_missing=False))

    def test_missing_or_reshaped_market_fails_closed(self) -> None:
        items = self.valid_items()
        items = [item for item in items if item["name"] != "ML"]
        next(item for item in items if item["name"] == "Player Shots")["shape"] = "outcomes"

        result = validate_catalog(items)

        self.assertFalse(result["ok"])
        self.assertFalse(result["provider_degraded"])
        self.assertEqual(result["missing_required_markets"], ["ML"])
        self.assertEqual(result["shape_mismatches"][0]["market"], "Player Shots")
        self.assertFalse(catalog_allows_sync(result, allow_provider_missing=True))

    def test_missing_market_without_contract_mismatch_is_provider_degraded(self) -> None:
        items = [item for item in self.valid_items() if item["name"] != "Player Fouls"]

        result = validate_catalog(items)

        self.assertFalse(result["ok"])
        self.assertTrue(result["provider_degraded"])
        self.assertEqual(result["missing_required_markets"], ["Player Fouls"])
        self.assertEqual(result["shape_mismatches"], [])
        self.assertEqual(result["parser_mismatches"], [])
        self.assertFalse(catalog_allows_sync(result, allow_provider_missing=False))
        self.assertTrue(catalog_allows_sync(result, allow_provider_missing=True))



    def test_catalogue_recovers_after_four_transient_503_responses(self) -> None:
        """The standard three-retry budget would have failed this request."""
        client = OddsApiClient(api_key="synthetic-test", retries=5, sleep_base=2, sleep_max=10)
        bad = SimpleNamespace(status_code=503, headers={}, text="unavailable")
        good = SimpleNamespace(status_code=200, headers={}, text="", json=lambda: self.valid_items())
        client.session.get = Mock(side_effect=[bad, bad, bad, bad, good])
        with patch("jxd.odds_api_client.time.sleep") as sleep:
            response = client.request("markets", {"sport": "football"})
        self.assertEqual(response, self.valid_items())
        self.assertEqual(client.stats.calls_by_endpoint["markets"], 5)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4, 8, 10])

    def test_catalogue_persistent_503_still_fails_after_bounded_attempts(self) -> None:
        client = OddsApiClient(api_key="synthetic-test", retries=5, sleep_base=2, sleep_max=10)
        bad = SimpleNamespace(status_code=503, headers={}, text="unavailable")
        client.session.get = Mock(return_value=bad)
        with patch("jxd.odds_api_client.time.sleep") as sleep:
            with self.assertRaisesRegex(OddsApiError, "Server error 503"):
                client.request("markets", {"sport": "football"})
        self.assertEqual(client.session.get.call_count, 6)
        self.assertEqual(len(sleep.call_args_list), 6)

    def test_catalogue_4xx_is_not_retried(self) -> None:
        client = OddsApiClient(api_key="synthetic-test", retries=5, sleep_base=2, sleep_max=10)
        denied = SimpleNamespace(status_code=403, headers={}, text="invalid key")
        client.session.get = Mock(return_value=denied)
        with patch("jxd.odds_api_client.time.sleep") as sleep:
            with self.assertRaisesRegex(OddsApiError, "HTTP 403"):
                client.request("markets", {"sport": "football"})
        self.assertEqual(client.session.get.call_count, 1)
        sleep.assert_not_called()

    def test_validator_writes_current_failure_report_not_stale_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "market-catalog.json"
            report.write_text('{"ok":true,"old":true}', encoding="utf-8")
            client = Mock()
            client.request.side_effect = OddsApiError("Odds API request failed for markets: Server error 503 for markets")
            client.stats.calls_by_endpoint = {"markets": 6}
            with patch("scripts.validate_odds_api_market_catalog.OddsApiClient", return_value=client) as constructor:
                with patch("sys.argv", ["validator", "--report-out", str(report), "--allow-provider-missing"]):
                    with self.assertRaisesRegex(SystemExit, "market catalogue unavailable"):
                        main()
            constructor.assert_called_once_with(retries=5, sleep_base=2, sleep_max=10)
            saved = json.loads(report.read_text(encoding="utf-8"))
            self.assertFalse(saved["ok"])
            self.assertEqual(saved["error_stage"], "provider_request")
            self.assertEqual(saved["provider_http_attempts"], 6)
            self.assertNotIn("old", saved)

    def test_successful_validator_preserves_market_contract_and_reports_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "market-catalog.json"
            client = Mock()
            client.request.return_value = self.valid_items()
            client.stats.calls_by_endpoint = {"markets": 5}
            with patch("scripts.validate_odds_api_market_catalog.OddsApiClient", return_value=client):
                with patch("sys.argv", ["validator", "--report-out", str(report)]):
                    main()
            saved = json.loads(report.read_text(encoding="utf-8"))
            self.assertTrue(saved["ok"])
            self.assertEqual(saved["provider_http_attempts"], 5)
            self.assertFalse(saved["provider_degraded"])

if __name__ == "__main__":
    unittest.main()
