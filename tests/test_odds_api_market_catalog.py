from __future__ import annotations

import unittest

from scripts.validate_odds_api_market_catalog import (
    REQUIRED_MARKETS,
    catalog_allows_sync,
    validate_catalog,
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


if __name__ == "__main__":
    unittest.main()
