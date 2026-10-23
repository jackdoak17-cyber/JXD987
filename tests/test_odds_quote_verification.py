from __future__ import annotations

import gzip
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.odds_quote_verification import (
    _load,
    _validate,
    deduplicate_observations,
    is_verifiable_quote_row,
    observation_from_row,
    write_observation_document,
)


class OddsQuoteVerificationTests(unittest.TestCase):
    @staticmethod
    def row(price: float = 5.0) -> dict[str, object]:
        return {
            "fixture_id": 19728194,
            "bookmaker_id": 2,
            "market_key": "moneyline",
            "selection_key": "home",
            "participant_type": "team",
            "participant_id": 51,
            "line": None,
            "price_decimal": price,
            "last_updated_at": datetime(2026, 10, 18, 10, 0, tzinfo=timezone.utc),
        }

    def test_unchanged_provider_quote_uses_observation_time_not_price_change_time(self) -> None:
        observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        quote = observation_from_row(self.row(), observed_at)

        self.assertEqual(quote["price_decimal"], 5.0)
        self.assertEqual(quote["source_last_updated_at"], "2026-10-18T10:00:00Z")
        self.assertEqual(quote["observed_at"], observed_at.replace("+00:00", "Z"))

    def test_atomic_compressed_document_survives_worker_claim_name(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pending" / "p1-test.json.gz"
            quote = observation_from_row(self.row(), datetime.now(timezone.utc).isoformat())
            write_observation_document(path, run_id="p1-20261021T120000-123", lane="p1", quotes=[quote])
            claimed = path.with_name(f".{path.name}.processing-test")
            path.rename(claimed)

            document = _load(claimed)

            self.assertEqual(_validate(document), [quote])
            with gzip.open(claimed, "rt", encoding="utf-8") as handle:
                self.assertIn("odds_api_provider_response", handle.read())

    def test_missing_selection_is_not_invented(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pending" / "empty.json.gz"
            write_observation_document(path, run_id="p1-20261021T120000-124", lane="p1", quotes=[])

            document = _load(path)

            self.assertEqual(_validate(document), [])

    def test_closed_or_invalid_prices_are_not_verifiable(self) -> None:
        self.assertFalse(is_verifiable_quote_row(self.row(1.0)))
        self.assertFalse(is_verifiable_quote_row(self.row(float("nan"))))
        self.assertFalse(is_verifiable_quote_row(self.row(501.0)))
        self.assertTrue(is_verifiable_quote_row(self.row(1.01)))

    def test_duplicate_and_expired_observations_fail_closed(self) -> None:
        quote = observation_from_row(self.row(), datetime.now(timezone.utc).isoformat())
        duplicate = {
            "schema_version": 1,
            "source": "odds_api_provider_response",
            "run_id": "p1-20261021T120000-125",
            "quotes": [quote, quote],
        }
        with self.assertRaisesRegex(ValueError, "duplicate"):
            _validate(duplicate)

        expired = dict(duplicate, quotes=[dict(quote, observed_at=(datetime.now(timezone.utc) - timedelta(days=8)).isoformat())])
        with self.assertRaisesRegex(ValueError, "invalid quote"):
            _validate(expired)

    def test_provider_duplicate_is_coalesced_to_latest_observation_before_spooling(self) -> None:
        first = observation_from_row(self.row(), "2026-10-21T12:00:00Z")
        second = dict(first, observed_at="2026-10-21T12:01:00Z")

        result = deduplicate_observations([first, second])

        self.assertEqual(result, [second])


if __name__ == "__main__":
    unittest.main()
