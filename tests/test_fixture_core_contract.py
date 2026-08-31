from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.fixture_core_contract import (
    FixtureCoreContractError,
    load_contract,
    parse_contract,
)


class FixtureCoreContractTests(unittest.TestCase):
    def test_production_contract_keeps_identity_and_source_windows_separate(self) -> None:
        contract = load_contract()

        self.assertEqual(contract.identity_window_days, 30)
        self.assertEqual(contract.history_window_days, 2)
        self.assertEqual(contract.source_buffer_days, 1)
        self.assertEqual(contract.source_window_days_forward, 31)
        self.assertEqual(contract.local_read_lookahead_days, 31)
        self.assertEqual(contract.odds_window_days, 14)
        self.assertEqual(contract.fixture_delivery_window_days, 14)
        self.assertEqual(contract.delivery_window_days, 43)
        self.assertEqual(contract.job_id, "run_p3_fixture_core_export")
        self.assertEqual(contract.time_basis, "UTC")

    def test_invalid_contract_rejects_unknown_fields(self) -> None:
        with self.assertRaises(FixtureCoreContractError):
            parse_contract(
                {
                    "version": 1,
                    "identity_window_days": 30,
                    "history_window_days": 2,
                    "source_buffer_days": 1,
                    "odds_window_days": 14,
                    "fixture_delivery_window_days": 14,
                    "delivery_window_days": 43,
                    "same_day_grace_hours": 6,
                    "max_job_age_minutes": 540,
                    "job_id": "run_p3_fixture_core_export",
                    "time_basis": "UTC",
                    "temporary_override": 31,
                }
            )

    def test_invalid_contract_rejects_delivery_horizon_shorter_than_identity(self) -> None:
        payload = json.loads(
            Path("config/fixture_core_contract.json").read_text(encoding="utf-8")
        )
        payload["delivery_window_days"] = 29
        with self.assertRaises(FixtureCoreContractError):
            parse_contract(payload)

    def test_invalid_contract_rejects_delivery_horizon_shorter_than_fixture_window(self) -> None:
        payload = json.loads(
            Path("config/fixture_core_contract.json").read_text(encoding="utf-8")
        )
        payload["delivery_window_days"] = 13
        with self.assertRaises(FixtureCoreContractError):
            parse_contract(payload)

    def test_contract_loader_reports_missing_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FixtureCoreContractError):
                load_contract(Path(directory) / "missing.json")


if __name__ == "__main__":
    unittest.main()
