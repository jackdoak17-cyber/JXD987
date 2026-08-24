from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class FixtureSettlementContractTests(unittest.TestCase):
    def test_reconcile_cli_exposes_settlement_options(self) -> None:
        result = subprocess.run(
            [sys.executable, "scripts/reconcile_recent_fixtures.py", "--help"],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(ROOT), "COLUMNS": "200"},
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("--completed-hours-back", result.stdout)
        self.assertIn("--report-json", result.stdout)

    def test_settlement_wrapper_is_shell_valid_and_publishes_delivery(self) -> None:
        wrapper = ROOT / "scripts/vps/run_postmatch_settlement.sh"
        result = subprocess.run(["bash", "-n", str(wrapper)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = wrapper.read_text(encoding="utf-8")
        self.assertIn("odds-sync.lock", source)
        self.assertIn("refresh_fixture_delivery.py", source)

    def test_supported_league_helper_excludes_cups(self) -> None:
        excluded = set(json.loads((ROOT / "config/odds_api_sync_excluded_leagues.json").read_text()))
        result = subprocess.run(
            [
                "bash",
                "-lc",
                'source scripts/vps/common.sh; REPO_ROOT="$PWD"; supported_league_csv',
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        supported = {int(value) for value in result.stdout.strip().split(",") if value}
        self.assertTrue(supported)
        self.assertTrue(excluded.isdisjoint(supported))

    def test_all_vps_wrappers_pass_shell_syntax(self) -> None:
        for wrapper in sorted((ROOT / "scripts/vps").glob("run_*.sh")):
            result = subprocess.run(["bash", "-n", str(wrapper)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, f"{wrapper}: {result.stderr}")


if __name__ == "__main__":
    unittest.main()
