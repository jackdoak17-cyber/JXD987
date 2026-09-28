from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/fixture_delivery_dirty_state.py"


class FixtureDeliveryDirtyStateTests(unittest.TestCase):
    def run_state(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_relevant_fixture_change_creates_durable_dirty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            state = tmp_path / "state" / "dirty.json"
            reconcile = tmp_path / "reconcile.json"
            export = tmp_path / "export.json"
            report = tmp_path / "report.json"
            reconcile.write_text(json.dumps({"fixtures_reconciled": 2}), encoding="utf-8")
            export.write_text(json.dumps({"fixtures_exported": 2}), encoding="utf-8")

            result = self.run_state(
                "--state-path", str(state),
                "mark-from-reports",
                "--reconcile-report", str(reconcile),
                "--export-report", str(export),
                "--report-out", str(report),
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(state.exists())
            payload = json.loads(state.read_text(encoding="utf-8"))
            self.assertTrue(payload["dirty"])
            self.assertEqual(payload["mark_count"], 1)
            self.assertIn("fixtures_reconciled=2", payload["reason"])
            self.assertIn("fixtures_exported=2", payload["reason"])
            self.assertTrue(report.exists())

    def test_no_relevant_change_does_not_create_dirty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            state = tmp_path / "state" / "dirty.json"
            reconcile = tmp_path / "reconcile.json"
            export = tmp_path / "export.json"
            reconcile.write_text(json.dumps({"fixtures_reconciled": 0}), encoding="utf-8")
            export.write_text(json.dumps({"fixtures_exported": 0}), encoding="utf-8")

            result = self.run_state(
                "--state-path", str(state),
                "mark-from-reports",
                "--reconcile-report", str(reconcile),
                "--export-report", str(export),
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(state.exists())
            output = json.loads(result.stdout)
            self.assertFalse(output["changed"])

    def test_failed_publication_preserves_dirty_state_and_success_clears_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "dirty.json"
            reconcile = Path(tmp) / "reconcile.json"
            export = Path(tmp) / "export.json"
            reconcile.write_text(json.dumps({"fixtures_reconciled": 1}), encoding="utf-8")
            export.write_text(json.dumps({"fixtures_exported": 1}), encoding="utf-8")
            self.assertEqual(self.run_state("--state-path", str(state), "mark-from-reports", "--reconcile-report", str(reconcile), "--export-report", str(export)).returncode, 0)
            self.assertEqual(self.run_state("--state-path", str(state), "failure", "--error", "boom").returncode, 0)
            self.assertTrue(state.exists())
            self.assertIn("boom", state.read_text(encoding="utf-8"))
            self.assertEqual(self.run_state("--state-path", str(state), "clear").returncode, 0)
            self.assertFalse(state.exists())


if __name__ == "__main__":
    unittest.main()
