from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class FixtureCorePipelineContractTests(unittest.TestCase):
    def test_runtime_entrypoints_have_valid_shell_syntax(self) -> None:
        for relative_path in (
            "scripts/vps/common.sh",
            "scripts/vps/run_p3.sh",
            "scripts/vps/run_p3_fixture_core.sh",
            "scripts/vps/run_postmatch_settlement.sh",
        ):
            result = subprocess.run(
                ["bash", "-n", str(ROOT / relative_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, f"{relative_path}: {result.stderr}")

    def test_fixture_core_job_uses_contract_window_and_strict_core_export(self) -> None:
        wrapper = (ROOT / "scripts/vps/run_p3_fixture_core.sh").read_text(encoding="utf-8")

        self.assertIn("--days-forward \"${FIXTURE_CORE_SOURCE_DAYS_FORWARD}\"", wrapper)
        self.assertIn('FIXTURE_CORE_REFRESH_DAYS_BACK="${FIXTURE_CORE_HISTORY_DAYS}"', wrapper)
        self.assertIn('--start-date "$(TZ=Europe/London date -d "-${FIXTURE_CORE_HISTORY_DAYS} days" +%F)"', wrapper)
        self.assertIn("--fixture-core-only", wrapper)
        self.assertIn("--skip-prune", wrapper)
        self.assertIn('"${FIXTURE_CORE_JOB_ID}"', wrapper)
        self.assertIn("run_recorded_pipeline_job", wrapper)
        self.assertIn('ODDS_SYNC_LOCK_RETRY_ATTEMPTS="${FIXTURE_CORE_LOCK_RETRY_ATTEMPTS:-4}"', wrapper)
        self.assertIn('ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS="${FIXTURE_CORE_LOCK_RETRY_DELAY_SECONDS:-15}"', wrapper)
        self.assertIn("--no-refresh-squads-missing", wrapper)
        self.assertIn("--no-refresh-sidelined-window", wrapper)
        self.assertIn('FIXTURE_CORE_REFRESH_DAYS_BACK="${FIXTURE_CORE_HISTORY_DAYS}"', wrapper)

    def test_p3_keeps_odds_window_separate_from_fixture_core_job(self) -> None:
        p3 = (ROOT / "scripts/vps/run_p3.sh").read_text(encoding="utf-8")

        self.assertIn('--days-forward "${DAYS_FORWARD}"', p3)
        self.assertIn('export DAYS_FORWARD="$(contract_value odds_window_days)"', p3)
        self.assertIn('export SETTLED_HISTORY_DAYS="$(contract_value history_window_days)"', p3)
        self.assertIn('export ODDS_EXPORT_DAYS_BACK="${SETTLED_HISTORY_DAYS}"', p3)
        self.assertIn('--priority settled-history', p3)
        self.assertIn('--calendar-window', p3)
        self.assertIn('export FIXTURE_CORE_HISTORY_DAYS="$(contract_value history_window_days)"', p3)
        self.assertIn("run_recorded_pipeline_job", p3)
        self.assertNotIn("python scripts/reconcile_recent_fixtures.py", p3)

    def test_runtime_manifest_contains_contract_and_core_entrypoint(self) -> None:
        entries = {
            line.strip()
            for line in (ROOT / "scripts/vps/runtime_files.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }

        self.assertTrue({
            "config/fixture_core_contract.json",
            "scripts/fixture_core_contract.py",
            "scripts/vps/run_p3_fixture_core.sh",
            "scripts/validate_moneyline_coverage.py",
        }.issubset(entries))

    def test_recorded_job_retries_a_transient_lock_handoff(self) -> None:
        script = r'''
source scripts/vps/common.sh
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS=5
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS=0
attempts=0
run_with_global_lock_and_timeout() {
  attempts=$((attempts + 1))
  if [[ "${attempts}" -eq 1 ]]; then
    return 2
  fi
  return 0
}
run_recorded_pipeline_job "test_fixture_core" "test fixture core" "true" ""
[[ "${attempts}" -eq 2 ]]
'''
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("[RETRY]", result.stdout)


if __name__ == "__main__":
    unittest.main()
