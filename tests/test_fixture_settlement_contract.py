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
        self.assertIn('ODDS_SYNC_JOB_PRIORITY="settlement"', source)
        self.assertIn('ODDS_SYNC_LOCK_WAIT_SECONDS="${SETTLEMENT_LOCK_WAIT_SECONDS}"', source)
        self.assertIn('SETTLEMENT_RUN_LOCK_FILE="${SETTLEMENT_RUN_LOCK_FILE:-/var/lock/odds-sync-settlement.lock}"', source)
        self.assertIn("--no-fail-on-sla-breach", source)

    def test_fixture_core_refresh_cannot_overwrite_fixture_detail(self) -> None:
        wrapper = ROOT / "scripts/vps/run_p3_fixture_core.sh"
        result = subprocess.run(["bash", "-n", str(wrapper)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = wrapper.read_text(encoding="utf-8")
        self.assertIn("--fixture-core-only", source)
        self.assertIn("--strict", source)
        self.assertNotIn("--with-details", source)

    def test_p3_fixture_refresh_does_not_consume_the_odds_writer_lease(self) -> None:
        source = (ROOT / "scripts/vps/run_p3.sh").read_text(encoding="utf-8")
        self.assertIn("--no-refresh-squads-missing", source)
        self.assertIn("--no-refresh-sidelined-window", source)

    def test_p3_delivery_refresh_covers_retained_rolling_schedule(self) -> None:
        wrapper = ROOT / "scripts/vps/run_p3.sh"
        source = wrapper.read_text(encoding="utf-8")
        self.assertIn('export FIXTURE_CORE_HISTORY_DAYS="$(contract_value history_window_days)"', source)
        self.assertIn('export FIXTURE_CORE_DELIVERY_DAYS_FORWARD="$(contract_value delivery_window_days)"', source)
        self.assertIn('--start-date "$(TZ=Europe/London date -d "-${FIXTURE_CORE_HISTORY_DAYS} days" +%F)"', source)

    def test_odds_wrappers_record_pipeline_heartbeats(self) -> None:
        for wrapper_name, job_id, job_name in (
            ("run_p1.sh", "run_p1", "P1 odds fetch"),
            ("run_p2.sh", "run_p2", "P2 reconciliation"),
        ):
            wrapper = ROOT / "scripts/vps" / wrapper_name
            result = subprocess.run(["bash", "-n", str(wrapper)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, f"{wrapper}: {result.stderr}")
            source = wrapper.read_text(encoding="utf-8")
            self.assertIn("run_recorded_pipeline_job", source)
            self.assertIn(f'"{job_id}"', source)
            self.assertIn(f'"{job_name}"', source)

    def test_settlement_delivery_refresh_preserves_the_rolling_horizon(self) -> None:
        wrapper = ROOT / "scripts/vps/run_postmatch_settlement.sh"
        source = wrapper.read_text(encoding="utf-8")
        self.assertIn('export SETTLEMENT_EXPORT_DAYS_BACK="$(contract_value history_window_days)"', source)
        self.assertIn('export SETTLEMENT_DELIVERY_DAYS_BACK="$(contract_value history_window_days)"', source)
        self.assertIn('export FIXTURE_DELIVERY_DAYS_FORWARD="$(contract_value delivery_window_days)"', source)
        self.assertIn(
            'SETTLEMENT_DELIVERY_DAYS_FORWARD="${FIXTURE_DELIVERY_DAYS_FORWARD}"',
            source,
        )

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

    def test_stats_reconciliation_supervisor_is_bounded(self) -> None:
        wrapper = ROOT / "scripts/vps/run_stats_reconciliation.sh"
        source = wrapper.read_text(encoding="utf-8")
        self.assertIn("--max-batches 1", source)
        self.assertIn("STATS_RECONCILE_SUPERVISOR_LOCK", source)
        self.assertIn("wait_for_live_window", source)
        self.assertIn("STATS_RECONCILE_LIVE_SETTLEMENT_GUARD_SECONDS", source)
        self.assertIn('$(supported_league_csv)', source)
        self.assertIn("run_with_global_lock_and_timeout", source)
        self.assertIn("STATS_RECONCILE_LOCK_HELD", source)
        self.assertIn("export REPO_ROOT", source)
        self.assertIn("export RUNTIME_RELEASE_ID", source)

    def test_shared_lock_has_priority_aware_settlement_handoff(self) -> None:
        source = (ROOT / "scripts/vps/common.sh").read_text(encoding="utf-8")
        self.assertIn("live settlement reservation active", source)
        self.assertIn('job_priority="${ODDS_SYNC_JOB_PRIORITY:-normal}"', source)
        self.assertIn('lock_wait_seconds="${ODDS_SYNC_LOCK_WAIT_SECONDS:-0}"', source)
        self.assertIn("settlement lock unavailable after", source)
        self.assertIn('effective_runtime="${max_runtime}"', source)
        self.assertIn('available_runtime=$((seconds_to_tick - live_grace_seconds))', source)
        self.assertIn('min_normal_lease_seconds="${ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS:-0}"', source)
        self.assertIn("normal-writer lease too short for bounded job", source)
        self.assertIn('timeout --signal=TERM --kill-after=5s "${effective_runtime}"', source)
        self.assertIn("normal-writer lease ended for settlement handoff", source)

    def test_experimental_model_lane_uses_a_separate_bounded_lock(self) -> None:
        common = (ROOT / "scripts/vps/common.sh").read_text(encoding="utf-8")
        models = (ROOT / "scripts/vps/run_models.sh").read_text(encoding="utf-8")
        self.assertIn("run_with_dedicated_lock_and_timeout", common)
        self.assertIn("/var/lock/models-experimental.lock", common)
        self.assertIn("experimental model process exceeded", common)
        self.assertIn("MODELS_EXPERIMENTAL_LOCK_FILE", models)
        self.assertIn("run_with_dedicated_lock_and_timeout", models)
        self.assertIn("experimental-only output uses a dedicated private-ledger lock", models)

    def test_stats_reconciliation_cron_installer_is_shell_valid_and_idempotent(self) -> None:
        installer = ROOT / "scripts/vps/install_stats_reconciliation_cron.sh"
        result = subprocess.run(["bash", "-n", str(installer)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = installer.read_text(encoding="utf-8")
        self.assertIn("crontab -l", source)
        self.assertIn("grep -Fqx", source)
        self.assertIn("run_stats_reconciliation.sh", source)

    def test_runtime_deploy_publishes_manifest_last(self) -> None:
        deploy = ROOT / "scripts/vps/deploy_runtime.sh"
        result = subprocess.run(["bash", "-n", str(deploy)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = deploy.read_text(encoding="utf-8")
        self.assertIn("Publish the manifest last", source)
        self.assertIn("scripts/vps/runtime_manifest.sha1", source)
        self.assertIn("required_runtime_entries", source)
        self.assertIn('TARGET_REPO_ROOT="${2:-${VPS_REPO_ROOT:-}}"', source)
        self.assertNotIn('/opt/odds-sync/JXD987}}', source)
        self.assertIn('"scripts/refresh_fixture_delivery.py"', source)
        self.assertIn('"scripts/vps/run_p3.sh"', source)
        self.assertIn('"scripts/vps/run_postmatch_settlement.sh"', source)
        self.assertIn("scripts/reconcile_stats_provider_queue.py", source)

    def test_heartbeat_report_truncation_cannot_abort_under_pipefail(self) -> None:
        source = (ROOT / "scripts/vps/common.sh").read_text(encoding="utf-8")
        self.assertIn('pathlib.Path(sys.argv[1]).read_text', source)
        self.assertNotIn("tr '\\n' ' ' < \"${evidence_file}\" | head -c", source)

    def test_manifest_required_file_check_cannot_false_negative_under_pipefail(self) -> None:
        source = (ROOT / "scripts/vps/common.sh").read_text(encoding="utf-8")
        self.assertIn('grep -Fqx -- "${relpath}" <<<"${manifest_paths}"', source)
        self.assertNotIn("printf '%s\\n' \"${manifest_paths}\" | grep -Fxq", source)


if __name__ == "__main__":
    unittest.main()
