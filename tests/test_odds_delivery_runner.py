import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[1] / "scripts/vps/run_odds_delivery.sh"


class DeliveryRunnerTest(unittest.TestCase):
    def run_case(self, status, *, explicit_runtime=True, runtime_override=None, verify_only=False):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "scripts/vps").mkdir(parents=True)
            (root / ".venv/bin").mkdir(parents=True)
            (root / "data").mkdir()
            (root / "data/jxd.sqlite").touch()
            (root / ".env").write_text("")
            (root / ".venv/bin/activate").write_text('export PATH="${REPO_ROOT}/.venv/bin:$PATH"\n')
            shutil.copy2(RUNNER, root / "scripts/vps/run_odds_delivery.sh")
            for name in (
                "export_odds_to_supabase_psql.py",
                "reconcile_fixture_delivery_odds.py",
                "refresh_fixture_delivery.py",
            ):
                shutil.copy2(RUNNER.parents[1] / name, root / "scripts" / name)
            (root / "scripts/vps/common.sh").write_text(
                '''
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
verify_runtime_manifest_or_exit() { return 0; }
require_runtime_manifest_entries_or_exit() { return 0; }
odds_league_csv() { echo 8,9; }
log_info() { printf '%s\n' "$*"; }
log_error() { printf '%s\n' "$*" >&2; }
run_recorded_pipeline_job() {
  printf '%s' "$1" > "${REPO_ROOT}/job"
  bash -c "$3"
}
'''
            )
            fake = root / ".venv/bin/python"
            fake.write_text(
                f'''#!/bin/bash
if [[ "$1" == "-" ]]; then
  exec {sys.executable!r} "$@"
fi
printf '%q ' "$@" >> "${{REPO_ROOT}}/calls"
printf '\n' >> "${{REPO_ROOT}}/calls"
if [[ "$1" == "scripts/export_odds_to_supabase_psql.py" ]]; then
  exit "${{TEST_EXIT}}"
fi
exit 0
'''
            )
            fake.chmod(0o755)
            env = {**os.environ, "TEST_EXIT": str(status)}
            if explicit_runtime:
                env["ODDS_DELIVERY_RUNTIME"] = runtime_override or str(root)
            else:
                env.pop("ODDS_DELIVERY_RUNTIME", None)
            if verify_only:
                env["ODDS_DELIVERY_VERIFY_ONLY"] = "true"

            result = subprocess.run(
                ["bash", str(root / "scripts/vps/run_odds_delivery.sh")],
                env=env,
                capture_output=True,
            )
            self.assertEqual(result.returncode, status, result.stderr.decode())
            if verify_only:
                self.assertFalse((root / "job").exists())
                self.assertFalse((root / "calls").exists())
                self.assertIn(f"runtime={root}", result.stdout.decode())
                return

            self.assertEqual((root / "job").read_text(), "run_odds_delivery")
            calls = (root / "calls").read_text().splitlines()
            self.assertEqual(len(calls), 2 if status == 0 else 1)
            args = calls[0].split()
            self.assertEqual(args[0], "scripts/export_odds_to_supabase_psql.py")
            for flag in ["--skip-retention", "--skip-retention-snapshots", "--no-include-fixture-leagues"]:
                self.assertIn(flag, args)
            self.assertEqual(args[args.index("--days-forward") + 1], "14")
            self.assertNotIn("--skip-verification", args)
            if status == 0:
                self.assertTrue(calls[1].startswith("scripts/reconcile_fixture_delivery_odds.py "))

    def test_success(self):
        self.run_case(0)

    def test_export_failure_propagates(self):
        self.run_case(7)

    def test_runtime_defaults_to_the_release_containing_the_runner(self):
        self.run_case(0, explicit_runtime=False)

    def test_external_runtime_override_cannot_redirect_the_release(self):
        self.run_case(0, runtime_override="/opt/odds-sync/obsolete-runtime")

    def test_verify_only_checks_complete_chain_without_exporting(self):
        self.run_case(0, explicit_runtime=False, verify_only=True)


if __name__ == "__main__":
    unittest.main()
