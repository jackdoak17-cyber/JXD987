import os
from pathlib import Path
import subprocess
import tempfile
import unittest

RUNNER = Path(__file__).resolve().parents[1] / 'scripts/vps/run_odds_delivery.sh'

class DeliveryRunnerTest(unittest.TestCase):
    def run_case(self, status):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'scripts/vps').mkdir(parents=True)
            (root / '.venv/bin').mkdir(parents=True)
            (root / '.env').write_text('')
            (root / '.venv/bin/activate').write_text('export PATH="${REPO_ROOT}/.venv/bin:$PATH"\n')
            (root / 'scripts/vps/common.sh').write_text('''
REPO_ROOT="${ODDS_DELIVERY_RUNTIME}"
verify_runtime_manifest_or_exit() { return 0; }
require_runtime_manifest_entries_or_exit() { return 0; }
odds_league_csv() { echo 8,9; }
run_recorded_pipeline_job() {
  printf '%s' "$1" > "${REPO_ROOT}/job"
  bash -c "$3"
}
''')
            fake = root / '.venv/bin/python'
            fake.write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "${REPO_ROOT}/args"\nexit "${TEST_EXIT}"\n')
            fake.chmod(0o755)
            result = subprocess.run(['bash', str(RUNNER)], env={**os.environ, 'ODDS_DELIVERY_RUNTIME': str(root), 'TEST_EXIT': str(status)}, capture_output=True)
            self.assertEqual(result.returncode, status, result.stderr.decode())
            self.assertEqual((root / 'job').read_text(), 'run_odds_delivery')
            args = (root / 'args').read_text().splitlines()
            self.assertEqual(args[0], 'scripts/export_odds_to_supabase_psql.py')
            for flag in ['--skip-retention', '--skip-retention-snapshots', '--no-include-fixture-leagues']:
                self.assertIn(flag, args)
            self.assertEqual(args[args.index('--days-forward') + 1], '14')
            self.assertNotIn('--skip-verification', args)
    def test_success(self): self.run_case(0)
    def test_export_failure_propagates(self): self.run_case(7)

if __name__ == '__main__': unittest.main()
