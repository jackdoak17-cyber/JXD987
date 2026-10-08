from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class FixtureDeliveryPublisherContractTests(unittest.TestCase):
    def test_publisher_wrapper_is_shell_valid_and_uses_durable_state(self) -> None:
        wrapper = ROOT / "scripts/vps/run_fixture_delivery_publisher.sh"
        result = subprocess.run(["bash", "-n", str(wrapper)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = wrapper.read_text(encoding="utf-8")
        self.assertIn("/var/lib/oddssearch/fixture-delivery/dirty-state.json", source)
        self.assertIn("fixture_delivery_dirty_state.py status", source)
        self.assertIn("fixture_delivery_dirty_state.py clear", source)
        self.assertIn("fixture_delivery_dirty_state.py failure", source)
        self.assertIn("refresh_fixture_delivery.py", source)
        self.assertIn('cd "${REPO_ROOT}"', source)
        self.assertIn("source .venv/bin/activate", source)
        self.assertIn('export PYTHONPATH="${REPO_ROOT}"', source)
        self.assertIn("--skip-if-publication-active", source)
        self.assertIn("--min-publish-interval-seconds", source)
        self.assertIn("record_pipeline_job_run", source)
        self.assertIn('"run_fixture_delivery_publisher"', source)
        self.assertIn('if [[ "${publisher_status}" -eq 2 ]]; then', source)

    def test_refresh_supports_guarded_complete_publication_without_changing_default_callers(self) -> None:
        source = (ROOT / "scripts/refresh_fixture_delivery.py").read_text(encoding="utf-8")
        self.assertIn("--skip-if-publication-active", source)
        self.assertIn("pg_try_advisory_lock", source)
        self.assertIn("recent_publication_satisfies_guard", source)
        self.assertIn("validate_release_components", source)
        self.assertIn("finalize_release", source)
        self.assertIn("return 2", source)

    def test_runtime_manifest_includes_guarded_publisher_files(self) -> None:
        entries = {
            line.strip()
            for line in (ROOT / "scripts/vps/runtime_files.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
        self.assertIn("scripts/fixture_delivery_dirty_state.py", entries)
        self.assertIn("scripts/vps/run_fixture_delivery_publisher.sh", entries)


if __name__ == "__main__":
    unittest.main()
