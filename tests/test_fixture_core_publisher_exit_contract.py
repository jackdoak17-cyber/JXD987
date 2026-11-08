from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone


ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc


class FixtureCorePublisherExitContractTests(unittest.TestCase):
    def test_full_admitted_chain_handles_only_verified_superseded_skip(self) -> None:
        wrapper = (ROOT / "scripts/vps/run_p3_fixture_core.sh").read_text()
        chain = wrapper.split("CHAIN_COMMAND=$(cat <<'CHAIN'\n", 1)[1].split("\nCHAIN\n)", 1)[0]
        now = datetime.now(UTC)
        current = {
            "release_id": "current-release",
            "published_at": (now - timedelta(hours=2)).isoformat(),
            "health_checked_at": now.isoformat(),
            "pin_expires_at": (now + timedelta(hours=2)).isoformat(),
            "metadata": {"last_source_freshness_evidence": {
                "verified_at": (now - timedelta(minutes=5)).isoformat(),
                "max_age_minutes": 540,
            }},
        }
        valid_report = {
            "skipped": True,
            "published": False,
            "skip_reason": "newer_current_source",
            "current_release_id": "current-release",
            "pin_expires_at": (now + timedelta(minutes=90)).isoformat(),
            "source_freshness": {"verified_at": (now - timedelta(hours=7)).isoformat()},
        }
        with tempfile.TemporaryDirectory(prefix="fixture-core-exit-") as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            (root / ".venv/bin").mkdir(parents=True)
            (root / "bin").mkdir()
            (root / ".venv/bin/activate").write_text("")
            (root / ".env").write_text(
                "SPORTMONKS_API_TOKEN=dummy\nSUPABASE_URL=https://example.invalid\n"
                "SUPABASE_SERVICE_ROLE_KEY=dummy\n"
                "SUPABASE_DB_URL=postgresql://user:pass@aws-0.pooler.supabase.com:5432/postgres\n"
            )
            (root / "scripts/refresh_fixture_delivery.py").write_bytes(
                (ROOT / "scripts/refresh_fixture_delivery.py").read_bytes()
            )
            fake_python = root / "bin/python"
            fake_python.write_text("""#!/usr/bin/env python3
import json, os, sys
from datetime import datetime
from pathlib import Path
with open(os.environ['MOCK_COMMAND_LOG'], 'a') as log:
    log.write(sys.argv[1] + '\\n')
if sys.argv[1] == 'scripts/refresh_fixture_delivery.py':
    if os.environ.get('MOCK_REPORT_JSON') != '__missing__':
        path = Path(sys.argv[sys.argv.index('--report-out') + 1])
        path.write_text(os.environ['MOCK_REPORT_JSON'])
    sys.exit(int(os.environ['MOCK_PUBLISH_STATUS']))
if sys.argv[1] == '-c':
    import scripts.refresh_fixture_delivery as publisher
    payload = json.loads(os.environ['MOCK_CURRENT_JSON'])
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def execute(self, *_): pass
        def fetchone(self):
            return (
                payload['release_id'],
                datetime.fromisoformat(payload['published_at']),
                datetime.fromisoformat(payload['health_checked_at']),
                datetime.fromisoformat(payload['pin_expires_at']),
                payload['metadata'],
            )
    class Connection:
        def set_session(self, **_): pass
        def cursor(self): return Cursor()
        def close(self): pass
    publisher.connection = Connection
    exec(sys.argv[2], {'__name__': '__main__'})
sys.exit(0)
""")
            fake_python.chmod(0o755)
            fake_date = root / "bin/date"
            fake_date.write_text("#!/bin/sh\ncase \"$1\" in -u) echo 2026-11-08T16:00:00Z;; *) echo 2026-11-08;; esac\n")
            fake_date.chmod(0o755)
            report_path = root / "publisher-report.json"
            chain = chain.replace("/tmp/fixture_core_delivery_report.json", str(report_path))

            def execute(status: int, payload: dict | str) -> tuple[int, list[str]]:
                command_log = root / "command.log"
                command_log.write_text("")
                env = os.environ.copy()
                env.update({
                    "PATH": f"{root / 'bin'}:{env['PATH']}",
                    "REPO_ROOT": str(root),
                    "PIPELINE_EVIDENCE_FILE": str(root / "source-report.json"),
                    "STATS_LEAGUES": "8",
                    "FIXTURE_CORE_REFRESH_DAYS_BACK": "45",
                    "FIXTURE_CORE_SOURCE_DAYS_FORWARD": "45",
                    "FIXTURE_CORE_HISTORY_DAYS": "45",
                    "FIXTURE_CORE_DELIVERY_DAYS_FORWARD": "14",
                    "FIXTURE_CORE_JOB_ID": "run_p3_fixture_core_export",
                    "FIXTURE_CORE_JOB_MAX_AGE_MINUTES": "540",
                    "MOCK_COMMAND_LOG": str(command_log),
                    "MOCK_CURRENT_JSON": json.dumps(current),
                    "MOCK_PUBLISH_STATUS": str(status),
                    "MOCK_REPORT_JSON": json.dumps(payload) if isinstance(payload, dict) else payload,
                })
                result = subprocess.run(["bash", "-c", chain], cwd=root, env=env, capture_output=True, text=True)
                return result.returncode, command_log.read_text().splitlines()

            status, commands = execute(2, valid_report)
            self.assertEqual(status, 0)
            self.assertIn("-c", commands)
            for malformed in (
                {**valid_report, "skip_reason": "publication_active"},
                {**valid_report, "skip_reason": "unknown"},
                "{not-json",
                "__missing__",
            ):
                with self.subTest(malformed=malformed):
                    status, commands = execute(2, malformed)
                    self.assertNotEqual(status, 0)
                    self.assertIn("-c", commands)
            status, commands = execute(1, "__missing__")
            self.assertEqual(status, 1)
            self.assertNotIn("-c", commands)
