from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.p3_stage_evidence_freshness import reusable


class P3StageEvidenceFreshnessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.report = Path(self.temporary.name) / "p3-report.json"
        self.now = datetime(2026, 11, 21, 18, 19, tzinfo=timezone.utc)

    def write_report(self, age: timedelta) -> None:
        self.report.write_text(
            json.dumps({"generated_at": (self.now - age).isoformat()}),
            encoding="utf-8",
        )

    def may_reuse_recent_evidence(self) -> bool:
        return reusable(
            self.report,
            max_age_hours=6,
            remaining_runtime_seconds=1800,
            safety_seconds=60,
            now=self.now,
        )

    def test_recent_interrupted_stage_can_be_reused(self) -> None:
        self.write_report(timedelta(hours=5))
        self.assertTrue(self.may_reuse_recent_evidence())

    def test_stage_inside_transport_window_but_too_old_for_validation_is_refetched(self) -> None:
        # The 18:19 production retry reused a 12:21 report under the old
        # eight-hour transport check, then validation rejected it at 18:21.
        self.write_report(timedelta(hours=5, minutes=58))
        self.assertFalse(self.may_reuse_recent_evidence())

    def test_missing_or_malformed_report_is_refetched(self) -> None:
        self.assertFalse(self.may_reuse_recent_evidence())
        self.report.write_text('{"generated_at":"not-a-time"}', encoding="utf-8")
        self.assertFalse(self.may_reuse_recent_evidence())


if __name__ == "__main__":
    unittest.main()
