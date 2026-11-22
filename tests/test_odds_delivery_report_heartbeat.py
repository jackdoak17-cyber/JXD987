"""The real exporter report must expose its CSV count in the bounded heartbeat."""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
EXPORTER = ROOT / "scripts/export_odds_to_supabase_psql.py"
COMMON = ROOT / "scripts/vps/common.sh"
TOTAL_ROWS_TOKEN = re.compile(r'"total_rows":\s*(\d+)')


def actual_report_expression():
    """Execute the exporter's report dictionary without opening its databases."""
    tree = ast.parse(EXPORTER.read_text(encoding="utf-8"), filename=str(EXPORTER))
    reports = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "report" for target in node.targets)
        and isinstance(node.value, ast.Dict)
    ]
    if len(reports) != 1:
        raise AssertionError(f"expected one exporter report constructor, found {len(reports)}")
    expression = ast.fix_missing_locations(ast.Expression(body=reports[0]))
    return compile(expression, str(EXPORTER), "eval")


REPORT_EXPRESSION = actual_report_expression()


def exporter_report(total_rows):
    metadata = "x" * 2000
    context = dict(
        ingest_ok=True,
        atomic_context=SimpleNamespace(
            committed=True,
            quote_report={"files": 0, "detail": metadata},
            release_report={"status": "noop", "detail": metadata},
        ),
        atomic_mode="delivery",
        error_stage=None,
        error_message=None,
        psql_err_tail=None,
        psql_out_tail=None,
        csv_summary_data={"total_rows": total_rows, "rows_by_bookmaker_id": {}},
        sqlite_bookmaker_counts={},
        supabase_bookmaker_counts={},
        start_iso="2026-11-22T04:34:08Z",
        end_iso="2026-11-22T04:34:53Z",
        start_time=0,
        end_time=45.21,
        args=SimpleNamespace(
            days_back=0,
            days_forward=14,
            calendar_window=True,
            retention_days_back=7,
            retention_days_forward=14,
            retention_snapshots_days=7,
        ),
        fixture_count=20,
        market_stats={},
        shot_market_coverage={},
        shot_coverage_by_day={},
        shot_coverage_by_hours={},
        warnings=[],
        market_allowlist=None,
        allowlist_enabled=False,
        allowlist_source=None,
        allowlist_dropped_rows=0,
        dropped_by_allowlist={},
        stored_by_market={},
        sync_report=None,
        effective_leagues=[384],
        total_rows_all=total_rows,
        total_rows_estimate=total_rows,
        total_rows=total_rows,
        invalid_goals_over_under=0,
        counts={"stage_count": total_rows, "inserted": 0, "updated": 0, "unchanged": total_rows},
        partial_run=False,
        last_fixture_id=101,
        coverage_total=0,
        coverage_mapped=0,
        coverage_pct=0,
        coverage_baseline_pct=0,
        retention_deleted=0,
        snapshots_deleted=0,
        csv_head_path=Path("/tmp/head.csv"),
        csv_summary_path=Path("/tmp/summary.json"),
        err_path="/tmp/error.txt",
        out_path="/tmp/output.txt",
        verification_outputs={},
    )
    return eval(REPORT_EXPRESSION, {"os": os}, context)  # noqa: S307 - trusted checked-in AST


def actual_bounded_heartbeat(report_text: str) -> str:
    """Source common.sh and stub only its psql sink; no database is contacted."""
    with tempfile.TemporaryDirectory() as directory:
        report_path = Path(directory) / "report.json"
        capture_path = Path(directory) / "evidence.txt"
        report_path.write_text(report_text, encoding="utf-8")
        env = {
            **os.environ,
            "COMMON_PATH": str(COMMON),
            "PIPELINE_EVIDENCE_FILE": str(report_path),
            "OPERATIONS_CHECK_RUNNER_DATABASE_URL": "stub-not-a-database",
            "CAPTURE_PATH": str(capture_path),
        }
        shell = r'''
set -euo pipefail
source "$COMMON_PATH"
psql() {
  local arg
  for arg in "$@"; do
    if [[ "$arg" == evidence=* ]]; then
      printf '%s' "${arg#evidence=}" > "$CAPTURE_PATH"
    fi
  done
  cat >/dev/null
}
record_pipeline_job_run run_odds_delivery 'Website odds delivery' 0 \
  '2026-11-22T04:34:08Z' '2026-11-22T04:34:53Z' 45000
'''
        subprocess.run(["bash", "-c", shell], env=env, check=True, capture_output=True, text=True)
        return capture_path.read_text(encoding="utf-8")


class DeliveryReportHeartbeatTests(unittest.TestCase):
    def test_nonzero_noop_export_count_survives_actual_500_character_heartbeat(self):
        report = exporter_report(171727)
        self.assertEqual(report["rows_exported_csv"], 171727)
        self.assertEqual(report["rows_copied"], 171727)
        self.assertEqual(report["rows_inserted"], 0)
        self.assertEqual(report["rows_updated"], 0)
        self.assertTrue(report["publication_committed"])
        self.assertFalse(report["partial_run"])

        evidence = actual_bounded_heartbeat(json.dumps(report, indent=2))
        self.assertEqual(len(evidence), 500)
        self.assertEqual(int(TOTAL_ROWS_TOKEN.search(evidence).group(1)), 171727)
        self.assertNotIn('"rows_exported_csv"', evidence)

    def test_zero_and_invalid_csv_counts_are_not_replaced_by_affected_rows(self):
        for value, expected in ((0, 0), (None, None), ("corrupt", None)):
            with self.subTest(value=value):
                report = exporter_report(value)
                evidence = actual_bounded_heartbeat(json.dumps(report, indent=2))
                match = TOTAL_ROWS_TOKEN.search(evidence)
                self.assertEqual(int(match.group(1)) if match else None, expected)

    def test_missing_or_corrupt_report_cannot_create_a_positive_export_count(self):
        for content in ("{}", '{"total_rows": null}', '{"total_rows": "corrupt"}'):
            with self.subTest(content=content):
                evidence = actual_bounded_heartbeat(content)
                self.assertIsNone(TOTAL_ROWS_TOKEN.search(evidence))


if __name__ == "__main__":
    unittest.main()
