from __future__ import annotations

import inspect
import subprocess
import unittest
from pathlib import Path

from scripts import reconcile_fixture_delivery_odds as reconciliation


ROOT = Path(__file__).resolve().parents[1]


class FixtureDeliveryOddsReconciliationTests(unittest.TestCase):
    def test_runner_invokes_reconciliation_only_after_successful_export(self) -> None:
        wrapper = ROOT / "scripts/vps/run_odds_delivery.sh"
        result = subprocess.run(["bash", "-n", str(wrapper)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = wrapper.read_text(encoding="utf-8")
        exporter = source.index("export_odds_to_supabase_psql.py}")
        reconciler = source.index("python scripts/reconcile_fixture_delivery_odds.py")
        self.assertLess(exporter, reconciler)
        self.assertIn("set -euo pipefail", source)

    def test_runtime_manifest_includes_reconciler(self) -> None:
        entries = {
            line.strip()
            for line in (ROOT / "scripts/vps/runtime_files.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
        self.assertIn("scripts/reconcile_fixture_delivery_odds.py", entries)

    def test_reconciliation_reuses_publisher_lock_and_one_transaction(self) -> None:
        source = inspect.getsource(reconciliation.reconcile)
        self.assertIn("pg_try_advisory_xact_lock", source)
        self.assertEqual(reconciliation.LOCK_NAME, "fixture_delivery_refresh")
        self.assertEqual(source.count("conn.commit()"), 2)  # no-op and changed paths
        self.assertNotIn("conn.commit()\n        cur.execute", source)

    def test_source_eligibility_is_shared_with_full_publisher(self) -> None:
        self.assertEqual(reconciliation.ACTIVE_BOOKMAKERS, {2, 4, 5, 8})
        self.assertIn("moneyline", reconciliation.MONEYLINE_MARKETS)
        self.assertEqual(reconciliation.EXCLUDED_CUPS, {24, 27, 109, 307, 390, 570})
        source = inspect.getsource(reconciliation.reconcile)
        self.assertIn("o.price_decimal > 1 and o.price_decimal <= 500", source)
        self.assertIn("lower(o.market_key) = any(%s)", source)

    def test_exact_match_is_a_zero_write_path(self) -> None:
        source = inspect.getsource(reconciliation.reconcile)
        guard = 'if not result["affected_release_fixture_pairs"]:'
        self.assertIn(guard, source)
        guarded = source[source.index(guard):source.index('cur.execute(\n            """\n            delete')]
        self.assertNotIn("delete from", guarded.lower())
        self.assertNotIn("insert into", guarded.lower())

    def test_projection_compares_all_mutable_card_odds_values(self) -> None:
        source = inspect.getsource(reconciliation.reconcile)
        for column in ("line", "price_decimal", "price_american", "source_last_updated_at"):
            self.assertIn(f"e.{column} is distinct from d.{column}", source)
        for key in ("participant_type", "participant_id", "line_key"):
            self.assertIn(f"e.{key} = d.{key}", source)

    def test_current_and_unexpired_pinned_releases_are_reconciled(self) -> None:
        source = inspect.getsource(reconciliation.reconcile)
        self.assertIn("r.id = p.release_id or r.pin_expires_at > now()", source)


if __name__ == "__main__":
    unittest.main()
