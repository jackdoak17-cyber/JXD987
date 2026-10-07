import importlib.util
from pathlib import Path
import unittest


PATH = Path(__file__).resolve().parents[1] / "scripts/odds_delivery_shadow.py"
spec = importlib.util.spec_from_file_location("odds_delivery_shadow", PATH)
shadow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shadow)


def row(
    fixture=1,
    bookmaker=10,
    market="moneyline",
    selection="home",
    line=None,
    decimal="2.10",
    american=110,
    participant_type=None,
    participant_id=None,
    updated="2026-10-07T09:00:00+00:00",
):
    return {
        "fixture_id": fixture,
        "bookmaker_id": bookmaker,
        "market_key": market,
        "selection_key": selection,
        "line": line,
        "price_decimal": decimal,
        "price_american": american,
        "participant_type": participant_type,
        "participant_id": participant_id,
        "last_updated_at": updated,
    }


class ShadowEquivalenceTest(unittest.TestCase):
    def assert_equivalent(self, target, stage):
        current = shadow.simulate_current_replacement(target, stage)
        proposed = shadow.simulate_incremental_diff(target, stage)
        self.assertEqual(shadow.normalized_dataset(current), shadow.normalized_dataset(proposed))

    def test_required_edge_cases_are_equivalent(self):
        base = row()
        cases = {
            "unchanged": ([base], [dict(base)]),
            "price_change": ([base], [row(decimal="2.20", american=120)]),
            "new_outcome": ([base], [base, row(selection="away")]),
            "new_market": ([base], [base, row(market="btts", selection="yes")]),
            "removed_outcome": ([base, row(selection="away")], [base]),
            "whole_market_absent": ([base, row(market="btts", selection="yes")], [base]),
            "whole_bookmaker_absent": ([base, row(bookmaker=11)], [base]),
            "suspended_invalid_price": ([base], [row(decimal="1.0")]),
            "line_change": ([row(line="2.5")], [row(line="3.5")]),
            "participant_change": (
                [row(participant_type="player", participant_id=1)],
                [row(participant_type="player", participant_id=2)],
            ),
            "participant_to_null": (
                [row(participant_type="player", participant_id=1)],
                [row(participant_type=None, participant_id=None)],
            ),
            "postponed_fixture_untouched": ([base], []),
            "cancelled_fixture_touched": ([base], [row(decimal="2.20", american=120)]),
            "overlapping_sources": (
                [base],
                [base, row(decimal="2.20", american=120, updated="2026-10-07T09:01:00+00:00")],
            ),
            "partial_response": ([base, row(selection="away")], [base]),
            "retry_idempotency": ([base], [base, base]),
            "malformed_legacy_line": ([row(line="-9999")], [row(line=None)]),
            "retention_scope_untouched": ([row(fixture=2)], [base]),
            "all_filtered_touched_market": ([base, row(selection="away")], [row(decimal="1.0")]),
            "timestamp_only": ([base], [row(updated="2026-10-07T09:01:00+00:00")]),
        }
        for name, (target, stage) in cases.items():
            with self.subTest(name=name):
                self.assert_equivalent(target, stage)

    def test_failure_before_commit_cannot_change_target(self):
        target = [row()]
        original = shadow.normalized_dataset(target)
        shadow.simulate_incremental_diff(target, [row(decimal="2.20")])
        self.assertEqual(original, shadow.normalized_dataset(target))

    def test_shadow_sql_is_read_only_for_persistent_table(self):
        sql = shadow.build_target_snapshot_sql().lower()
        self.assertIn("from public.odds_outcomes", sql)
        self.assertNotIn("insert into public.odds_outcomes", sql)
        self.assertNotIn("update public.odds_outcomes", sql)
        self.assertNotIn("delete from public.odds_outcomes", sql)
        self.assertIn("join odds_outcomes_shadow_scopes", sql)
        fixture_sql = shadow.build_fixture_state_sql([8, 384], 0, 14, True).lower()
        self.assertIn("is_settled", fixture_sql)
        self.assertIn("date_trunc('day'", fixture_sql)
        self.assertIn("f.league_id in (8,384)", fixture_sql)

    def test_touched_scope_precedes_price_filter(self):
        self.assert_equivalent([row(), row(selection="away")], [row(decimal="1.0")])

    def test_change_telemetry_separates_timestamp_only_from_price(self):
        target_rows = [
            shadow.normalize_row(row(selection="home")),
            shadow.normalize_row(row(selection="away")),
            shadow.normalize_row(row(selection="draw")),
        ]
        stage_rows = [
            shadow.normalize_row(row(selection="home", updated="2026-10-07T09:01:00+00:00")),
            shadow.normalize_row(row(selection="away", decimal="2.20", american=120)),
            shadow.normalize_row(row(selection="new")),
        ]
        target = {shadow.canonical_key(item): item for item in target_rows}
        stage = {shadow.canonical_key(item): item for item in stage_rows}
        comparison = shadow.compare_canonical_maps(stage, target, 3, 0, 1)
        metrics = comparison["metrics"]
        self.assertEqual(metrics["timestamp_only_changes"], 1)
        self.assertEqual(metrics["price_changes"], 1)
        self.assertEqual(metrics["new_keys"], 1)
        self.assertEqual(metrics["removed_keys"], 1)
        self.assertEqual(comparison["differing_canonical_rows"], 0)
        self.assertEqual(comparison["differing_fixture_hashes"], 0)
        self.assertEqual(comparison["differing_scope_hashes"], 0)

    def test_settled_existing_rows_are_immutable_but_missing_keys_insert(self):
        target_rows = [shadow.normalize_row(row(selection="home"))]
        stage_rows = [
            shadow.normalize_row(row(selection="home", decimal="2.20", american=120)),
            shadow.normalize_row(row(selection="away")),
        ]
        target = {shadow.canonical_key(item): item for item in target_rows}
        stage = {shadow.canonical_key(item): item for item in stage_rows}
        comparison = shadow.compare_canonical_maps(
            stage,
            target,
            raw_count=2,
            invalid_count=0,
            touched_scope_count=1,
            settled_fixture_ids={1},
        )
        metrics = comparison["metrics"]
        self.assertEqual(metrics["new_keys"], 1)
        self.assertEqual(metrics["updated_keys"], 0)
        self.assertEqual(metrics["settled_existing_changes_ignored"], 1)
        self.assertEqual(metrics["predicted_current_persistent_writes"], 1)
        self.assertEqual(metrics["predicted_diff_persistent_writes"], 1)
        self.assertEqual(comparison["differing_canonical_rows"], 0)

    def test_incremental_sql_is_bounded_and_preserves_exact_null_semantics(self):
        exporter = (
            Path(__file__).resolve().parents[1]
            / "scripts/export_odds_to_supabase_psql.py"
        ).read_text(encoding="utf-8")
        self.assertIn("create temp table odds_outcomes_scopes", exporter)
        self.assertIn("create temp table odds_outcomes_src", exporter)
        self.assertIn("from odds_outcomes_src src", exporter)
        self.assertIn("participant_type = excluded.participant_type", exporter)
        self.assertIn("participant_id = excluded.participant_id", exporter)
        self.assertIn("last_updated_at = excluded.last_updated_at", exporter)
        self.assertNotIn(
            "participant_type = coalesce(excluded.participant_type, o.participant_type)",
            exporter,
        )


if __name__ == "__main__":
    unittest.main()
