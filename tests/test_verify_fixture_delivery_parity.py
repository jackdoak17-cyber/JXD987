from __future__ import annotations

import inspect
import unittest
from datetime import datetime, timezone

from scripts import verify_fixture_delivery_parity as parity


UTC = timezone.utc


class FixtureDeliveryParityTests(unittest.TestCase):
    def test_verifier_keeps_source_calculation_independent_from_refresh_worker(self) -> None:
        source = inspect.getsource(parity)
        self.assertNotIn("from scripts.refresh_fixture_delivery", source)
        self.assertNotIn("import refresh_fixture_delivery", source)

    def test_fixture_date_uses_london_calendar_at_utc_midnight_boundary(self) -> None:
        self.assertEqual(
            parity.expected_fixture_date(datetime(2026, 8, 22, 23, 30, tzinfo=UTC)),
            "2026-08-23",
        )

    def test_history_order_has_a_deterministic_fixture_id_tie_break(self) -> None:
        rows = [
            {"id": 2, "starting_at": datetime(2026, 8, 22, tzinfo=UTC)},
            {"id": 1, "starting_at": datetime(2026, 8, 22, tzinfo=UTC)},
        ]
        self.assertEqual([row["id"] for row in parity.order_history_rows(rows)], [1, 2])

    def test_completed_target_is_included_but_upcoming_target_is_not(self) -> None:
        history = [
            {
                "id": 10,
                "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
                "status": "FT",
                "status_code": "FT",
                "home_score": 1,
                "away_score": 0,
            },
        ]
        completed_target = {
            "id": 10,
            "starting_at": datetime(2026, 8, 22, tzinfo=UTC),
            "status": "FT",
            "status_code": "FT",
            "home_score": 1,
            "away_score": 0,
        }
        upcoming_target = {
            **completed_target,
            "status": "NS",
            "status_code": "NS",
            "home_score": None,
            "away_score": None,
        }
        self.assertEqual(
            [row["id"] for row in parity.select_expected_history(history, completed_target)],
            [10],
        )
        self.assertEqual(parity.select_expected_history(history, upcoming_target), [])

    def test_null_season_schedule_has_an_empty_expected_history(self) -> None:
        fixture = {
            "starting_at": datetime(2026, 8, 23, tzinfo=UTC),
            "season_id": None,
        }
        self.assertEqual(parity.select_expected_history([], fixture), [])


if __name__ == "__main__":
    unittest.main()
