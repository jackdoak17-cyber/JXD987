from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.verify_fixture_delivery_parity import (
    expected_fixture_date,
    select_expected_history,
)


UTC = timezone.utc


class FixtureDeliveryParityTests(unittest.TestCase):
    def test_expected_fixture_date_uses_london_calendar(self) -> None:
        self.assertEqual(
            expected_fixture_date(datetime(2026, 8, 22, 23, 30, tzinfo=UTC)),
            "2026-08-23",
        )

    def test_expected_history_excludes_upcoming_target_result(self) -> None:
        target = {
            "id": 2,
            "starting_at": datetime(2026, 8, 28, tzinfo=UTC),
            "status": "NS",
            "home_score": None,
            "away_score": None,
        }
        history = [
            {"id": 2, "starting_at": target["starting_at"], "home_score": 2, "away_score": 0},
            {"id": 1, "starting_at": datetime(2026, 8, 22, tzinfo=UTC), "home_score": 1, "away_score": 0},
        ]
        self.assertEqual([row["id"] for row in select_expected_history(history, target)], [1])


if __name__ == "__main__":
    unittest.main()
