import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from validate_moneyline_coverage import expected_moneyline_pairs, has_complete_api_moneyline


class ValidateMoneylineCoverageTests(unittest.TestCase):
    def test_provider_complete_moneyline_requires_three_sides(self) -> None:
        self.assertTrue(
            has_complete_api_moneyline(
                [{"name": "h2h", "odds": [{"home": 1.9, "draw": 3.4, "away": 4.1}]}],
            )
        )
        self.assertFalse(
            has_complete_api_moneyline(
                [{"name": "h2h", "odds": [{"home": 1.9, "away": 4.1}]}],
            )
        )
        self.assertFalse(
            has_complete_api_moneyline(
                [{"name": "h2h", "odds": [{"home": 91, "draw": 67, "away": 1}]}],
            )
        )

    def test_expected_pairs_include_only_provider_supported_books(self) -> None:
        expected, by_fixture = expected_moneyline_pairs(
            [
                {
                    "fixture_id": 19629715,
                    "bookmakers_payload": {
                        "BetMGM": [
                            {"name": "h2h", "odds": [{"home": 1.9, "draw": 3.4, "away": 4.1}]}
                        ],
                        "Bet365": [
                            {"name": "h2h", "odds": [{"home": 1.9, "away": 4.1}]}
                        ],
                    },
                }
            ],
            requested_bookmaker_keys={"betmgm", "bet365"},
            bookmaker_id_allowlist={2, 8},
        )

        self.assertEqual(set(expected), {(19629715, 8)})
        self.assertEqual(by_fixture, {19629715: {8}})


if __name__ == "__main__":
    unittest.main()
