import unittest
from datetime import datetime, timedelta

from scripts.sync_odds import (
    filter_fixtures_by_priority,
    fixture_priority_bucket,
    fixture_priority_in_scope,
)


class OddsPriorityLaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 9, 8, 18, 0, 0)

    def test_single_writer_ownership_boundaries_are_unchanged(self) -> None:
        self.assertEqual(fixture_priority_bucket(self.now + timedelta(hours=2), self.now), "p1")
        self.assertEqual(fixture_priority_bucket(self.now + timedelta(hours=24), self.now), "p2")
        self.assertEqual(fixture_priority_bucket(self.now + timedelta(hours=25), self.now), "p3")

    def test_p2_observes_p1_window(self) -> None:
        kickoff = self.now + timedelta(minutes=30)
        self.assertTrue(fixture_priority_in_scope(kickoff, self.now, "p1"))
        self.assertTrue(fixture_priority_in_scope(kickoff, self.now, "p2"))

    def test_p2_overlaps_p3_by_three_hours(self) -> None:
        kickoff = self.now + timedelta(hours=26, minutes=59)
        self.assertEqual(fixture_priority_bucket(kickoff, self.now), "p3")
        self.assertTrue(fixture_priority_in_scope(kickoff, self.now, "p2"))
        self.assertTrue(fixture_priority_in_scope(kickoff, self.now, "p3"))

    def test_overlap_is_marked_evidence_only(self) -> None:
        fixtures = [{"starting_at": datetime.now() + timedelta(hours=25)}]
        filtered = filter_fixtures_by_priority(fixtures, "p2")
        self.assertEqual(len(filtered), 1)
        self.assertFalse(filtered[0]["_priority_write_owned"])

    def test_overlap_is_bounded(self) -> None:
        kickoff = self.now + timedelta(hours=27, seconds=1)
        self.assertFalse(fixture_priority_in_scope(kickoff, self.now, "p2"))
        self.assertTrue(fixture_priority_in_scope(kickoff, self.now, "p3"))

    def test_past_fixture_is_not_in_live_lane(self) -> None:
        kickoff = self.now - timedelta(seconds=1)
        self.assertFalse(fixture_priority_in_scope(kickoff, self.now, "p1"))
        self.assertFalse(fixture_priority_in_scope(kickoff, self.now, "p2"))
        self.assertFalse(fixture_priority_in_scope(kickoff, self.now, "p3"))


if __name__ == "__main__":
    unittest.main()
