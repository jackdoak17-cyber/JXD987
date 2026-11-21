import unittest
from datetime import datetime, timedelta, timezone
from scripts.refresh_fixture_delivery import publication_skip_reason

class RetentionPinTests(unittest.TestCase):
    def test_equal_source_refresh_does_not_shorten_retained_snapshot(self):
        now = datetime(2026, 11, 21, 8, tzinfo=timezone.utc)
        evidence = {'verified_at': (now - timedelta(hours=2)).isoformat()}
        retained_until = now + timedelta(hours=8)
        current = {'published_at': now - timedelta(hours=2),
                   'health_checked_at': now - timedelta(hours=2),
                   'pin_expires_at': retained_until,
                   'metadata': {'last_source_freshness_evidence': evidence}}
        for guarded in (False, True):
            with self.subTest(guarded=guarded):
                self.assertIsNone(publication_skip_reason(
                    current, evidence, now + timedelta(hours=1), guarded=guarded,
                    min_interval_seconds=3600, now=now))
                self.assertEqual(current['pin_expires_at'], retained_until)

    def test_older_source_still_cannot_replace_newer_publication(self):
        now = datetime(2026, 11, 21, 8, tzinfo=timezone.utc)
        current = {'pin_expires_at': now + timedelta(hours=8),
                   'metadata': {'last_source_freshness_evidence': {'verified_at': now.isoformat()}}}
        self.assertEqual(publication_skip_reason(
            current, {'verified_at': (now - timedelta(hours=1)).isoformat()},
            now + timedelta(hours=1), guarded=False, min_interval_seconds=0, now=now),
            'newer_current_source')
