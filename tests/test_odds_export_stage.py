import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

PATH = Path(__file__).resolve().parents[1] / 'scripts/export_odds_to_supabase_psql.py'
spec = importlib.util.spec_from_file_location('exporter', PATH)
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)

class StageTest(unittest.TestCase):
    def test_generated_copy_does_not_allocate_live_ids(self):
        captured = []
        def capture(_url, sql_path, **kwargs):
            captured.append(Path(sql_path).read_text())
            return ''
        with tempfile.TemporaryDirectory() as folder:
            csv = Path(folder) / 'odds.csv'
            with patch.object(exporter.shutil, 'which', return_value='/usr/bin/psql'), patch.object(exporter, 'run_psql', side_effect=capture):
                exporter.stage_and_upsert('unused', csv, 'test', [8], 0, 14, exporter.DEFAULT_MARKET_ALLOWLIST, False, None, None)
        sql = captured[0]
        stage = sql.split('create temp table odds_outcomes_stage')[1].split(';')[0]
        self.assertIn('on commit drop as', stage)
        self.assertIn('with no data', stage)
        self.assertNotIn('including defaults', stage)
        self.assertIn('last_updated_at', stage)
        self.assertNotIn('nextval', stage)
        self.assertIn('on conflict', sql.lower())

if __name__ == '__main__': unittest.main()
