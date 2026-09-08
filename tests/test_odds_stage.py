from __future__ import annotations

import json
import hashlib
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select

from jxd.db import get_engine, get_session
from jxd.models import Base, Fixture, OddsOutcome
from scripts.sync_odds import OddsStageWriter, apply_odds_stage


class OddsStageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "jxd.sqlite"
        self.env = patch.dict(
            os.environ,
            {"JXD_DB_PATH": str(self.db_path), "JXD_DB_URL": ""},
            clear=False,
        )
        self.env.start()
        self.engine = get_engine()
        Base.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.env.stop()
        self.temp_dir.cleanup()

    @staticmethod
    def row(fixture_id: int, price: float) -> dict[str, object]:
        return {
            "fixture_id": fixture_id,
            "bookmaker_id": 2,
            "market_key": "player_shots",
            "selection_key": "over",
            "participant_type": "player",
            "participant_id": 99,
            "line": 1.5,
            "price_decimal": price,
            "price_american": 120,
            "last_updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }

    def add_fixture(self, fixture_id: int, hours_from_now: int) -> None:
        session = get_session(self.engine)
        session.add(
            Fixture(
                id=fixture_id,
                league_id=8,
                starting_at=datetime.now(timezone.utc).replace(tzinfo=None)
                + timedelta(hours=hours_from_now),
            )
        )
        session.commit()
        session.close()

    def prices(self, fixture_id: int) -> list[float]:
        session = get_session(self.engine)
        values = session.execute(
            select(OddsOutcome.price_decimal).where(OddsOutcome.fixture_id == fixture_id)
        ).scalars().all()
        session.close()
        return [float(value) for value in values]

    def test_complete_stage_applies_idempotently(self) -> None:
        self.add_fixture(1001, 48)
        stage = Path(self.temp_dir.name) / "p3.ndjson.gz"
        writer = OddsStageWriter(stage, priority="p3", calendar_history=False)
        writer.write_operation(
            fixture_id=1001,
            bookmaker_id=2,
            market_keys=["player_shots"],
            rows=[self.row(1001, 2.2)],
        )
        writer.complete()

        first = apply_odds_stage(stage, 60)
        second = apply_odds_stage(stage, 60)

        self.assertEqual(first["operations_applied"], 1)
        self.assertEqual(second["operations_applied"], 1)
        self.assertEqual(self.prices(1001), [2.2])

    def test_incomplete_stage_rolls_back_every_operation(self) -> None:
        self.add_fixture(1002, 48)
        stage = Path(self.temp_dir.name) / "incomplete.ndjson"
        stage.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "type": "header",
                            "schema_version": 1,
                            "generated_at": datetime.now(timezone.utc).isoformat(),
                            "priority": "p3",
                            "calendar_history": False,
                        }
                    ),
                    json.dumps(
                        {
                            "type": "operation",
                            "fixture_id": 1002,
                            "bookmaker_id": 2,
                            "market_keys": ["player_shots"],
                            "rows": [self.row(1002, 2.4)],
                        },
                        default=str,
                    ),
                ]
            ),
            encoding="utf-8",
        )
        stage.with_name(f"{stage.name}.sha256").write_text(
            f"{hashlib.sha256(stage.read_bytes()).hexdigest()}  {stage.name}\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(SystemExit, "stage is incomplete"):
            apply_odds_stage(stage, 60)

        self.assertEqual(self.prices(1002), [])

    def test_p3_stage_cannot_overwrite_a_fixture_now_owned_by_p1(self) -> None:
        self.add_fixture(1003, 2)
        stage = Path(self.temp_dir.name) / "p3.ndjson.gz"
        writer = OddsStageWriter(stage, priority="p3", calendar_history=False)
        writer.write_operation(
            fixture_id=1003,
            bookmaker_id=2,
            market_keys=["player_shots"],
            rows=[self.row(1003, 2.6)],
        )
        writer.complete()

        result = apply_odds_stage(stage, 60)

        self.assertEqual(result["operations_applied"], 0)
        self.assertEqual(result["operations_skipped_out_of_scope"], 1)
        self.assertEqual(self.prices(1003), [])

    def test_modified_stage_is_rejected_before_database_changes(self) -> None:
        self.add_fixture(1004, 48)
        stage = Path(self.temp_dir.name) / "p3.ndjson.gz"
        writer = OddsStageWriter(stage, priority="p3", calendar_history=False)
        writer.write_operation(
            fixture_id=1004,
            bookmaker_id=2,
            market_keys=["player_shots"],
            rows=[self.row(1004, 2.8)],
        )
        writer.complete()
        stage.write_bytes(stage.read_bytes() + b"modified")

        with self.assertRaisesRegex(SystemExit, "checksum does not match"):
            apply_odds_stage(stage, 60)

        self.assertEqual(self.prices(1004), [])


if __name__ == "__main__":
    unittest.main()
