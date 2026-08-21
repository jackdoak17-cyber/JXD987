from datetime import datetime, timedelta
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jxd.models import Base, Fixture, Season
from jxd.sync import SyncService


class FakeClient:
    def __init__(self, rows_by_endpoint):
        self.rows_by_endpoint = rows_by_endpoint
        self.calls = []

    def fetch_collection(self, endpoint, params=None, includes=None, per_page=50):
        self.calls.append((endpoint, includes, per_page))
        yield from self.rows_by_endpoint.get(endpoint, [])


def make_service(client):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, future=True)()
    return SyncService(client, session), session


class TeamHistoryTests(unittest.TestCase):
    def test_backfill_uses_team_endpoint_and_persists_season_metadata(self):
        now = datetime.utcnow()
        client = FakeClient({})
        service, session = make_service(client)
        session.add(
            Fixture(
                id=900,
                league_id=109,
                season_id=300,
                starting_at=now + timedelta(days=2),
                home_team_id=10,
                away_team_id=20,
            )
        )
        for index in range(5):
            session.add(
                Fixture(
                    id=1000 + index,
                    league_id=85,
                    season_id=301,
                    starting_at=now - timedelta(days=index + 1),
                    home_team_id=20,
                    away_team_id=30,
                    home_score=1,
                    away_score=0,
                )
            )
        session.commit()

        start = (now - timedelta(days=365)).date().isoformat()
        end = now.date().isoformat()
        endpoint = f"fixtures/between/{start}/{end}/10"
        client.rows_by_endpoint[endpoint] = [
            {
                "id": 901,
                "starting_at": (now - timedelta(days=10)).isoformat(),
                "status": "FT",
                "participants": [
                    {"id": 10, "name": "Team 10", "meta": {"location": "home", "score": 2}},
                    {"id": 11, "name": "Team 11", "meta": {"location": "away", "score": 1}},
                ],
                "scores": [
                    {"type_id": 1525, "score": {"participant": "home", "goals": 2}},
                    {"type_id": 1525, "score": {"participant": "away", "goals": 1}},
                ],
                "season": {
                    "id": 400,
                    "league_id": 777,
                    "name": "Support season",
                    "starting_at": "2025-08-01",
                    "ending_at": "2026-05-31",
                },
                "league": {"id": 777, "name": "Support league"},
                "state": {"short_name": "FT"},
            }
        ]

        result = service.sync_team_history_for_recent_fixtures(
            [109],
            history_days=365,
            minimum_completed_matches=5,
            batch_size=10,
            batch_index=0,
        )

        self.assertEqual(
            result,
            {"teams_considered": 2, "teams_selected": 1, "fixtures_synced": 1, "teams_failed": 0},
        )
        self.assertEqual(
            client.calls,
            [(endpoint, ["participants", "scores", "state", "season", "league"], 50)],
        )
        self.assertEqual(session.get(Fixture, 901).home_score, 2)
        self.assertEqual(session.get(Fixture, 901).away_score, 1)
        self.assertEqual(session.get(Fixture, 901).league_id, 777)
        self.assertEqual(session.get(Fixture, 901).season_id, 400)
        self.assertEqual(session.get(Season, 400).league_id, 777)

    def test_backfill_does_not_call_provider_when_all_teams_are_complete(self):
        now = datetime.utcnow()
        client = FakeClient({})
        service, session = make_service(client)
        session.add(
            Fixture(
                id=900,
                league_id=8,
                season_id=300,
                starting_at=now + timedelta(days=1),
                home_team_id=10,
                away_team_id=20,
            )
        )
        for index in range(5):
            session.add(
                Fixture(
                    id=1000 + index,
                    league_id=8,
                    season_id=300,
                    starting_at=now - timedelta(days=index + 1),
                    home_team_id=10,
                    away_team_id=20,
                    home_score=1,
                    away_score=1,
                )
            )
        session.commit()

        result = service.sync_team_history_for_recent_fixtures([8], minimum_completed_matches=5)

        self.assertEqual(result["teams_considered"], 2)
        self.assertEqual(result["teams_selected"], 0)
        self.assertEqual(client.calls, [])
