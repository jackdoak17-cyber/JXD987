import unittest
from unittest.mock import Mock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jxd.models import (
    Base,
    Player,
    PlayerTeamHistory,
    Season,
    Team,
    TeamSquadMembership,
    TeamSquadSnapshot,
)
from jxd.sync import SyncService
from scripts import sync_sparse_squads


class FakeSquadClient:
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
    session.add_all([Team(id=15, name="Aston Villa"), Team(id=19, name="Arsenal")])
    session.commit()
    return SyncService(client, session), session


def squad_row(player_id, name, start):
    return {
        "player_id": player_id,
        "start": start,
        "player": {
            "id": player_id,
            "name": name,
            "display_name": name,
        },
    }


class SquadReconciliationTests(unittest.TestCase):
    def test_exported_count_supports_deployed_exporter_return_shapes(self):
        self.assertEqual(sync_sparse_squads.exported_count(7), 7)
        self.assertEqual(sync_sparse_squads.exported_count((7, "ignored")), 7)

    def test_current_provider_teams_are_based_on_current_seasons(self):
        client = FakeSquadClient({"seasons": [], "teams/seasons/25583": []})
        service, session = make_service(client)
        session.add(Season(id=25583, league_id=8, is_current=True))
        session.commit()

        provider_seasons, provider_teams = sync_sparse_squads.refresh_current_provider_teams(
            session, service, [8]
        )

        self.assertEqual(provider_seasons, [25583])
        self.assertEqual(provider_teams, [])
        self.assertEqual(client.calls, [("seasons", ["league"], 200), ("teams/seasons/25583", ["venue"], 200)])

    def test_transfer_reconciles_projection_memberships_and_history(self):
        client = FakeSquadClient(
            {
                "squads/teams/15": [squad_row(7124, "Ezri Konsa", "2019-07-11")],
                "squads/teams/19": [squad_row(7124, "Ezri Konsa", "2026-08-21")],
            }
        )
        service, session = make_service(client)

        service.sync_squads_for_teams([15])
        self.assertEqual(session.get(Player, 7124).team_id, 15)
        self.assertTrue(session.get(TeamSquadMembership, (15, 7124)).is_active)

        service.sync_squads_for_teams([19])

        player = session.get(Player, 7124)
        self.assertEqual(player.team_id, 19)
        self.assertFalse(session.get(TeamSquadMembership, (15, 7124)).is_active)
        self.assertTrue(session.get(TeamSquadMembership, (19, 7124)).is_active)
        history = (
            session.query(PlayerTeamHistory)
            .filter(PlayerTeamHistory.player_id == 7124)
            .order_by(PlayerTeamHistory.effective_from)
            .all()
        )
        self.assertEqual(
            [(row.team_id, row.effective_to is None) for row in history],
            [(15, False), (19, True)],
        )

    def test_successful_snapshot_removes_departures_but_empty_snapshot_preserves_state(self):
        client = FakeSquadClient(
            {
                "squads/teams/15": [squad_row(7124, "Ezri Konsa", "2019-07-11")],
            }
        )
        service, session = make_service(client)
        service.sync_squads_for_teams([15])

        client.rows_by_endpoint["squads/teams/15"] = [squad_row(9000, "Replacement Player", "2026-08-01")]
        service.sync_squads_for_teams([15])
        self.assertIsNone(session.get(Player, 7124).team_id)
        self.assertFalse(session.get(TeamSquadMembership, (15, 7124)).is_active)
        former_assignment = (
            session.query(PlayerTeamHistory)
            .filter(PlayerTeamHistory.player_id == 7124)
            .one()
        )
        self.assertIsNotNone(former_assignment.effective_to)

        client.rows_by_endpoint["squads/teams/15"] = []
        service.sync_squads_for_teams([15])
        self.assertIsNone(session.get(Player, 7124).team_id)
        self.assertFalse(session.get(TeamSquadMembership, (15, 7124)).is_active)
        self.assertEqual(
            session.query(TeamSquadSnapshot).order_by(TeamSquadSnapshot.id.desc()).first().status,
            "empty",
        )

    def test_remote_detach_is_scoped_to_the_team_at_write_time(self):
        get_response = Mock(ok=True)
        get_response.json.return_value = [{"id": 7124}, {"id": 9000}]
        patch_response = Mock(ok=True)

        with patch.object(sync_sparse_squads, "SUPABASE_URL", "https://example.supabase.co"), patch.object(
            sync_sparse_squads, "rest_headers", return_value={"Authorization": "Bearer service"}
        ), patch.object(sync_sparse_squads.requests, "get", return_value=get_response), patch.object(
            sync_sparse_squads.requests, "patch", return_value=patch_response
        ) as patch_request:
            result = sync_sparse_squads.detach_remote_players_missing_from_squads(
                [15], [{"id": 7124, "team_id": 15}], dry_run=False
            )

        self.assertEqual(result, {"15": 1})
        self.assertEqual(
            patch_request.call_args.kwargs["params"],
            {"id": "in.(9000)", "team_id": "eq.15"},
        )
