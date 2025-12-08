from __future__ import annotations

import typer

from .api import SportMonksClient
from .config import settings
from .db import make_session
from .logging_utils import configure_logging
from .sync import SyncService, bootstrap_schema

app = typer.Typer(add_completion=False)


def get_service() -> SyncService:
    if not settings.sportmonks_api_token:
        raise typer.BadParameter("SPORTMONKS_API_TOKEN is missing (set env or .env)")
    client = SportMonksClient(
        api_token=settings.sportmonks_api_token,
        requests_per_hour=settings.requests_per_hour,
    )
    session = make_session(settings.database_url)
    bootstrap_schema(session)
    return SyncService(client=client, session=session)


@app.callback()
def main() -> None:
    configure_logging(settings.log_level)


@app.command("sync-static")
def sync_static() -> None:
    """
    Sync base reference data: countries, leagues, seasons, venues.
    """
    service = get_service()
    service.sync_countries()
    service.sync_leagues()
    service.sync_seasons()
    service.sync_venues()


@app.command("sync-teams")
def sync_teams(season_id: int = typer.Option(None, help="Limit teams to a season ID")) -> None:
    """
    Sync teams (optionally for a specific season).
    """
    service = get_service()
    service.sync_teams(season_id=season_id)


@app.command("sync-players")
def sync_players(
    season_id: int = typer.Option(None, help="Limit players to a season ID"),
    team_id: int = typer.Option(None, help="Limit players to a team ID"),
) -> None:
    """
    Sync players (season and/or team scoped to reduce volume).
    """
    service = get_service()
    service.sync_players(season_id=season_id, team_id=team_id)


@app.command("sync-fixtures")
def sync_fixtures(
    season_id: int = typer.Option(None, help="Limit fixtures to a season ID"),
    team_ids: str = typer.Option(
        None, help="Comma-separated team IDs to filter fixtures (optional)"
    ),
) -> None:
    """
    Sync fixtures; optionally filter by season or teams.
    """
    teams = [int(t.strip()) for t in team_ids.split(",")] if team_ids else None
    service = get_service()
    service.sync_fixtures(season_id=season_id, team_ids=teams)


@app.command("sync-h2h")
def sync_h2h(team_a: int = typer.Argument(...), team_b: int = typer.Argument(...)) -> None:
    """
    Sync head-to-head data for two teams.
    """
    service = get_service()
    service.sync_h2h(team_a_id=team_a, team_b_id=team_b)


if __name__ == "__main__":
    app()
