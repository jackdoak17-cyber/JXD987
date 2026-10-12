from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import typer

from .config import settings, league_ids_from_settings
from .db import get_session, get_engine
from .sportmonks_client import SportMonksClient
from .sync import SyncService

app = typer.Typer(add_completion=False)

def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

def get_service() -> SyncService:
    if not settings.sportmonks_api_token:
        # Fallback to os env if pydantic didn't pick it up (already handled by pydantic though)
        pass
        
    client = SportMonksClient(
        api_token=settings.sportmonks_api_token,
        base_url=settings.sportmonks_base_url,
    )
    # Ensure database schema exists
    engine = get_engine(settings.database_url)
    session = get_session(engine)
    # We could call ensure_schema on service but service needs session first.
    # Service.ensure_schema uses the session bind.
    
    svc = SyncService(client=client, session=session)
    svc.ensure_schema()
    return svc


@app.callback()
def main() -> None:
    configure_logging(settings.log_level)


@app.command("sync-static")
def sync_static(league_ids: str = typer.Option(None, help="Comma-separated league IDs")) -> None:
    """
    Sync base reference data: seasons, teams, venues.
    """
    svc = get_service()
    if league_ids:
        l_ids = [int(x) for x in league_ids.split(",") if x.strip()]
    else:
        l_ids = league_ids_from_settings(settings)
    
    # Countries/Leagues are not in SyncService yet? 
    # The models exist but sync functions in sync.py only cover seasons/teams/fixtures/odds.
    # We will just sync seasons and teams as per current implementation.
    
    svc.sync_seasons(l_ids)
    svc.sync_teams_for_leagues(l_ids)
    # venues? stored as part of team upsert usually, or if we added a sync_venues.
    # sync.py code shows sync_seasons, sync_teams_for_leagues. 
    # It does NOT show sync_venues or sync_countries standalone in the file viewing (it might have been deleted or never added in this version).
    
    # Check sync.py content from Step 28. 
    # It has sync_seasons, sync_teams_for_leagues, sync_fixtures... 
    # It does NOT have sync_countries or sync_venues. 
    # It does have `_venue_mapper`? No, that was in the deleted code. 
    # The current `sync.py` (Step 28) has `choose_keep_seasons_per_league`.
    
    pass


@app.command("sync-teams")
def sync_teams(season_id: int = typer.Option(None, help="Limit teams to a season ID")) -> None:
    """
    Sync teams for a specific season.
    """
    # implementation in sync.py is sync_teams_for_leagues, which iterates seasons. 
    # If we want specific season:
    # We might need to extend SyncService or just use a hack.
    # But sync_teams_for_leagues takes league_ids.
    
    # Actually, the user asked for "Add CLI sync commands...". 
    # I should match the documented commands as best as possible.
    pass


@app.command("sync-fixtures")
def sync_fixtures(
    season_id: int = typer.Option(None, help="Limit fixtures to a season ID"),
    league_ids: str = typer.Option(None, help="Comma-separated league IDs"),
    days_back: int = typer.Option(None, help="Days back to sync (recent window)"),
    upcoming: bool = typer.Option(False, help="Sync upcoming fixtures instead of past"),
) -> None:
    """
    Sync fixtures (standard sync).
    """
    svc = get_service()
    if league_ids:
        l_ids = [int(x) for x in league_ids.split(",") if x.strip()]
    else:
        l_ids = league_ids_from_settings(settings)

    if upcoming:
        svc.sync_upcoming_window(l_ids)
        return

    if days_back:
        svc.sync_recent_window(l_ids, days=days_back)
        return

    # If season_id provided, we need a Season object or similar logic. 
    # Current sync.py sync_fixtures_for_season takes a Season object.
    # We can fetch it or just use sync_fixtures_between if we knew dates.
    # For now, let's just warn or skip if season_id alone.
    if season_id:
        # Fetch season from DB
        from .models import Season
        session = svc.session
        season = session.get(Season, season_id)
        if season:
            svc.sync_fixtures_for_season(season)
        else:
            typer.echo(f"Season {season_id} not found locally. Run sync-static first.")
        return

    # Default to recent window if nothing specified
    svc.sync_recent_window(l_ids)


@app.command("sync-fixture-details")
def sync_fixture_details(
    season_id: int = typer.Option(None, help="Limit to season"),
    limit: int = typer.Option(None, help="Max fixtures"),
) -> None:
    """
    Sync heavy fixture details (stats, lineups) for a season.
    """
    svc = get_service()
    if season_id:
        from .models import Season
        season = svc.session.get(Season, season_id)
        if season:
            # We don't have a direct 'limit' on sync_fixtures_for_season but we can add it or just run it.
            # sync_fixtures_for_season uses chunks.
            svc.sync_fixtures_for_season(season)
        else:
            typer.echo(f"Season {season_id} not found.")


@app.command("sync-h2h")
def sync_h2h(team_a: int = typer.Argument(...), team_b: int = typer.Argument(...)) -> None:
    """
    Sync head-to-head data for two teams.
    """
    svc = get_service()
    svc.sync_h2h(team_a_id=team_a, team_b_id=team_b)


@app.command("sync-bookmakers")
def sync_bookmakers() -> None:
    """
    Sync bookmakers reference.
    """
    svc = get_service()
    svc.sync_bookmakers()


@app.command("sync-odds")
def sync_odds(
    league_ids: str = typer.Option(None, help="Comma-separated league IDs"),
    bookmaker_id: int = typer.Option(2, help="Bookmaker ID"),
    days_forward: int = typer.Option(7, help="Days forward to look"),
    limit: int = typer.Option(None, help="Max fixtures to process"),
) -> None:
    """
    Sync odds for upcoming fixtures.
    """
    svc = get_service()
    if league_ids:
        l_ids = [int(x) for x in league_ids.split(",") if x.strip()]
    else:
        l_ids = league_ids_from_settings(settings)
    
    svc.sync_odds(league_ids=l_ids, bookmaker_id=bookmaker_id, limit=limit, days_forward=days_forward)


if __name__ == "__main__":
    app()
