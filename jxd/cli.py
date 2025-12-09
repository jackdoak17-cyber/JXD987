from __future__ import annotations

import typer

from .api import SportMonksClient
from .config import league_ids_from_settings, settings
from .db import make_session
from .logging_utils import configure_logging
from .sync import SyncService, bootstrap_schema
from .aggregate import bulk_compute_forms, normalize_odds

app = typer.Typer(add_completion=False)


def get_service() -> SyncService:
    if not settings.sportmonks_api_token:
        raise typer.BadParameter("SPORTMONKS_API_TOKEN is missing (set env or .env)")
    client = SportMonksClient(
        api_token=settings.sportmonks_api_token,
        requests_per_hour=settings.requests_per_hour,
        base_url=settings.sportmonks_base_url,
        use_filters_populate=settings.use_filters_populate,
    )
    session = make_session(settings.database_url)
    bootstrap_schema(session)
    return SyncService(client=client, session=session)


def _parse_csv(csv: str | None) -> list[int]:
    if not csv:
        return []
    out = []
    for piece in csv.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            out.append(int(piece))
        except ValueError:
            continue
    return out


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
    league_ids: str = typer.Option(
        None, help="Comma-separated league IDs to filter fixtures (optional)"
    ),
) -> None:
    """
    Sync fixtures; optionally filter by season or teams.
    """
    teams = [int(t.strip()) for t in team_ids.split(",")] if team_ids else None
    leagues = _parse_csv(league_ids) or league_ids_from_settings(settings)
    service = get_service()
    service.sync_fixtures(season_id=season_id, team_ids=teams, league_ids=leagues)

@app.command("sync-fixtures-between")
def sync_fixtures_between(
    start_date: str = typer.Argument(..., help="Start date YYYY-MM-DD"),
    end_date: str = typer.Argument(..., help="End date YYYY-MM-DD"),
    league_ids: str = typer.Option(
        None, help="Comma-separated league IDs to filter fixtures (optional)"
    ),
    with_details: bool = typer.Option(
        False, help="Include statistics + lineups (heavier, fewer per-hour)"
    ),
) -> None:
    """
    Sync fixtures between two dates (useful for upcoming/past windows).
    """
    leagues = _parse_csv(league_ids) or league_ids_from_settings(settings)
    service = get_service()
    service.sync_fixtures_between(
        start_date=start_date,
        end_date=end_date,
        league_ids=leagues,
        with_details=with_details,
    )


@app.command("sync-fixture-details")
def sync_fixture_details(
    season_id: int = typer.Option(None, help="Limit fixtures to a season ID"),
    league_ids: str = typer.Option(
        None, help="Comma-separated league IDs to filter fixtures (optional)"
    ),
    limit: int = typer.Option(
        None, help="Limit number of fixtures processed (debug/safety switch)"
    ),
) -> None:
    """
    Sync fixtures with heavy includes (participants, statistics, lineups) into team/player stats tables.
    """
    leagues = _parse_csv(league_ids) or league_ids_from_settings(settings)
    service = get_service()
    service.sync_fixture_details(season_id=season_id, league_ids=leagues, limit=limit)


@app.command("sync-bookmakers")
def sync_bookmakers() -> None:
    """
    Sync bookmaker reference data (odds providers).
    """
    service = get_service()
    service.sync_bookmakers()


@app.command("sync-odds")
def sync_odds(
    fixture_ids: str = typer.Option(
        None, help="Comma-separated fixture IDs to fetch odds for (optional)"
    ),
    league_ids: str = typer.Option(
        None, help="Comma-separated league IDs to pull odds for (optional)"
    ),
    bookmaker_id: int = typer.Option(None, help="Bookmaker ID (default Bet365=2)"),
    limit: int = typer.Option(
        None, help="Limit number of fixtures processed (safety switch)"
    ),
) -> None:
    """
    Sync odds for fixtures (defaults to Bet365 bookmaker).
    """
    service = get_service()
    fixture_list = _parse_csv(fixture_ids) if fixture_ids else None
    leagues = _parse_csv(league_ids) or league_ids_from_settings(settings)
    service.sync_odds(
        fixture_ids=fixture_list,
        bookmaker_id=bookmaker_id or settings.bookmaker_id,
        league_ids=leagues,
        limit=limit,
    )


@app.command("sync-h2h")
def sync_h2h(team_a: int = typer.Argument(...), team_b: int = typer.Argument(...)) -> None:
    """
    Sync head-to-head data for two teams.
    """
    service = get_service()
    service.sync_h2h(team_a_id=team_a, team_b_id=team_b)


@app.command("compute-forms")
def compute_forms(
    sample_size: int = typer.Option(10, help="Number of recent games to average"),
    availability_sample: int = typer.Option(2, help="Number of recent games to gauge availability"),
) -> None:
    """
    Compute team/player form aggregates and availability for quick querying.
    """
    session = make_session(settings.database_url)
    bootstrap_schema(session)
    t_count, p_count, a_count = bulk_compute_forms(
        session, sample_size=sample_size, availability_sample=availability_sample
    )
    typer.echo(
        f"Computed forms: teams={t_count}, players={p_count}, availability={a_count}, sample={sample_size}, availability_sample={availability_sample}"
    )


@app.command("normalize-odds")
def normalize_odds_cmd() -> None:
    """
    Snapshot latest odds into a fast lookup table.
    """
    session = make_session(settings.database_url)
    bootstrap_schema(session)
    rows = normalize_odds(session)
    typer.echo(f"Normalized odds rows: {rows}")


if __name__ == "__main__":
    app()
