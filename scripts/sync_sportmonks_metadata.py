#!/usr/bin/env python3
"""
Refresh SportMonks metadata required before fixture exports.

This avoids fixture/history sync. It only ensures seasons, rounds, and teams
exist locally so export_to_supabase.py can choose kept seasons and resolve
fixture participants.
"""

import logging
from typing import List

import typer
from sqlalchemy import select

from jxd import SportMonksClient, SyncService
from jxd.db import get_engine, get_session
from jxd.models import Round, Season, Team
from jxd.sync import _parse_date, _upsert

app = typer.Typer(add_completion=False)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _parse_leagues(league_csv: str) -> List[int]:
    return [int(value) for value in league_csv.split(",") if value.strip()]


def _fetch_collection_limited(
    client: SportMonksClient,
    endpoint: str,
    *,
    includes: List[str] | None = None,
    per_page: int = 200,
    max_pages: int = 10,
):
    for page in range(1, max_pages + 1):
        params = {"per_page": per_page, "page": page}
        if includes:
            params["include"] = ";".join(includes)
        payload = client.request("GET", endpoint, params=params)
        rows = payload.get("data") if isinstance(payload, dict) else []
        if isinstance(rows, dict):
            rows = [rows]
        if not rows:
            return
        for row in rows:
            yield row
        if len(rows) < per_page:
            return


def _current_seasons(session, league_ids: List[int]) -> List[Season]:
    stmt = (
        select(Season)
        .where(Season.league_id.in_(league_ids), Season.is_current.is_(True))
        .order_by(Season.league_id)
    )
    return list(session.execute(stmt).scalars())


def _sync_rounds_for_seasons(session, client: SportMonksClient, seasons: List[Season]) -> int:
    count = 0
    for season in seasons:
        for item in _fetch_collection_limited(client, f"rounds/seasons/{season.id}"):
            _upsert(
                session,
                Round,
                {
                    "id": item.get("id"),
                    "league_id": item.get("league_id") or season.league_id,
                    "season_id": item.get("season_id") or season.id,
                    "stage_id": item.get("stage_id"),
                    "name": item.get("name"),
                    "starting_at": _parse_date(item.get("starting_at")),
                    "ending_at": _parse_date(item.get("ending_at")),
                    "is_current": bool(item.get("is_current")),
                    "games_in_current_week": bool(item.get("games_in_current_week")),
                    "finished": bool(item.get("finished")),
                    "extra": item,
                },
            )
            count += 1
    session.commit()
    return count


def _sync_teams_for_seasons(session, client: SportMonksClient, seasons: List[Season]) -> int:
    count = 0
    seen_team_ids = set()
    for season in seasons:
        for item in _fetch_collection_limited(
            client,
            f"teams/seasons/{season.id}",
            includes=["venue"],
        ):
            team_id = item.get("id")
            if not team_id or team_id in seen_team_ids:
                continue
            image_path = item.get("image_path") or item.get("logo_path")
            data = {
                "id": team_id,
                "name": item.get("name"),
                "short_code": item.get("short_code"),
                "extra": item,
            }
            if image_path:
                data["image_path"] = image_path
            _upsert(session, Team, data)
            seen_team_ids.add(team_id)
            count += 1
    session.commit()
    return count


@app.command()
def main(
    leagues: str = typer.Option(
        ...,
        help="Comma-separated SportMonks league IDs.",
    ),
):
    league_ids = _parse_leagues(leagues)
    if not league_ids:
        raise typer.BadParameter("No league IDs provided")

    engine = get_engine()
    session = get_session(engine)
    client = SportMonksClient()
    svc = SyncService(client, session)
    svc.ensure_schema()

    log.info("Refreshing metadata for leagues %s", league_ids)
    seasons = svc.sync_seasons(league_ids)
    current_seasons = _current_seasons(session, league_ids)
    rounds = _sync_rounds_for_seasons(session, client, current_seasons)
    teams = _sync_teams_for_seasons(session, client, current_seasons)
    log.info(
        "Metadata refresh complete: seasons=%s current_seasons=%s rounds=%s teams=%s",
        seasons,
        len(current_seasons),
        rounds,
        teams,
    )


if __name__ == "__main__":
    app()
