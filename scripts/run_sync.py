#!/usr/bin/env python3
"""
Entry point to sync SportMonks data into SQLite.

Modes:
- recent: seasons + teams + recent window (default 120 days back)
- history: seasons + teams + full kept seasons (current + previous per league)
- full: recent + history
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

import typer
from sqlalchemy import text

from jxd import SportMonksClient, SyncService, choose_keep_seasons_per_league
from jxd.db import get_session, get_engine

app = typer.Typer(add_completion=False)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

EZE_PLAYER_ID = 7643
SHOTS_TYPE_ID = 42


def _parse_leagues(league_csv: str) -> List[int]:
    return [int(x) for x in league_csv.split(",") if x.strip()]


def _parse_dt(raw: Optional[object]) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    text_val = str(raw).replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(text_val)
    except Exception:
        return None


def _check_eze_stats(session, league_ids: List[int]) -> None:
    if not league_ids:
        return
    league_list = ",".join(str(x) for x in league_ids)
    max_fixture_row = session.execute(
        text(
            f"""
            select max(starting_at)
            from fixtures
            where league_id in ({league_list})
              and home_score is not null
              and away_score is not null
            """
        )
    ).scalar()
    max_stat_row = session.execute(
        text(
            f"""
            select max(f.starting_at)
            from fixture_player_statistics fps
            join fixtures f on f.id = fps.fixture_id
            where fps.player_id = :player_id
              and fps.type_id = :type_id
              and f.league_id in ({league_list})
            """
        ),
        {"player_id": EZE_PLAYER_ID, "type_id": SHOTS_TYPE_ID},
    ).scalar()
    max_fixture_dt = _parse_dt(max_fixture_row)
    max_stat_dt = _parse_dt(max_stat_row)
    if not max_fixture_dt or not max_stat_dt:
        log.warning(
            "Eze stats check skipped (max fixtures=%s, max stats=%s).",
            max_fixture_row,
            max_stat_row,
        )
        return
    cutoff = datetime(2025, 12, 1)
    if max_stat_dt < cutoff:
        log.warning(
            "Eze shots stats missing for recent fixtures (last=%s, expected >= %s).",
            max_stat_dt,
            cutoff.date(),
        )
        return
    if max_fixture_dt - max_stat_dt > timedelta(days=7):
        log.warning(
            "Eze shots stats stale (last=%s vs fixtures=%s). Check lineups.details coverage.",
            max_stat_dt,
            max_fixture_dt,
        )


@app.command()
def main(
    leagues: str = typer.Option("8", help="Comma-separated league IDs"),
    mode: str = typer.Option("recent", help="recent|history|full"),
    recent_days: int = typer.Option(120, help="Days back for recent window"),
):
    league_ids = _parse_leagues(leagues)
    engine = get_engine()
    session = get_session(engine)
    client = SportMonksClient()
    svc = SyncService(client, session)
    svc.ensure_schema()

    log.info("Syncing seasons for leagues %s", league_ids)
    svc.sync_seasons(league_ids)
    log.info("Syncing teams for leagues %s", league_ids)
    svc.sync_teams_for_leagues(league_ids)

    if mode in ("recent", "full"):
        log.info("Running recent window sync (%s days)", recent_days)
        svc.sync_recent_window(league_ids, days=recent_days)

    if mode in ("history", "full"):
        keep_ids = choose_keep_seasons_per_league(session)
        log.info("Running history sync for kept seasons %s", keep_ids)
        svc.sync_history_window(league_ids, keep_ids)

    _check_eze_stats(session, league_ids)

    log.info("Sync complete (mode=%s)", mode)


if __name__ == "__main__":
    app()
