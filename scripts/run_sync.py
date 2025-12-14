#!/usr/bin/env python3
"""
Entry point to sync SportMonks data into SQLite.

Modes:
- recent: seasons + teams + recent window (default 120 days back)
- history: seasons + teams + full kept seasons (current + previous per league)
- full: recent + history
"""

import logging
from typing import List

import typer

from jxd import SportMonksClient, SyncService, choose_keep_seasons_per_league
from jxd.db import get_session, get_engine

app = typer.Typer(add_completion=False)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _parse_leagues(league_csv: str) -> List[int]:
    return [int(x) for x in league_csv.split(",") if x.strip()]


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

    log.info("Sync complete (mode=%s)", mode)


if __name__ == "__main__":
    app()
