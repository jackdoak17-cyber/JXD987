#!/usr/bin/env python3
"""
Refresh an active fixture window by re-fetching SportMonks fixture details and upserting.
"""
from datetime import datetime, timedelta
import logging
from typing import List, Optional

import typer

from jxd import SportMonksClient, SyncService
from jxd.db import get_engine, get_session

app = typer.Typer(add_completion=False)
LIGHTWEIGHT_INCLUDES = ["participants", "scores", "state"]
DETAIL_INCLUDES = [
    "participants",
    "scores",
    "state",
    "statistics",
    "statistics.type",
    "lineups.details",
    "lineups.position",
    "lineups.detailedposition",
    "lineups.player",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _parse_leagues(league_csv: str) -> List[int]:
    return [int(x) for x in league_csv.split(",") if x.strip()]


def _parse_fixture_ids(fixture_csv: Optional[str]) -> List[int]:
    if not fixture_csv:
        return []
    return [int(x) for x in fixture_csv.split(",") if x.strip()]


@app.command()
def main(
    leagues: str = typer.Option(
        "8,384",
        help="Comma-separated league IDs",
    ),
    days_back: int = typer.Option(
        2,
        "--days",
        "--days-back",
        help="Days back for the active fixture refresh window.",
    ),
    days_forward: int = typer.Option(
        3,
        help="Days forward for the active fixture refresh window.",
    ),
    fixture_ids: Optional[str] = typer.Option(
        None,
        help="Optional comma-separated fixture IDs to reconcile explicitly",
    ),
    with_details: bool = typer.Option(
        False,
        help="Include statistics and lineups instead of the lightweight fixture refresh payload.",
    ),
):
    league_ids = _parse_leagues(leagues)
    fixtures = _parse_fixture_ids(fixture_ids)

    engine = get_engine()
    session = get_session(engine)
    client = SportMonksClient()
    svc = SyncService(client, session)
    svc.ensure_schema()
    includes = DETAIL_INCLUDES if with_details else LIGHTWEIGHT_INCLUDES

    if fixtures:
        log.info("Reconciling %s fixtures by ID", len(fixtures))
        svc.reconcile_fixtures(fixtures, includes=includes)
        return

    start = (datetime.utcnow() - timedelta(days=max(days_back, 0))).date()
    end = (datetime.utcnow() + timedelta(days=max(days_forward, 0))).date()
    log.info(
        "Refreshing active fixture window from %s to %s for leagues %s (%s mode)",
        start,
        end,
        league_ids,
        "detailed" if with_details else "lightweight",
    )
    svc.sync_fixtures_between(start, end, league_ids=league_ids, includes=includes)


if __name__ == "__main__":
    app()
