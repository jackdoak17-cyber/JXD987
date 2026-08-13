#!/usr/bin/env python3
"""
Refresh an active fixture window by re-fetching SportMonks fixture details and upserting.
"""
from datetime import datetime, timedelta
import json
import logging
from typing import List, Optional

import typer

from jxd import SportMonksClient, SyncService
from jxd.db import get_engine, get_session
from jxd.models import Fixture

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


def _completed_fixture_ids(session, league_ids: List[int], hours_back: int) -> List[int]:
    cutoff = datetime.utcnow() - timedelta(hours=max(hours_back, 0))
    # Select every fixture that has started in the window. The local status can
    # itself be stale (for example NS/0-0 after SportMonks has moved to FT), so
    # filtering on local completion prevents this reconciliation from repairing
    # exactly the rows it is intended to refresh.
    query = (
        session.query(Fixture.id)
        .filter(Fixture.starting_at >= cutoff)
        .filter(Fixture.starting_at <= datetime.utcnow())
    )
    if league_ids:
        query = query.filter(Fixture.league_id.in_(league_ids))
    return [row.id for row in query.order_by(Fixture.starting_at.desc()).all()]


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
    completed_hours_back: Optional[int] = typer.Option(
        None,
        help="Reconcile every fixture started within this many hours so stale local statuses can be refreshed.",
    ),
    report_json: Optional[str] = typer.Option(
        None,
        help="Optional path to write a JSON reconciliation summary.",
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

    mode = "window"
    if fixtures:
        mode = "explicit"
        log.info("Reconciling %s fixtures by ID", len(fixtures))
        count = svc.reconcile_fixtures(fixtures, includes=includes)
        if report_json:
            with open(report_json, "w", encoding="utf-8") as fh:
                json.dump({"mode": mode, "fixtures_selected": len(fixtures), "fixtures_reconciled": count}, fh)
        return

    if completed_hours_back is not None:
        mode = "started-window"
        fixtures = _completed_fixture_ids(session, league_ids, completed_hours_back)
        log.info(
            "Reconciling %s started fixtures from the last %s hours for leagues %s",
            len(fixtures),
            completed_hours_back,
            league_ids,
        )
        count = svc.reconcile_fixtures(fixtures, includes=includes)
        if report_json:
            with open(report_json, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "mode": mode,
                        "hours_back": completed_hours_back,
                        "fixtures_selected": len(fixtures),
                        "fixtures_reconciled": count,
                        "fixture_ids": fixtures,
                    },
                    fh,
                )
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
    count = svc.sync_fixtures_between(start, end, league_ids=league_ids, includes=includes)
    if report_json:
        with open(report_json, "w", encoding="utf-8") as fh:
            json.dump({"mode": mode, "fixtures_reconciled": count}, fh)


if __name__ == "__main__":
    app()
