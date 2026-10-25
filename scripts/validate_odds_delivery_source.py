#!/usr/bin/env python3
"""Fail closed when website odds delivery is pointed at the wrong SQLite DB."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


def validate_source(path: Path, league_ids: list[int], days_forward: int) -> dict[str, object]:
    if not path.is_absolute():
        raise ValueError("odds delivery database path must be absolute")
    resolved = path.resolve(strict=True)
    start = datetime.now(timezone.utc).date().isoformat()
    end = (datetime.now(timezone.utc).date() + timedelta(days=days_forward + 1)).isoformat()
    placeholders = ",".join("?" for _ in league_ids)
    league_clause = f"and f.league_id in ({placeholders})" if league_ids else ""
    parameters: list[object] = [start, end, *league_ids]

    uri = f"file:{resolved}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        fixture_count = connection.execute(
            f"""
            select count(*)
            from fixtures f
            where datetime(f.starting_at) >= datetime(?)
              and datetime(f.starting_at) < datetime(?)
              {league_clause}
            """,
            parameters,
        ).fetchone()[0]
        moneyline_count, latest_source = connection.execute(
            f"""
            select count(*), max(o.last_updated_at)
            from odds_outcomes o
            join fixtures f on f.id = o.fixture_id
            where datetime(f.starting_at) >= datetime(?)
              and datetime(f.starting_at) < datetime(?)
              {league_clause}
              and o.market_key = 'moneyline'
            """,
            parameters,
        ).fetchone()

    if fixture_count < 1:
        raise RuntimeError("odds delivery source has zero fixtures in the current delivery window")
    if moneyline_count < 1:
        raise RuntimeError("odds delivery source has zero moneyline rows in the current delivery window")
    return {
        "database": str(resolved),
        "fixture_count": int(fixture_count),
        "moneyline_count": int(moneyline_count),
        "latest_moneyline_source_at": latest_source,
        "window_start": start,
        "window_end": end,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--leagues", default="")
    parser.add_argument("--days-forward", type=int, default=14)
    args = parser.parse_args()
    league_ids = [int(value) for value in args.leagues.split(",") if value.strip()]
    print(json.dumps(validate_source(args.db, league_ids, args.days_forward), sort_keys=True))


if __name__ == "__main__":
    main()
