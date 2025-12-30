#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List


def parse_league_ids(raw: str) -> List[int]:
    return [int(x) for x in raw.split(",") if x.strip()]


def fetch_fixture_league_ids(conn: sqlite3.Connection, days_forward: int) -> List[int]:
    today = datetime.utcnow().date()
    end = today + timedelta(days=days_forward)
    cur = conn.cursor()
    cur.execute(
        """
        select distinct league_id
        from fixtures
        where league_id is not null
          and date(starting_at) >= ? and date(starting_at) <= ?
        """,
        (today.isoformat(), end.isoformat()),
    )
    return [row[0] for row in cur.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--leagues",
        default="8,384",
        help="Comma-separated league IDs",
    )
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--db", default="data/jxd.sqlite")
    parser.add_argument(
        "--out",
        default="/tmp/odds_pid_patch.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--no-include-fixture-leagues",
        dest="include_fixture_leagues",
        action="store_false",
        help="Only use --leagues list (default includes leagues from upcoming fixtures).",
    )
    parser.set_defaults(include_fixture_leagues=True)
    args = parser.parse_args()

    today = datetime.utcnow().date()
    end = today + timedelta(days=args.days_forward)
    today_iso = today.isoformat()
    end_iso = end.isoformat()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    league_ids = parse_league_ids(args.leagues)
    fixture_league_ids = fetch_fixture_league_ids(conn, args.days_forward) if args.include_fixture_leagues else []
    effective_leagues = sorted({*league_ids, *fixture_league_ids})

    league_clause = ""
    params: List[object] = [today_iso, end_iso]
    if effective_leagues:
        placeholders = ",".join("?" for _ in effective_leagues)
        league_clause = f"and f.league_id in ({placeholders})"
        params.extend(effective_leagues)

    cur.execute(
        f"""
        select o.fixture_id,
               o.bookmaker_id,
               o.market_key,
               o.selection_key,
               o.line,
               o.participant_id,
               o.participant_type
        from odds_outcomes o
        join fixtures f on f.id = o.fixture_id
        where o.participant_id is not null
          and o.participant_type = 'player'
          and date(f.starting_at) >= ? and date(f.starting_at) <= ?
          {league_clause}
        """,
        params,
    )
    rows = cur.fetchall()
    conn.close()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "fixture_id",
                "bookmaker_id",
                "market_key",
                "selection_key",
                "line",
                "participant_id",
                "participant_type",
            ]
        )
        for r in rows:
            writer.writerow(r)

    print(f"wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
