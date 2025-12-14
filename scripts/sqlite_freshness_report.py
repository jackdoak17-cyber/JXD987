#!/usr/bin/env python3
"""
Report SQLite fixture freshness using fixture scores and participant scores.
"""

import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(os.environ.get("JXD_DB_PATH", "data/jxd.sqlite"))
AGE_DAYS = int(os.environ.get("FRESHNESS_MAX_AGE_DAYS", "30"))
ARSENAL_ID = 19
LEAGUE_ID = 8
MIN_DATE = "2025-08-01"


def query_one(cur: sqlite3.Cursor, sql: str, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def main() -> int:
    if not DB_PATH.exists():
        print(json.dumps({"error": f"sqlite not found at {DB_PATH}"}))
        return 1

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    fixtures_total = query_one(cur, "select count(*) from fixtures")
    fixtures_scored = query_one(cur, "select count(*) from fixtures where home_score is not null and away_score is not null")
    max_scored_fixture = query_one(
        cur,
        "select max(starting_at) from fixtures where home_score is not null and away_score is not null",
    )

    participants_scored = query_one(
        cur,
        """
        select count(*) from (
            select fp.fixture_id
            from fixture_participants fp
            join fixtures f on f.id = fp.fixture_id
            group by fp.fixture_id
            having sum(case when lower(coalesce(fp.location,''))='home' and fp.score is not null then 1 else 0 end) >= 1
               and sum(case when lower(coalesce(fp.location,''))='away' and fp.score is not null then 1 else 0 end) >= 1
        )
        """,
    )
    max_participant_scored = query_one(
        cur,
        """
        select max(f.starting_at) from fixture_participants fp
        join fixtures f on f.id = fp.fixture_id
        group by fp.fixture_id
        having sum(case when lower(coalesce(fp.location,''))='home' and fp.score is not null then 1 else 0 end) >= 1
           and sum(case when lower(coalesce(fp.location,''))='away' and fp.score is not null then 1 else 0 end) >= 1
        order by max(f.starting_at) desc
        limit 1
        """,
    )

    league_scored_since = query_one(
        cur,
        """
        select count(*) from fixtures
        where league_id=? and starting_at >= ? and home_score is not null and away_score is not null
        """,
        (LEAGUE_ID, MIN_DATE),
    )

    arsenal_scored_since = query_one(
        cur,
        """
        select count(*) from fixture_participants fp
        join fixtures f on f.id = fp.fixture_id
        where fp.team_id=? and f.starting_at >= ?
          and f.home_score is not null and f.away_score is not null
        """,
        (ARSENAL_ID, MIN_DATE),
    )

    now = datetime.utcnow()
    cutoff = now - timedelta(days=AGE_DAYS)

    def _parse_dt(val):
        if not val:
            return None
        try:
            return datetime.fromisoformat(str(val))
        except Exception:
            return None

    max_fixture_dt = _parse_dt(max_scored_fixture)
    max_participant_dt = _parse_dt(max_participant_scored)
    dates = [d for d in (max_fixture_dt, max_participant_dt) if d]
    stale = not dates or all(d < cutoff for d in dates)

    report = {
        "fixtures_total": fixtures_total,
        "fixtures_scored_in_fixtures_table": fixtures_scored,
        "max_scored_starting_at_in_fixtures_table": max_scored_fixture,
        "fixtures_with_participant_scores_both_sides": participants_scored,
        "max_scored_starting_at_from_participants": max_participant_scored,
        "league_8_scored_since_2025_08_01": league_scored_since,
        "team_19_arsenal_scored_since_2025_08_01": arsenal_scored_since,
        "stale": stale,
        "now_utc": now.isoformat(),
        "age_cutoff_days": AGE_DAYS,
    }
    print(json.dumps(report, default=str))
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
