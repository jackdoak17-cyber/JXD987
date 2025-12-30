#!/usr/bin/env python3
"""
Patch odds participant mappings in Supabase using psql.

Builds a CSV from SQLite, loads into a temp table, and updates odds_outcomes
using IS NOT DISTINCT FROM on line. Writes a JSON report for CI artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")

PLAYER_MARKET_EXTRA = (
    "goalscorers",
    "1st_goal_scorer",
    "last_goal_scorer",
    "multi_scorers",
)


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


def fetch_patch_rows(
    conn: sqlite3.Connection,
    league_ids: Iterable[int],
    days_forward: int,
) -> List[Tuple]:
    today = datetime.utcnow().date()
    end = today + timedelta(days=days_forward)
    params: List[object] = [today.isoformat(), end.isoformat()]
    league_clause = ""
    if league_ids:
        placeholders = ",".join("?" for _ in league_ids)
        league_clause = f"and f.league_id in ({placeholders})"
        params.extend(league_ids)

    cur = conn.cursor()
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
    return cur.fetchall()


def write_csv(path: Path, rows: List[Tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
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
        for row in rows:
            writer.writerow(row)


def run_psql(db_url: str, sql: str, from_file: bool = False) -> str:
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-At", "-F", "\t"]
    if from_file:
        cmd.extend(["-f", sql])
    else:
        cmd.extend(["-c", sql])
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout.strip()


def apply_patch(db_url: str, csv_path: Path) -> Tuple[Dict[str, int], List[Dict[str, Optional[str]]]]:
    sql = f"""
\\set ON_ERROR_STOP on
create temporary table odds_pid_patch (
  fixture_id bigint,
  bookmaker_id int,
  market_key text,
  selection_key text,
  line numeric,
  participant_id bigint,
  participant_type text
);
\\copy odds_pid_patch (fixture_id, bookmaker_id, market_key, selection_key, line, participant_id, participant_type)
  from '{csv_path.as_posix()}'
  with (format csv, header true);

select 'patch_candidates', count(*)::bigint from odds_pid_patch;
select 'matched_rows', count(*)::bigint
from odds_pid_patch p
join public.odds_outcomes o
  on o.fixture_id = p.fixture_id
 and o.bookmaker_id = p.bookmaker_id
 and o.market_key = p.market_key
 and o.selection_key = p.selection_key
 and o.line is not distinct from p.line;

with updated as (
  update public.odds_outcomes o
  set participant_id = p.participant_id,
      participant_type = p.participant_type
  from odds_pid_patch p
  where o.fixture_id = p.fixture_id
    and o.bookmaker_id = p.bookmaker_id
    and o.market_key = p.market_key
    and o.selection_key = p.selection_key
    and o.line is not distinct from p.line
  returning 1
)
select 'patched_rows', count(*)::bigint from updated;

select 'sample',
       o.fixture_id::text,
       o.selection_key,
       o.participant_id::text,
       coalesce(o.line::text, ''),
       o.market_key
from public.odds_outcomes o
join odds_pid_patch p
  on o.fixture_id = p.fixture_id
 and o.bookmaker_id = p.bookmaker_id
 and o.market_key = p.market_key
 and o.selection_key = p.selection_key
 and o.line is not distinct from p.line
where o.participant_id is not null
order by o.fixture_id desc, o.selection_key, o.market_key
limit 20;
"""

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sql") as f:
        f.write(sql)
        sql_path = f.name

    output = run_psql(db_url, sql_path, from_file=True)
    os.unlink(sql_path)

    counts: Dict[str, int] = {}
    samples: List[Dict[str, Optional[str]]] = []
    for line in output.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        label = parts[0]
        if label in ("patch_candidates", "matched_rows", "patched_rows"):
            try:
                counts[label] = int(parts[1]) if len(parts) > 1 else 0
            except ValueError:
                counts[label] = 0
        elif label == "sample" and len(parts) >= 6:
            samples.append(
                {
                    "fixture_id": parts[1] or None,
                    "selection_key": parts[2] or None,
                    "participant_id": parts[3] or None,
                    "line": parts[4] or None,
                    "market_key": parts[5] or None,
                }
            )

    return counts, samples


def sql_array(values: List[int]) -> str:
    if not values:
        return "array[]::int[]"
    items = ",".join(str(v) for v in values)
    return f"array[{items}]::int[]"


def coverage_query(days_forward: int, league_ids: List[int]) -> str:
    league_filter = ""
    if league_ids:
        league_filter = f"league_id = any({sql_array(league_ids)}) and"
    market_keys = ",".join(f"'{k}'" for k in PLAYER_MARKET_EXTRA)
    return f"""
with fixtures_in_range as (
  select id
  from public.fixtures
  where {league_filter}
    (starting_at at time zone 'Europe/London')::date >= current_date
    and (starting_at at time zone 'Europe/London')::date < (current_date + interval '{days_forward} days')
), scoped as (
  select o.participant_id
  from public.odds_outcomes o
  join fixtures_in_range f on f.id = o.fixture_id
  where (o.market_key like 'player_%' or o.market_key in ({market_keys}))
)
select
  coalesce(count(*), 0)::bigint as total,
  coalesce(count(*) filter (where participant_id is not null), 0)::bigint as mapped,
  coalesce(round(100.0 * count(*) filter (where participant_id is not null) / nullif(count(*),0), 2), 0) as mapped_pct
from scoped;
"""


def escape_literal(text: str) -> str:
    return text.replace("'", "''")


def fixture_sample_query(fixture_id: int, selection_keys: List[str]) -> str:
    if not selection_keys:
        return "select null where false;"
    keys = ",".join(f"'{escape_literal(k)}'" for k in selection_keys)
    return f"""
select selection_key,
       participant_id::text,
       coalesce(line::text, ''),
       market_key
from public.odds_outcomes
where fixture_id = {fixture_id}
  and selection_key in ({keys})
order by selection_key, market_key;
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8,384", help="Comma-separated league IDs")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--csv-out", default="/tmp/odds_pid_patch.csv")
    parser.add_argument("--report-out", default="/tmp/odds_patch_report.json")
    parser.add_argument("--sample-fixture-id", type=int, default=19427629)
    parser.add_argument(
        "--sample-selection-keys",
        default="cole_palmer_anytime,cole_palmer_1_5,morgan_rogers_0_5,wesley_fofana_0_5",
    )
    parser.add_argument(
        "--no-include-fixture-leagues",
        dest="include_fixture_leagues",
        action="store_false",
        help="Only use --leagues list (default includes leagues from upcoming fixtures).",
    )
    parser.set_defaults(include_fixture_leagues=True)
    args = parser.parse_args()

    if not DB_URL:
        raise SystemExit("Missing SUPABASE_DB_URL")
    if not os.path.exists(args.db):
        raise SystemExit(f"SQLite DB not found at {args.db}")

    conn = sqlite3.connect(args.db)
    league_ids = parse_league_ids(args.leagues)
    fixture_league_ids = fetch_fixture_league_ids(conn, args.days_forward) if args.include_fixture_leagues else []
    effective_leagues = sorted({*league_ids, *fixture_league_ids})

    rows = fetch_patch_rows(conn, effective_leagues, args.days_forward)
    conn.close()

    csv_path = Path(args.csv_out)
    write_csv(csv_path, rows)

    counts, samples = apply_patch(DB_URL, csv_path)

    coverage_sql = coverage_query(args.days_forward, effective_leagues)
    coverage_out = run_psql(DB_URL, coverage_sql)
    coverage_parts = coverage_out.split("\t") if coverage_out else ["0", "0", "0"]
    try:
        coverage_total = int(coverage_parts[0])
    except ValueError:
        coverage_total = 0
    try:
        coverage_mapped = int(coverage_parts[1])
    except ValueError:
        coverage_mapped = 0
    try:
        coverage_pct = float(coverage_parts[2])
    except ValueError:
        coverage_pct = 0.0

    selection_keys = [k.strip() for k in args.sample_selection_keys.split(",") if k.strip()]
    fixture_sql = fixture_sample_query(args.sample_fixture_id, selection_keys)
    fixture_out = run_psql(DB_URL, fixture_sql)
    fixture_rows = []
    for line in fixture_out.splitlines():
        parts = line.split("\t")
        if len(parts) >= 4:
            fixture_rows.append(
                {
                    "selection_key": parts[0] or None,
                    "participant_id": parts[1] or None,
                    "line": parts[2] or None,
                    "market_key": parts[3] or None,
                }
            )

    report = {
        "psql_ok": os.environ.get("PSQL_OK"),
        "rest_ok": os.environ.get("REST_OK"),
        "rest_http": os.environ.get("REST_HTTP"),
        "window_days": args.days_forward,
        "league_ids": effective_leagues,
        "patch_counts": counts,
        "coverage": {
            "total": coverage_total,
            "mapped": coverage_mapped,
            "mapped_pct": coverage_pct,
        },
        "sample_rows": samples,
        "fixture_check": {
            "fixture_id": args.sample_fixture_id,
            "rows": fixture_rows,
        },
    }

    report_path = Path(args.report_out)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"patched_rows={counts.get('patched_rows', 0)}")
    print(f"coverage_mapped_pct={coverage_pct}")
    if fixture_rows:
        print("fixture_sample_rows:")
        for row in fixture_rows:
            print(json.dumps(row))


if __name__ == "__main__":
    main()
