#!/usr/bin/env python3
"""
Export odds from SQLite to Supabase via psql COPY + upsert.

- Builds a CSV for odds_outcomes within the fixture window.
- Loads into a staging table via COPY.
- Upserts into public.odds_outcomes with a change-only WHERE clause.
- Writes a JSON report for workflow artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import subprocess
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")


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


def sql_array(values: List[int]) -> str:
    if not values:
        return "array[]::int[]"
    items = ",".join(str(v) for v in values)
    return f"array[{items}]::int[]"


def run_psql(db_url: str, sql: str, from_file: bool = False) -> str:
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-At", "-F", "\t"]
    if from_file:
        cmd.extend(["-f", sql])
    else:
        cmd.extend(["-c", sql])
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout.strip()


def build_outcomes_csv(
    conn: sqlite3.Connection,
    league_ids: Iterable[int],
    days_forward: int,
    out_path: Path,
    progress_every: int,
    progress_fixtures: int,
    total_rows_estimate: int,
    total_fixtures_estimate: int,
    max_runtime_seconds: int,
) -> Tuple[int, bool, Optional[int]]:
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
               o.price_decimal,
               o.price_american,
               o.participant_type,
               o.participant_id,
               o.last_updated_at
        from odds_outcomes o
        join fixtures f on f.id = o.fixture_id
        where date(f.starting_at) >= ? and date(f.starting_at) <= ?
          {league_clause}
        order by o.fixture_id
        """,
        params,
    )

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
                "price_decimal",
                "price_american",
                "participant_type",
                "participant_id",
                "last_updated_at",
            ]
        )

        total_rows = 0
        total_fixtures = 0
        last_report_rows = 0
        last_report_fixtures = 0
        last_fixture_id: Optional[int] = None
        partial_run = False
        start = time.time()
        while True:
            rows = cur.fetchmany(2000)
            if not rows:
                break
            for row in rows:
                fixture_id = row[0]
                if fixture_id != last_fixture_id:
                    total_fixtures += 1
                    last_fixture_id = fixture_id
                writer.writerow(row)
                total_rows += 1
            elapsed = time.time() - start
            should_log_rows = progress_every and (total_rows - last_report_rows) >= progress_every
            should_log_fixtures = progress_fixtures and (total_fixtures - last_report_fixtures) >= progress_fixtures
            if should_log_rows or should_log_fixtures:
                rows_per_sec = total_rows / elapsed if elapsed > 0 else 0.0
                fixtures_per_sec = total_fixtures / elapsed if elapsed > 0 else 0.0
                eta_seconds = None
                if total_rows_estimate > 0 and rows_per_sec > 0:
                    eta_seconds = max(0.0, (total_rows_estimate - total_rows) / rows_per_sec)
                elif total_fixtures_estimate > 0 and fixtures_per_sec > 0:
                    eta_seconds = max(0.0, (total_fixtures_estimate - total_fixtures) / fixtures_per_sec)
                eta_text = f"{eta_seconds:.0f}" if eta_seconds is not None else "n/a"
                print(
                    "CSV progress "
                    f"rows={total_rows} fixtures={total_fixtures} "
                    f"rows_per_sec={rows_per_sec:.1f} fixtures_per_sec={fixtures_per_sec:.2f} "
                    f"elapsed_sec={elapsed:.0f} eta_sec={eta_text} "
                    f"last_fixture_id={last_fixture_id}",
                    flush=True,
                )
                if should_log_rows:
                    last_report_rows = total_rows
                if should_log_fixtures:
                    last_report_fixtures = total_fixtures
            if max_runtime_seconds and elapsed > max_runtime_seconds:
                partial_run = True
                break
            if partial_run:
                break

    return total_rows, partial_run, last_fixture_id


def stage_and_upsert(db_url: str, csv_path: Path) -> Dict[str, int]:
    sql = f"""
\\set ON_ERROR_STOP on
begin;
create temp table odds_outcomes_stage
  (like public.odds_outcomes including defaults) on commit drop;
\\copy odds_outcomes_stage (
  fixture_id, bookmaker_id, market_key, selection_key, line,
  price_decimal, price_american, participant_type, participant_id, last_updated_at
) from '{csv_path.as_posix()}' with (format csv, header true);

select 'stage_count', count(*)::bigint from odds_outcomes_stage;

with upserted as (
  insert into public.odds_outcomes as o (
    fixture_id, bookmaker_id, market_key, selection_key, line,
    price_decimal, price_american, participant_type, participant_id, last_updated_at
  )
  select
    fixture_id, bookmaker_id, market_key, selection_key, line,
    price_decimal, price_american, participant_type, participant_id, last_updated_at
  from odds_outcomes_stage
  on conflict (fixture_id, bookmaker_id, market_key, selection_key, line)
  do update set
    price_decimal = excluded.price_decimal,
    price_american = excluded.price_american,
    participant_type = coalesce(excluded.participant_type, o.participant_type),
    participant_id = coalesce(excluded.participant_id, o.participant_id),
    last_updated_at = coalesce(excluded.last_updated_at, o.last_updated_at)
  where
    o.price_decimal is distinct from excluded.price_decimal
    or o.price_american is distinct from excluded.price_american
    or o.participant_type is distinct from coalesce(excluded.participant_type, o.participant_type)
    or o.participant_id is distinct from coalesce(excluded.participant_id, o.participant_id)
    or o.last_updated_at is distinct from coalesce(excluded.last_updated_at, o.last_updated_at)
  returning (xmax = 0) as inserted
)
select 'upserted_total', count(*)::bigint from upserted;
select 'inserted', count(*)::bigint filter (where inserted) from upserted;
select 'updated', count(*)::bigint filter (where not inserted) from upserted;
commit;
"""

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sql") as f:
        f.write(sql)
        sql_path = f.name

    output = run_psql(db_url, sql_path, from_file=True)
    os.unlink(sql_path)

    counts = {"stage_count": 0, "upserted_total": 0, "inserted": 0, "updated": 0}
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0] in counts:
            try:
                counts[parts[0]] = int(parts[1])
            except ValueError:
                counts[parts[0]] = 0
    counts["unchanged"] = max(0, counts["stage_count"] - counts["upserted_total"])
    return counts


def coverage_query(days_forward: int, league_ids: List[int]) -> str:
    league_filter = ""
    if league_ids:
        league_filter = f"league_id = any({sql_array(league_ids)}) and"
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
  where o.participant_type = 'player'
)
select
  coalesce(count(*), 0)::bigint as total,
  coalesce(count(*) filter (where participant_id is not null), 0)::bigint as mapped,
  coalesce(round(100.0 * count(*) filter (where participant_id is not null) / nullif(count(*),0), 2), 0) as mapped_pct
from scoped;
"""


def coverage_baseline_query(days_back: int, league_ids: List[int]) -> str:
    league_filter = ""
    if league_ids:
        league_filter = f"f.league_id = any({sql_array(league_ids)}) and"
    return f"""
with days as (
  select generate_series(current_date - interval '{days_back} days', current_date, interval '1 day')::date as day
), counts as (
  select
    d.day,
    coalesce((
      select count(*)
      from public.odds_outcomes o
      join public.fixtures f on f.id = o.fixture_id
      where {league_filter}
        o.participant_type = 'player'
        and (coalesce(o.last_updated_at, now()) at time zone 'Europe/London')::date = d.day
    ), 0) as total,
    coalesce((
      select count(*)
      from public.odds_outcomes o
      join public.fixtures f on f.id = o.fixture_id
      where {league_filter}
        o.participant_type = 'player'
        and o.participant_id is not null
        and (coalesce(o.last_updated_at, now()) at time zone 'Europe/London')::date = d.day
    ), 0) as mapped
  from days d
)
select coalesce(
  percentile_cont(0.5) within group (
    order by case when total = 0 then 0 else 100.0 * mapped / total end
  ),
  0
) as median_mapped_pct;
"""


def verification_queries(days_forward: int) -> List[str]:
    queries = []
    queries.append(
        f"""
select
  count(*) as total,
  count(*) filter (where participant_type='player' and participant_id is not null) as mapped_players
from public.odds_outcomes o
join public.fixtures f on f.id=o.fixture_id
where date(f.starting_at) >= current_date
  and date(f.starting_at) < current_date + {days_forward};
"""
    )
    queries.append(
        f"""
select market_key, line,
       count(distinct participant_id) filter (where participant_id is not null) as distinct_players
from public.odds_outcomes o
join public.fixtures f on f.id=o.fixture_id
where market_key in ('player_shots','player_shots_on_target')
  and date(f.starting_at) >= current_date
  and date(f.starting_at) < current_date + {days_forward}
group by market_key, line
order by market_key, distinct_players desc
limit 20;
"""
    )
    return queries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8,384", help="Comma-separated league IDs")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--csv-out", default="/tmp/odds_outcomes_export.csv")
    parser.add_argument("--report-out", default="/tmp/odds_ingest_report.json")
    parser.add_argument("--progress-rows", type=int, default=10000)
    parser.add_argument("--progress-fixtures", type=int, default=100)
    parser.add_argument("--max-runtime-minutes", type=int, default=25)
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

    start_time = time.time()
    start_iso = datetime.utcnow().isoformat() + "Z"
    max_runtime_seconds = max(0, int(args.max_runtime_minutes) * 60)

    conn = sqlite3.connect(args.db)
    league_ids = parse_league_ids(args.leagues)
    fixture_league_ids = fetch_fixture_league_ids(conn, args.days_forward) if args.include_fixture_leagues else []
    effective_leagues = sorted({*league_ids, *fixture_league_ids})

    fixture_count = 0
    if effective_leagues:
        placeholders = ",".join("?" for _ in effective_leagues)
        today = datetime.utcnow().date()
        end = today + timedelta(days=args.days_forward)
        fixture_count = conn.execute(
            f"""
            select count(*)
            from fixtures
            where date(starting_at) >= ? and date(starting_at) <= ?
              and league_id in ({placeholders})
            """,
            [today.isoformat(), end.isoformat(), *effective_leagues],
        ).fetchone()[0]

    total_rows_estimate = 0
    if effective_leagues:
        placeholders = ",".join("?" for _ in effective_leagues)
        today = datetime.utcnow().date()
        end = today + timedelta(days=args.days_forward)
        total_rows_estimate = conn.execute(
            f"""
            select count(*)
            from odds_outcomes o
            join fixtures f on f.id = o.fixture_id
            where date(f.starting_at) >= ? and date(f.starting_at) <= ?
              and f.league_id in ({placeholders})
            """,
            [today.isoformat(), end.isoformat(), *effective_leagues],
        ).fetchone()[0]

    print(
        f"Exporting odds_outcomes fixtures={fixture_count} leagues={len(effective_leagues)} window_days={args.days_forward}",
        flush=True,
    )

    total_rows, partial_run, last_fixture_id = build_outcomes_csv(
        conn,
        effective_leagues,
        args.days_forward,
        Path(args.csv_out),
        args.progress_rows,
        args.progress_fixtures,
        total_rows_estimate,
        fixture_count,
        max_runtime_seconds,
    )
    conn.close()

    print(f"CSV build complete rows={total_rows} partial_run={partial_run}", flush=True)
    if partial_run:
        print(
            f"Runtime guard hit (>{args.max_runtime_minutes} min). "
            f"Last fixture_id exported: {last_fixture_id}",
            flush=True,
        )

    counts = stage_and_upsert(DB_URL, Path(args.csv_out))
    print(
        f"Stage count={counts['stage_count']} inserted={counts['inserted']} "
        f"updated={counts['updated']} unchanged={counts['unchanged']}",
        flush=True,
    )

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

    baseline_sql = coverage_baseline_query(6, effective_leagues)
    baseline_out = run_psql(DB_URL, baseline_sql)
    try:
        coverage_baseline_pct = float(baseline_out.strip()) if baseline_out else 0.0
    except ValueError:
        coverage_baseline_pct = 0.0

    print(
        f"Coverage total={coverage_total} mapped={coverage_mapped} "
        f"pct={coverage_pct} baseline_pct={coverage_baseline_pct}",
        flush=True,
    )

    verification_outputs: List[str] = []
    for idx, query in enumerate(verification_queries(args.days_forward), start=1):
        print(f"Verification query {idx} output:", flush=True)
        out = run_psql(DB_URL, query)
        verification_outputs.append(out)
        print(out, flush=True)

    end_time = time.time()
    end_iso = datetime.utcnow().isoformat() + "Z"

    report = {
        "start_time": start_iso,
        "end_time": end_iso,
        "runtime_seconds": round(end_time - start_time, 2),
        "window_days": args.days_forward,
        "fixture_count": fixture_count,
        "league_ids": effective_leagues,
        "league_id": effective_leagues[0] if len(effective_leagues) == 1 else None,
        "sqlite_rows_exported": total_rows,
        "rows_exported_csv": total_rows,
        "copy_rows": counts.get("stage_count", 0),
        "rows_copied": counts.get("stage_count", 0),
        "inserted_rows": counts.get("inserted", 0),
        "rows_inserted": counts.get("inserted", 0),
        "updated_rows": counts.get("updated", 0),
        "rows_updated": counts.get("updated", 0),
        "unchanged_rows": counts.get("unchanged", 0),
        "rows_unchanged_skipped": counts.get("unchanged", 0),
        "partial_run": partial_run,
        "last_fixture_id": last_fixture_id,
        "psql_ok": os.environ.get("PSQL_OK"),
        "coverage": {
            "total": coverage_total,
            "mapped": coverage_mapped,
            "mapped_pct": coverage_pct,
            "baseline_pct": coverage_baseline_pct,
        },
        "rest_ok": os.environ.get("REST_OK"),
        "rest_http": os.environ.get("REST_HTTP"),
        "verification": verification_outputs,
    }

    Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(
        "Ingest summary: rows_exported="
        f"{total_rows} inserted={counts.get('inserted', 0)} "
        f"updated={counts.get('updated', 0)} unchanged={counts.get('unchanged', 0)} "
        f"runtime_sec={report['runtime_seconds']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
