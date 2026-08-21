#!/usr/bin/env python3
"""
Validate complete moneyline coverage for upcoming visible fixtures in Supabase.

This script is intended to run after odds ingest. It checks the same class of
home/draw/away prices the fixtures page needs and fails when a league drops
below a configured coverage threshold inside the user-facing window.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import psycopg2
from psycopg2.extras import RealDictCursor


MONEYLINE_MARKET_KEYS = [
    "moneyline",
    "match_result",
    "match_winner",
    "match_winner_90",
    "match_winner_90_min",
    "full_time_result",
    "full_time_result_90",
    "1x2",
    "home_draw_away",
]

HIDDEN_FIXTURE_STATUSES = [
    "POSTP",
    "POSTPONED",
    "CANCL",
    "CANCELLED",
    "CANCELED",
    "ABANDONED",
    "SUSPENDED",
    "INTERRUPTED",
]

DEFAULT_EXCLUDED_LEAGUE_IDS = {24, 27, 109, 307, 390, 570}


def load_excluded_league_ids() -> List[int]:
    """Return competitions excluded from paid Odds-API ingestion."""
    path = Path(__file__).resolve().parent.parent / "config" / "odds_api_sync_excluded_leagues.json"
    if not path.exists():
        return sorted(DEFAULT_EXCLUDED_LEAGUE_IDS)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit(f"Expected a JSON array in {path}")
    return sorted({int(value) for value in raw})


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_db_url() -> str:
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")
    if db_url:
        return db_url
    fallback = Path("/tmp/supabase_db_url")
    if fallback.exists():
        return fallback.read_text(encoding="utf-8").strip()
    raise SystemExit("Missing SUPABASE_DB_URL (or /tmp/supabase_db_url)")


def parse_league_ids(raw: str) -> List[int]:
    return [int(value) for value in raw.split(",") if value.strip()]


def fetch_league_coverage(
    conn,
    league_ids: List[int],
    days_forward: int,
    min_price: float,
    max_price: float,
) -> List[Dict[str, object]]:
    excluded_league_ids = load_excluded_league_ids()
    league_ids = [league_id for league_id in league_ids if league_id not in excluded_league_ids]
    league_filter = ""
    params: List[object] = [f"{days_forward} days"]
    if league_ids:
        league_filter = "and f.league_id = any(%s)"
        params.append(league_ids)

    params.extend(
        [
            excluded_league_ids,
            HIDDEN_FIXTURE_STATUSES,
            HIDDEN_FIXTURE_STATUSES,
            min_price,
            max_price,
            min_price,
            max_price,
            MONEYLINE_MARKET_KEYS,
        ]
    )

    query = f"""
with visible_fixtures as (
  select
    f.id,
    f.league_id,
    f.starting_at,
    f.home_team_id,
    f.away_team_id
  from public.fixtures f
  where f.starting_at >= (now() at time zone 'utc')
    and f.starting_at < (now() at time zone 'utc') + interval %s
    {league_filter}
    and f.league_id <> all(%s)
    and upper(regexp_replace(coalesce(f.status, ''), '[^A-Z0-9]+', '_', 'g')) <> all(%s)
    and upper(regexp_replace(coalesce(f.status_code, ''), '[^A-Z0-9]+', '_', 'g')) <> all(%s)
), moneyline_by_fixture as (
  select
    o.fixture_id,
    bool_or(
      o.participant_type = 'team'
      and o.participant_id = vf.home_team_id
      and o.price_decimal > %s
      and o.price_decimal <= %s
    ) as has_home,
    bool_or(
      o.participant_type = 'team'
      and o.participant_id = vf.away_team_id
      and o.price_decimal > %s
      and o.price_decimal <= %s
    ) as has_away,
    bool_or(
      (
        coalesce(o.participant_type, 'match') = 'match'
        and lower(regexp_replace(coalesce(o.selection_key, ''), '[^a-z0-9]+', '_', 'g')) in ('draw', 'x')
      )
      or lower(coalesce(o.selection_key, '')) like '%%draw%%'
    ) as has_draw
  from public.odds_outcomes o
  join visible_fixtures vf on vf.id = o.fixture_id
  where o.market_key = any(%s)
  group by o.fixture_id
), fixture_coverage as (
  select
    vf.league_id,
    vf.id as fixture_id,
    vf.starting_at,
    coalesce(m.has_home, false) as has_home,
    coalesce(m.has_draw, false) as has_draw,
    coalesce(m.has_away, false) as has_away,
    (
      coalesce(m.has_home, false)
      and coalesce(m.has_draw, false)
      and coalesce(m.has_away, false)
    ) as has_complete_moneyline
  from visible_fixtures vf
  left join moneyline_by_fixture m on m.fixture_id = vf.id
)
select
  league_id,
  count(*)::bigint as fixtures_in_window,
  count(*) filter (where has_complete_moneyline)::bigint as fixtures_with_complete_moneyline,
  coalesce(
    round(
      100.0 * count(*) filter (where has_complete_moneyline)
      / nullif(count(*), 0),
      2
    ),
    0
  ) as coverage_pct,
  coalesce(
    array_agg(fixture_id order by starting_at) filter (where not has_complete_moneyline),
    array[]::bigint[]
  ) as missing_fixture_ids,
  min(starting_at) filter (where not has_complete_moneyline) as first_missing_starting_at
from fixture_coverage
group by league_id
order by league_id;
"""

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    normalized: List[Dict[str, object]] = []
    for row in rows:
        missing_fixture_ids = [int(value) for value in (row.get("missing_fixture_ids") or [])]
        first_missing = row.get("first_missing_starting_at")
        normalized.append(
            {
                "league_id": int(row["league_id"]),
                "fixtures_in_window": int(row["fixtures_in_window"] or 0),
                "fixtures_with_complete_moneyline": int(row["fixtures_with_complete_moneyline"] or 0),
                "coverage_pct": float(row["coverage_pct"] or 0),
                "missing_fixture_ids": missing_fixture_ids,
                "first_missing_starting_at": first_missing.isoformat() if first_missing else None,
            }
        )
    return normalized


def evaluate_failures(leagues: List[Dict[str, object]], fail_below_pct: float) -> List[Dict[str, object]]:
    failures: List[Dict[str, object]] = []
    for league in leagues:
        fixtures_in_window = int(league.get("fixtures_in_window") or 0)
        coverage_pct = float(league.get("coverage_pct") or 0)
        if fixtures_in_window == 0:
            continue
        if coverage_pct + 1e-9 < fail_below_pct:
            failures.append(
                {
                    "league_id": int(league["league_id"]),
                    "fixtures_in_window": fixtures_in_window,
                    "fixtures_with_complete_moneyline": int(
                        league.get("fixtures_with_complete_moneyline") or 0
                    ),
                    "coverage_pct": coverage_pct,
                    "missing_fixture_ids": list(league.get("missing_fixture_ids") or []),
                    "first_missing_starting_at": league.get("first_missing_starting_at"),
                }
            )
    return failures


def build_markdown_report(report: Dict[str, object]) -> str:
    lines = [
        "# Moneyline Coverage Report",
        "",
        f"Generated: {report['generated_at']}",
        f"Days forward: {report['days_forward']}",
        f"Fail below coverage %: {report['fail_below_pct']}",
        f"Status: {'PASS' if report['ok'] else 'FAIL'}",
        "",
        "| League | Fixtures | Complete moneyline | Coverage % | Missing fixture IDs |",
        "|---|---:|---:|---:|---|",
    ]
    for league in report["leagues"]:
        missing_fixture_ids = ", ".join(str(value) for value in league["missing_fixture_ids"]) or "-"
        lines.append(
            f"| {league['league_id']} | {league['fixtures_in_window']} | "
            f"{league['fixtures_with_complete_moneyline']} | {league['coverage_pct']:.2f} | "
            f"{missing_fixture_ids} |"
        )
    if report["failures"]:
        lines.extend(["", "## Failures", ""])
        for failure in report["failures"]:
            lines.append(
                f"- League {failure['league_id']}: "
                f"{failure['fixtures_with_complete_moneyline']}/{failure['fixtures_in_window']} "
                f"fixtures with complete moneyline "
                f"({failure['coverage_pct']:.2f}%)."
            )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="")
    parser.add_argument(
        "--days-forward",
        type=int,
        default=int(os.environ.get("MONEYLINE_COVERAGE_DAYS_FORWARD", "7")),
    )
    parser.add_argument(
        "--fail-below-pct",
        type=float,
        default=float(os.environ.get("MONEYLINE_COVERAGE_MIN_PCT", "100")),
    )
    parser.add_argument("--out-json", default="")
    parser.add_argument("--out-md", default="")
    args = parser.parse_args()

    db_url = get_db_url()
    league_ids = parse_league_ids(args.leagues)
    excluded_league_ids = load_excluded_league_ids()
    league_ids = [league_id for league_id in league_ids if league_id not in excluded_league_ids]
    min_price = float(os.environ.get("ODDS_MIN_PRICE", "1.0"))
    max_price = float(os.environ.get("ODDS_MAX_PRICE", "500"))

    conn = psycopg2.connect(db_url)
    try:
        leagues = fetch_league_coverage(conn, league_ids, args.days_forward, min_price, max_price)
    finally:
        conn.close()

    failures = evaluate_failures(leagues, args.fail_below_pct)
    report = {
        "generated_at": utc_now_iso(),
        "days_forward": args.days_forward,
        "fail_below_pct": args.fail_below_pct,
        "league_ids": league_ids,
        "excluded_league_ids": excluded_league_ids,
        "leagues": leagues,
        "failures": failures,
        "ok": len(failures) == 0,
    }

    if args.out_json:
        Path(args.out_json).write_text(json.dumps(report, indent=2), encoding="utf-8")
    if args.out_md:
        Path(args.out_md).write_text(build_markdown_report(report), encoding="utf-8")

    if failures:
        raise SystemExit("moneyline coverage validation failed")


if __name__ == "__main__":
    main()
