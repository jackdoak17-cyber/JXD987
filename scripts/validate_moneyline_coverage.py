#!/usr/bin/env python3
"""
Validate moneyline ingestion fidelity for upcoming fixtures.

This is not a product-coverage threshold. It asks one question: when the live
Odds API has complete moneyline prices for a matched SportMonks fixture and
bookmaker, did that exact fixture/bookmaker moneyline land in Supabase?
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from jxd.db import get_engine, get_session
from sync_odds import (
    BOOKMAKER_CANONICAL,
    BOOKMAKER_NAME_TO_ID,
    DEFAULT_BOOKMAKERS,
    MONEYLINE_MARKET_KEYS,
    canonicalize_bookmakers,
    fetch_league_odds_payload,
    filter_fixtures_by_priority,
    load_fixtures,
    load_league_map,
    normalize_bookmaker_key,
    parse_float,
    resolve_market_key,
)

ACTIVE_MONEYLINE_BOOKMAKER_IDS = (2, 4, 5, 8)
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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_db_url() -> str:
    for key in ("SUPABASE_DB_URL_SESSION", "SUPABASE_DB_URL", "DATABASE_URL"):
        value = os.environ.get(key)
        if value:
            return value
    fallback = Path("/tmp/supabase_db_url")
    if fallback.exists():
        value = fallback.read_text(encoding="utf-8").strip()
        if value:
            return value
    raise SystemExit("Missing SUPABASE_DB_URL_SESSION, SUPABASE_DB_URL, or DATABASE_URL")


def parse_league_ids(raw: str) -> List[int]:
    return [int(value) for value in raw.split(",") if value.strip()]


def parse_bookmaker_ids(raw: str) -> List[int]:
    if not raw.strip():
        return list(ACTIVE_MONEYLINE_BOOKMAKER_IDS)
    return [int(value) for value in raw.split(",") if value.strip()]


def fixture_label(fixture: Dict[str, Any]) -> str:
    home = str(fixture.get("home_name") or "home")
    away = str(fixture.get("away_name") or "away")
    kickoff = fixture.get("starting_at")
    kickoff_text = kickoff.isoformat() if hasattr(kickoff, "isoformat") else str(kickoff or "")
    return f"{home} vs {away} ({kickoff_text})"


def is_hidden_fixture(fixture: Dict[str, Any]) -> bool:
    hidden = {str(value).upper().replace("-", "_").replace(" ", "_") for value in HIDDEN_FIXTURE_STATUSES}
    return any(
        str(fixture.get(field) or "").upper().replace("-", "_").replace(" ", "_") in hidden
        for field in ("status", "status_code")
    )


def has_complete_api_moneyline(
    markets: Iterable[Dict[str, Any]],
    min_price: float = 1.0,
    max_price: float = 500.0,
) -> bool:
    for market in markets or []:
        if resolve_market_key(str(market.get("name") or "")) not in MONEYLINE_MARKET_KEYS:
            continue
        for odd in market.get("odds") or []:
            if not isinstance(odd, dict):
                continue
            home = parse_float(odd.get("home"))
            draw = parse_float(odd.get("draw"))
            away = parse_float(odd.get("away"))
            if all(
                price is not None and min_price < price <= max_price
                for price in (home, draw, away)
            ):
                return True
    return False


def expected_moneyline_pairs(
    odds_records: Iterable[Dict[str, Any]],
    requested_bookmaker_keys: set[str],
    bookmaker_id_allowlist: set[int],
    min_price: float = 1.0,
    max_price: float = 500.0,
) -> Tuple[Dict[Tuple[int, int], Dict[str, Any]], Dict[int, set[int]]]:
    expected: Dict[Tuple[int, int], Dict[str, Any]] = {}
    expected_by_fixture: Dict[int, set[int]] = {}
    for record in odds_records:
        fixture_id = int(record["fixture_id"])
        bookmakers_payload = record.get("bookmakers_payload") or {}
        for bookmaker_name, markets in bookmakers_payload.items():
            book_key = normalize_bookmaker_key(str(bookmaker_name))
            if book_key not in requested_bookmaker_keys:
                continue
            bookmaker_id = BOOKMAKER_NAME_TO_ID.get(book_key)
            if bookmaker_id is None or bookmaker_id not in bookmaker_id_allowlist:
                continue
            if not has_complete_api_moneyline(
                markets or [],
                min_price=min_price,
                max_price=max_price,
            ):
                continue
            expected[(fixture_id, bookmaker_id)] = {
                "fixture_id": fixture_id,
                "bookmaker_id": bookmaker_id,
                "bookmaker_name": BOOKMAKER_CANONICAL.get(book_key, str(bookmaker_name)),
            }
            expected_by_fixture.setdefault(fixture_id, set()).add(bookmaker_id)
    return expected, expected_by_fixture


def fetch_db_complete_pairs(
    conn: Any,
    fixture_ids: List[int],
    bookmaker_ids: List[int],
    min_price: float,
    max_price: float,
) -> set[Tuple[int, int]]:
    if not fixture_ids:
        return set()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            with expected_fixtures as (
              select
                f.id,
                f.home_team_id,
                f.away_team_id
              from public.fixtures f
              where f.id = any(%s)
                and upper(regexp_replace(coalesce(f.status, ''), '[^A-Z0-9]+', '_', 'g')) <> all(%s)
                and upper(regexp_replace(coalesce(f.status_code, ''), '[^A-Z0-9]+', '_', 'g')) <> all(%s)
            ), moneyline as (
              select
                o.fixture_id,
                o.bookmaker_id,
                bool_or(
                  o.participant_type = 'team'
                  and o.participant_id = ef.home_team_id
                  and o.price_decimal > %s
                  and o.price_decimal <= %s
                ) as has_home,
                bool_or(
                  o.participant_type = 'team'
                  and o.participant_id = ef.away_team_id
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
              join expected_fixtures ef on ef.id = o.fixture_id
              where o.market_key = any(%s)
                and o.bookmaker_id = any(%s)
              group by o.fixture_id, o.bookmaker_id
            )
            select fixture_id, bookmaker_id
            from moneyline
            where has_home and has_draw and has_away;
            """,
            (
                fixture_ids,
                HIDDEN_FIXTURE_STATUSES,
                HIDDEN_FIXTURE_STATUSES,
                min_price,
                max_price,
                min_price,
                max_price,
                sorted(MONEYLINE_MARKET_KEYS),
                bookmaker_ids,
            ),
        )
        return {(int(row["fixture_id"]), int(row["bookmaker_id"])) for row in cur.fetchall()}


def build_markdown_report(report: Dict[str, Any]) -> str:
    scope = report.get("priority") or "whole-window"
    lines = [
        "# Moneyline Ingestion Fidelity Report",
        "",
        f"Generated: {report['generated_at']}",
        f"Window: -{report['days_back']} days to +{report['days_forward']} days",
        f"Scope: {scope}",
        f"Status: {'PASS' if report['ok'] else 'FAIL'}",
        "",
        "| League | Fixtures | API events | Matched events | API ML fixtures | API ML pairs | DB complete pairs | Missing pairs | Coverage status |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for league in report["leagues"]:
        lines.append(
            f"| {league['league_id']} {league['league_name']} | {league['sportmonks_fixtures_in_window']} | "
            f"{league['api_events_returned']} | {league['api_events_matched']} | "
            f"{league['api_moneyline_fixtures']} | {league['api_moneyline_pairs']} | "
            f"{league['db_complete_pairs']} | {league['missing_pairs']} | {league['coverage_status']} |"
        )
    if report["failures"]:
        lines += ["", "## Missing API-Available Moneylines", ""]
        for failure in report["failures"]:
            missing = ", ".join(failure["missing_bookmakers"])
            lines.append(
                f"- {failure['league_name']} fixture {failure['fixture_id']} "
                f"{failure['fixture_label']}: missing {missing}"
            )
    if report.get("api_failures"):
        lines += ["", "## API Fetch Failures", ""]
        for failure in report["api_failures"]:
            lines.append(f"- {failure['league_name']}: {failure['error']}")
    if report["informational"]:
        lines += ["", "## Informational Product Coverage", ""]
        for item in report["informational"]:
            lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="")
    parser.add_argument("--days-back", type=int, default=int(os.environ.get("MONEYLINE_COVERAGE_DAYS_BACK", "0")))
    parser.add_argument(
        "--days-forward",
        type=int,
        default=int(os.environ.get("MONEYLINE_COVERAGE_DAYS_FORWARD", "7")),
    )
    parser.add_argument(
        "--fail-below-pct",
        type=float,
        default=float(os.environ.get("MONEYLINE_COVERAGE_MIN_PCT", "100")),
        help="Accepted for backwards compatibility; pass/fail is missing API-available DB pairs.",
    )
    parser.add_argument("--bookmakers", default=os.environ.get("ODDS_BOOKMAKERS", ",".join(DEFAULT_BOOKMAKERS)))
    parser.add_argument("--bookmaker-ids", default=os.environ.get("MONEYLINE_COVERAGE_BOOKMAKER_IDS", ""))
    parser.add_argument("--per-league-limit", type=int, default=int(os.environ.get("MONEYLINE_COVERAGE_PER_LEAGUE_LIMIT", "0")))
    parser.add_argument(
        "--priority",
        choices=("p1", "p2", "p3"),
        default=os.environ.get("MONEYLINE_COVERAGE_PRIORITY", ""),
        help="Restrict validation to the same priority-owned fixture slice used by sync_odds.py.",
    )
    parser.add_argument("--out-json", default="")
    parser.add_argument("--out-md", default="")
    args = parser.parse_args()

    league_ids = parse_league_ids(args.leagues)
    if not league_ids:
        raise SystemExit("No league IDs provided")

    raw_bookmakers = [item.strip() for item in args.bookmakers.split(",") if item.strip()]
    bookmakers, unknown_bookmakers = canonicalize_bookmakers(raw_bookmakers)
    if not bookmakers:
        raise SystemExit("No valid bookmakers provided")
    requested_keys = {normalize_bookmaker_key(name) for name in bookmakers}
    bookmaker_ids = parse_bookmaker_ids(args.bookmaker_ids)
    bookmaker_id_allowlist = set(bookmaker_ids)
    min_price = float(os.environ.get("ODDS_MIN_PRICE", "1.0"))
    max_price = float(os.environ.get("ODDS_MAX_PRICE", "500"))

    repo_root = Path(__file__).resolve().parent.parent
    league_map = load_league_map(repo_root / "config" / "odds_api_leagues.json")
    session = get_session(get_engine())
    conn = psycopg2.connect(get_db_url())
    try:
        fixtures = load_fixtures(session, league_ids, args.days_back, args.days_forward)
        if args.priority:
            fixtures = filter_fixtures_by_priority(fixtures, args.priority)
        fixtures_by_id = {int(fixture["fixture_id"]): fixture for fixture in fixtures}
        db_complete_pairs: set[Tuple[int, int]] = set()
        all_expected_pairs: Dict[Tuple[int, int], Dict[str, Any]] = {}
        leagues: List[Dict[str, Any]] = []
        informational: List[str] = []

        for league_id in league_ids:
            odds_league = league_map.get(int(league_id))
            league_fixtures = [fixture for fixture in fixtures if int(fixture["league_id"]) == int(league_id)]
            if not odds_league:
                leagues.append(
                    {
                        "league_id": league_id,
                        "league_name": "unmapped",
                        "sportmonks_fixtures_in_window": len(league_fixtures),
                        "api_events_returned": 0,
                        "api_events_matched": 0,
                        "api_moneyline_fixtures": 0,
                        "api_moneyline_pairs": 0,
                        "db_complete_pairs": 0,
                        "missing_pairs": 0,
                        "coverage_status": "not_mapped_to_odds_api",
                    }
                )
                informational.append(f"League {league_id}: not mapped to Odds API")
                continue

            result = fetch_league_odds_payload(
                league_id=int(league_id),
                odds_league=odds_league,
                league_fixtures=league_fixtures,
                sport="football",
                days_back=args.days_back,
                days_forward=args.days_forward,
                bookmakers=bookmakers,
                per_league_limit=args.per_league_limit,
            )
            expected_pairs, expected_by_fixture = expected_moneyline_pairs(
                result.odds_records,
                requested_keys,
                bookmaker_id_allowlist,
                min_price=min_price,
                max_price=max_price,
            )
            # Keep provider expectations aligned with the DB query's existing
            # hidden-fixture scope. A provider event can remain visible after
            # the local fixture is postponed or cancelled; that is not an
            # active ingestion gap.
            active_fixture_ids = {
                int(fixture["fixture_id"])
                for fixture in league_fixtures
                if not is_hidden_fixture(fixture)
            }
            hidden_fixture_ids = sorted(
                int(fixture["fixture_id"])
                for fixture in league_fixtures
                if is_hidden_fixture(fixture)
            )
            expected_pairs = {
                key: value for key, value in expected_pairs.items() if key[0] in active_fixture_ids
            }
            expected_by_fixture = {
                fixture_id: bookmaker_set
                for fixture_id, bookmaker_set in expected_by_fixture.items()
                if fixture_id in active_fixture_ids
            }
            all_expected_pairs.update(expected_pairs)
            expected_fixture_ids = sorted(expected_by_fixture)
            league_db_pairs = fetch_db_complete_pairs(
                conn,
                expected_fixture_ids,
                bookmaker_ids,
                min_price,
                max_price,
            )
            db_complete_pairs.update(league_db_pairs)
            missing_pairs = sorted(set(expected_pairs).difference(league_db_pairs))
            if not league_fixtures:
                coverage_status = "no_fixtures_in_window_or_out_of_season"
                informational.append(f"{odds_league}: no SportMonks fixtures in window")
            elif result.events_returned == 0:
                coverage_status = "fixtures_exist_but_no_api_events"
                informational.append(f"{odds_league}: fixtures exist but live API returned no events")
            elif not expected_pairs:
                coverage_status = "fixtures_exist_but_no_api_moneyline"
                informational.append(f"{odds_league}: fixtures exist but live API returned no complete moneyline")
            elif missing_pairs:
                coverage_status = "api_moneyline_missing_from_database"
            else:
                coverage_status = "api_moneyline_ingested"
            leagues.append(
                {
                    "league_id": int(league_id),
                    "league_name": odds_league,
                    "sportmonks_fixtures_in_window": len(league_fixtures),
                    "api_events_returned": int(result.events_returned or 0),
                    "api_events_matched": int(result.events_matched or 0),
                    "api_moneyline_fixtures": len(expected_fixture_ids),
                    "api_moneyline_pairs": len(expected_pairs),
                    "db_complete_pairs": len(league_db_pairs),
                    "missing_pairs": len(missing_pairs),
                    "hidden_fixture_ids": hidden_fixture_ids,
                    "coverage_status": coverage_status,
                    "unmatched_api_event_samples": result.unmatched_samples[:5],
                    "error": str(result.error) if result.error else None,
                }
            )

        failures_by_fixture: Dict[Tuple[int, str], Dict[str, Any]] = {}
        api_failures: List[Dict[str, Any]] = []
        for league in leagues:
            if league.get("error"):
                api_failures.append(
                    {
                        "league_id": league["league_id"],
                        "league_name": league["league_name"],
                        "error": league["error"],
                    }
                )
        for key, expected in all_expected_pairs.items():
            if key in db_complete_pairs:
                continue
            fixture_id, bookmaker_id = key
            fixture = fixtures_by_id.get(fixture_id, {})
            league_id = int(fixture.get("league_id") or 0)
            league_name = league_map.get(league_id, str(league_id))
            group_key = (fixture_id, league_name)
            failure = failures_by_fixture.setdefault(
                group_key,
                {
                    "league_id": league_id,
                    "league_name": league_name,
                    "fixture_id": fixture_id,
                    "fixture_label": fixture_label(fixture),
                    "missing_bookmakers": [],
                },
            )
            failure["missing_bookmakers"].append(str(expected["bookmaker_name"]))

        failures = list(failures_by_fixture.values())
        for failure in failures:
            failure["missing_bookmakers"] = sorted(failure["missing_bookmakers"])

        report = {
            "ok": not failures and not api_failures,
            "generated_at": utc_now_iso(),
            "days_back": args.days_back,
            "days_forward": args.days_forward,
            "priority": args.priority or None,
            "bookmakers": bookmakers,
            "bookmaker_ids": bookmaker_ids,
            "unknown_bookmakers_requested": unknown_bookmakers,
            "fail_below_pct_argument_ignored": args.fail_below_pct,
            "leagues": leagues,
            "failures": failures,
            "api_failures": api_failures,
            "informational": informational,
        }
    finally:
        conn.close()
        session.close()

    if args.out_json:
        Path(args.out_json).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    if args.out_md:
        Path(args.out_md).write_text(build_markdown_report(report), encoding="utf-8")

    print(
        "moneyline_ingestion_fidelity "
        f"ok={report['ok']} api_pairs={sum(int(row['api_moneyline_pairs']) for row in report['leagues'])} "
        f"missing_pairs={sum(int(row['missing_pairs']) for row in report['leagues'])}"
    )
    if report["api_failures"]:
        raise SystemExit("moneyline API fetch failed during ingestion fidelity validation")
    if failures:
        raise SystemExit("moneyline ingestion fidelity validation failed")


if __name__ == "__main__":
    main()
