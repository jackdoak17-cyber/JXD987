#!/usr/bin/env python3
"""Validate the player-prop odds feed required by betting model publishing.

This checks the exact Supabase contract consumed by Models/scripts/top_ev_sot1_weekend.mjs:
- player_shots / player_shots_on_target markets
- 0.5 / 1.5 / 2.5 over lines
- supported bookmaker IDs
- mapped player participant IDs
- standard over selection keys
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import psycopg2
from psycopg2.extras import RealDictCursor

PLAYER_MARKETS = ("player_shots", "player_shots_on_target")
MODEL_LINES = (0.5, 1.5, 2.5)
SUPPORTED_BOOKMAKER_IDS = (2, 4, 5)
BETMGM_BOOKMAKER_ID = 8


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


def parse_csv_ints(raw: str, default: Iterable[int]) -> List[int]:
    if not raw.strip():
        return list(default)
    values: List[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values or list(default)


def fetch_summary(conn: Any, days_back: int, days_forward: int, bookmaker_ids: List[int]) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        statement_timeout_ms = int(os.environ.get("PLAYER_PROP_VALIDATE_STATEMENT_TIMEOUT_MS", "30000"))
        cur.execute("set statement_timeout = %s", (statement_timeout_ms,))
        cur.execute(
            """
            with fixtures_in_window as (
              select id
              from public.fixtures
              where starting_at >= (now() at time zone 'utc') - (%s || ' days')::interval
                and starting_at <  (now() at time zone 'utc') + (%s || ' days')::interval
            )
            select
              count(distinct f.id)::bigint as fixtures_in_window,
              count(o.*)::bigint as player_prop_rows,
              count(distinct o.fixture_id)::bigint as fixtures_with_player_props,
              count(o.*) filter (
                where abs(o.line - 0.5) <= 0.05
                   or abs(o.line - 1.5) <= 0.05
                   or abs(o.line - 2.5) <= 0.05
              )::bigint as model_line_rows,
              count(o.*) filter (
                where (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
                  and (
                    o.selection_key = 'over'
                    or (
                      o.selection_key ~ '_over$'
                      and o.selection_key !~ '(^|_)to_have(_|$)'
                      and o.selection_key !~ '(^|_)to_score(_|$)'
                      and o.selection_key !~ '(^|_)to_assist(_|$)'
                      and o.selection_key !~ '(^|_)to_be(_|$)'
                    )
                  )
              )::bigint as standard_over_rows,
              count(o.*) filter (
                where o.participant_id is not null
                  and (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
                  and (
                    o.selection_key = 'over'
                    or (
                      o.selection_key ~ '_over$'
                      and o.selection_key !~ '(^|_)to_have(_|$)'
                      and o.selection_key !~ '(^|_)to_score(_|$)'
                      and o.selection_key !~ '(^|_)to_assist(_|$)'
                      and o.selection_key !~ '(^|_)to_be(_|$)'
                    )
                  )
              )::bigint as mapped_standard_over_rows,
              count(distinct o.participant_id) filter (
                where o.participant_id is not null
                  and (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
                  and (
                    o.selection_key = 'over'
                    or (
                      o.selection_key ~ '_over$'
                      and o.selection_key !~ '(^|_)to_have(_|$)'
                      and o.selection_key !~ '(^|_)to_score(_|$)'
                      and o.selection_key !~ '(^|_)to_assist(_|$)'
                      and o.selection_key !~ '(^|_)to_be(_|$)'
                    )
                  )
              )::bigint as mapped_players,
              count(distinct o.fixture_id) filter (
                where (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
                  and (
                    o.selection_key = 'over'
                    or (
                      o.selection_key ~ '_over$'
                      and o.selection_key !~ '(^|_)to_have(_|$)'
                      and o.selection_key !~ '(^|_)to_score(_|$)'
                      and o.selection_key !~ '(^|_)to_assist(_|$)'
                      and o.selection_key !~ '(^|_)to_be(_|$)'
                    )
                  )
              )::bigint as fixtures_with_model_player_props,
              count(o.*) filter (
                where o.price_decimal >= 1.72
                  and (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
              )::bigint as value_odds_rows,
              count(o.*) filter (
                where o.price_decimal < 1.72
                  and (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
              )::bigint as high_probability_odds_rows
            from fixtures_in_window f
            left join public.odds_outcomes o
              on o.fixture_id = f.id
             and o.market_key = any(%s)
             and o.bookmaker_id = any(%s)
             and o.price_decimal is not null
             and (o.participant_type = 'player' or o.participant_type is null);
            """,
            (days_back, days_forward, list(PLAYER_MARKETS), bookmaker_ids),
        )
        summary = dict(cur.fetchone() or {})

        if int(summary.get("player_prop_rows") or 0) == 0:
            cur.execute(
                """
                with fixtures_in_window as (
                  select id, league_id
                  from public.fixtures
                  where starting_at >= (now() at time zone 'utc') - (%s || ' days')::interval
                    and starting_at <  (now() at time zone 'utc') + (%s || ' days')::interval
                )
                select
                  count(o.*)::bigint as player_prop_rows,
                  count(distinct o.fixture_id)::bigint as fixtures_with_player_props,
                  count(distinct f.league_id)::bigint as leagues_with_player_props,
                  coalesce(
                    array_agg(distinct f.league_id order by f.league_id)
                      filter (where f.league_id is not null),
                    array[]::bigint[]
                  ) as league_ids_with_player_props
                from public.odds_outcomes o
                join fixtures_in_window f on f.id = o.fixture_id
                where o.market_key = any(%s)
                  and o.bookmaker_id = %s
                  and o.price_decimal is not null
                  and (o.participant_type = 'player' or o.participant_type is null);
                """,
                (days_back, days_forward, list(PLAYER_MARKETS), BETMGM_BOOKMAKER_ID),
            )
            betmgm = dict(cur.fetchone() or {})
            summary["by_market_line"] = []
            summary["likely_player_match"] = {
                "likely_players_in_window": None,
                "odds_players_in_window": 0,
                "likely_players_with_odds": 0,
                "not_checked_reason": "player_prop_rows=0",
            }
            summary["betmgm_player_prop_contribution"] = {
                "bookmaker_id": BETMGM_BOOKMAKER_ID,
                "player_prop_rows": int(betmgm.get("player_prop_rows") or 0),
                "fixtures_with_player_props": int(betmgm.get("fixtures_with_player_props") or 0),
                "leagues_with_player_props": int(betmgm.get("leagues_with_player_props") or 0),
                "league_ids_with_player_props": [
                    int(value) for value in (betmgm.get("league_ids_with_player_props") or [])
                ],
                "required": False,
            }
            return summary

        cur.execute(
            """
            with fixtures_in_window as (
              select id
              from public.fixtures
              where starting_at >= (now() at time zone 'utc') - (%s || ' days')::interval
                and starting_at <  (now() at time zone 'utc') + (%s || ' days')::interval
            )
            select
              o.market_key,
              round(o.line::numeric, 1)::text as line,
              count(*)::bigint as rows,
              count(*) filter (where o.participant_id is not null)::bigint as mapped_rows,
              count(distinct o.participant_id) filter (where o.participant_id is not null)::bigint as mapped_players,
              count(distinct o.fixture_id)::bigint as fixtures,
              count(*) filter (where o.price_decimal >= 1.72)::bigint as value_odds_rows,
              count(*) filter (where o.price_decimal < 1.72)::bigint as high_probability_odds_rows
            from public.odds_outcomes o
            join fixtures_in_window f on f.id = o.fixture_id
            where o.market_key = any(%s)
              and o.bookmaker_id = any(%s)
              and o.price_decimal is not null
              and (o.participant_type = 'player' or o.participant_type is null)
              and (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
            group by o.market_key, round(o.line::numeric, 1)
            order by o.market_key, round(o.line::numeric, 1);
            """,
            (days_back, days_forward, list(PLAYER_MARKETS), bookmaker_ids),
        )
        by_market_line = [dict(row) for row in cur.fetchall()]

        cur.execute(
            """
            with fixtures_in_window as (
              select id, home_team_id, away_team_id
              from public.fixtures
              where starting_at >= (now() at time zone 'utc') - (%s || ' days')::interval
                and starting_at <  (now() at time zone 'utc') + (%s || ' days')::interval
            ), odds_players as (
              select distinct o.fixture_id, o.participant_id
              from public.odds_outcomes o
              join fixtures_in_window f on f.id = o.fixture_id
              where o.market_key = any(%s)
                and o.bookmaker_id = any(%s)
                and o.participant_id is not null
                and o.price_decimal is not null
                and (abs(o.line - 0.5) <= 0.05 or abs(o.line - 1.5) <= 0.05 or abs(o.line - 2.5) <= 0.05)
            ), likely_players as (
              select f.id as fixture_id, tlp.player_id
              from fixtures_in_window f
              join public.team_likely_players tlp
                on tlp.team_id in (f.home_team_id, f.away_team_id)
            )
            select
              (select count(distinct player_id) from likely_players)::bigint as likely_players_in_window,
              (select count(distinct participant_id) from odds_players)::bigint as odds_players_in_window,
              count(distinct op.participant_id)::bigint as likely_players_with_odds
            from odds_players op
            join likely_players lp
              on lp.fixture_id = op.fixture_id
             and lp.player_id = op.participant_id;
            """,
            (days_back, days_forward, list(PLAYER_MARKETS), bookmaker_ids),
        )
        likely = dict(cur.fetchone() or {})

        cur.execute(
            """
            with fixtures_in_window as (
              select id, league_id
              from public.fixtures
              where starting_at >= (now() at time zone 'utc') - (%s || ' days')::interval
                and starting_at <  (now() at time zone 'utc') + (%s || ' days')::interval
            )
            select
              count(o.*)::bigint as player_prop_rows,
              count(distinct o.fixture_id)::bigint as fixtures_with_player_props,
              count(distinct f.league_id)::bigint as leagues_with_player_props,
              coalesce(
                array_agg(distinct f.league_id order by f.league_id)
                  filter (where f.league_id is not null),
                array[]::bigint[]
              ) as league_ids_with_player_props
            from public.odds_outcomes o
            join fixtures_in_window f on f.id = o.fixture_id
            where o.market_key = any(%s)
              and o.bookmaker_id = %s
              and o.price_decimal is not null
              and (o.participant_type = 'player' or o.participant_type is null);
            """,
            (days_back, days_forward, list(PLAYER_MARKETS), BETMGM_BOOKMAKER_ID),
        )
        betmgm = dict(cur.fetchone() or {})

    summary["by_market_line"] = by_market_line
    summary["likely_player_match"] = likely
    summary["betmgm_player_prop_contribution"] = {
        "bookmaker_id": BETMGM_BOOKMAKER_ID,
        "player_prop_rows": int(betmgm.get("player_prop_rows") or 0),
        "fixtures_with_player_props": int(betmgm.get("fixtures_with_player_props") or 0),
        "leagues_with_player_props": int(betmgm.get("leagues_with_player_props") or 0),
        "league_ids_with_player_props": [
            int(value) for value in (betmgm.get("league_ids_with_player_props") or [])
        ],
        "required": False,
    }
    return summary


def build_failures(summary: Dict[str, Any], args: argparse.Namespace) -> List[str]:
    failures: List[str] = []
    if int(summary.get("fixtures_in_window") or 0) < args.min_fixtures:
        failures.append(
            f"fixtures_in_window {summary.get('fixtures_in_window')} < required {args.min_fixtures}"
        )
    if int(summary.get("player_prop_rows") or 0) < args.min_player_prop_rows:
        failures.append(
            f"player_prop_rows {summary.get('player_prop_rows')} < required {args.min_player_prop_rows}"
        )
    if int(summary.get("mapped_standard_over_rows") or 0) < args.min_mapped_standard_over_rows:
        failures.append(
            "mapped_standard_over_rows "
            f"{summary.get('mapped_standard_over_rows')} < required {args.min_mapped_standard_over_rows}"
        )
    if int(summary.get("mapped_players") or 0) < args.min_mapped_players:
        failures.append(f"mapped_players {summary.get('mapped_players')} < required {args.min_mapped_players}")
    if int(summary.get("fixtures_with_model_player_props") or 0) < args.min_fixtures_with_model_player_props:
        failures.append(
            "fixtures_with_model_player_props "
            f"{summary.get('fixtures_with_model_player_props')} < required {args.min_fixtures_with_model_player_props}"
        )
    return failures


def write_markdown(report: Dict[str, Any], path: str) -> None:
    lines = [
        "# Player Prop Odds Feed Validation",
        "",
        f"Generated: {report['generated_at']}",
        f"Status: {'PASS' if report['ok'] else 'FAIL'}",
        f"Window: -{report['days_back']} days to +{report['days_forward']} days",
        f"Bookmakers: {', '.join(str(x) for x in report['bookmaker_ids'])}",
        "",
        "## Summary",
        "",
    ]
    summary = report["summary"]
    for key in (
        "fixtures_in_window",
        "player_prop_rows",
        "fixtures_with_player_props",
        "model_line_rows",
        "standard_over_rows",
        "mapped_standard_over_rows",
        "mapped_players",
        "fixtures_with_model_player_props",
        "value_odds_rows",
        "high_probability_odds_rows",
    ):
        lines.append(f"- {key}: {summary.get(key, 0)}")
    likely = summary.get("likely_player_match") or {}
    if likely:
        lines += [
            "",
            "## Likely Player Match",
            "",
            f"- likely_players_in_window: {likely.get('likely_players_in_window', 0)}",
            f"- odds_players_in_window: {likely.get('odds_players_in_window', 0)}",
            f"- likely_players_with_odds: {likely.get('likely_players_with_odds', 0)}",
        ]
    betmgm = summary.get("betmgm_player_prop_contribution") or {}
    lines += [
        "",
        "## BetMGM Contribution Evidence",
        "",
        f"- required: {betmgm.get('required', False)}",
        f"- player_prop_rows: {betmgm.get('player_prop_rows', 0)}",
        f"- fixtures_with_player_props: {betmgm.get('fixtures_with_player_props', 0)}",
        f"- leagues_with_player_props: {betmgm.get('leagues_with_player_props', 0)}",
        f"- league_ids_with_player_props: {', '.join(str(x) for x in betmgm.get('league_ids_with_player_props', [])) or '-'}",
    ]
    if report["failures"]:
        lines += ["", "## Failures", ""]
        lines.extend(f"- {failure}" for failure in report["failures"])
    lines += ["", "## Market/Line Breakdown", ""]
    lines.append("| Market | Line | Rows | Mapped Rows | Players | Fixtures | Value Odds | High Prob Odds |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in summary.get("by_market_line") or []:
        lines.append(
            "| {market_key} | {line} | {rows} | {mapped_rows} | {mapped_players} | {fixtures} | "
            "{value_odds_rows} | {high_probability_odds_rows} |".format(**row)
        )
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=int(os.environ.get("PLAYER_PROP_VALIDATE_DAYS_BACK", "2")))
    parser.add_argument("--days-forward", type=int, default=int(os.environ.get("PLAYER_PROP_VALIDATE_DAYS_FORWARD", "14")))
    parser.add_argument("--bookmaker-ids", default=os.environ.get("PLAYER_PROP_VALIDATE_BOOKMAKER_IDS", ""))
    parser.add_argument("--min-fixtures", type=int, default=int(os.environ.get("PLAYER_PROP_VALIDATE_MIN_FIXTURES", "1")))
    parser.add_argument("--min-player-prop-rows", type=int, default=int(os.environ.get("PLAYER_PROP_VALIDATE_MIN_ROWS", "1")))
    parser.add_argument(
        "--min-mapped-standard-over-rows",
        type=int,
        default=int(os.environ.get("PLAYER_PROP_VALIDATE_MIN_MAPPED_ROWS", "1")),
    )
    parser.add_argument("--min-mapped-players", type=int, default=int(os.environ.get("PLAYER_PROP_VALIDATE_MIN_PLAYERS", "1")))
    parser.add_argument(
        "--min-fixtures-with-model-player-props",
        type=int,
        default=int(os.environ.get("PLAYER_PROP_VALIDATE_MIN_FIXTURES_WITH_PROPS", "1")),
    )
    parser.add_argument("--out-json", default="/tmp/player_prop_odds_feed_report.json")
    parser.add_argument("--out-md", default="/tmp/player_prop_odds_feed_report.md")
    args = parser.parse_args()

    bookmaker_ids = parse_csv_ints(args.bookmaker_ids, SUPPORTED_BOOKMAKER_IDS)
    conn = psycopg2.connect(get_db_url())
    try:
        summary = fetch_summary(conn, args.days_back, args.days_forward, bookmaker_ids)
    finally:
        conn.close()

    failures = build_failures(summary, args)
    report = {
        "ok": not failures,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days_back": args.days_back,
        "days_forward": args.days_forward,
        "bookmaker_ids": bookmaker_ids,
        "thresholds": {
            "min_fixtures": args.min_fixtures,
            "min_player_prop_rows": args.min_player_prop_rows,
            "min_mapped_standard_over_rows": args.min_mapped_standard_over_rows,
            "min_mapped_players": args.min_mapped_players,
            "min_fixtures_with_model_player_props": args.min_fixtures_with_model_player_props,
        },
        "failures": failures,
        "summary": summary,
    }
    Path(args.out_json).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    write_markdown(report, args.out_md)

    print(
        "player_prop_odds_feed "
        f"ok={report['ok']} rows={summary.get('player_prop_rows', 0)} "
        f"mapped_over_rows={summary.get('mapped_standard_over_rows', 0)} "
        f"mapped_players={summary.get('mapped_players', 0)} "
        f"fixtures={summary.get('fixtures_with_model_player_props', 0)} "
        f"betmgm_rows={(summary.get('betmgm_player_prop_contribution') or {}).get('player_prop_rows', 0)}"
    )
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
