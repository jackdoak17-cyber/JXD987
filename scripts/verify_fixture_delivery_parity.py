#!/usr/bin/env python3
"""Independently verify one published fixture-delivery release.

This verifier intentionally does not import the refresh worker's calculation
functions. It reads source tables and the selected published release, computes
the expected bounded projections independently, and emits a redacted JSON
report suitable for certification evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor


UTC = timezone.utc
LONDON = ZoneInfo("Europe/London")
EXCLUDED_CUPS = {24, 27, 109, 307, 390, 570}
HIDDEN_STATUSES = {
    "POSTP", "POSTPONED", "CANCL", "CANCELLED", "CANCELED", "ABANDONED",
    "SUSPENDED", "INTERRUPTED",
}
FINISHED_STATUSES = {"FT", "AET", "PEN", "FT_PEN"}
HISTORICAL_SCORE_STATUSES = {"AU", "AWAITING_UPDATES", "AWAR", "AWARDED"}
ACTIVE_BOOKMAKERS = {2, 4, 5, 8}
MONEYLINE_MARKETS = {
    "moneyline", "match_result", "match_winner", "match_winner_90",
    "match_winner_90_min", "full_time_result", "full_time_result_90", "1x2",
    "home_draw_away", "h2h",
}
DEFAULT_LEAGUES = [8, 9, 12, 14, 208, 271, 384, 387, 462, 591, 564, 567, 82, 301, 600, 501, 444, 72, 648, 651, 779, 944, 968, 989]


def expected_fixture_date(value: datetime | date) -> str:
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return aware.astimezone(LONDON).date().isoformat()
    return value.isoformat()


def normalize_status(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "_").replace(" ", "_")


def is_hidden(row: dict[str, Any]) -> bool:
    return normalize_status(row.get("status")) in HIDDEN_STATUSES or normalize_status(row.get("status_code")) in HIDDEN_STATUSES


def is_finished(row: dict[str, Any], now: datetime) -> bool:
    if normalize_status(row.get("status")) in FINISHED_STATUSES or normalize_status(row.get("status_code")) in FINISHED_STATUSES:
        return True
    if row.get("home_score") is None or row.get("away_score") is None:
        return False
    if not normalize_status(row.get("status")) and not normalize_status(row.get("status_code")):
        return True
    if normalize_status(row.get("status")) in HISTORICAL_SCORE_STATUSES or normalize_status(row.get("status_code")) in HISTORICAL_SCORE_STATUSES:
        started = row.get("starting_at")
        return isinstance(started, datetime) and now - started >= timedelta(hours=48)
    return False


def select_expected_history(history: list[dict[str, Any]], target: dict[str, Any], now: datetime | None = None) -> list[dict[str, Any]]:
    """Select prior rows and include a completed target at the same kickoff."""
    now = now or datetime.now(UTC)
    target_time = target["starting_at"]
    include_equal = is_finished(target, now)
    return [
        row for row in history
        if row["starting_at"] < target_time or (include_equal and row["starting_at"] == target_time)
    ]


def parse_leagues(raw: str | None) -> list[int]:
    values = raw or ",".join(str(value) for value in DEFAULT_LEAGUES)
    result: list[int] = []
    for token in values.split(","):
        try:
            value = int(token.strip())
        except ValueError:
            continue
        if value not in EXCLUDED_CUPS and value not in result:
            result.append(value)
    if not result:
        raise ValueError("no non-cup leagues supplied")
    return result


def connection():
    url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL_POOLER") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("SUPABASE_DB_URL_SESSION, SUPABASE_DB_URL_POOLER, or SUPABASE_DB_URL is required")
    return psycopg2.connect(url, connect_timeout=20)


def query_rows(cur, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    return [dict(row) for row in cur.fetchall()]


def load_source_schedule(cur, start: date, end: date, leagues: list[int]) -> list[dict[str, Any]]:
    return query_rows(
        cur,
        """
        select f.id as fixture_id, f.starting_at, f.status, f.status_code,
               f.league_id, f.season_id, f.home_team_id, f.away_team_id,
               f.home_score, f.away_score
          from public.fixtures f
         where f.starting_at >= (%s::date at time zone 'Europe/London')
           and f.starting_at < (%s::date at time zone 'Europe/London')
           and f.league_id = any(%s)
           and f.league_id <> all(%s)
         order by f.starting_at, f.league_id, f.id
        """,
        (start, end, leagues, list(EXCLUDED_CUPS)),
    )


def load_source_history(cur, leagues: list[int]) -> list[dict[str, Any]]:
    rows = query_rows(
        cur,
        """
        select id, starting_at, status, status_code, league_id, season_id,
               home_team_id, away_team_id, home_score, away_score,
               home_ht_score, away_ht_score
          from public.fixtures
         where league_id = any(%s)
           and league_id <> all(%s)
           and home_score is not null and away_score is not null
           and starting_at <= now()
         order by starting_at desc
        """,
        (leagues, list(EXCLUDED_CUPS)),
    )
    now = datetime.now(UTC)
    return [row for row in rows if not is_hidden(row) and is_finished(row, now)]


def load_published_release(cur, release_id: str | None) -> dict[str, Any]:
    if release_id:
        rows = query_rows(
            cur,
            "select id, status, requested_start, requested_end, source_watermark from public.fixture_delivery_releases where id = %s",
            (release_id,),
        )
    else:
        rows = query_rows(
            cur,
            """
            select r.id, r.status, r.requested_start, r.requested_end, r.source_watermark
              from public.fixture_delivery_current_publication p
              join public.fixture_delivery_releases r on r.id = p.release_id
             where p.publication_key = 'fixtures'
            """,
        )
    if not rows or rows[0]["status"] != "published":
        raise RuntimeError("selected fixture delivery release is not published")
    return rows[0]


def load_delivery_schedule(cur, release_id: str, start: date, end: date) -> list[dict[str, Any]]:
    return query_rows(
        cur,
        """
        select fixture_id, fixture_date, starting_at, status, status_code,
               league_id, season_id, home_team_id, away_team_id, home_score, away_score
          from public.fixture_delivery_schedule
         where release_id = %s and fixture_date >= %s and fixture_date < %s
         order by starting_at, league_id, fixture_id
        """,
        (release_id, start, end),
    )


def load_delivery_metrics(cur, release_id: str, fixture_ids: list[int]) -> list[dict[str, Any]]:
    if not fixture_ids:
        return []
    return query_rows(
        cur,
        """
        select fixture_id, team_id, side, metrics_window, metrics_mode,
               metrics, league_rank, source_max_starting_at
          from public.fixture_delivery_metrics
         where release_id = %s and fixture_id = any(%s)
        """,
        (release_id, fixture_ids),
    )


def load_delivery_standings(cur, release_id: str) -> list[dict[str, Any]]:
    return query_rows(
        cur,
        """
        select league_id, season_id, team_id, rank, points, played,
               goals_for, goals_against, goal_diff
          from public.fixture_delivery_standings
         where release_id = %s
        """,
        (release_id,),
    )


def load_source_odds(cur, fixture_ids: list[int]) -> list[dict[str, Any]]:
    if not fixture_ids:
        return []
    return query_rows(
        cur,
        """
        select o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key,
               coalesce(o.participant_type, 'team') as participant_type,
               coalesce(o.participant_id, 0) as participant_id,
               o.line, o.price_decimal, o.price_american,
               o.last_updated_at as source_last_updated_at
          from public.odds_outcomes o
         where o.fixture_id = any(%s)
           and o.bookmaker_id = any(%s)
           and lower(o.market_key) = any(%s)
           and o.price_decimal > 1 and o.price_decimal <= 500
        """,
        (fixture_ids, list(ACTIVE_BOOKMAKERS), list(MONEYLINE_MARKETS)),
    )


def load_delivery_odds(cur, release_id: str, fixture_ids: list[int]) -> list[dict[str, Any]]:
    if not fixture_ids:
        return []
    return query_rows(
        cur,
        """
        select fixture_id, bookmaker_id, market_key, selection_key,
               participant_type, participant_id, line, line_key,
               price_decimal, price_american, source_last_updated_at
          from public.fixture_delivery_odds
         where release_id = %s and fixture_id = any(%s)
        """,
        (release_id, fixture_ids),
    )


def expected_standings(history: list[dict[str, Any]]) -> dict[tuple[int, int], dict[int, dict[str, int]]]:
    grouped: dict[tuple[int, int], dict[int, dict[str, int]]] = defaultdict(dict)
    for row in history:
        if row.get("season_id") is None:
            continue
        key = (int(row["league_id"]), int(row["season_id"]))
        table = grouped[key]
        home_id, away_id = int(row["home_team_id"]), int(row["away_team_id"])
        hs, aws = int(row["home_score"]), int(row["away_score"])
        for team_id in (home_id, away_id):
            table.setdefault(team_id, {"points": 0, "played": 0, "goals_for": 0, "goals_against": 0})
        table[home_id]["played"] += 1
        table[away_id]["played"] += 1
        table[home_id]["goals_for"] += hs
        table[home_id]["goals_against"] += aws
        table[away_id]["goals_for"] += aws
        table[away_id]["goals_against"] += hs
        if hs > aws:
            table[home_id]["points"] += 3
        elif aws > hs:
            table[away_id]["points"] += 3
        else:
            table[home_id]["points"] += 1
            table[away_id]["points"] += 1
    for table in grouped.values():
        ranked = sorted(table.items(), key=lambda item: (-item[1]["points"], -(item[1]["goals_for"] - item[1]["goals_against"]), -item[1]["goals_for"]))
        for rank, (_, values) in enumerate(ranked, start=1):
            values["rank"] = rank
    return grouped


def empty_metrics() -> dict[str, Any]:
    return {
        "avgPoints": None, "over15Pct": None, "over25Pct": None, "bttsPct": None,
        "wins": 0, "draws": 0, "losses": 0, "goalsScored": 0, "goalsConceded": 0,
        "cleanSheets": 0, "avgGoalsScored": None, "avgGoalsConceded": None,
        "avgTotalGoals": None, "firstGoalBefore30Pct": None, "zeroZeroHtPct": None,
        "bttsHtPct": None, "sample": 0, "lastFiveForm": [],
    }


def calculate_expected_metrics(history: list[dict[str, Any]], team_id: int, window: int, venue: str | None) -> tuple[dict[str, Any], datetime | None]:
    selected: list[dict[str, Any]] = []
    for row in history:
        is_home = int(row["home_team_id"]) == team_id
        if venue == "home" and not is_home:
            continue
        if venue == "away" and is_home:
            continue
        selected.append(row)
        if len(selected) >= window:
            break
    if not selected:
        return empty_metrics(), None
    points: list[int] = []
    hits15 = hits25 = btts = wins = draws = losses = goals_for = goals_against = clean = 0
    zero_ht = btts_ht = ht_sample = 0
    forms: list[str] = []
    for row in selected:
        is_home = int(row["home_team_id"]) == team_id
        team_goals = int(row["home_score"] if is_home else row["away_score"])
        opponent_goals = int(row["away_score"] if is_home else row["home_score"])
        goals_for += team_goals
        goals_against += opponent_goals
        total = int(row["home_score"]) + int(row["away_score"])
        hits15 += int(total >= 2)
        hits25 += int(total >= 3)
        btts += int(int(row["home_score"]) > 0 and int(row["away_score"]) > 0)
        clean += int(opponent_goals == 0)
        if team_goals > opponent_goals:
            points.append(3); wins += 1; forms.append("W")
        elif team_goals < opponent_goals:
            points.append(0); losses += 1; forms.append("L")
        else:
            points.append(1); draws += 1; forms.append("D")
        if row.get("home_ht_score") is not None and row.get("away_ht_score") is not None:
            ht_sample += 1
            zero_ht += int(int(row["home_ht_score"]) + int(row["away_ht_score"]) == 0)
            btts_ht += int(int(row["home_ht_score"]) > 0 and int(row["away_ht_score"]) > 0)
    games = len(selected)
    return {
        "avgPoints": sum(points) / games,
        "over15Pct": hits15 / games * 100,
        "over25Pct": hits25 / games * 100,
        "bttsPct": btts / games * 100,
        "wins": wins, "draws": draws, "losses": losses,
        "goalsScored": goals_for, "goalsConceded": goals_against, "cleanSheets": clean,
        "avgGoalsScored": goals_for / games, "avgGoalsConceded": goals_against / games,
        "avgTotalGoals": (goals_for + goals_against) / games,
        "firstGoalBefore30Pct": None,
        "zeroZeroHtPct": zero_ht / ht_sample * 100 if ht_sample else None,
        "bttsHtPct": btts_ht / ht_sample * 100 if ht_sample else None,
        "sample": games, "lastFiveForm": forms[:5],
    }, max(row["starting_at"] for row in selected)


def with_provenance(metrics: dict[str, Any], mode: str) -> dict[str, Any]:
    result = dict(metrics)
    sample = int(result.get("sample") or 0)
    result["metricsSource"] = mode
    result["sampleStatus"] = "none" if sample <= 0 else "complete" if sample >= 5 else "partial"
    return result


def normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def close_enough(actual: Any, expected: Any) -> bool:
    if isinstance(actual, dict) and isinstance(expected, dict):
        return set(actual) == set(expected) and all(close_enough(actual[key], expected[key]) for key in expected)
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(close_enough(a, e) for a, e in zip(actual, expected))
    if isinstance(actual, (int, float, Decimal)) and isinstance(expected, (int, float, Decimal)):
        return abs(float(actual) - float(expected)) <= 1e-9
    return normalize_value(actual) == normalize_value(expected)


def key_source_odd(row: dict[str, Any]) -> tuple[Any, ...]:
    line = row.get("line")
    line_key = Decimal("-9999") if line is None else line
    return (
        int(row["fixture_id"]), int(row["bookmaker_id"]), str(row["market_key"]),
        str(row["selection_key"]), str(row.get("participant_type") or "team"),
        int(row.get("participant_id") or 0), normalize_value(line_key),
    )


def verify(cur, start: date, end: date, leagues: list[int], release_id: str | None) -> dict[str, Any]:
    release = load_published_release(cur, release_id)
    selected_id = str(release["id"])
    source_schedule = [row for row in load_source_schedule(cur, start, end, leagues) if not is_hidden(row)]
    delivery_schedule = load_delivery_schedule(cur, selected_id, start, end)
    source_by_id = {int(row["fixture_id"]): row for row in source_schedule}
    delivery_by_id = {int(row["fixture_id"]): row for row in delivery_schedule}
    mismatches: list[dict[str, Any]] = []

    if set(source_by_id) != set(delivery_by_id):
        mismatches.append({"kind": "schedule_identity", "source_only": sorted(set(source_by_id) - set(delivery_by_id)), "delivery_only": sorted(set(delivery_by_id) - set(source_by_id))})
    for fixture_id in sorted(set(source_by_id) & set(delivery_by_id)):
        source = source_by_id[fixture_id]
        delivered = delivery_by_id[fixture_id]
        fields = ("fixture_date", "league_id", "season_id", "home_team_id", "away_team_id", "home_score", "away_score", "status", "status_code")
        expected = {
            "fixture_date": expected_fixture_date(source["starting_at"]),
            "league_id": source["league_id"], "season_id": source["season_id"],
            "home_team_id": source["home_team_id"], "away_team_id": source["away_team_id"],
            "home_score": source["home_score"], "away_score": source["away_score"],
            "status": source["status"], "status_code": source["status_code"],
        }
        for field in fields:
            if not close_enough(delivered[field], expected[field]):
                mismatches.append({"kind": "schedule_value", "fixture_id": fixture_id, "field": field, "expected": normalize_value(expected[field]), "actual": normalize_value(delivered[field])})

    source_history = load_source_history(cur, leagues)
    history_by_key: dict[tuple[int, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in source_history:
        if row.get("season_id") is None:
            continue
        key = (int(row["league_id"]), int(row["season_id"]), int(row["home_team_id"]))
        history_by_key[key].append(row)
        history_by_key[(int(row["league_id"]), int(row["season_id"]), int(row["away_team_id"]))].append(row)
    standings = expected_standings(source_history)
    metrics = load_delivery_metrics(cur, selected_id, sorted(delivery_by_id))
    actual_metrics = {(int(row["fixture_id"]), int(row["team_id"]), str(row["side"]), int(row["metrics_window"]), str(row["metrics_mode"])): row for row in metrics}
    expected_metric_count = len(delivery_schedule) * 2 * 11 * 2
    if len(metrics) != expected_metric_count:
        mismatches.append({"kind": "metrics_count", "expected": expected_metric_count, "actual": len(metrics)})
    for fixture_id, fixture in delivery_by_id.items():
        season_id = fixture.get("season_id")
        for side, team_id in (("home", int(fixture["home_team_id"])), ("away", int(fixture["away_team_id"]))):
            history = history_by_key.get((int(fixture["league_id"]), int(season_id), team_id), []) if season_id is not None else []
            prior = select_expected_history(history, fixture)
            rank = standings.get((int(fixture["league_id"]), int(season_id)), {}).get(team_id, {}).get("rank") if season_id is not None else None
            for window in range(5, 16):
                for mode, venue in (("overall", None), ("venue", side)):
                    expected, source_max = calculate_expected_metrics(prior, team_id, window, venue)
                    expected = with_provenance(expected, mode)
                    key = (fixture_id, team_id, side, window, mode)
                    actual = actual_metrics.get(key)
                    if actual is None:
                        mismatches.append({"kind": "metric_missing", "key": key})
                        continue
                    if not close_enough(actual.get("metrics"), expected):
                        mismatches.append({"kind": "metric_value", "key": key, "expected": expected, "actual": actual.get("metrics")})
                    if not close_enough(actual.get("league_rank"), rank):
                        mismatches.append({"kind": "metric_rank", "key": key, "expected": rank, "actual": actual.get("league_rank")})
                    if not close_enough(actual.get("source_max_starting_at"), source_max):
                        mismatches.append({"kind": "metric_source_watermark", "key": key, "expected": normalize_value(source_max), "actual": normalize_value(actual.get("source_max_starting_at"))})

    delivery_standings = load_delivery_standings(cur, selected_id)
    expected_standings_rows = {
        (league_id, season_id, team_id): values
        for (league_id, season_id), table in standings.items()
        for team_id, values in table.items()
    }
    if len(delivery_standings) != len(expected_standings_rows):
        mismatches.append({"kind": "standings_count", "expected": len(expected_standings_rows), "actual": len(delivery_standings)})
    for row in delivery_standings:
        key = (int(row["league_id"]), int(row["season_id"]), int(row["team_id"]))
        expected = expected_standings_rows.get(key)
        if expected is None:
            mismatches.append({"kind": "standings_extra", "key": key})
            continue
        for field in ("rank", "points", "played", "goals_for", "goals_against", "goal_diff"):
            expected_value = expected[field] if field != "goal_diff" else expected["goals_for"] - expected["goals_against"]
            if not close_enough(row[field], expected_value):
                mismatches.append({"kind": "standings_value", "key": key, "field": field, "expected": expected_value, "actual": row[field]})

    source_odds = load_source_odds(cur, sorted(delivery_by_id))
    delivery_odds = load_delivery_odds(cur, selected_id, sorted(delivery_by_id))
    source_odd_keys = {key_source_odd(row): row for row in source_odds}
    delivery_odd_keys = {key_source_odd(row): row for row in delivery_odds}
    if set(source_odd_keys) != set(delivery_odd_keys):
        mismatches.append({"kind": "odds_identity", "source_only": [list(key) for key in sorted(set(source_odd_keys) - set(delivery_odd_keys), key=str)][:50], "delivery_only": [list(key) for key in sorted(set(delivery_odd_keys) - set(source_odd_keys), key=str)][:50]})
    for key in sorted(set(source_odd_keys) & set(delivery_odd_keys), key=str):
        source = source_odd_keys[key]
        delivered = delivery_odd_keys[key]
        for field in ("price_decimal", "price_american", "source_last_updated_at"):
            if not close_enough(delivered[field], source[field]):
                mismatches.append({"kind": "odds_value", "key": list(key), "field": field, "expected": normalize_value(source[field]), "actual": normalize_value(delivered[field])})

    source_watermark = max((row["starting_at"] for row in source_schedule), default=None)
    if not close_enough(release.get("source_watermark"), source_watermark):
        mismatches.append({"kind": "release_source_watermark", "expected": normalize_value(source_watermark), "actual": normalize_value(release.get("source_watermark"))})
    return {
        "release_id": selected_id,
        "status": release["status"],
        "requested_start": normalize_value(release["requested_start"]),
        "requested_end": normalize_value(release["requested_end"]),
        "source_schedule_rows": len(source_schedule),
        "delivery_schedule_rows": len(delivery_schedule),
        "delivery_metrics_rows": len(metrics),
        "source_odds_rows": len(source_odds),
        "delivery_odds_rows": len(delivery_odds),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:500],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--leagues", default=None)
    parser.add_argument("--release-id", default=None)
    parser.add_argument("--report-out", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if end <= start:
        raise SystemExit("end date must be after start date")
    conn = connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            report = verify(cur, start, end, parse_leagues(args.leagues or os.environ.get("FIXTURE_DELIVERY_LEAGUES")), args.release_id)
    finally:
        conn.close()
    with open(args.report_out, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=str)
    print(json.dumps({key: report[key] for key in ("release_id", "mismatch_count", "source_schedule_rows", "delivery_schedule_rows", "delivery_metrics_rows", "source_odds_rows", "delivery_odds_rows")}))
    return 0 if report["mismatch_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
