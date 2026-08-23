#!/usr/bin/env python3
"""Refresh the persistent Fixtures Data Delivery v2 read models.

This job deliberately runs after the source/odds sync on the VPS.  It reads
the authoritative Supabase source tables, calculates the exact fixture-card
metrics once, and publishes idempotent delivery rows.  The web application
never performs these historical scans during a request.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json, execute_values


LOG = logging.getLogger("refresh_fixture_delivery")
UTC = timezone.utc
EXCLUDED_CUPS = {24, 27, 109, 307, 390, 570}
HIDDEN_STATUSES = {
    "POSTP",
    "POSTPONED",
    "CANCL",
    "CANCELLED",
    "CANCELED",
    "ABANDONED",
    "SUSPENDED",
    "INTERRUPTED",
}
FINISHED_STATUSES = {"FT", "AET", "PEN", "FT_PEN"}
HISTORICAL_SCORE_STATUSES = {"AU", "AWAITING_UPDATES", "AWAR", "AWARDED"}
ACTIVE_BOOKMAKERS = {2, 4, 5, 8}
MONEYLINE_MARKETS = {
    "moneyline",
    "match_result",
    "match_winner",
    "match_winner_90",
    "match_winner_90_min",
    "full_time_result",
    "full_time_result_90",
    "1x2",
    "home_draw_away",
    "h2h",
}
MIN_FORM_SAMPLE = 5
DEFAULT_DAYS_FORWARD = 31


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def iso_date(value: datetime | date) -> str:
    return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()


def normalize_status(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "_").replace(" ", "_")


def is_hidden(row: dict[str, Any]) -> bool:
    return normalize_status(row.get("status")) in HIDDEN_STATUSES or normalize_status(
        row.get("status_code")
    ) in HIDDEN_STATUSES


def is_finished(row: dict[str, Any], now: datetime) -> bool:
    status = normalize_status(row.get("status"))
    status_code = normalize_status(row.get("status_code"))
    if status in FINISHED_STATUSES or status_code in FINISHED_STATUSES:
        return True
    if row.get("home_score") is None or row.get("away_score") is None:
        return False
    if not status and not status_code:
        return True
    if status in HISTORICAL_SCORE_STATUSES or status_code in HISTORICAL_SCORE_STATUSES:
        started = row.get("starting_at")
        return isinstance(started, datetime) and now - started >= timedelta(hours=48)
    return False


def parse_leagues(raw: str | None) -> list[int]:
    values = raw or "8,9,12,14,208,271,384,387,462,591,564,567,82,301,600,501,444,72,648,651,779,944,968,989"
    result: list[int] = []
    for token in values.split(","):
        try:
            value = int(token.strip())
        except ValueError:
            continue
        if value not in EXCLUDED_CUPS and value not in result:
            result.append(value)
    if not result:
        raise SystemExit("FIXTURE_DELIVERY_LEAGUES produced an empty non-cup allowlist")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default=None, help="London date, inclusive (default today)")
    parser.add_argument("--end-date", default=None, help="London date, exclusive (default today + 31 days)")
    parser.add_argument("--leagues", default=None, help="Comma-separated supported league IDs")
    parser.add_argument("--report-out", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def get_dates(args: argparse.Namespace) -> tuple[date, date]:
    start = date.fromisoformat(args.start_date) if args.start_date else date.today()
    end = date.fromisoformat(args.end_date) if args.end_date else start + timedelta(days=DEFAULT_DAYS_FORWARD)
    if end <= start:
        raise SystemExit("end date must be after start date")
    return start, end


def connection():
    url = (
        os.environ.get("SUPABASE_DB_URL_SESSION")
        or os.environ.get("SUPABASE_DB_URL_POOLER")
        or os.environ.get("SUPABASE_DB_URL")
    )
    if not url:
        raise SystemExit("SUPABASE_DB_URL_SESSION, SUPABASE_DB_URL_POOLER, or SUPABASE_DB_URL is required")
    return psycopg2.connect(url, connect_timeout=20)


def start_run(cur, component: str, start: date, end: date) -> str:
    cur.execute(
        """
        insert into public.fixture_delivery_refresh_runs
          (component, requested_start, requested_end)
        values (%s, %s, %s)
        returning id
        """,
        (component, start, end),
    )
    return str(cur.fetchone()[0])


def finish_run(cur, run_id: str, status: str, stats: dict[str, Any], error: str | None = None) -> None:
    cur.execute(
        """
        update public.fixture_delivery_refresh_runs
           set completed_at = now(), status = %s,
               rows_read = %s, rows_written = %s, rows_rejected = %s,
               rows_missing = %s, rejected_cup_rows = %s,
               source_watermark = %s, error_message = %s, metadata = %s
         where id = %s
        """,
        (
            status,
            int(stats.get("rows_read", 0)),
            int(stats.get("rows_written", 0)),
            int(stats.get("rows_rejected", 0)),
            int(stats.get("rows_missing", 0)),
            int(stats.get("rejected_cup_rows", 0)),
            stats.get("source_watermark"),
            error,
            Json(stats, dumps=lambda value: json.dumps(value, default=str)),
            run_id,
        ),
    )


def source_fixtures(cur, start: date, end: date, leagues: list[int]) -> list[dict[str, Any]]:
    cur.execute(
        """
        select f.id, f.starting_at, f.status, f.status_code, f.league_id,
               f.season_id, f.home_team_id, f.away_team_id,
               f.home_score, f.away_score, f.home_ht_score, f.away_ht_score,
               l.name as league_name, l.image_path as league_logo,
               ht.name as home_team_name, ht.short_code as home_team_short_code,
               ht.image_path as home_team_image_path,
               at.name as away_team_name, at.short_code as away_team_short_code,
               at.image_path as away_team_image_path
          from public.fixtures f
          left join public.leagues l on l.id = f.league_id
          left join public.teams ht on ht.id = f.home_team_id
          left join public.teams at on at.id = f.away_team_id
         where f.starting_at >= (%s::date at time zone 'Europe/London')
           and f.starting_at < (%s::date at time zone 'Europe/London')
           and f.league_id = any(%s)
           and f.league_id <> all(%s)
         order by f.starting_at, f.league_id, f.id
        """,
        (start, end, leagues, list(EXCLUDED_CUPS)),
    )
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def missing_delivery_fields(row: dict[str, Any]) -> list[str]:
    """Return fields that would make a source fixture unsafe to publish.

    The delivery table intentionally has non-null identity/name columns.  A
    source query must therefore report incomplete metadata explicitly instead
    of silently dropping the fixture through an inner join.
    """
    missing: list[str] = []
    for field in ("starting_at", "league_id", "home_team_id", "away_team_id", "league_name", "home_team_name", "away_team_name"):
        if row.get(field) is None:
            missing.append(field)
    if row.get("home_team_id") is not None and row.get("home_team_id") == row.get("away_team_id"):
        missing.append("distinct_teams")
    return missing


def classify_source_fixtures(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int, list[dict[str, Any]]]:
    """Split source rows into publishable, hidden, Cup, and incomplete rows."""
    valid: list[dict[str, Any]] = []
    rejected_cups = 0
    rejected_hidden = 0
    incomplete: list[dict[str, Any]] = []
    for row in rows:
        if row.get("league_id") in EXCLUDED_CUPS:
            rejected_cups += 1
            continue
        if is_hidden(row):
            rejected_hidden += 1
            continue
        missing = missing_delivery_fields(row)
        if missing:
            incomplete.append({"fixture_id": int(row["id"]), "missing": missing})
            continue
        valid.append(row)
    return valid, rejected_cups, rejected_hidden, incomplete


def all_completed_fixtures(cur, leagues: list[int]) -> list[dict[str, Any]]:
    cur.execute(
        """
        select id, starting_at, status, status_code, league_id, season_id,
               home_team_id, away_team_id, home_score, away_score,
               home_ht_score, away_ht_score
          from public.fixtures
         where league_id = any(%s)
           and league_id <> all(%s)
           and home_score is not null and away_score is not null
           and starting_at <= now()
         order by starting_at desc, id desc
        """,
        (leagues, list(EXCLUDED_CUPS)),
    )
    columns = [description[0] for description in cur.description]
    now = datetime.now(UTC)
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    return [row for row in rows if not is_hidden(row) and is_finished(row, now)]


def upsert_schedule(cur, rows: list[dict[str, Any]], start: date, end: date) -> int:
    values = [
        (
            row["id"],
            iso_date(row["starting_at"]),
            row["starting_at"],
            row.get("status"),
            row.get("status_code"),
            row["league_id"],
            row.get("season_id"),
            row["home_team_id"],
            row["away_team_id"],
            row.get("home_score"),
            row.get("away_score"),
            row["league_name"],
            row.get("league_logo"),
            row["home_team_name"],
            row.get("home_team_short_code"),
            row.get("home_team_image_path"),
            row["away_team_name"],
            row.get("away_team_short_code"),
            row.get("away_team_image_path"),
            datetime.now(UTC),
        )
        for row in rows
        if row["league_id"] not in EXCLUDED_CUPS and not is_hidden(row)
    ]
    if values:
        execute_values(
            cur,
            """
            insert into public.fixture_delivery_schedule
              (fixture_id, fixture_date, starting_at, status, status_code,
               league_id, season_id, home_team_id, away_team_id, home_score,
               away_score, league_name, league_logo, home_team_name,
               home_team_short_code, home_team_image_path, away_team_name,
               away_team_short_code, away_team_image_path, source_updated_at)
            values %s
            on conflict (fixture_id) do update set
              fixture_date = excluded.fixture_date,
              starting_at = excluded.starting_at,
              status = excluded.status,
              status_code = excluded.status_code,
              league_id = excluded.league_id,
              season_id = excluded.season_id,
              home_team_id = excluded.home_team_id,
              away_team_id = excluded.away_team_id,
              home_score = excluded.home_score,
              away_score = excluded.away_score,
              league_name = excluded.league_name,
              league_logo = excluded.league_logo,
              home_team_name = excluded.home_team_name,
              home_team_short_code = excluded.home_team_short_code,
              home_team_image_path = excluded.home_team_image_path,
              away_team_name = excluded.away_team_name,
              away_team_short_code = excluded.away_team_short_code,
              away_team_image_path = excluded.away_team_image_path,
              source_updated_at = excluded.source_updated_at,
              published_at = now()
            """,
            values,
            page_size=500,
        )
    valid_ids = [row["id"] for row in rows if row["league_id"] not in EXCLUDED_CUPS and not is_hidden(row)]
    if valid_ids:
        cur.execute(
            """
            delete from public.fixture_delivery_schedule
             where fixture_date >= %s and fixture_date < %s
               and not (fixture_id = any(%s))
            """,
            (start, end, valid_ids),
        )
    else:
        cur.execute(
            "delete from public.fixture_delivery_schedule where fixture_date >= %s and fixture_date < %s",
            (start, end),
        )
    return len(values)


def validate_schedule_projection(cur, rows: list[dict[str, Any]]) -> None:
    """Fail the publication if the delivery projection disagrees with source rows."""
    if not rows:
        return
    fixture_ids = [int(row["id"]) for row in rows]
    cur.execute(
        """
        select fixture_id, status, status_code, home_score, away_score
          from public.fixture_delivery_schedule
         where fixture_id = any(%s)
        """,
        (fixture_ids,),
    )
    projected = {int(row[0]): row[1:] for row in cur.fetchall()}
    mismatches: list[str] = []
    for source in rows:
        fixture_id = int(source["id"])
        actual = projected.get(fixture_id)
        expected = (
            source.get("status"),
            source.get("status_code"),
            source.get("home_score"),
            source.get("away_score"),
        )
        if actual is None or tuple(actual) != expected:
            mismatches.append(
                f"{fixture_id}: expected={expected!r} actual={actual!r}"
            )
    if mismatches:
        raise RuntimeError(
            "fixture delivery projection mismatch after upsert: "
            + "; ".join(mismatches[:20])
        )


def validate_schedule_completeness(
    cur,
    rows: list[dict[str, Any]],
    start: date,
    end: date,
) -> None:
    """Ensure every eligible source fixture is present, and nothing extra is.

    This is deliberately checked after the delete/upsert inside the same
    transaction.  A missing row aborts the transaction, leaving the previous
    known-good read model intact rather than publishing a partial window.
    """
    expected_ids = {int(row["id"]) for row in rows}
    cur.execute(
        """
        select fixture_id
          from public.fixture_delivery_schedule
         where fixture_date >= %s and fixture_date < %s
        """,
        (start, end),
    )
    actual_ids = {int(row[0]) for row in cur.fetchall()}
    missing = sorted(expected_ids - actual_ids)
    unexpected = sorted(actual_ids - expected_ids)
    if missing or unexpected:
        parts: list[str] = []
        if missing:
            parts.append(f"missing eligible fixtures={missing[:20]}")
        if unexpected:
            parts.append(f"unexpected published fixtures={unexpected[:20]}")
        raise RuntimeError("fixture delivery completeness check failed: " + "; ".join(parts))


def compute_standings(completed: list[dict[str, Any]]) -> dict[tuple[int, int], dict[int, dict[str, int]]]:
    grouped: dict[tuple[int, int], dict[int, dict[str, int]]] = defaultdict(dict)
    for row in completed:
        key = (int(row["league_id"]), int(row["season_id"]))
        home_id, away_id = int(row["home_team_id"]), int(row["away_team_id"])
        hs, aws = int(row["home_score"]), int(row["away_score"])
        table = grouped[key]
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
        ranked = sorted(
            table.items(),
            key=lambda item: (
                -item[1]["points"],
                -(item[1]["goals_for"] - item[1]["goals_against"]),
                -item[1]["goals_for"],
                item[0],
            ),
        )
        for rank, (team_id, values) in enumerate(ranked, start=1):
            values["rank"] = rank
    return grouped


def write_standings(cur, standings: dict[tuple[int, int], dict[int, dict[str, int]]], completed: list[dict[str, Any]]) -> int:
    latest: dict[tuple[int, int], datetime] = {}
    for row in completed:
        key = (int(row["league_id"]), int(row["season_id"]))
        latest[key] = max(latest.get(key, datetime.min.replace(tzinfo=UTC)), row["starting_at"])
    values = []
    for (league_id, season_id), table in standings.items():
        for team_id, stats in table.items():
            values.append(
                (
                    league_id,
                    season_id,
                    team_id,
                    stats["rank"],
                    stats["points"],
                    stats["played"],
                    stats["goals_for"],
                    stats["goals_against"],
                    stats["goals_for"] - stats["goals_against"],
                    latest[(league_id, season_id)],
                )
            )
    if values:
        execute_values(
            cur,
            """
            insert into public.fixture_delivery_standings
              (league_id, season_id, team_id, rank, points, played,
               goals_for, goals_against, goal_diff, source_max_starting_at)
            values %s
            on conflict (league_id, season_id, team_id) do update set
              rank = excluded.rank, points = excluded.points, played = excluded.played,
              goals_for = excluded.goals_for, goals_against = excluded.goals_against,
              goal_diff = excluded.goal_diff, source_max_starting_at = excluded.source_max_starting_at,
              computed_at = now()
            """,
            values,
            page_size=1000,
        )
    return len(values)


def empty_metrics() -> dict[str, Any]:
    return {
        "avgPoints": None,
        "over15Pct": None,
        "over25Pct": None,
        "bttsPct": None,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goalsScored": 0,
        "goalsConceded": 0,
        "cleanSheets": 0,
        "avgGoalsScored": None,
        "avgGoalsConceded": None,
        "avgTotalGoals": None,
        "firstGoalBefore30Pct": None,
        "zeroZeroHtPct": None,
        "bttsHtPct": None,
        "sample": 0,
        "lastFiveForm": [],
    }


def sample_status(sample: int) -> str:
    if sample <= 0:
        return "none"
    return "complete" if sample >= MIN_FORM_SAMPLE else "partial"


def add_metrics_provenance(metrics: dict[str, Any], mode: str) -> dict[str, Any]:
    """Persist the selected bucket and sample state alongside its values."""
    sample = int(metrics.get("sample") or 0)
    metrics["metricsSource"] = mode
    metrics["sampleStatus"] = sample_status(sample)
    return metrics


def calculate_metrics(history: list[dict[str, Any]], team_id: int, window: int, venue: str | None) -> tuple[dict[str, Any], datetime | None]:
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
    hits15 = hits25 = btts = wins = draws = losses = gf = ga = clean = 0
    zero_ht = btts_ht = ht_sample = 0
    forms: list[str] = []
    for row in selected:
        is_home = int(row["home_team_id"]) == team_id
        team_goals = int(row["home_score"] if is_home else row["away_score"])
        opp_goals = int(row["away_score"] if is_home else row["home_score"])
        gf += team_goals
        ga += opp_goals
        total = int(row["home_score"]) + int(row["away_score"])
        hits15 += int(total >= 2)
        hits25 += int(total >= 3)
        btts += int(int(row["home_score"]) > 0 and int(row["away_score"]) > 0)
        clean += int(opp_goals == 0)
        if team_goals > opp_goals:
            points.append(3); wins += 1; forms.append("W")
        elif team_goals < opp_goals:
            points.append(0); losses += 1; forms.append("L")
        else:
            points.append(1); draws += 1; forms.append("D")
        if row.get("home_ht_score") is not None and row.get("away_ht_score") is not None:
            ht_sample += 1
            zero_ht += int(int(row["home_ht_score"]) + int(row["away_ht_score"]) == 0)
            btts_ht += int(int(row["home_ht_score"]) > 0 and int(row["away_ht_score"]) > 0)
    games = len(selected)
    metrics = {
        "avgPoints": sum(points) / games,
        "over15Pct": hits15 / games * 100,
        "over25Pct": hits25 / games * 100,
        "bttsPct": btts / games * 100,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "goalsScored": gf,
        "goalsConceded": ga,
        "cleanSheets": clean,
        "avgGoalsScored": gf / games,
        "avgGoalsConceded": ga / games,
        "avgTotalGoals": (gf + ga) / games,
        "firstGoalBefore30Pct": None,
        "zeroZeroHtPct": zero_ht / ht_sample * 100 if ht_sample else None,
        "bttsHtPct": btts_ht / ht_sample * 100 if ht_sample else None,
        "sample": games,
        "lastFiveForm": forms[:5],
    }
    return metrics, max(row["starting_at"] for row in selected)


def write_metrics(
    cur,
    schedule: list[dict[str, Any]],
    completed: list[dict[str, Any]],
    standings: dict[tuple[int, int], dict[int, dict[str, int]]],
) -> tuple[int, dict[str, int]]:
    season_watermarks: dict[tuple[int, int], datetime] = {}
    for row in completed:
        key = (int(row["league_id"]), int(row["season_id"]))
        season_watermarks[key] = max(season_watermarks.get(key, datetime.min.replace(tzinfo=UTC)), row["starting_at"])
    seasons_by_league: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for (league_id, season_id), watermark in season_watermarks.items():
        seasons_by_league[league_id].append((season_id, int(watermark.timestamp())))
    for league_id in seasons_by_league:
        seasons_by_league[league_id].sort(key=lambda item: item[1], reverse=True)
    def rank_for(league_id: int, season_id: int, team_id: int) -> int | None:
        current = standings.get((league_id, season_id), {}).get(team_id)
        if current:
            return current["rank"]
        # At a season restart, preserve the existing product contract: use the
        # team's latest prior non-cup league table until the current season has
        # recorded a match. This remains a persisted value, never request work.
        for prior_season, _ in seasons_by_league.get(league_id, [])[1:]:
            prior = standings.get((league_id, prior_season), {}).get(team_id)
            if prior:
                return prior["rank"]
        for other_league, season_keys in seasons_by_league.items():
            if other_league in EXCLUDED_CUPS:
                continue
            for other_season, _ in season_keys:
                prior = standings.get((other_league, other_season), {}).get(team_id)
                if prior:
                    return prior["rank"]
        return None

    history_by_team: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in completed:
        history_by_team[int(row["home_team_id"])].append(row)
        history_by_team[int(row["away_team_id"])].append(row)
    for rows in history_by_team.values():
        rows.sort(key=lambda row: (row["starting_at"], row["id"]), reverse=True)

    values = []
    coverage = {
        "overall_none": 0,
        "overall_partial": 0,
        "overall_complete": 0,
        "venue_none": 0,
        "venue_partial": 0,
        "venue_complete": 0,
        "venue_empty_overall_available": 0,
    }
    for fixture in schedule:
        fixture_time = fixture["starting_at"]
        for side, team_id in (("home", int(fixture["home_team_id"])), ("away", int(fixture["away_team_id"]))):
            prior = [row for row in history_by_team.get(team_id, []) if row["starting_at"] < fixture_time]
            for window in range(5, 16):
                samples: dict[str, int] = {}
                for mode, venue in (("overall", None), ("venue", side)):
                    metrics, max_source = calculate_metrics(prior, team_id, window, venue)
                    add_metrics_provenance(metrics, mode)
                    status = str(metrics["sampleStatus"])
                    coverage[f"{mode}_{status}"] += 1
                    samples[mode] = int(metrics.get("sample") or 0)
                    values.append(
                        (
                            fixture["id"], team_id, side, window, mode, Json(metrics),
                            rank_for(int(fixture["league_id"]), int(fixture["season_id"] or 0), team_id), max_source,
                        )
                    )
                if samples["venue"] == 0 and samples["overall"] > 0:
                    coverage["venue_empty_overall_available"] += 1
    if values:
        execute_values(
            cur,
            """
            insert into public.fixture_delivery_metrics
              (fixture_id, team_id, side, metrics_window, metrics_mode,
               metrics, league_rank, source_max_starting_at)
            values %s
            on conflict (fixture_id, team_id, side, metrics_window, metrics_mode) do update set
              metrics = excluded.metrics, league_rank = excluded.league_rank,
              source_max_starting_at = excluded.source_max_starting_at, computed_at = now()
            """,
            values,
            page_size=1000,
        )
    return len(values), coverage


def source_odds(cur, fixture_ids: list[int]) -> list[dict[str, Any]]:
    if not fixture_ids:
        return []
    cur.execute(
        """
        select o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key,
               coalesce(o.participant_type, 'team') as participant_type,
               coalesce(o.participant_id, 0) as participant_id,
               o.line, o.price_decimal, o.price_american,
               o.last_updated_at as source_last_updated_at
          from public.odds_outcomes o
          join public.fixture_delivery_schedule s on s.fixture_id = o.fixture_id
         where o.fixture_id = any(%s)
           and o.bookmaker_id = any(%s)
           and lower(o.market_key) = any(%s)
           and o.price_decimal > 1 and o.price_decimal <= 500
           and s.league_id <> all(%s)
        """,
        (fixture_ids, list(ACTIVE_BOOKMAKERS), list(MONEYLINE_MARKETS), list(EXCLUDED_CUPS)),
    )
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def write_odds(cur, rows: list[dict[str, Any]]) -> int:
    values = []
    for row in rows:
        line = row.get("line")
        line_key = line if line is not None else Decimal("-9999")
        values.append(
            (
                row["fixture_id"], row["bookmaker_id"], row["market_key"], row["selection_key"],
                row["participant_type"], row["participant_id"], line, line_key,
                row["price_decimal"], row.get("price_american"), row.get("source_last_updated_at"),
                datetime.now(UTC),
            )
        )
    if values:
        execute_values(
            cur,
            """
            insert into public.fixture_delivery_odds
              (fixture_id, bookmaker_id, market_key, selection_key, participant_type,
               participant_id, line, line_key, price_decimal, price_american,
               source_last_updated_at, observed_at)
            values %s
            on conflict (fixture_id, bookmaker_id, market_key, selection_key,
                         participant_type, participant_id, line_key) do update set
              line = excluded.line, price_decimal = excluded.price_decimal,
              price_american = excluded.price_american,
              source_last_updated_at = excluded.source_last_updated_at,
              observed_at = excluded.observed_at
            """,
            values,
            page_size=1000,
        )
    return len(values)


def run(args: argparse.Namespace) -> dict[str, Any]:
    start, end = get_dates(args)
    leagues = parse_leagues(args.leagues or os.environ.get("FIXTURE_DELIVERY_LEAGUES"))
    report: dict[str, Any] = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "leagues": leagues,
        "excluded_cups": sorted(EXCLUDED_CUPS),
        "components": {},
    }
    if args.dry_run:
        LOG.info("dry-run: non-cup leagues=%s range=%s..%s", leagues, start, end)
        return report

    conn = connection()
    conn.autocommit = False
    active_component: str | None = None
    active_stats: dict[str, Any] = {}
    try:
        with conn.cursor() as cur:
            # Schedule, settlement, and P3 can overlap on the same VPS.  Hold
            # one session-level lock across all component commits so a second
            # refresh cannot delete a window using a different source snapshot.
            cur.execute("select pg_advisory_lock(hashtextextended('oddssearch.fixture_delivery_v2', 0))")

            active_component = "schedule"
            schedule_run = start_run(cur, "schedule", start, end)
            source = source_fixtures(cur, start, end, leagues)
            valid_schedule, rejected_cups, rejected_hidden, incomplete = classify_source_fixtures(source)
            active_stats = {
                "rows_read": len(source),
                "rows_rejected": len(source) - len(valid_schedule),
                "rows_missing": len(incomplete),
                "rejected_cup_rows": rejected_cups,
                "hidden_rows": rejected_hidden,
                "incomplete_rows": incomplete[:100],
                "source_watermark": max((row["starting_at"] for row in source), default=None),
            }
            if incomplete:
                error = (
                    "eligible fixture metadata incomplete; refusing partial publication: "
                    + json.dumps(incomplete[:20], default=str)
                )
                finish_run(cur, schedule_run, "failed", active_stats, error)
                conn.commit()
                active_component = None
                raise RuntimeError(error)
            schedule_written = upsert_schedule(cur, valid_schedule, start, end)
            validate_schedule_projection(cur, valid_schedule)
            validate_schedule_completeness(cur, valid_schedule, start, end)
            active_stats["rows_written"] = schedule_written
            finish_run(cur, schedule_run, "succeeded", active_stats)
            conn.commit()
            active_component = None
            report["components"]["schedule"] = active_stats

            completed = all_completed_fixtures(cur, leagues)
            standings = compute_standings(completed)
            active_component = "standings"
            standings_run = start_run(cur, "standings", start, end)
            active_stats = {"rows_read": len(completed)}
            standings_written = write_standings(cur, standings, completed)
            active_stats["rows_written"] = standings_written
            finish_run(cur, standings_run, "succeeded", active_stats)
            conn.commit()
            active_component = None
            report["components"]["standings"] = active_stats

            active_component = "metrics"
            metrics_run = start_run(cur, "metrics", start, end)
            active_stats = {"rows_read": len(completed)}
            metrics_written, metrics_coverage = write_metrics(cur, valid_schedule, completed, standings)
            active_stats.update({"rows_written": metrics_written, "rows_missing": metrics_coverage["venue_none"], "coverage": metrics_coverage})
            finish_run(cur, metrics_run, "succeeded", active_stats)
            conn.commit()
            active_component = None
            report["components"]["metrics"] = active_stats

            active_component = "odds"
            odds_run = start_run(cur, "odds", start, end)
            active_stats = {}
            odds_source = source_odds(cur, [int(row["id"]) for row in valid_schedule])
            odds_written = write_odds(cur, odds_source)
            active_stats.update({"rows_read": len(odds_source), "rows_written": odds_written})
            finish_run(cur, odds_run, "succeeded", active_stats)
            conn.commit()
            active_component = None
            report["components"]["odds"] = active_stats
    except Exception as error:
        conn.rollback()
        if active_component:
            try:
                with conn.cursor() as failure_cur:
                    failed_run = start_run(failure_cur, active_component, start, end)
                    finish_run(failure_cur, failed_run, "failed", active_stats, str(error))
                conn.commit()
            except Exception:
                conn.rollback()
                LOG.exception("unable to persist failed fixture delivery run")
        LOG.exception("fixtures delivery refresh failed")
        raise
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("select pg_advisory_unlock(hashtextextended('oddssearch.fixture_delivery_v2', 0))")
        except Exception:
            LOG.exception("unable to release fixture delivery advisory lock")
        conn.close()
    return report


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    report = run(args)
    if args.report_out:
        with open(args.report_out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, default=str)
    LOG.info("fixtures delivery refresh complete: %s", json.dumps(report, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
