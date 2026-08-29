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
from uuid import uuid4
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import Json, execute_values


LOG = logging.getLogger("refresh_fixture_delivery")
UTC = timezone.utc
LONDON = ZoneInfo("Europe/London")
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


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def iso_date(value: datetime | date) -> str:
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return aware.astimezone(LONDON).date().isoformat()
    return value.isoformat()


def london_today(moment: datetime | None = None) -> str:
    """Return the current (or supplied) calendar date in the product timezone."""
    instant = moment or datetime.now(UTC)
    aware = instant if instant.tzinfo is not None else instant.replace(tzinfo=UTC)
    return aware.astimezone(LONDON).date().isoformat()


def order_history_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return newest history first with a deterministic fixture-id tie break."""
    return sorted(
        rows,
        key=lambda row: (row["starting_at"], -int(row["id"])),
        reverse=True,
    )


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
    parser.add_argument("--end-date", default=None, help="London date, exclusive (default today + 14 days)")
    parser.add_argument("--leagues", default=None, help="Comma-separated supported league IDs")
    parser.add_argument("--report-out", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def get_dates(args: argparse.Namespace) -> tuple[date, date]:
    start = date.fromisoformat(args.start_date) if args.start_date else date.fromisoformat(london_today())
    end = date.fromisoformat(args.end_date) if args.end_date else start + timedelta(days=14)
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


def start_run(cur, component: str, start: date, end: date, release_id: str | None = None) -> str:
    cur.execute(
        """
        insert into public.fixture_delivery_refresh_runs
          (component, requested_start, requested_end, release_id)
        values (%s, %s, %s, %s)
        returning id
        """,
        (component, start, end, release_id),
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


DELIVERY_COMPONENTS = ("schedule", "standings", "metrics", "odds")


def create_release(cur, start: date, end: date) -> str:
    release_id = str(uuid4())
    cur.execute(
        """
        insert into public.fixture_delivery_releases (id, requested_start, requested_end)
        values (%s, %s, %s)
        """,
        (release_id, start, end),
    )
    return release_id


def lock_refresh_publication(cur) -> None:
    """Serialize refresh builds so an older run cannot publish after a newer run."""
    cur.execute("select pg_advisory_xact_lock(hashtextextended('fixture_delivery_refresh', 0))")


def validate_release_components(
    cur,
    release_id: str,
    schedule_fixture_ids: list[int],
) -> dict[str, int]:
    """Validate component completeness before exposing a release."""
    counts: dict[str, int] = {}
    for component, table in (
        ("schedule", "fixture_delivery_schedule"),
        ("standings", "fixture_delivery_standings"),
        ("metrics", "fixture_delivery_metrics"),
        ("odds", "fixture_delivery_odds"),
    ):
        cur.execute(
            f"select count(*) from public.{table} where release_id = %s",
            (release_id,),
        )
        counts[component] = int(cur.fetchone()[0])

    cur.execute(
        "select fixture_id from public.fixture_delivery_schedule where release_id = %s",
        (release_id,),
    )
    actual_schedule_ids = [int(row[0]) for row in cur.fetchall()]
    if set(actual_schedule_ids) != set(int(value) for value in schedule_fixture_ids) or len(actual_schedule_ids) != len(schedule_fixture_ids):
        raise RuntimeError(
            "fixture delivery schedule identity mismatch: "
            f"expected={sorted(set(schedule_fixture_ids))[:20]} actual={sorted(set(actual_schedule_ids))[:20]}"
        )

    cur.execute(
        """
        select fixture_id, team_id, side, metrics_window, metrics_mode
          from public.fixture_delivery_metrics
         where release_id = %s
        """,
        (release_id,),
    )
    metric_rows = [
        {
            "fixture_id": int(row[0]),
            "team_id": int(row[1]),
            "side": str(row[2]),
            "metrics_window": int(row[3]),
            "metrics_mode": str(row[4]),
        }
        for row in cur.fetchall()
    ]
    cur.execute(
        """
        select fixture_id, home_team_id, away_team_id
          from public.fixture_delivery_schedule
         where release_id = %s
        """,
        (release_id,),
    )
    schedule_teams = {
        int(row[0]): {"home": int(row[1]), "away": int(row[2])}
        for row in cur.fetchall()
    }
    validate_metric_identity_set(schedule_fixture_ids, metric_rows, schedule_teams)
    if counts["metrics"] != len(metric_rows):
        raise RuntimeError(
            f"fixture delivery metrics row count mismatch: expected={len(metric_rows)} actual={counts['metrics']}"
        )

    cur.execute(
        "select distinct fixture_id from public.fixture_delivery_odds where release_id = %s",
        (release_id,),
    )
    odds_fixture_ids = {int(row[0]) for row in cur.fetchall()}
    unknown_odds_ids = odds_fixture_ids - set(int(value) for value in schedule_fixture_ids)
    if unknown_odds_ids:
        raise RuntimeError(
            f"fixture delivery odds identity mismatch: unknown fixture ids={sorted(unknown_odds_ids)[:20]}"
        )
    return counts


def validate_metric_identity_set(
    schedule_fixture_ids: Iterable[int],
    metric_rows: Iterable[dict[str, Any]],
    schedule_teams: dict[int, dict[str, int]] | None = None,
) -> None:
    """Require exactly two sides, eleven windows, both modes, and valid teams."""
    fixture_ids = {int(value) for value in schedule_fixture_ids}
    expected = {
        (
            fixture_id,
            schedule_teams[fixture_id][side] if schedule_teams else None,
            side,
            window,
            mode,
        )
        for fixture_id in fixture_ids
        for side in ("home", "away")
        for window in range(5, 16)
        for mode in ("overall", "venue")
    }
    actual = {
        (
            int(row["fixture_id"]),
            int(row["team_id"]) if schedule_teams else None,
            str(row["side"]),
            int(row["metrics_window"]),
            str(row["metrics_mode"]),
        )
        for row in metric_rows
    }
    if actual != expected:
        missing = sorted(expected - actual, key=str)
        extra = sorted(actual - expected, key=str)
        raise RuntimeError(
            "fixture delivery metrics identity mismatch: "
            f"missing={missing[:20]} extra={extra[:20]}"
        )


def publish_release(
    cur,
    release_id: str,
    counts: dict[str, int],
    source_watermark: datetime | None,
) -> None:
    """Switch the sole customer-visible pointer inside the caller's transaction."""
    cur.execute(
        """
        update public.fixture_delivery_releases
           set status = 'published', published_at = now(),
               health_checked_at = now(),
               pin_expires_at = now() + interval '2 hours',
               source_watermark = %s,
               schedule_rows = %s, standings_rows = %s,
               metrics_rows = %s, odds_rows = %s
         where id = %s and status = 'building'
        """,
        (
            source_watermark,
            counts.get("schedule", 0),
            counts.get("standings", 0),
            counts.get("metrics", 0),
            counts.get("odds", 0),
            release_id,
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError(f"fixture delivery release {release_id} is not buildable")
    cur.execute(
        """
        insert into public.fixture_delivery_current_publication (publication_key, release_id)
        values ('fixtures', %s)
        on conflict (publication_key) do update
          set release_id = excluded.release_id, updated_at = now()
        """,
        (release_id,),
    )


def fail_release(release_id: str, error_message: str) -> None:
    """Persist failure state after the build transaction has rolled back."""
    conn = connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.fixture_delivery_releases
                   set status = 'failed', failed_at = now(), error_message = %s
                 where id = %s and status = 'building'
                """,
                (error_message[:4000], release_id),
            )
        conn.commit()
    finally:
        conn.close()


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
          join public.leagues l on l.id = f.league_id
          join public.teams ht on ht.id = f.home_team_id
          join public.teams at on at.id = f.away_team_id
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
         order by starting_at desc, id asc
        """,
        (leagues, list(EXCLUDED_CUPS)),
    )
    columns = [description[0] for description in cur.description]
    now = datetime.now(UTC)
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    return order_history_rows(row for row in rows if not is_hidden(row) and is_finished(row, now))


def upsert_schedule(cur, rows: list[dict[str, Any]], start: date, end: date, release_id: str) -> int:
    values = [
        (
            release_id,
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
              (release_id, fixture_id, fixture_date, starting_at, status, status_code,
               league_id, season_id, home_team_id, away_team_id, home_score,
               away_score, league_name, league_logo, home_team_name,
               home_team_short_code, home_team_image_path, away_team_name,
               away_team_short_code, away_team_image_path, source_updated_at)
            values %s
            on conflict (release_id, fixture_id) do update set
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
             where release_id = %s and fixture_date >= %s and fixture_date < %s
               and not (fixture_id = any(%s))
            """,
            (release_id, start, end, valid_ids),
        )
    else:
        cur.execute(
            "delete from public.fixture_delivery_schedule where release_id = %s and fixture_date >= %s and fixture_date < %s",
            (release_id, start, end),
        )
    return len(values)


def validate_schedule_projection(cur, rows: list[dict[str, Any]], release_id: str) -> None:
    """Fail the publication if the delivery projection disagrees with source rows."""
    if not rows:
        return
    fixture_ids = [int(row["id"]) for row in rows]
    cur.execute(
        """
        select fixture_id, status, status_code, home_score, away_score
          from public.fixture_delivery_schedule
         where release_id = %s and fixture_id = any(%s)
        """,
        (release_id, fixture_ids),
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


def compute_standings(completed: list[dict[str, Any]]) -> dict[tuple[int, int], dict[int, dict[str, int]]]:
    grouped: dict[tuple[int, int], dict[int, dict[str, int]]] = defaultdict(dict)
    for row in completed:
        if row.get("season_id") is None:
            continue
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
            ),
        )
        for rank, (team_id, values) in enumerate(ranked, start=1):
            values["rank"] = rank
    return grouped


def strict_current_season_rank(
    standings: dict[tuple[int, int], dict[int, dict[str, int]]],
    league_id: int,
    season_id: int,
    team_id: int,
) -> int | None:
    current = standings.get((league_id, season_id), {}).get(team_id)
    return int(current["rank"]) if current else None


def write_standings(cur, standings: dict[tuple[int, int], dict[int, dict[str, int]]], completed: list[dict[str, Any]], release_id: str) -> int:
    latest: dict[tuple[int, int], datetime] = {}
    for row in completed:
        if row.get("season_id") is None:
            continue
        key = (int(row["league_id"]), int(row["season_id"]))
        latest[key] = max(latest.get(key, datetime.min.replace(tzinfo=UTC)), row["starting_at"])
    values = []
    for (league_id, season_id), table in standings.items():
        for team_id, stats in table.items():
            values.append(
                (
                    release_id,
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
              (release_id, league_id, season_id, team_id, rank, points, played,
               goals_for, goals_against, goal_diff, source_max_starting_at)
            values %s
            on conflict (release_id, league_id, season_id, team_id) do update set
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


def build_season_scoped_history(
    completed: list[dict[str, Any]],
) -> dict[tuple[int, int, int], list[dict[str, Any]]]:
    """Index completed fixtures by league, season, and participating team."""
    history: dict[tuple[int, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in completed:
        if row.get("season_id") is None:
            continue
        league_id = int(row["league_id"])
        season_id = int(row["season_id"])
        history[(league_id, season_id, int(row["home_team_id"]))].append(row)
        history[(league_id, season_id, int(row["away_team_id"]))].append(row)
    for key, rows in history.items():
        history[key] = order_history_rows(rows)
    return history


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


def history_rows_for_fixture(
    history: list[dict[str, Any]],
    fixture: dict[str, Any],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Select completed history without leaking an upcoming fixture result."""
    fixture_time = fixture["starting_at"]
    cutoff_inclusive = is_finished(fixture, now or datetime.now(UTC))
    return [
        row
        for row in history
        if row["starting_at"] < fixture_time
        or (cutoff_inclusive and row["starting_at"] == fixture_time)
    ]


def write_metrics(
    cur,
    schedule: list[dict[str, Any]],
    completed: list[dict[str, Any]],
    standings: dict[tuple[int, int], dict[int, dict[str, int]]],
    release_id: str,
) -> tuple[int, dict[str, int]]:
    history_by_team_season = build_season_scoped_history(completed)
    now = datetime.now(UTC)

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
        for side, team_id in (("home", int(fixture["home_team_id"])), ("away", int(fixture["away_team_id"]))):
            history_key = (
                int(fixture["league_id"]),
                int(fixture["season_id"]),
                team_id,
            ) if fixture.get("season_id") is not None else None
            prior = history_rows_for_fixture(
                history_by_team_season.get(history_key, []) if history_key is not None else [], fixture, now
            )
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
                            release_id,
                            fixture["id"], team_id, side, window, mode, Json(metrics),
                            strict_current_season_rank(
                                standings,
                                int(fixture["league_id"]),
                                int(fixture["season_id"]) if fixture.get("season_id") is not None else 0,
                                team_id,
                            ),
                            max_source,
                        )
                    )
                if samples["venue"] == 0 and samples["overall"] > 0:
                    coverage["venue_empty_overall_available"] += 1
    if values:
        execute_values(
            cur,
            """
            insert into public.fixture_delivery_metrics
              (release_id, fixture_id, team_id, side, metrics_window, metrics_mode,
               metrics, league_rank, source_max_starting_at)
            values %s
            on conflict (release_id, fixture_id, team_id, side, metrics_window, metrics_mode) do update set
              metrics = excluded.metrics, league_rank = excluded.league_rank,
              source_max_starting_at = excluded.source_max_starting_at, computed_at = now()
            """,
            values,
            page_size=1000,
        )
    return len(values), coverage


def source_odds(cur, fixture_ids: list[int], release_id: str) -> list[dict[str, Any]]:
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
          join public.fixture_delivery_schedule s
            on s.release_id = %s and s.fixture_id = o.fixture_id
         where o.fixture_id = any(%s)
           and o.bookmaker_id = any(%s)
           and lower(o.market_key) = any(%s)
           and o.price_decimal > 1 and o.price_decimal <= 500
           and s.league_id <> all(%s)
        """,
        (release_id, fixture_ids, list(ACTIVE_BOOKMAKERS), list(MONEYLINE_MARKETS), list(EXCLUDED_CUPS)),
    )
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def write_odds(cur, rows: list[dict[str, Any]], release_id: str) -> int:
    values = []
    for row in rows:
        line = row.get("line")
        line_key = line if line is not None else Decimal("-9999")
        values.append(
            (
                release_id,
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
              (release_id, fixture_id, bookmaker_id, market_key, selection_key, participant_type,
               participant_id, line, line_key, price_decimal, price_american,
               source_last_updated_at, observed_at)
            values %s
            on conflict (release_id, fixture_id, bookmaker_id, market_key, selection_key,
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
    release_id: str | None = None
    try:
        with conn.cursor() as cur:
            release_id = create_release(cur, start, end)
        # Keep the build marker durable so a failed build can be marked failed
        # after its data transaction is rolled back.
        conn.commit()

        with conn.cursor() as cur:
            lock_refresh_publication(cur)
            schedule_run = start_run(cur, "schedule", start, end, release_id)
            source = source_fixtures(cur, start, end, leagues)
            valid_schedule = [row for row in source if row["league_id"] not in EXCLUDED_CUPS and not is_hidden(row)]
            rejected_cups = sum(row["league_id"] in EXCLUDED_CUPS for row in source)
            schedule_written = upsert_schedule(cur, valid_schedule, start, end, release_id)
            validate_schedule_projection(cur, valid_schedule, release_id)
            finish_run(cur, schedule_run, "succeeded", {
                "rows_read": len(source), "rows_written": schedule_written,
                "rows_rejected": len(source) - len(valid_schedule), "rejected_cup_rows": rejected_cups,
                "source_watermark": max((row["starting_at"] for row in source), default=None),
            })
            report["components"]["schedule"] = {"rows_read": len(source), "rows_written": schedule_written, "rejected_cup_rows": rejected_cups}

            completed = all_completed_fixtures(cur, leagues)
            standings = compute_standings(completed)
            standings_run = start_run(cur, "standings", start, end, release_id)
            standings_written = write_standings(cur, standings, completed, release_id)
            finish_run(cur, standings_run, "succeeded", {"rows_read": len(completed), "rows_written": standings_written})
            report["components"]["standings"] = {"rows_read": len(completed), "rows_written": standings_written}

            metrics_run = start_run(cur, "metrics", start, end, release_id)
            metrics_written, metrics_coverage = write_metrics(cur, valid_schedule, completed, standings, release_id)
            finish_run(cur, metrics_run, "succeeded", {
                "rows_read": len(completed),
                "rows_written": metrics_written,
                "rows_missing": metrics_coverage["venue_none"],
                "coverage": metrics_coverage,
            })
            report["components"]["metrics"] = {
                "rows_read": len(completed),
                "rows_written": metrics_written,
                "coverage": metrics_coverage,
            }

            odds_run = start_run(cur, "odds", start, end, release_id)
            odds_source = source_odds(cur, [int(row["id"]) for row in valid_schedule], release_id)
            odds_written = write_odds(cur, odds_source, release_id)
            finish_run(cur, odds_run, "succeeded", {"rows_read": len(odds_source), "rows_written": odds_written})
            report["components"]["odds"] = {"rows_read": len(odds_source), "rows_written": odds_written}
            counts = validate_release_components(cur, release_id, [int(row["id"]) for row in valid_schedule])
            source_watermark = max((row["starting_at"] for row in source), default=None)
            publish_release(cur, release_id, counts, source_watermark)
            cur.execute("select public.fixture_delivery_gc()")
            report["garbage_collected_releases"] = int(cur.fetchone()[0] or 0)
            report["release_id"] = release_id
            report["published"] = True
            conn.commit()
    except Exception as error:
        conn.rollback()
        if release_id is not None:
            try:
                fail_release(release_id, str(error))
            except Exception:
                LOG.exception("could not mark fixture delivery release %s failed", release_id)
        LOG.exception("fixtures delivery refresh failed")
        raise
    finally:
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
