from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import Settings, get_settings
from .utils import (
    alias_similarity,
    normalize_text,
    recency_weighted_average,
    recency_weighted_per90,
    safe_ratio,
    venue_average,
    venue_per90,
)


MARKET_TYPE_TO_TYPE_ID = {
    "shots": 42,
    "onTargetScoringAttempt": 86,
}


@dataclass
class MatchedTarget:
    public_group_key: str
    public_player_name: str
    public_team_name: str
    public_home_team_name: str
    public_away_team_name: str
    public_market_type: str
    public_line: float
    public_match_timestamp: int
    public_model_name: str
    public_model_fair_over_odds: float | None
    public_model_fair_under_odds: float | None
    public_best_over_odds: float | None
    public_best_over_edge: float | None
    fixture_id: int
    fixture_timestamp: datetime
    league_id: int
    season_id: int | None
    player_id: int
    player_name_db: str
    team_id: int
    opponent_team_id: int
    team_is_home: bool
    player_name_score: float


@dataclass
class UpcomingCandidate:
    market_key: str
    public_market_type: str
    fixture_id: int
    fixture_timestamp: datetime
    league_id: int
    season_id: int | None
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    player_id: int
    player_name_db: str
    selection_key: str
    line: float
    best_over_odds: float
    team_id: int
    opponent_team_id: int
    team_is_home: bool
    resolution_source: str


@contextmanager
def readonly_connection(settings: Settings | None = None):
    settings = settings or get_settings()
    conn = psycopg2.connect(settings.supabase_db_url, sslmode="require")
    conn.set_session(readonly=True, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def _fetch_all(cur, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    cur.execute(query, params or ())
    return list(cur.fetchall())


def count_hits(values: Sequence[Any] | None, threshold: float) -> int:
    if not values:
        return 0
    hits = 0
    for value in values:
        if value is None:
            continue
        if float(value) >= threshold:
            hits += 1
    return hits


def load_fixtures_in_window(cur, min_ts: int, max_ts: int) -> list[dict[str, Any]]:
    min_dt = datetime.fromtimestamp(min_ts, tz=timezone.utc) - timedelta(days=1)
    max_dt = datetime.fromtimestamp(max_ts, tz=timezone.utc) + timedelta(days=1)
    return _fetch_all(
        cur,
        """
        select
          f.id,
          f.league_id,
          f.season_id,
          f.starting_at,
          f.home_team_id,
          f.away_team_id,
          ht.name as home_team_name,
          at.name as away_team_name
        from public.fixtures f
        join public.teams ht on ht.id = f.home_team_id
        join public.teams at on at.id = f.away_team_id
        where f.starting_at between %s and %s
        order by f.starting_at asc, f.id asc
        """,
        (min_dt, max_dt),
    )


def load_fixture_players(cur, fixture_ids: Sequence[int]) -> list[dict[str, Any]]:
    return _fetch_all(
        cur,
        """
        select
          fp.fixture_id,
          fp.player_id,
          fp.team_id,
          p.name,
          p.common_name,
          p.short_name,
          p.display_name,
          t.name as team_name
        from public.fixture_players fp
        join public.players p on p.id = fp.player_id
        left join public.teams t on t.id = fp.team_id
        where fp.fixture_id = any(%s)
        """,
        (list(fixture_ids),),
    )


def load_team_rosters(cur, team_ids: Sequence[int]) -> list[dict[str, Any]]:
    return _fetch_all(
        cur,
        """
        select
          p.id as player_id,
          p.team_id,
          p.name,
          p.common_name,
          p.short_name,
          p.display_name,
          t.name as team_name
        from public.players p
        left join public.teams t on t.id = p.team_id
        where p.team_id = any(%s)
        """,
        (list(team_ids),),
    )


def match_public_targets(cur, public_targets: Sequence[Any]) -> list[MatchedTarget]:
    fixtures = load_fixtures_in_window(
        cur,
        min(target.public_match_timestamp for target in public_targets),
        max(target.public_match_timestamp for target in public_targets),
    )
    fixture_index: dict[tuple[int, str, str], list[dict[str, Any]]] = {}
    for fixture in fixtures:
        ts = int(fixture["starting_at"].replace(tzinfo=timezone.utc).timestamp())
        key = (
            ts,
            normalize_text(fixture["home_team_name"]),
            normalize_text(fixture["away_team_name"]),
        )
        fixture_index.setdefault(key, []).append(fixture)

    matched_fixtures: dict[str, dict[str, Any]] = {}
    for target in public_targets:
        key = (
            target.public_match_timestamp,
            normalize_text(target.public_home_team_name),
            normalize_text(target.public_away_team_name),
        )
        candidates = fixture_index.get(key, [])
        if len(candidates) == 1:
            matched_fixtures[target.public_group_key] = candidates[0]
        elif candidates:
            matched_fixtures[target.public_group_key] = candidates[0]

    fixture_players = load_fixture_players(cur, [fixture["id"] for fixture in matched_fixtures.values()])
    players_by_fixture: dict[int, list[dict[str, Any]]] = {}
    for row in fixture_players:
        players_by_fixture.setdefault(row["fixture_id"], []).append(row)
    team_ids = sorted(
        {
            int(fixture["home_team_id"])
            for fixture in matched_fixtures.values()
        }
        | {
            int(fixture["away_team_id"])
            for fixture in matched_fixtures.values()
        }
    )
    roster_rows = load_team_rosters(cur, team_ids)
    players_by_team: dict[int, list[dict[str, Any]]] = {}
    for row in roster_rows:
        players_by_team.setdefault(int(row["team_id"]), []).append(row)

    matched_targets: list[MatchedTarget] = []
    for target in public_targets:
        fixture = matched_fixtures.get(target.public_group_key)
        if not fixture:
            continue
        fixture_player_rows = players_by_fixture.get(fixture["id"], [])
        public_team_norm = normalize_text(target.public_team_name)
        team_candidates = [
            row for row in fixture_player_rows if normalize_text(row["team_name"]) == public_team_norm
        ] or fixture_player_rows
        if not team_candidates:
            fallback_team_id = (
                int(fixture["home_team_id"])
                if normalize_text(fixture["home_team_name"]) == public_team_norm
                else int(fixture["away_team_id"])
            )
            team_candidates = players_by_team.get(fallback_team_id, [])
        player_match = None
        best_score = 0.0
        for row in team_candidates:
            aliases = [row["name"], row["common_name"], row["short_name"], row["display_name"]]
            score = max(alias_similarity(target.public_player_name, alias) for alias in aliases if alias)
            if score > best_score:
                best_score = score
                player_match = row
        if not player_match or best_score < 0.72:
            continue
        team_is_home = int(player_match["team_id"]) == int(fixture["home_team_id"])
        opponent_team_id = int(fixture["away_team_id"] if team_is_home else fixture["home_team_id"])
        matched_targets.append(
            MatchedTarget(
                public_group_key=target.public_group_key,
                public_player_name=target.public_player_name,
                public_team_name=target.public_team_name,
                public_home_team_name=target.public_home_team_name,
                public_away_team_name=target.public_away_team_name,
                public_market_type=target.public_market_type,
                public_line=target.public_line,
                public_match_timestamp=target.public_match_timestamp,
                public_model_name=target.public_model_name,
                public_model_fair_over_odds=target.public_model_fair_over_odds,
                public_model_fair_under_odds=target.public_model_fair_under_odds,
                public_best_over_odds=target.public_best_over_odds,
                public_best_over_edge=target.public_best_over_edge,
                fixture_id=int(fixture["id"]),
                fixture_timestamp=fixture["starting_at"],
                league_id=int(fixture["league_id"]),
                season_id=int(fixture["season_id"]) if fixture["season_id"] is not None else None,
                player_id=int(player_match["player_id"]),
                player_name_db=player_match["name"],
                team_id=int(player_match["team_id"]),
                opponent_team_id=opponent_team_id,
                team_is_home=team_is_home,
                player_name_score=best_score,
            )
        )
    return matched_targets


def fetch_player_window(
    cur,
    league_id: int,
    player_id: int,
    type_id: int,
    player_window: int,
    cutoff_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    rows = _fetch_all(
        cur,
        """
        with last_n as (
          select
            fp.fixture_id,
            f.starting_at,
            coalesce(fp.minutes_played, 90)::numeric as minutes_played,
            (fp.team_id = f.home_team_id) as team_is_home
          from public.fixture_players fp
          join public.fixtures f on f.id = fp.fixture_id
          where fp.player_id = %s
            and f.league_id = %s
            and f.home_score is not null
            and f.away_score is not null
            and fp.is_starter is true
            and (%s is null or f.starting_at < %s)
          order by f.starting_at desc, f.id desc
          limit %s
        ),
        stats as (
          select
            ln.fixture_id,
            ln.starting_at,
            ln.minutes_played,
            ln.team_is_home,
            coalesce(max(fps.value), 0)::numeric as stat_value
          from last_n ln
          left join public.fixture_player_statistics fps
            on fps.fixture_id = ln.fixture_id
           and fps.player_id = %s
           and fps.type_id = %s
          group by ln.fixture_id, ln.starting_at, ln.minutes_played, ln.team_is_home
        )
        select
          count(*)::int as games,
          sum(stat_value)::numeric as total_value,
          avg(stat_value)::numeric as avg_value,
          array_agg(stat_value order by starting_at desc, fixture_id desc)::numeric[] as last_values,
          array_agg(minutes_played order by starting_at desc, fixture_id desc)::numeric[] as last_minutes,
          array_agg(team_is_home order by starting_at desc, fixture_id desc)::boolean[] as last_is_home,
          sum(minutes_played)::numeric as total_minutes,
          avg(minutes_played)::numeric as avg_minutes,
          case
            when sum(minutes_played) > 0 then (sum(stat_value) * 90.0 / sum(minutes_played))::numeric
            else null
          end as per90_value
        from stats
        having count(*) >= %s
        """,
        (
            player_id,
            league_id,
            cutoff_timestamp,
            cutoff_timestamp,
            player_window,
            player_id,
            type_id,
            get_settings().minimum_matches,
        ),
    )
    return rows[0] if rows else None


def fetch_team_conceded_window(
    cur,
    league_id: int,
    type_id: int,
    window: int,
    opponent_team_id: int,
    opponent_is_home: bool,
    threshold: float,
    cutoff_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    rows = _fetch_all(
        cur,
        """
        with appearances as (
          select
            f.id as fixture_id,
            f.starting_at,
            f.home_team_id as team_id,
            f.away_team_id as opponent_team_id,
            true as is_home
          from public.fixtures f
          where f.league_id = %s
            and f.home_score is not null
            and f.away_score is not null
            and f.home_team_id = %s
            and (%s is null or f.starting_at < %s)
          union all
          select
            f.id as fixture_id,
            f.starting_at,
            f.away_team_id as team_id,
            f.home_team_id as opponent_team_id,
            false as is_home
          from public.fixtures f
          where f.league_id = %s
            and f.home_score is not null
            and f.away_score is not null
            and f.away_team_id = %s
            and (%s is null or f.starting_at < %s)
        ),
        filtered as (
          select
            fixture_id,
            starting_at,
            opponent_team_id
          from appearances
          where is_home = %s
          order by starting_at desc, fixture_id desc
          limit %s
        ),
        conceded as (
          select
            filtered.fixture_id,
            filtered.starting_at,
            coalesce(sum(case when fp.team_id = filtered.opponent_team_id then fps.value::numeric else 0 end), 0)::numeric as stat_value
          from filtered
          left join public.fixture_players fp on fp.fixture_id = filtered.fixture_id
          left join public.fixture_player_statistics fps
            on fps.fixture_id = filtered.fixture_id
           and fps.player_id = fp.player_id
           and fps.type_id = %s
          group by filtered.fixture_id, filtered.starting_at
        )
        select
          count(*)::int as games,
          avg(stat_value)::numeric as avg_value,
          sum(case when stat_value >= %s then 1 else 0 end)::int as games_hit
        from conceded
        having count(*) >= %s
        """,
        (
            league_id,
            opponent_team_id,
            cutoff_timestamp,
            cutoff_timestamp,
            league_id,
            opponent_team_id,
            cutoff_timestamp,
            cutoff_timestamp,
            opponent_is_home,
            window,
            type_id,
            threshold,
            get_settings().minimum_matches,
        ),
    )
    row = rows[0] if rows else None
    if not row:
        return None
    games = int(row["games"])
    games_hit = int(row["games_hit"] or 0)
    return {
        "games": games,
        "avg_value": float(row["avg_value"]) if row["avg_value"] is not None else None,
        "games_hit": games_hit,
        "hit_rate": (games_hit / games) if games else None,
    }


def fetch_league_conceded_average(
    cur,
    league_id: int,
    type_id: int,
    window: int,
    opponent_is_home: bool,
    threshold: float,
    cutoff_timestamp: datetime | None = None,
) -> dict[str, float] | None:
    rows = _fetch_all(
        cur,
        """
        with appearances as (
          select
            f.id as fixture_id,
            f.starting_at,
            f.home_team_id as team_id,
            f.away_team_id as opponent_team_id,
            true as is_home
          from public.fixtures f
          where f.league_id = %s
            and f.home_score is not null
            and f.away_score is not null
            and (%s is null or f.starting_at < %s)
          union all
          select
            f.id as fixture_id,
            f.starting_at,
            f.away_team_id as team_id,
            f.home_team_id as opponent_team_id,
            false as is_home
          from public.fixtures f
          where f.league_id = %s
            and f.home_score is not null
            and f.away_score is not null
            and (%s is null or f.starting_at < %s)
        ),
        ranked as (
          select
            team_id,
            fixture_id,
            starting_at,
            opponent_team_id,
            row_number() over (partition by team_id order by starting_at desc, fixture_id desc) as rn
          from appearances
          where is_home = %s
        ),
        filtered as (
          select *
          from ranked
          where rn <= %s
        ),
        conceded as (
          select
            filtered.team_id,
            filtered.fixture_id,
            filtered.starting_at,
            coalesce(sum(case when fp.team_id = filtered.opponent_team_id then fps.value::numeric else 0 end), 0)::numeric as stat_value
          from filtered
          left join public.fixture_players fp on fp.fixture_id = filtered.fixture_id
          left join public.fixture_player_statistics fps
            on fps.fixture_id = filtered.fixture_id
           and fps.player_id = fp.player_id
           and fps.type_id = %s
          group by filtered.team_id, filtered.fixture_id, filtered.starting_at
        ),
        agg as (
          select
            team_id,
            count(*)::int as games,
            avg(stat_value)::numeric as avg_value,
            sum(case when stat_value >= %s then 1 else 0 end)::int as games_hit
          from conceded
          group by team_id
          having count(*) >= %s
        )
        select
          avg(avg_value)::numeric as mean_avg_value,
          avg((games_hit::numeric / nullif(games, 0)))::numeric as mean_hit_rate
        from agg
        """,
        (
            league_id,
            cutoff_timestamp,
            cutoff_timestamp,
            league_id,
            cutoff_timestamp,
            cutoff_timestamp,
            opponent_is_home,
            window,
            type_id,
            threshold,
            get_settings().minimum_matches,
        ),
    )
    row = rows[0] if rows else None
    if not row or row["mean_avg_value"] is None:
        return None
    return {
        "mean_avg_value": float(row["mean_avg_value"]),
        "mean_hit_rate": float(row["mean_hit_rate"]) if row["mean_hit_rate"] is not None else None,
    }


def build_feature_rows(
    cur,
    matched_targets: Sequence[MatchedTarget],
    settings: Settings | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    player_cache: dict[tuple[int, int, int, int, datetime], dict[str, Any] | None] = {}
    opponent_cache: dict[tuple[int, int, int, int, bool, float, datetime], dict[str, Any] | None] = {}
    league_cache: dict[tuple[int, int, int, bool, float, datetime], dict[str, float] | None] = {}
    rows: list[dict[str, Any]] = []

    for index, target in enumerate(matched_targets, start=1):
        type_id = MARKET_TYPE_TO_TYPE_ID.get(target.public_market_type)
        if not type_id:
            continue
        cutoff_timestamp = target.fixture_timestamp
        player_windows: dict[int, dict[str, Any]] = {}
        for player_window in settings.player_windows:
            player_key = (target.league_id, type_id, target.player_id, player_window, cutoff_timestamp)
            if player_key not in player_cache:
                player_cache[player_key] = fetch_player_window(
                    cur,
                    target.league_id,
                    target.player_id,
                    type_id,
                    player_window,
                    cutoff_timestamp=cutoff_timestamp,
                )
            player_window_row = player_cache[player_key]
            if player_window_row:
                player_windows[player_window] = player_window_row
        if settings.player_window not in player_windows:
            continue

        feature_row: dict[str, Any] = {
            "public_group_key": target.public_group_key,
            "public_player_name": target.public_player_name,
            "player_name_db": target.player_name_db,
            "player_name_score": target.player_name_score,
            "public_model_name": target.public_model_name,
            "public_market_type": target.public_market_type,
            "public_line": target.public_line,
            "public_match_timestamp": target.public_match_timestamp,
            "fixture_id": target.fixture_id,
            "league_id": target.league_id,
            "season_id": target.season_id,
            "player_id": target.player_id,
            "team_id": target.team_id,
            "opponent_team_id": target.opponent_team_id,
            "team_is_home": target.team_is_home,
            "target_fair_over_odds": target.public_model_fair_over_odds,
            "target_fair_under_odds": target.public_model_fair_under_odds,
            "target_best_over_odds": target.public_best_over_odds,
            "target_best_over_edge": target.public_best_over_edge,
        }
        for player_window, player_window_row in player_windows.items():
            player_last_values = [float(value) for value in (player_window_row.get("last_values") or [])]
            player_last_minutes = [float(value) for value in (player_window_row.get("last_minutes") or [])]
            player_last_is_home = [bool(value) for value in (player_window_row.get("last_is_home") or [])]
            player_games = int(player_window_row["games"])
            player_hits = count_hits(player_last_values, target.public_line)
            total_minutes = float(player_window_row["total_minutes"]) if player_window_row.get("total_minutes") is not None else None
            avg_minutes = float(player_window_row["avg_minutes"]) if player_window_row.get("avg_minutes") is not None else None
            per90_value = float(player_window_row["per90_value"]) if player_window_row.get("per90_value") is not None else None
            rec_avg_value = recency_weighted_average(player_last_values)
            rec_per90_value = recency_weighted_per90(player_last_values, player_last_minutes)
            venue_avg_value = venue_average(player_last_values, player_last_is_home, target.team_is_home, float(player_window_row["avg_value"]))
            venue_per90_value = venue_per90(player_last_values, player_last_minutes, player_last_is_home, target.team_is_home, per90_value)
            feature_row[f"player_avg_{player_window}"] = float(player_window_row["avg_value"])
            feature_row[f"player_total_{player_window}"] = float(player_window_row["total_value"])
            feature_row[f"player_games_{player_window}"] = player_games
            feature_row[f"player_hits_{player_window}"] = player_hits
            feature_row[f"player_hit_rate_{player_window}"] = (player_hits / player_games) if player_games else None
            feature_row[f"player_total_minutes_{player_window}"] = total_minutes
            feature_row[f"player_avg_minutes_{player_window}"] = avg_minutes
            feature_row[f"player_per90_{player_window}"] = per90_value
            feature_row[f"player_rec_avg_{player_window}"] = rec_avg_value
            feature_row[f"player_rec_per90_{player_window}"] = rec_per90_value
            feature_row[f"player_venue_avg_{player_window}"] = venue_avg_value
            feature_row[f"player_venue_per90_{player_window}"] = venue_per90_value
        opponent_is_home = not target.team_is_home
        threshold_key = round(target.public_line, 3)
        for window in settings.opponent_windows:
            opponent_key = (
                target.league_id,
                type_id,
                target.opponent_team_id,
                window,
                opponent_is_home,
                threshold_key,
                cutoff_timestamp,
            )
            if opponent_key not in opponent_cache:
                opponent_cache[opponent_key] = fetch_team_conceded_window(
                    cur,
                    target.league_id,
                    type_id,
                    window,
                    target.opponent_team_id,
                    opponent_is_home,
                    target.public_line,
                    cutoff_timestamp=cutoff_timestamp,
                )
            league_key = (
                target.league_id,
                type_id,
                window,
                opponent_is_home,
                threshold_key,
                cutoff_timestamp,
            )
            if league_key not in league_cache:
                league_cache[league_key] = fetch_league_conceded_average(
                    cur,
                    target.league_id,
                    type_id,
                    window,
                    opponent_is_home,
                    target.public_line,
                    cutoff_timestamp=cutoff_timestamp,
                )
            opponent_row = opponent_cache[opponent_key]
            league_row = league_cache[league_key]
            opponent_avg = opponent_row["avg_value"] if opponent_row and opponent_row["avg_value"] is not None else None
            league_avg = league_row["mean_avg_value"] if league_row else None
            opponent_hit_rate = opponent_row["hit_rate"] if opponent_row else None
            league_hit_rate = league_row["mean_hit_rate"] if league_row else None
            feature_row[f"opp_avg_{window}"] = opponent_avg
            feature_row[f"league_opp_avg_{window}"] = league_avg
            feature_row[f"opp_ratio_{window}"] = safe_ratio(opponent_avg, league_avg)
            feature_row[f"opp_hit_rate_{window}"] = opponent_hit_rate
            feature_row[f"league_opp_hit_rate_{window}"] = league_hit_rate
            feature_row[f"opp_hit_ratio_{window}"] = safe_ratio(opponent_hit_rate, league_hit_rate)
        rows.append(feature_row)
        if verbose and index % 25 == 0:
            print(f"feature_rows_progress={index}/{len(matched_targets)}", flush=True)
    return rows


def load_upcoming_over_candidates(
    cur,
    days: int,
    bookmaker_id: int = 2,
    league_ids: Sequence[int] | None = None,
) -> list[UpcomingCandidate]:
    params: list[Any] = [days, bookmaker_id]
    league_filter = ""
    if league_ids:
        league_filter = "and f.league_id = any(%s)"
        params.append(list(league_ids))
    rows = _fetch_all(
        cur,
        f"""
        with priced as (
          select
            o.market_key,
            o.fixture_id,
            o.participant_id as player_id,
            o.selection_key,
            o.line,
            max(o.price_decimal)::numeric as best_over_odds
          from public.odds_outcomes o
          join public.fixtures f on f.id = o.fixture_id
          where f.starting_at >= now()
            and f.starting_at < (now() + (%s || ' days')::interval)
            and o.bookmaker_id = %s
            and o.market_key in ('player_shots', 'player_shots_on_target')
            and o.selection_key like '%%over'
            and o.participant_id is not null
            and o.line is not null
            and o.price_decimal is not null
            {league_filter}
          group by 1,2,3,4,5
        )
        select
          priced.market_key,
          priced.fixture_id,
          f.starting_at,
          f.league_id,
          f.season_id,
          f.home_team_id,
          f.away_team_id,
          ht.name as home_team_name,
          at.name as away_team_name,
          priced.player_id,
          coalesce(p.display_name, p.common_name, p.name) as player_name_db,
          priced.selection_key,
          priced.line,
          priced.best_over_odds
        from priced
        join public.fixtures f on f.id = priced.fixture_id
        join public.teams ht on ht.id = f.home_team_id
        join public.teams at on at.id = f.away_team_id
        left join public.players p on p.id = priced.player_id
        order by f.starting_at asc, priced.market_key, priced.best_over_odds desc
        """,
        params,
    )
    fixture_ids = [int(row["fixture_id"]) for row in rows]
    player_ids = [int(row["player_id"]) for row in rows]
    if not rows:
        return []

    fixture_player_rows = _fetch_all(
        cur,
        """
        select fixture_id, player_id, team_id
        from public.fixture_players
        where fixture_id = any(%s)
          and player_id = any(%s)
        """,
        (fixture_ids, player_ids),
    )
    fixture_player_map = {
        (int(row["fixture_id"]), int(row["player_id"])): int(row["team_id"]) for row in fixture_player_rows
    }

    player_rows = _fetch_all(
        cur,
        """
        select id, team_id
        from public.players
        where id = any(%s)
        """,
        (player_ids,),
    )
    players_team_map = {int(row["id"]): int(row["team_id"]) if row["team_id"] is not None else None for row in player_rows}

    likely_rows = _fetch_all(
        cur,
        """
        select team_id, player_id, confidence
        from public.team_likely_players
        where player_id = any(%s)
        """,
        (player_ids,),
    )
    likely_by_player: dict[int, list[dict[str, Any]]] = {}
    for row in likely_rows:
        likely_by_player.setdefault(int(row["player_id"]), []).append(row)

    history_rows = _fetch_all(
        cur,
        """
        select player_id, team_id, effective_from, effective_to
        from public.player_team_history
        where player_id = any(%s)
        order by player_id, effective_from desc
        """,
        (player_ids,),
    )
    history_by_player: dict[int, list[dict[str, Any]]] = {}
    for row in history_rows:
        history_by_player.setdefault(int(row["player_id"]), []).append(row)

    resolved: list[UpcomingCandidate] = []
    market_map = {
        "player_shots": "shots",
        "player_shots_on_target": "onTargetScoringAttempt",
    }
    for row in rows:
        fixture_id = int(row["fixture_id"])
        player_id = int(row["player_id"])
        home_team_id = int(row["home_team_id"])
        away_team_id = int(row["away_team_id"])
        team_id: int | None = None
        resolution_source = ""

        fixture_key = (fixture_id, player_id)
        if fixture_key in fixture_player_map:
            team_id = fixture_player_map[fixture_key]
            resolution_source = "fixture_players"
        else:
            for hist in history_by_player.get(player_id, []):
                effective_from = hist["effective_from"]
                effective_to = hist["effective_to"]
                if effective_from and effective_from <= row["starting_at"] and (effective_to is None or row["starting_at"] < effective_to):
                    candidate_team_id = int(hist["team_id"])
                    if candidate_team_id in (home_team_id, away_team_id):
                        team_id = candidate_team_id
                        resolution_source = "player_team_history"
                        break
        if team_id is None:
            likely = [
                candidate
                for candidate in likely_by_player.get(player_id, [])
                if int(candidate["team_id"]) in (home_team_id, away_team_id)
            ]
            if likely:
                likely.sort(key=lambda candidate: float(candidate["confidence"] or 0), reverse=True)
                team_id = int(likely[0]["team_id"])
                resolution_source = "team_likely_players"
        if team_id is None:
            player_team_id = players_team_map.get(player_id)
            if player_team_id in (home_team_id, away_team_id):
                team_id = int(player_team_id)
                resolution_source = "players.team_id"
        if team_id is None:
            continue

        team_is_home = team_id == home_team_id
        resolved.append(
            UpcomingCandidate(
                market_key=row["market_key"],
                public_market_type=market_map[row["market_key"]],
                fixture_id=fixture_id,
                fixture_timestamp=row["starting_at"],
                league_id=int(row["league_id"]),
                season_id=int(row["season_id"]) if row["season_id"] is not None else None,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                home_team_name=row["home_team_name"],
                away_team_name=row["away_team_name"],
                player_id=player_id,
                player_name_db=row["player_name_db"] or str(player_id),
                selection_key=row["selection_key"],
                line=float(row["line"]),
                best_over_odds=float(row["best_over_odds"]),
                team_id=team_id,
                opponent_team_id=away_team_id if team_is_home else home_team_id,
                team_is_home=team_is_home,
                resolution_source=resolution_source,
            )
        )
    return resolved


def build_upcoming_feature_rows(
    cur,
    candidates: Sequence[UpcomingCandidate],
    settings: Settings | None = None,
    cutoff_timestamp: datetime | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    shared_cutoff_timestamp = cutoff_timestamp or datetime.now(timezone.utc)
    player_cache: dict[tuple[int, int, int, int, datetime], dict[str, Any] | None] = {}
    opponent_cache: dict[tuple[int, int, int, int, bool, float, datetime], dict[str, Any] | None] = {}
    league_cache: dict[tuple[int, int, int, bool, float, datetime], dict[str, float] | None] = {}
    rows: list[dict[str, Any]] = []

    for index, candidate in enumerate(candidates, start=1):
        type_id = MARKET_TYPE_TO_TYPE_ID.get(candidate.public_market_type)
        if not type_id:
            continue
        player_windows: dict[int, dict[str, Any]] = {}
        for player_window in settings.player_windows:
            player_key = (candidate.league_id, type_id, candidate.player_id, player_window, shared_cutoff_timestamp)
            if player_key not in player_cache:
                player_cache[player_key] = fetch_player_window(
                    cur,
                    candidate.league_id,
                    candidate.player_id,
                    type_id,
                    player_window,
                    cutoff_timestamp=shared_cutoff_timestamp,
                )
            player_window_row = player_cache[player_key]
            if player_window_row:
                player_windows[player_window] = player_window_row
        if settings.player_window not in player_windows:
            continue
        row: dict[str, Any] = {
            "market_key": candidate.market_key,
            "public_market_type": candidate.public_market_type,
            "fixture_id": candidate.fixture_id,
            "fixture_timestamp": candidate.fixture_timestamp,
            "league_id": candidate.league_id,
            "season_id": candidate.season_id,
            "home_team_name": candidate.home_team_name,
            "away_team_name": candidate.away_team_name,
            "player_id": candidate.player_id,
            "player_name_db": candidate.player_name_db,
            "selection_key": candidate.selection_key,
            "public_line": candidate.line,
            "best_over_odds": candidate.best_over_odds,
            "team_id": candidate.team_id,
            "opponent_team_id": candidate.opponent_team_id,
            "team_is_home": candidate.team_is_home,
            "resolution_source": candidate.resolution_source,
        }
        for player_window, player_window_row in player_windows.items():
            player_last_values = [float(value) for value in (player_window_row.get("last_values") or [])]
            player_last_minutes = [float(value) for value in (player_window_row.get("last_minutes") or [])]
            player_last_is_home = [bool(value) for value in (player_window_row.get("last_is_home") or [])]
            player_games = int(player_window_row["games"])
            player_hits = count_hits(player_last_values, candidate.line)
            total_minutes = float(player_window_row["total_minutes"]) if player_window_row.get("total_minutes") is not None else None
            avg_minutes = float(player_window_row["avg_minutes"]) if player_window_row.get("avg_minutes") is not None else None
            per90_value = float(player_window_row["per90_value"]) if player_window_row.get("per90_value") is not None else None
            rec_avg_value = recency_weighted_average(player_last_values)
            rec_per90_value = recency_weighted_per90(player_last_values, player_last_minutes)
            venue_avg_value = venue_average(player_last_values, player_last_is_home, candidate.team_is_home, float(player_window_row["avg_value"]))
            venue_per90_value = venue_per90(player_last_values, player_last_minutes, player_last_is_home, candidate.team_is_home, per90_value)
            row[f"player_avg_{player_window}"] = float(player_window_row["avg_value"])
            row[f"player_total_{player_window}"] = float(player_window_row["total_value"])
            row[f"player_games_{player_window}"] = player_games
            row[f"player_hits_{player_window}"] = player_hits
            row[f"player_hit_rate_{player_window}"] = (player_hits / player_games) if player_games else None
            row[f"player_total_minutes_{player_window}"] = total_minutes
            row[f"player_avg_minutes_{player_window}"] = avg_minutes
            row[f"player_per90_{player_window}"] = per90_value
            row[f"player_rec_avg_{player_window}"] = rec_avg_value
            row[f"player_rec_per90_{player_window}"] = rec_per90_value
            row[f"player_venue_avg_{player_window}"] = venue_avg_value
            row[f"player_venue_per90_{player_window}"] = venue_per90_value
        opponent_is_home = not candidate.team_is_home
        threshold_key = round(candidate.line, 3)
        for window in settings.opponent_windows:
            opponent_key = (
                candidate.league_id,
                type_id,
                candidate.opponent_team_id,
                window,
                opponent_is_home,
                threshold_key,
                shared_cutoff_timestamp,
            )
            if opponent_key not in opponent_cache:
                opponent_cache[opponent_key] = fetch_team_conceded_window(
                    cur,
                    candidate.league_id,
                    type_id,
                    window,
                    candidate.opponent_team_id,
                    opponent_is_home,
                    candidate.line,
                    cutoff_timestamp=shared_cutoff_timestamp,
                )
            league_key = (
                candidate.league_id,
                type_id,
                window,
                opponent_is_home,
                threshold_key,
                shared_cutoff_timestamp,
            )
            if league_key not in league_cache:
                league_cache[league_key] = fetch_league_conceded_average(
                    cur,
                    candidate.league_id,
                    type_id,
                    window,
                    opponent_is_home,
                    candidate.line,
                    cutoff_timestamp=shared_cutoff_timestamp,
                )
            opponent_row = opponent_cache[opponent_key]
            league_row = league_cache[league_key]
            opponent_avg = opponent_row["avg_value"] if opponent_row and opponent_row["avg_value"] is not None else None
            league_avg = league_row["mean_avg_value"] if league_row else None
            opponent_hit_rate = opponent_row["hit_rate"] if opponent_row else None
            league_hit_rate = league_row["mean_hit_rate"] if league_row else None
            row[f"opp_avg_{window}"] = opponent_avg
            row[f"league_opp_avg_{window}"] = league_avg
            row[f"opp_ratio_{window}"] = safe_ratio(opponent_avg, league_avg)
            row[f"opp_hit_rate_{window}"] = opponent_hit_rate
            row[f"league_opp_hit_rate_{window}"] = league_hit_rate
            row[f"opp_hit_ratio_{window}"] = safe_ratio(opponent_hit_rate, league_hit_rate)
        rows.append(row)
        if verbose and index % 500 == 0:
            print(f"upcoming_feature_rows_progress={index}/{len(candidates)}", flush=True)
    return rows
