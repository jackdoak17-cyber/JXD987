from __future__ import annotations

import json
from datetime import datetime, date
import re
from typing import List, Dict, Any, Optional, Tuple, Union

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, aliased

from .config import settings
from .db import make_session
from .sync import ensure_player_form_columns
from .models import PlayerForm, TeamForm, Player, Team, Fixture, FixtureParticipant, OddsLatest, League

app = FastAPI(title="JXD Chat", version="0.1")


def get_session() -> Session:
    return make_session(settings.database_url)


@app.on_event("startup")
def _startup_schema():
    # Ensure newer columns exist for older SQLite files.
    session = get_session()
    try:
        ensure_player_form_columns(session)
    finally:
        session.close()


def parse_query(text: str) -> Dict[str, Any]:
    """
    Very light parser for common betting-style phrases.
    Extracts:
      - stat_type (shots, sot, goals, assists)
      - min_value (threshold, default 2 shots)
      - sample_size (default 10)
      - min_pct (default 100%, can be 80% or 4/5)
      - odds_max (optional)
      - require_today (True if mentions today/tonight)
      - require_favorites (True if mentions favorites)
      - exclude_favorites (True if mentions not favorites/underdogs)
    """
    lowered = text.lower()

    def default_threshold(stat: str) -> Union[int, float]:
        if stat in ("goals", "goals_for", "goals_against"):
            return 1
        if stat in ("assists",):
            return 1
        return 2

    def resolve_stat() -> Tuple[str, str]:
        # Returns (entity, stat_key). entity: player|team. stat_key for players: shots,sot,goals,assists. teams: shots_for/against, sot_for/against, goals_for/against, corners_for/against, yellows_for/against, reds_for/against.
        against = any(word in lowered for word in ["concede", "allowed", "against", "allow", "conceded"])
        scored_words = any(w in lowered for w in ["scored", "score", "scoring", "scorer", "scorers"])
        match_total = "match" in lowered and "goal" in lowered
        if "yellow" in lowered:
            return "team", "yellows_against" if against else "yellows_for"
        if "red" in lowered and "card" in lowered:
            return "team", "reds_against" if against else "reds_for"
        if "corner" in lowered:
            return "team", "corners_against" if against else "corners_for"
        if "booking" in lowered or "card" in lowered:
            if "red" in lowered:
                return "team", "reds_against" if against else "reds_for"
            # default to yellows for generic "cards"
            return "team", "yellows_against" if against else "yellows_for"
        if match_total:
            return "team", "match_goals"
        if ("goal" in lowered or scored_words) and "assist" not in lowered:
            if "team" in lowered or against:
                return "team", "goals_against" if against else "goals_for"
            return "player", "goals"
        if "assist" in lowered:
            return "player", "assists"
        if "sot" in lowered or "on target" in lowered:
            if "team" in lowered or against:
                return "team", "sot_against" if against else "sot_for"
            return "player", "sot"
        # shots default
        if "team" in lowered or against:
            return "team", "shots_against" if against else "shots_for"
        return "player", "shots"

    entity, stat_key = resolve_stat()

    # threshold (supports decimals like 2.5+ goals)
    min_value: Union[int, float] = default_threshold(stat_key)
    m = re.search(r"(\d+(?:\.\d+)?)\+?\s*(shot|sot|on target|goal|assist|corner|card)", lowered)
    if m:
        try:
            val = float(m.group(1))
            min_value = int(val) if val.is_integer() else val
        except Exception:
            pass
    # sample size
    sample_size = 10
    m = re.search(r"last\s+(\d+)", lowered)
    if m:
        try:
            sample_size = int(m.group(1))
        except Exception:
            pass
    # percentage or fraction (e.g., 4/5, 80%)
    min_pct = 1.0
    m = re.search(r"(\d+)\s*/\s*(\d+)", lowered)
    if m:
        try:
            num, den = int(m.group(1)), int(m.group(2))
            if den > 0:
                min_pct = num / den
                sample_size = den
        except Exception:
            pass
    m = re.search(r"(\d+)\s*%", lowered)
    if m:
        try:
            pct = int(m.group(1))
            min_pct = pct / 100.0
        except Exception:
            pass
    # odds cap / min
    odds_max: Optional[float] = None
    odds_min: Optional[float] = None
    m = re.search(r"odds\s*<=\s*([0-9]+\.?[0-9]*)", lowered) or re.search(
        r"(less than|under|below)\s*odds\s*([0-9]+\.?[0-9]*)", lowered
    )
    if m:
        try:
            odds_max = float(m.group(len(m.groups())))
        except Exception:
            odds_max = None
    m = (
        re.search(r"odds\s*>=\s*([0-9]+\.?[0-9]*)", lowered)
        or re.search(r"odds\s*>\s*([0-9]+\.?[0-9]*)", lowered)
        or re.search(r"(greater than|over|above|more than)\s*odds\s*([0-9]+\.?[0-9]*)", lowered)
        or re.search(r"odds\s*>\s*([0-9]+\.?[0-9]*)", lowered)
    )
    if m:
        try:
            odds_min = float(m.group(len(m.groups())))
        except Exception:
            odds_min = None
    # fallback loose pattern only if neither bound was set
    if odds_max is None and odds_min is None:
        m = re.search(r"odds[^\d]*([0-9]+\.?[0-9]*)", lowered)
        if m:
            try:
                odds_max = float(m.group(1))
            except Exception:
                odds_max = None
    require_today = "today" in lowered or "tonight" in lowered
    require_fav = any(
        kw in lowered
        for kw in [
            "favorite",
            "favourite",
            "only favorites",
            "favourites only",
            "favs only",
            "keep favorites",
        ]
    )
    exclude_fav = "not favorite" in lowered or "non favorite" in lowered or "exclude favorites" in lowered
    if "remove underdog" in lowered or "remove underdogs" in lowered or "no underdogs" in lowered:
        require_fav = True
        exclude_fav = False
    if "underdog" in lowered and not require_fav:
        exclude_fav = True
    if exclude_fav:
        require_fav = False
    if "remove teams not favorites" in lowered or "remove non favorites" in lowered:
        require_fav = True
        exclude_fav = False
    location = None
    if " at home" in lowered or " home " in lowered or "home only" in lowered or "home games" in lowered:
        location = "home"
    elif " away " in lowered or "away only" in lowered or "away games" in lowered or "away fixtures" in lowered:
        location = "away"
    # entity hints / avg filter
    min_avg = None
    m = re.search(r"average\s*(?:less\s*than|under|below)\s*([0-9]+\.?[0-9]*)", lowered)
    if m:
        try:
            min_avg = float(m.group(1))
        except Exception:
            min_avg = None
    # entity hints
    if "team" in lowered and entity != "team":
        entity = "team"
    explicit_stat = any(
        kw in lowered
        for kw in [
            "shot",
            "sot",
            "on target",
            "goal",
            "assist",
            "corner",
            "card",
            "booking",
        ]
    )
    if require_fav and not explicit_stat:
        entity = "team"
        stat_key = "shots_for"
        min_value = 0
        min_pct = 0.0
        sample_size = min(sample_size, 3)
    if "top scorer" in lowered or "top scorers" in lowered or "leading scorer" in lowered:
        entity = "player"
        stat_key = "goals"
        min_pct = min(min_pct, 0.6)
        sample_size = min(sample_size, 5)
    if "ranked" in lowered or "ranking" in lowered or "rank by" in lowered or "sort by" in lowered:
        min_pct = 0.0
    if "rank by odds" in lowered or "sort by odds" in lowered:
        sort_by = "odds_desc" if "highest" in lowered or "top" in lowered else "odds_asc"
    sort_by = None

    return {
        "entity": entity,
        "stat_key": stat_key,
        "min_value": min_value,
        "sample_size": sample_size,
        "min_pct": min_pct,
        "odds_max": odds_max,
        "odds_min": odds_min,
        "require_today": require_today,
        "require_favorites": require_fav,
        "exclude_favorites": exclude_fav,
        "location": location,
        "sort_by": sort_by,
        "min_avg": min_avg,
    }


def favorite_team_odds(session: Session, odds_cap: Optional[float], days_forward: int = 7) -> Dict[int, float]:
    """
    Return team_id -> favorite decimal odds for upcoming fixtures (market match_result=1).
    Looks from today through days_forward.
    """
    today_date = date.today()
    end_date = date.fromordinal(today_date.toordinal() + max(days_forward, 0))
    rows = (
        session.query(OddsLatest, Fixture)
        .join(Fixture, OddsLatest.fixture_id == Fixture.id)
        .filter(OddsLatest.market_id == 1)
        .filter(Fixture.starting_at != None)  # noqa: E711
        .filter(func.date(Fixture.starting_at) >= today_date)
        .filter(func.date(Fixture.starting_at) <= end_date)
        .all()
    )
    favs: Dict[int, float] = {}
    for odds, fx in rows:
        selection = (odds.selection or "").lower()
        if selection == "draw":
            continue
        team_id = fx.home_team_id if selection == "home" else fx.away_team_id if selection == "away" else None
        if team_id is None:
            continue
        dec = odds.decimal_odds or 999.0
        if odds_cap is not None and dec > odds_cap:
            continue
        prev = favs.get(team_id)
        if prev is None or dec < prev:
            favs[team_id] = dec
    return favs


def detect_league_ids(session: Session, text: str) -> List[int]:
    """
    Lightweight league matching based on name fragments (e.g., 'premier league', 'la liga').
    Falls back to empty list if no matches.
    """
    lowered = text.lower()
    # quick explicit IDs if the user writes "league 8" etc.
    ids: List[int] = []
    for match in re.findall(r"league\s*(\d+)", lowered):
        try:
            ids.append(int(match))
        except Exception:
            continue
    # fuzzy by name
    leagues = session.query(League.id, League.name).all()
    for lid, name in leagues:
        nm = (name or "").lower()
        if nm and nm in lowered:
            ids.append(lid)
    # dedupe preserving order
    seen = set()
    ordered = []
    for lid in ids:
        if lid in seen:
            continue
        seen.add(lid)
        ordered.append(lid)
    return ordered


def teams_playing_today(session: Session) -> List[int]:
    today_date = date.today()
    rows = (
        session.query(FixtureParticipant.team_id)
        .join(Fixture, FixtureParticipant.fixture_id == Fixture.id)
        .filter(Fixture.starting_at != None)  # noqa: E711
        .filter(func.date(Fixture.starting_at) == today_date)
        .distinct()
        .all()
    )
    return [r[0] for r in rows]


def shot_predicate(min_shots: int):
    if min_shots >= 3:
        return PlayerForm.shots_ge_3_pct >= 1
    if min_shots == 2:
        return PlayerForm.shots_ge_2_pct >= 1
    return PlayerForm.shots_ge_1_pct >= 1


def stat_pass(
    form: PlayerForm, stat_type: str, threshold: int, sample_size: int, min_pct: float, location: Optional[str] = None
) -> Dict[str, Any]:
    """
    Evaluate whether the form meets the threshold for the chosen stat across its sample.
    Returns dict with hit_all, pct, avg.
    """
    fixtures = form.raw_fixtures or []
    if location:
        fixtures = [fx for fx in fixtures if fx.get("location") == location]
    if not fixtures:
        return {"hit_all": False, "pct": 0, "avg": 0, "values": [], "meets_pct": False}
    # Use the most recent fixtures (raw_fixtures are newest-first)
    fixtures = fixtures[:sample_size]
    values = []
    for fx in fixtures:
        if stat_type == "shots":
            values.append(fx.get("shots", 0))
        elif stat_type == "sot":
            values.append(fx.get("shots_on", 0))
        elif stat_type == "goals":
            values.append(fx.get("goals", 0))
        elif stat_type == "assists":
            values.append(fx.get("assists", 0))
    if not values:
        return {"hit_all": False, "pct": 0, "avg": 0, "values": [], "meets_pct": False}
    games = len(values)
    if games < sample_size:
        pct = (sum(1 for v in values if v >= threshold) / games) if games else 0
        return {"hit_all": False, "pct": pct, "avg": 0, "values": values, "meets_pct": False, "hits": 0, "games": games}
    hits = sum(1 for v in values if v >= threshold)
    avg = sum(values) / games if games else 0
    pct = hits / games if games else 0
    return {
        "hit_all": hits == games,
        "pct": pct,
        "avg": avg,
        "hits": hits,
        "games": games,
        "meets_pct": pct >= min_pct,
        "values": values,
    }


def stat_pass_team(
    form: TeamForm, stat_key: str, threshold: int, sample_size: int, min_pct: float, location: Optional[str] = None
) -> Dict[str, Any]:
    fixtures = form.raw_fixtures or []
    if location:
        fixtures = [fx for fx in fixtures if fx.get("location") == location]
    if not fixtures:
        return {"hit_all": False, "pct": 0, "avg": 0, "values": [], "meets_pct": False}
    fixtures = fixtures[:sample_size]
    values = []
    alias = {"goals_for": "gf", "goals_against": "ga"}
    for fx in fixtures:
        if stat_key == "match_goals":
            gf = fx.get("gf", 0) or 0
            ga = fx.get("ga", 0) or 0
            val = (gf or 0) + (ga or 0)
        else:
            val = fx.get(stat_key, 0) or fx.get(alias.get(stat_key, ""), 0) or 0
        values.append(val)
    games = len(values)
    if games < sample_size:
        pct = (sum(1 for v in values if v >= threshold) / games) if games else 0
        return {"hit_all": False, "pct": pct, "avg": 0, "values": values, "meets_pct": False, "hits": 0, "games": games}
    hits = sum(1 for v in values if v >= threshold)
    avg = sum(values) / games if games else 0
    pct = hits / games if games else 0
    return {
        "hit_all": hits == games,
        "pct": pct,
        "avg": avg,
        "hits": hits,
        "games": games,
        "meets_pct": pct >= min_pct,
        "values": values,
    }


def extract_team_names_from_extra(extra: Any) -> Dict[int, str]:
    """
    Grab participant team names from a fixture.extra payload.
    """
    if not extra:
        return {}
    payload: Any = extra
    if isinstance(extra, str):
        try:
            payload = json.loads(extra)
        except Exception:
            return {}
    if isinstance(payload, list):
        participants = payload
    elif isinstance(payload, dict):
        participants = payload.get("participants") or payload.get("lineup") or []
    else:
        participants = []
    names: Dict[int, str] = {}
    for part in participants or []:
        try:
            tid = int(part.get("id"))
        except Exception:
            continue
        name = part.get("name")
        if tid and name:
            names[tid] = name
    return names


def extract_team_name_from_participant(extra: Any) -> Optional[str]:
    """
    Extract team name from a fixture_participants.extra payload.
    """
    if not extra:
        return None
    payload: Any = extra
    if isinstance(extra, str):
        try:
            payload = json.loads(extra)
        except Exception:
            return None
    if isinstance(payload, dict):
        name = payload.get("name")
        return name or None
    return None


def resolve_team_names(session: Session, team_ids: set[int]) -> Dict[int, str]:
    """
    Resolve team_id -> name, using the teams table first, then fixture.extra participants as a fallback.
    """
    if not team_ids:
        return {}
    names: Dict[int, str] = {}
    rows = session.query(Team.id, Team.name).filter(Team.id.in_(team_ids)).all()
    for tid, name in rows:
        if tid is not None and name:
            names[tid] = name
    remaining = [tid for tid in team_ids if tid not in names]
    if not remaining:
        return names
    # Fixtures.extra participants
    fixtures = (
        session.query(Fixture.home_team_id, Fixture.away_team_id, Fixture.extra)
        .filter(or_(Fixture.home_team_id.in_(remaining), Fixture.away_team_id.in_(remaining)))
        .order_by(Fixture.starting_at.desc())
        .all()
    )
    for home_id, away_id, extra in fixtures:
        parts = extract_team_names_from_extra(extra)
        for tid, name in parts.items():
            if tid in remaining and tid not in names and name:
                names[tid] = name
    # Fixture participants extras
    still_missing = [tid for tid in remaining if tid not in names]
    if still_missing:
        participants = (
            session.query(FixtureParticipant.team_id, FixtureParticipant.extra)
            .filter(FixtureParticipant.team_id.in_(still_missing))
            .order_by(FixtureParticipant.updated_at.desc())
            .all()
        )
        for tid, extra in participants:
            if tid in names:
                continue
            nm = extract_team_name_from_participant(extra)
            if nm:
                names[tid] = nm
    return names


def _normalize_name_simple(name: str) -> str:
    return re.sub(r"\\s+", " ", (name or "").strip().lower())


def _extract_line_from_raw(raw_obj: Any) -> Optional[float]:
    payload = raw_obj
    if isinstance(raw_obj, str):
        try:
            payload = json.loads(raw_obj)
        except Exception:
            return None
    if isinstance(payload, dict):
        for key in ("label", "line", "handicap", "total"):
            val = payload.get(key)
            try:
                if val is None or val == "":
                    continue
                return float(val)
            except Exception:
                continue
    return None


def attach_player_prop_odds(
    session: Session,
    results: List[Dict[str, Any]],
    stat_type: str,
    threshold: Union[int, float],
    odds_min: Optional[float],
    odds_max: Optional[float],
) -> None:
    """
    Attach player prop odds (shots/sot/goals/assists) from odds_latest when available.
    Falls back to team odds already present.
    """
    if not results:
        return
    market_map = {
        "shots": [268],
        "sot": [267],
        "goals": [331],
        "assists": [332],
    }
    market_ids = market_map.get(stat_type)
    if not market_ids:
        return
    name_keys = {_normalize_name_simple(r.get("player")) for r in results if r.get("player")}
    name_keys = {n for n in name_keys if n}
    if not name_keys:
        return
    rows = (
        session.query(OddsLatest)
        .filter(OddsLatest.market_id.in_(market_ids))
        .filter(func.lower(OddsLatest.selection).in_(name_keys))
        .all()
    )
    best: Dict[str, float] = {}
    for row in rows:
        key = _normalize_name_simple(row.selection)
        if not key:
            continue
        line_val = _extract_line_from_raw(row.raw) or 0.0
        # Allow small offset: threshold 1 => line 0.5, threshold 2 => line 1.5, etc.
        if isinstance(threshold, (int, float)):
            if line_val < max(threshold - 0.5, 0):
                continue
        dec = row.decimal_odds
        if dec is None:
            continue
        prev = best.get(key)
        if prev is None or dec < prev:
            best[key] = dec
    # Update results
    filtered = []
    for r in results:
        key = _normalize_name_simple(r.get("player"))
        prop_odds = best.get(key)
        if prop_odds is not None:
            r["odds_prop"] = prop_odds
            r["odds"] = prop_odds
        else:
            r["odds_prop"] = None
            r["odds"] = r.get("odds")  # keep team odds if present
        if odds_max is not None:
            if r.get("odds") is None or r["odds"] > odds_max:
                continue
        if odds_min is not None:
            if r.get("odds") is None or r["odds"] < odds_min:
                continue
        filtered.append(r)
    results[:] = filtered


def search_players(session: Session, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
    stat_type = parsed["stat_key"]
    min_value = parsed["min_value"]
    sample_size = parsed["sample_size"]
    min_pct = parsed["min_pct"]
    odds_max = parsed["odds_max"]
    odds_min = parsed.get("odds_min")
    min_avg = parsed.get("min_avg")
    require_today = parsed["require_today"]
    require_fav = parsed["require_favorites"]
    exclude_fav = parsed["exclude_favorites"]
    league_ids: Optional[List[int]] = parsed.get("league_ids")
    location = parsed.get("location")

    today_teams = set(teams_playing_today(session)) if require_today else set()
    fav_map = favorite_team_odds(session, odds_max)  # always fetch to display odds when present

    TeamCurrent = aliased(Team)
    query = (
        session.query(PlayerForm, Player, Team, TeamCurrent)
        .join(Player, Player.id == PlayerForm.player_id)
        .outerjoin(Team, Team.id == PlayerForm.team_id)
        .outerjoin(TeamCurrent, TeamCurrent.id == Player.current_team_id)
        .filter(PlayerForm.games_played >= sample_size)
        .order_by(PlayerForm.sample_size.asc(), PlayerForm.updated_at.desc())
    )
    if league_ids:
        query = query.filter(PlayerForm.league_id.in_(league_ids))
    if require_today:
        if not today_teams:
            return []
        query = query.filter(PlayerForm.team_id.in_(today_teams))
    if require_fav:
        if not fav_map:
            return []
        query = query.filter(PlayerForm.team_id.in_(fav_map.keys()))
    if exclude_fav and fav_map:
        query = query.filter(~PlayerForm.team_id.in_(fav_map.keys()))

    rows = query.all()
    seen_players = set()
    results = []
    missing_team_ids: set[int] = set()
    for form, player, team, team_current in rows:
        if player.id in seen_players:
            continue
        # ensure enough fixtures in the raw list for the requested window
        if not form.raw_fixtures or len(form.raw_fixtures) < sample_size:
            continue
        stat_eval = stat_pass(form, stat_type, min_value, sample_size, min_pct, location=location)
        if not stat_eval["meets_pct"]:
            continue
        if min_avg is not None and stat_eval.get("avg") is not None and stat_eval["avg"] < min_avg:
            continue
        seen_players.add(player.id)
        team_name = (team_current.name if team_current and team_current.name else None) or (team.name if team else None)
        if not team_name and form.team_id:
            missing_team_ids.add(form.team_id)
        results.append(
            {
                "player": player.display_name or f"{player.first_name or ''} {player.last_name or ''}".strip(),
                "team_id": form.team_id,
                "team": team_name or f"Team {form.team_id}",
                "stat_type": stat_type,
                "threshold": min_value,
                "avg": stat_eval["avg"],
                "pct": stat_eval["pct"],
                "hits": stat_eval.get("hits"),
                "games": stat_eval.get("games"),
                "sample_size": sample_size,
                "games_played": form.games_played,
                "values": stat_eval.get("values"),
                "odds_team": fav_map.get(team.id) if fav_map else None,
                "odds": fav_map.get(team.id) if fav_map else None,
            }
        )
    if missing_team_ids:
        name_map = resolve_team_names(session, missing_team_ids)
        for r in results:
            tid = r.get("team_id")
            if tid and tid in name_map:
                r["team"] = name_map[tid]
    attach_player_prop_odds(session, results, stat_type, min_value, odds_min, odds_max)
    # If user asked for odds bounds, drop rows without any odds
    if odds_min is not None or odds_max is not None:
        results = [r for r in results if r.get("odds") is not None]
    if parsed.get("sort_by") == "odds_desc":
        results.sort(key=lambda r: (r.get("odds") is None, -(r.get("odds") or 0)))
    elif parsed.get("sort_by") == "odds_asc":
        results.sort(key=lambda r: (r.get("odds") is None, r.get("odds") or 0))
    return results


def search_teams(session: Session, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
    stat_key = parsed["stat_key"]
    min_value = parsed["min_value"]
    sample_size = parsed["sample_size"]
    min_pct = parsed["min_pct"]
    odds_max = parsed.get("odds_max")
    odds_min = parsed.get("odds_min")
    min_avg = parsed.get("min_avg")
    league_ids: Optional[List[int]] = parsed.get("league_ids")
    location = parsed.get("location")
    require_today = parsed["require_today"]
    require_fav = parsed["require_favorites"]
    exclude_fav = parsed["exclude_favorites"]

    today_teams = set(teams_playing_today(session)) if require_today else set()

    query = (
        session.query(TeamForm, Team)
        .join(Team, Team.id == TeamForm.team_id)
        .filter(TeamForm.games_played >= sample_size)
        .order_by(TeamForm.sample_size.asc(), TeamForm.updated_at.desc())
    )
    if league_ids:
        query = query.filter(TeamForm.league_id.in_(league_ids))
    if require_today:
        if not today_teams:
            return []
        query = query.filter(TeamForm.team_id.in_(today_teams))

    fav_map = favorite_team_odds(session, odds_max)
    if require_fav and not fav_map:
        return []
    if require_fav:
        query = query.filter(TeamForm.team_id.in_(fav_map.keys()))
    if exclude_fav and fav_map:
        query = query.filter(~TeamForm.team_id.in_(fav_map.keys()))

    rows = query.all()
    results = []
    seen = set()
    missing_team_ids: set[int] = set()
    for form, team in rows:
        fixtures = form.raw_fixtures or []
        if not fixtures or len(fixtures) < sample_size:
            continue
        stat_eval = stat_pass_team(form, stat_key, min_value, sample_size, min_pct, location=location)
        if not stat_eval["meets_pct"]:
            continue
        if min_avg is not None and stat_eval.get("avg") is not None and stat_eval["avg"] < min_avg:
            continue
        if form.team_id in seen:
            continue
        seen.add(form.team_id)
        if (not team or not team.name) and form.team_id:
            missing_team_ids.add(form.team_id)
        odds_val = fav_map.get(form.team_id)
        if odds_max is not None and odds_val is not None and odds_val > odds_max:
            continue
        if odds_min is not None and odds_val is not None and odds_val < odds_min:
            continue
        results.append(
            {
                "team_id": form.team_id,
                "team": (team.name if team else None) or f"Team {form.team_id}",
                "stat_type": stat_key,
                "threshold": min_value,
                "avg": stat_eval["avg"],
                "pct": stat_eval["pct"],
                "hits": stat_eval.get("hits"),
                "games": stat_eval.get("games"),
                "sample_size": sample_size,
                "values": stat_eval.get("values"),
                "odds": odds_val,
            }
        )
    if missing_team_ids:
        name_map = resolve_team_names(session, missing_team_ids)
        for r in results:
            tid = r.get("team_id")
            if tid and tid in name_map:
                r["team"] = name_map[tid]
    if parsed.get("sort_by") == "odds_desc":
        results.sort(key=lambda r: (r.get("odds") is None, -(r.get("odds") or 0)))
    elif parsed.get("sort_by") == "odds_asc":
        results.sort(key=lambda r: (r.get("odds") is None, r.get("odds") or 0))
    return results


@app.post("/api/chat")
async def chat_api(payload: Dict[str, str]):
    text = payload.get("query", "") if isinstance(payload, dict) else ""
    if not text:
        return JSONResponse({"error": "query required"}, status_code=400)
    parsed = parse_query(text)
    session = get_session()
    try:
        parsed["league_ids"] = detect_league_ids(session, text)
        if parsed.get("entity") == "team":
            results = search_teams(session, parsed)
        else:
            results = search_players(session, parsed)
    finally:
        session.close()
    interpretation = {
        "entity": parsed.get("entity"),
        "stat": parsed["stat_key"],
        "threshold": parsed["min_value"],
        "sample_size": parsed["sample_size"],
        "min_pct": parsed["min_pct"],
        "today_only": parsed["require_today"],
        "favorites_only": parsed["require_favorites"],
        "exclude_favorites": parsed["exclude_favorites"],
        "odds_cap": parsed["odds_max"],
        "odds_min": parsed.get("odds_min"),
        "location": parsed.get("location"),
        "league_ids": parsed.get("league_ids"),
        "sort_by": parsed.get("sort_by"),
        "note": "Uses most recent games; requires hit rate >= min_pct",
    }
    return {"query": parsed, "interpretation": interpretation, "results": results}


INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>JXD Chat</title>
  <style>
    body { font-family: sans-serif; max-width: 720px; margin: 40px auto; padding: 0 16px; }
    #log { border: 1px solid #ccc; border-radius: 8px; padding: 12px; min-height: 200px; }
    .msg { margin-bottom: 12px; }
    .user { font-weight: 600; }
    textarea { width: 100%; min-height: 80px; }
    button { padding: 8px 16px; margin-top: 8px; }
  </style>
</head>
<body>
  <h1>JXD Chat</h1>
  <div id="log"></div>
  <div style="margin-top:16px;">
    <textarea id="input" placeholder="Ask: list players playing today who had 2+ shots in all of last 10 and odds <=1.4; remove teams not favorites"></textarea>
    <button onclick="send()">Send</button>
  </div>
<script>
async function send() {
  const box = document.getElementById('input');
  const text = box.value.trim();
  if (!text) return;
  const log = document.getElementById('log');
  log.innerHTML += `<div class='msg'><span class='user'>You:</span> ${text}</div>`;
  box.value = '';
  const res = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({query: text})
  });
  const data = await res.json();
  if (data.error) {
    log.innerHTML += `<div class='msg'>Error: ${data.error}</div>`;
    return;
  }
  if (data.interpretation) {
    log.innerHTML += `<div class='msg'>Query understood as: ${JSON.stringify(data.interpretation)}</div>`;
  }
  if (!data.results || data.results.length === 0) {
    log.innerHTML += `<div class='msg'>No matches.</div>`;
    return;
  }
  const interp = data.interpretation || {};
  const oddsFiltered = interp.odds_cap != null || interp.odds_min != null;
  const statLabel = interp.stat || 'stat';
  const header = `${interp.threshold || ''}+ ${statLabel} in ${interp.sample_size || ''}/${interp.sample_size || ''}`.trim();
  const rows = data.results.map(r => {
    const values = Array.isArray(r.values) ? r.values.join(',') : '';
    const name = r.player ? `${r.player} (${r.team || ''})` : r.team;
    const odds = oddsFiltered && typeof r.odds === 'number' ? ` | odds: ${r.odds.toFixed(2)}` : '';
    return `${name} | values: ${values}${odds}`;
  }).join('<br>');
  log.innerHTML += `<div class='msg'>${header ? header + '<br>' : ''}${rows}</div>`;
}
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def home(_: Request):
    return HTMLResponse(INDEX_HTML)


# For local dev: uvicorn jxd.chat_server:app --reload
