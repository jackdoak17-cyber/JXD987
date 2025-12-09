from __future__ import annotations

from datetime import datetime, date
import re
from typing import List, Dict, Any, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func
from sqlalchemy.orm import Session

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

    def resolve_stat() -> Tuple[str, str]:
        # Returns (entity, stat_key). entity: player|team. stat_key for players: shots,sot,goals,assists. teams: shots_for/against, sot_for/against, goals_for/against, corners_for/against, yellows_for/against, reds_for/against.
        against = any(word in lowered for word in ["concede", "allowed", "against", "allow", "conceded"])
        if "corner" in lowered:
            return "team", "corners_against" if against else "corners_for"
        if "booking" in lowered or "card" in lowered:
            if "red" in lowered:
                return "team", "reds_against" if against else "reds_for"
            # default to yellows for generic "cards"
            return "team", "yellows_against" if against else "yellows_for"
        if "goal" in lowered and not "assist" in lowered:
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

    # threshold
    min_value = 2
    m = re.search(r"(\d+)\+?\s*(shot|sot|on target|goal|assist|corner|card)", lowered)
    if m:
        try:
            min_value = int(m.group(1))
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
    # odds cap
    odds_max: Optional[float] = None
    m = re.search(r"odds[^\d]*([0-9]+\.?[0-9]*)", lowered)
    if m:
        try:
            odds_max = float(m.group(1))
        except Exception:
            odds_max = None
    require_today = "today" in lowered or "tonight" in lowered
    require_fav = "favorite" in lowered or "favourite" in lowered
    exclude_fav = "not favorite" in lowered or "non favorite" in lowered or "underdog" in lowered or "exclude favorites" in lowered
    if exclude_fav:
        require_fav = False
    location = None
    if " at home" in lowered or " home " in lowered or "home only" in lowered or "home games" in lowered:
        location = "home"
    elif " away " in lowered or "away only" in lowered or "away games" in lowered or "away fixtures" in lowered:
        location = "away"
    # entity hints
    if "team" in lowered and entity != "team":
        entity = "team"
    return {
        "entity": entity,
        "stat_key": stat_key,
        "min_value": min_value,
        "sample_size": sample_size,
        "min_pct": min_pct,
        "odds_max": odds_max,
        "require_today": require_today,
        "require_favorites": require_fav,
        "exclude_favorites": exclude_fav,
        "location": location,
    }


def favorite_teams_today(session: Session, odds_cap: Optional[float]) -> Dict[int, float]:
    """
    Return team_id -> favorite decimal odds for fixtures today (market match_result=1).
    """
    today_date = date.today()
    rows = (
        session.query(OddsLatest, Fixture)
        .join(Fixture, OddsLatest.fixture_id == Fixture.id)
        .filter(OddsLatest.market_id == 1)
        .filter(Fixture.starting_at != None)  # noqa: E711
        .filter(func.date(Fixture.starting_at) == today_date)
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
        return {"hit_all": False, "pct": 0, "avg": 0, "values": []}
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
        return {"hit_all": False, "pct": 0, "avg": 0, "values": []}
    games = len(values)
    if games < sample_size:
        return {"hit_all": False, "pct": 0, "avg": 0, "values": values}
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
        return {"hit_all": False, "pct": 0, "avg": 0, "values": []}
    fixtures = fixtures[:sample_size]
    values = []
    for fx in fixtures:
        values.append(fx.get(stat_key, 0) or 0)
    games = len(values)
    if games < sample_size:
        return {"hit_all": False, "pct": 0, "avg": 0, "values": values}
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


def search_players(session: Session, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
    stat_type = parsed["stat_key"]
    min_value = parsed["min_value"]
    sample_size = parsed["sample_size"]
    min_pct = parsed["min_pct"]
    odds_max = parsed["odds_max"]
    require_today = parsed["require_today"]
    require_fav = parsed["require_favorites"]
    exclude_fav = parsed["exclude_favorites"]
    league_ids: Optional[List[int]] = parsed.get("league_ids")
    location = parsed.get("location")

    today_teams = set(teams_playing_today(session)) if require_today else set()
    fav_map = favorite_teams_today(session, odds_max) if require_fav or exclude_fav or odds_max is not None else {}

    query = (
        session.query(PlayerForm, Player, Team)
        .join(Player, Player.id == PlayerForm.player_id)
        .outerjoin(Team, Team.id == PlayerForm.team_id)
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
    for form, player, team in rows:
        if player.id in seen_players:
            continue
        # ensure enough fixtures in the raw list for the requested window
        if not form.raw_fixtures or len(form.raw_fixtures) < sample_size:
            continue
        odds_val = fav_map.get(team.id) if fav_map else None
        if odds_max is not None and odds_val is not None and odds_val > odds_max:
            continue
        stat_eval = stat_pass(form, stat_type, min_value, sample_size, min_pct, location=location)
        if not stat_eval["meets_pct"]:
            continue
        seen_players.add(player.id)
        results.append(
            {
                "player": player.display_name or f"{player.first_name or ''} {player.last_name or ''}".strip(),
                "team": team.name if team else f"Team {form.team_id}",
                "stat_type": stat_type,
                "threshold": min_value,
                "avg": stat_eval["avg"],
                "pct": stat_eval["pct"],
                "hits": stat_eval.get("hits"),
                "games": stat_eval.get("games"),
                "sample_size": sample_size,
                "games_played": form.games_played,
                "values": stat_eval.get("values"),
                "odds": odds_val,
            }
        )
    return results


def search_teams(session: Session, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
    stat_key = parsed["stat_key"]
    min_value = parsed["min_value"]
    sample_size = parsed["sample_size"]
    min_pct = parsed["min_pct"]
    league_ids: Optional[List[int]] = parsed.get("league_ids")
    location = parsed.get("location")
    require_today = parsed["require_today"]

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

    rows = query.all()
    results = []
    for form, team in rows:
        fixtures = form.raw_fixtures or []
        if not fixtures or len(fixtures) < sample_size:
            continue
        stat_eval = stat_pass_team(form, stat_key, min_value, sample_size, min_pct, location=location)
        if not stat_eval["meets_pct"]:
            continue
        results.append(
            {
                "team": team.name if team else f"Team {form.team_id}",
                "stat_type": stat_key,
                "threshold": min_value,
                "avg": stat_eval["avg"],
                "pct": stat_eval["pct"],
                "hits": stat_eval.get("hits"),
                "games": stat_eval.get("games"),
                "sample_size": sample_size,
                "values": stat_eval.get("values"),
            }
        )
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
        "location": parsed.get("location"),
        "league_ids": parsed.get("league_ids"),
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
  const rows = data.results.map(r => {
    const hits = r.hits != null ? r.hits : '';
    const games = r.games != null ? r.games : r.sample_size;
    const values = Array.isArray(r.values) ? r.values.join(',') : '';
    const pct = typeof r.pct === 'number' ? (r.pct * 100).toFixed(0) + '%' : '';
    const name = r.player ? `${r.player} (${r.team || ''})` : r.team;
    return `${name} - ${r.stat_type} ${r.threshold}+ in ${hits}/${games} (${pct}) | values: ${values}`;
  }).join('<br>');
  log.innerHTML += `<div class='msg'>Results:<br>${rows}</div>`;
}
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def home(_: Request):
    return HTMLResponse(INDEX_HTML)


# For local dev: uvicorn jxd.chat_server:app --reload
