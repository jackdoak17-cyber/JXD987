#!/usr/bin/env python3
"""
Sync pre-match odds from Odds-API.io into SQLite.

- Maps Odds-API events to SportMonks fixtures in SQLite (by league + kickoff + team names).
- Fetches odds via /odds/multi for selected bookmakers.
- Normalizes markets into odds_outcomes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import unicodedata
import difflib
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import bindparam, text

from jxd.db import get_engine, get_session
from jxd.models import Base
from jxd.odds_api_client import OddsApiClient, OddsApiError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

DEFAULT_BOOKMAKERS = ["Bet365", "Kambi", "Paddy Power"]
BOOKMAKER_NAME_TO_ID = {
    "bet365": 2,
    "kambi": 3,
    "paddypower": 4,
}
BOOKMAKER_CANONICAL = {
    "bet365": "Bet365",
    "kambi": "Kambi",
    "paddypower": "Paddy Power",
}

DEFAULT_MARKET_ALLOWLIST = {
    "moneyline",
    "double_chance",
    "draw_no_bet",
    "goals_over_under",
    "btts",
    "match_shots",
    "match_shots_on_target",
    "team_shots",
    "team_shots_on_target",
    "player_shots",
    "player_shots_on_target",
    "player_fouls_committed",
    "player_fouls_drawn",
    "player_to_score",
    "player_to_assist",
    "player_to_score_or_assist",
    "player_card",
}

TEAM_MARKETS = {"team_shots", "team_shots_on_target"}
MATCH_MARKETS = {"match_shots", "match_shots_on_target", "goals_over_under", "btts"}

TEAM_NAME_ALIAS = {
    "manutd": "manchesterunited",
    "manunited": "manchesterunited",
    "manchesterutd": "manchesterunited",
    "mancity": "manchestercity",
    "manchestercity": "manchestercity",
    "manchester city": "manchestercity",
    "psg": "parissaintgermain",
    "paris saint germain": "parissaintgermain",
    "spurs": "tottenhamhotspur",
    "tottenham": "tottenhamhotspur",
    "inter": "internazionale",
    "acmilan": "milan",
}

TEAM_TOKEN_DROP = {
    "fc",
    "cf",
    "sc",
    "ac",
    "afc",
    "cfc",
    "club",
    "the",
    "de",
    "da",
    "cd",
}


MARKET_NAME_MAP = {
    "ml": "moneyline",
    "match_result": "moneyline",
    "full_time_result": "moneyline",
    "full_time_result_90": "moneyline",
    "draw_no_bet": "draw_no_bet",
    "double_chance": "double_chance",
    "goals_over_under": "goals_over_under",
    "totals": "goals_over_under",
    "both_teams_to_score": "btts",
    "btts": "btts",
    "match_shots": "match_shots",
    "match_shots_on_target": "match_shots_on_target",
    "total_shots": "match_shots",
    "total_shots_on_target": "match_shots_on_target",
    "team_shots_home": "team_shots_home",
    "team_shots_away": "team_shots_away",
    "team_shots_on_target_home": "team_shots_on_target_home",
    "team_shots_on_target_away": "team_shots_on_target_away",
    "total_shots_home": "team_shots_home",
    "total_shots_away": "team_shots_away",
    "total_shots_on_target_home": "team_shots_on_target_home",
    "total_shots_on_target_away": "team_shots_on_target_away",
    "player_shots": "player_shots",
    "player_shots_over_under": "player_shots",
    "player_shots_on_target": "player_shots_on_target",
    "player_shots_on_target_over_under": "player_shots_on_target",
    "player_fouls": "player_fouls_committed",
    "player_fouls_committed": "player_fouls_committed",
    "player_fouls_drawn": "player_fouls_drawn",
    "player_fouls_won": "player_fouls_drawn",
    "player_to_be_fouled": "player_fouls_drawn",
    "player_cards": "player_card",
    "player_to_be_booked": "player_card",
    "player_booked": "player_card",
    "anytime_goalscorer": "player_to_score",
    "player_to_score": "player_to_score",
    "player_to_assist": "player_to_assist",
    "player_to_score_or_assist": "player_to_score_or_assist",
}


def normalize_slug(value: str) -> str:
    text_val = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text_val or "unknown"


def normalize_name_tokens(value: str) -> List[str]:
    if not value:
        return []
    text_val = unicodedata.normalize("NFKD", value)
    text_val = "".join(ch for ch in text_val if not unicodedata.combining(ch))
    text_val = re.sub(r"[^a-z0-9]+", " ", text_val.lower()).strip()
    if not text_val:
        return []
    return [token for token in text_val.split() if token]


def normalize_name(value: str) -> str:
    tokens = normalize_name_tokens(value)
    return "".join(tokens)


def normalize_team_name(value: str) -> str:
    if not value:
        return ""
    raw = value.lower().strip()
    raw = raw.replace("&", "and")
    tokens = [token for token in normalize_name_tokens(raw) if token not in TEAM_TOKEN_DROP]
    normalized = "".join(tokens)
    normalized = TEAM_NAME_ALIAS.get(normalized, normalized)
    return normalized


def team_aliases(value: str, short_code: Optional[str] = None) -> List[str]:
    aliases = set()
    if value:
        aliases.add(normalize_team_name(value))
    if short_code:
        aliases.add(normalize_team_name(short_code))
    for item in list(aliases):
        if item in TEAM_NAME_ALIAS:
            aliases.add(TEAM_NAME_ALIAS[item])
    return [alias for alias in aliases if alias]


def name_variants(value: str) -> List[str]:
    tokens = normalize_name_tokens(value)
    if not tokens:
        return []
    variants = {normalize_name(value)}
    if len(tokens) == 2:
        first, last = tokens
        variants.add(last + first)
        variants.add(first[0] + last)
        variants.add(last + first[0])
    elif len(tokens) >= 3:
        first = tokens[0]
        last = tokens[-1]
        middle = tokens[1:-1]
        tail = "".join(tokens[1:])
        variants.add(last + "".join([first, *middle]))
        variants.add(first + last)
        variants.add(last + first)
        variants.add(first[0] + last)
        variants.add(last + first[0])
        if tail:
            variants.add(tail)
            variants.add(last + "".join(middle))
        if len(tokens[1]) == 1:
            variants.add(first + last)
            variants.add(last + first)
    return [variant for variant in variants if variant]


def normalize_market_key(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9_]+", "_", value.strip().lower()).strip("_")


def normalize_bookmaker_key(value: str) -> str:
    raw = re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())
    if "bet365" in raw:
        return "bet365"
    if "kambi" in raw:
        return "kambi"
    if "paddypower" in raw or ("paddy" in raw and "power" in raw):
        return "paddypower"
    return raw


def canonicalize_bookmakers(raw_items: Iterable[str]) -> Tuple[List[str], List[str]]:
    canonical: List[str] = []
    unknown: List[str] = []
    seen = set()
    for item in raw_items:
        key = normalize_bookmaker_key(item)
        name = BOOKMAKER_CANONICAL.get(key)
        if not name:
            unknown.append(item)
            continue
        if name in seen:
            continue
        seen.add(name)
        canonical.append(name)
    return canonical, unknown


def load_market_allowlist() -> Optional[set]:
    raw = (os.environ.get("ODDS_MARKET_ALLOWLIST") or "").strip()
    if raw:
        if raw.lower() in {"all", "*"}:
            return None
        items = {normalize_market_key(item) for item in raw.split(",") if item.strip()}
        return {item for item in items if item}
    return set(DEFAULT_MARKET_ALLOWLIST)


def parse_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        num = float(value)
        return num if math.isfinite(num) else None
    except Exception:
        try:
            num = float(str(value).replace("%", ""))
            return num if math.isfinite(num) else None
        except Exception:
            return None


def parse_timestamp(value: Optional[object]) -> Optional[datetime]:
    if not value:
        return None
    text_val = str(value).replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(text_val)
    except Exception:
        return None


def decimal_to_american(decimal_price: Optional[float]) -> Optional[int]:
    if decimal_price is None or not math.isfinite(decimal_price) or decimal_price <= 1:
        return None
    if decimal_price >= 2:
        return int(round((decimal_price - 1) * 100))
    return int(round(-100 / (decimal_price - 1)))


def extract_player_name(raw_name: str) -> str:
    name = (raw_name or "").strip()
    if not name:
        return name
    if "(" in name and ")" in name:
        prefix = name.split("(")[0].strip()
        if prefix:
            name = prefix
    if " - " in name:
        name = name.split(" - ")[0].strip()
    if " @ " in name:
        name = name.split(" @ ")[0].strip()
    if " vs " in name:
        name = name.split(" vs ")[0].strip()
    if " v " in name:
        name = name.split(" v ")[0].strip()
    if "," in name:
        parts = [part.strip() for part in name.split(",") if part.strip()]
        if len(parts) >= 2:
            name = " ".join([parts[1], parts[0]])
    return name


def resolve_market_key(market_name: str) -> Optional[str]:
    key = normalize_market_key(market_name)
    mapped = MARKET_NAME_MAP.get(key)
    if mapped in {
        "team_shots_home",
        "team_shots_away",
        "team_shots_on_target_home",
        "team_shots_on_target_away",
    }:
        return mapped
    if mapped:
        return mapped
    if key in DEFAULT_MARKET_ALLOWLIST:
        return key
    return None


def load_league_map(path: Path) -> Dict[int, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {int(k): str(v) for k, v in raw.items() if v}


def fixture_window_bounds(days_forward: int) -> Tuple[datetime, datetime]:
    start_dt = datetime.utcnow()
    end_dt = start_dt + timedelta(days=days_forward)
    return start_dt, end_dt


def load_fixtures(session, league_ids: List[int], days_forward: int) -> List[Dict[str, object]]:
    if not league_ids:
        return []
    start_dt, end_dt = fixture_window_bounds(days_forward)
    stmt = text(
        """
        select f.id,
               f.league_id,
               f.starting_at,
               f.home_team_id,
               f.away_team_id,
               th.name as home_name,
               th.short_code as home_short,
               ta.name as away_name,
               ta.short_code as away_short
        from fixtures f
        left join teams th on th.id = f.home_team_id
        left join teams ta on ta.id = f.away_team_id
        where f.league_id in :league_ids
          and f.starting_at >= :start_dt
          and f.starting_at < :end_dt
        """
    ).bindparams(bindparam("league_ids", expanding=True))
    rows = session.execute(
        stmt,
        {
            "league_ids": league_ids,
            "start_dt": start_dt,
            "end_dt": end_dt,
        },
    ).fetchall()

    fixtures = []
    for row in rows:
        try:
            start_val = row.starting_at
            if isinstance(start_val, str):
                start_val = datetime.fromisoformat(start_val.replace("Z", "+00:00"))
            if isinstance(start_val, datetime) and start_val.tzinfo:
                start_val = start_val.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            start_val = None
        home_alias = team_aliases(str(row.home_name or ""), str(row.home_short or ""))
        away_alias = team_aliases(str(row.away_name or ""), str(row.away_short or ""))
        fixtures.append(
            {
                "fixture_id": int(row.id),
                "league_id": int(row.league_id),
                "starting_at": start_val,
                "home_team_id": row.home_team_id,
                "away_team_id": row.away_team_id,
                "home_alias": home_alias,
                "away_alias": away_alias,
            }
        )
    return fixtures


def score_name_match(event_name: str, aliases: Iterable[str]) -> float:
    event_norm = normalize_team_name(event_name)
    if not event_norm:
        return 0.0
    best = 0.0
    for alias in aliases:
        if not alias:
            continue
        if event_norm == alias:
            return 1.0
        score = difflib.SequenceMatcher(None, event_norm, alias).ratio()
        if score > best:
            best = score
    return best


def match_event_to_fixture(event: Dict[str, object], fixtures: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
    if not fixtures:
        return None
    event_home = str(event.get("home") or "")
    event_away = str(event.get("away") or "")
    event_date = event.get("date") or ""
    try:
        event_dt = datetime.fromisoformat(str(event_date).replace("Z", "+00:00"))
        if event_dt.tzinfo:
            event_dt = event_dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        event_dt = None
    best = None
    best_score = 0.0
    best_min = 0.0
    second_best = 0.0
    for fixture in fixtures:
        fixture_dt = fixture.get("starting_at")
        if event_dt and fixture_dt:
            delta = abs((fixture_dt - event_dt).total_seconds())
            if delta > 3 * 3600:
                continue
        home_score = score_name_match(event_home, fixture.get("home_alias") or [])
        away_score = score_name_match(event_away, fixture.get("away_alias") or [])
        combined = home_score + away_score
        if combined > best_score:
            second_best = best_score
            best_score = combined
            best_min = min(home_score, away_score)
            best = fixture

    if not best:
        return None
    if best_min < 0.8:
        return None
    if best_score >= 1.85:
        return best
    if best_score >= 1.7 and (best_score - second_best) >= 0.1:
        return best
    return None


def load_fixture_player_map(session, fixture_id: int) -> Dict[str, List[Tuple[int, Optional[int]]]]:
    rows = session.execute(
        text(
            """
            select fp.player_id,
                   fp.team_id,
                   p.name,
                   p.common_name,
                   p.short_name,
                   fp.name as fixture_name
            from fixture_players fp
            left join players p on p.id = fp.player_id
            where fp.fixture_id = :fixture_id
            """
        ),
        {"fixture_id": fixture_id},
    ).fetchall()
    mapping: Dict[str, List[Tuple[int, Optional[int]]]] = {}
    for player_id, team_id, name, common_name, short_name, fixture_name in rows:
        if not player_id:
            continue
        for candidate in (name, common_name, short_name, fixture_name):
            if not candidate:
                continue
            for normalized in name_variants(str(candidate)):
                if not normalized:
                    continue
                mapping.setdefault(normalized, []).append(
                    (int(player_id), int(team_id) if team_id else None)
                )
    return mapping


def load_team_player_map(session, team_ids: Iterable[int]) -> Dict[str, List[Tuple[int, Optional[int]]]]:
    ids = [int(x) for x in team_ids if x]
    if not ids:
        return {}
    stmt = text(
        """
        select id, team_id, name, common_name, short_name
        from players
        where team_id in :team_ids
        """
    ).bindparams(bindparam("team_ids", expanding=True))
    rows = session.execute(stmt, {"team_ids": ids}).fetchall()
    mapping: Dict[str, List[Tuple[int, Optional[int]]]] = {}
    for player_id, team_id, name, common_name, short_name in rows:
        if not player_id:
            continue
        for candidate in (name, common_name, short_name):
            if not candidate:
                continue
            for normalized in name_variants(str(candidate)):
                if not normalized:
                    continue
                mapping.setdefault(normalized, []).append(
                    (int(player_id), int(team_id) if team_id else None)
                )
    return mapping


def build_fuzzy_candidates(session, team_ids: Iterable[int]) -> List[Dict[str, object]]:
    ids = [int(x) for x in team_ids if x]
    if not ids:
        return []
    stmt = text(
        """
        select id, team_id, name, common_name, short_name
        from players
        where team_id in :team_ids
        """
    ).bindparams(bindparam("team_ids", expanding=True))
    rows = session.execute(stmt, {"team_ids": ids}).fetchall()
    candidates: List[Dict[str, object]] = []
    seen = set()
    for player_id, team_id, name, common_name, short_name in rows:
        for candidate_name in (name, common_name, short_name):
            if not candidate_name:
                continue
            tokens = normalize_name_tokens(str(candidate_name))
            if not tokens:
                continue
            first_initial = tokens[0][0]
            last = tokens[-1]
            key = (int(player_id), first_initial, last)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "player_id": int(player_id),
                    "team_id": int(team_id) if team_id else None,
                    "first_initial": first_initial,
                    "last": last,
                    "name": str(candidate_name),
                }
            )
    return candidates


def fuzzy_match_player(
    raw_name: str,
    candidates: Iterable[Dict[str, object]],
    min_ratio: float = 0.9,
) -> Optional[Tuple[Dict[str, object], float]]:
    tokens = normalize_name_tokens(raw_name)
    if len(tokens) < 2:
        return None
    first_initial = tokens[0][0]
    last = tokens[-1]
    best = None
    best_score = 0.0
    tie = False
    for candidate in candidates:
        cand_first = candidate.get("first_initial") or ""
        if cand_first and cand_first != first_initial:
            continue
        cand_last = candidate.get("last") or ""
        if not cand_last:
            continue
        score = difflib.SequenceMatcher(None, last, str(cand_last)).ratio()
        if score < min_ratio:
            continue
        if score > best_score + 1e-6:
            best_score = score
            best = candidate
            tie = False
        elif abs(score - best_score) <= 1e-6:
            tie = True
    if best and not tie:
        return best, best_score
    return None


def resolve_player_id(
    raw_name: str,
    fixture_map: Dict[str, List[Tuple[int, Optional[int]]]],
    team_map: Dict[str, List[Tuple[int, Optional[int]]]],
    team_ids: Optional[Iterable[int]] = None,
    fuzzy_candidates: Optional[List[Dict[str, object]]] = None,
) -> Optional[int]:
    team_id_set = {int(x) for x in team_ids or [] if x}
    variants = name_variants(raw_name)
    if not variants:
        return None
    for variant in variants:
        candidates = fixture_map.get(variant)
        if candidates:
            filtered = [
                (pid, tid)
                for pid, tid in candidates
                if not team_id_set or (tid and tid in team_id_set)
            ]
            if filtered:
                candidates = filtered
            unique = {pid for pid, _ in candidates}
            if len(unique) == 1:
                return next(iter(unique))
            return candidates[0][0]
    for variant in variants:
        candidates = team_map.get(variant)
        if candidates:
            filtered = [
                (pid, tid)
                for pid, tid in candidates
                if not team_id_set or (tid and tid in team_id_set)
            ]
            if filtered:
                candidates = filtered
            unique = {pid for pid, _ in candidates}
            if len(unique) == 1:
                return next(iter(unique))
            return candidates[0][0]
    if fuzzy_candidates:
        fuzzy_match = fuzzy_match_player(raw_name, fuzzy_candidates)
        if fuzzy_match:
            candidate, _score = fuzzy_match
            return int(candidate["player_id"])
    return None


def upsert_outcomes(session, rows: List[Dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        insert into odds_outcomes (
          fixture_id, bookmaker_id, market_key, selection_key,
          participant_type, participant_id, line,
          price_decimal, price_american, last_updated_at
        ) values (
          :fixture_id, :bookmaker_id, :market_key, :selection_key,
          :participant_type, :participant_id, :line,
          :price_decimal, :price_american, :last_updated_at
        )
        on conflict(fixture_id, bookmaker_id, market_key, selection_key, line)
        do update set
          participant_type = coalesce(excluded.participant_type, odds_outcomes.participant_type),
          participant_id = coalesce(excluded.participant_id, odds_outcomes.participant_id),
          price_decimal = excluded.price_decimal,
          price_american = excluded.price_american,
          last_updated_at = excluded.last_updated_at
        """
    )
    session.execute(sql, rows)


def market_over_under_rows(
    fixture_id: int,
    bookmaker_id: int,
    market_key: str,
    odds_list: List[Dict[str, object]],
    participant_type: Optional[str],
    participant_id: Optional[int],
    updated_at: Optional[datetime],
    selection_prefix: Optional[str] = None,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for odd in odds_list or []:
        line = parse_float(odd.get("hdp"))
        over_price = parse_float(odd.get("over"))
        under_price = parse_float(odd.get("under"))
        if line is None:
            continue
        if over_price is not None:
            sel = "over" if not selection_prefix else f"{selection_prefix}_over"
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": market_key,
                    "selection_key": sel,
                    "participant_type": participant_type,
                    "participant_id": participant_id,
                    "line": line,
                    "price_decimal": over_price,
                    "price_american": decimal_to_american(over_price),
                    "last_updated_at": updated_at,
                }
            )
        if under_price is not None:
            sel = "under" if not selection_prefix else f"{selection_prefix}_under"
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": market_key,
                    "selection_key": sel,
                    "participant_type": participant_type,
                    "participant_id": participant_id,
                    "line": line,
                    "price_decimal": under_price,
                    "price_american": decimal_to_american(under_price),
                    "last_updated_at": updated_at,
                }
            )
    return rows


def market_yes_no_rows(
    fixture_id: int,
    bookmaker_id: int,
    market_key: str,
    odds_list: List[Dict[str, object]],
    updated_at: Optional[datetime],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for odd in odds_list or []:
        yes_price = parse_float(odd.get("yes"))
        no_price = parse_float(odd.get("no"))
        if yes_price is not None:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": market_key,
                    "selection_key": "yes",
                    "participant_type": None,
                    "participant_id": None,
                    "line": None,
                    "price_decimal": yes_price,
                    "price_american": decimal_to_american(yes_price),
                    "last_updated_at": updated_at,
                }
            )
        if no_price is not None:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": market_key,
                    "selection_key": "no",
                    "participant_type": None,
                    "participant_id": None,
                    "line": None,
                    "price_decimal": no_price,
                    "price_american": decimal_to_american(no_price),
                    "last_updated_at": updated_at,
                }
            )
    return rows


def market_moneyline_rows(
    fixture_id: int,
    bookmaker_id: int,
    odds_list: List[Dict[str, object]],
    updated_at: Optional[datetime],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for odd in odds_list or []:
        home_price = parse_float(odd.get("home"))
        draw_price = parse_float(odd.get("draw"))
        away_price = parse_float(odd.get("away"))
        if home_price is not None and home_team_id:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": "moneyline",
                    "selection_key": "home",
                    "participant_type": "team",
                    "participant_id": home_team_id,
                    "line": None,
                    "price_decimal": home_price,
                    "price_american": decimal_to_american(home_price),
                    "last_updated_at": updated_at,
                }
            )
        if away_price is not None and away_team_id:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": "moneyline",
                    "selection_key": "away",
                    "participant_type": "team",
                    "participant_id": away_team_id,
                    "line": None,
                    "price_decimal": away_price,
                    "price_american": decimal_to_american(away_price),
                    "last_updated_at": updated_at,
                }
            )
        if draw_price is not None:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": "moneyline",
                    "selection_key": "draw",
                    "participant_type": None,
                    "participant_id": None,
                    "line": None,
                    "price_decimal": draw_price,
                    "price_american": decimal_to_american(draw_price),
                    "last_updated_at": updated_at,
                }
            )
    return rows


def parse_markets_for_fixture(
    fixture: Dict[str, object],
    markets: List[Dict[str, object]],
    bookmaker_name: str,
    market_allowlist: Optional[set],
    session,
    unmatched_details: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    bookmaker_id = BOOKMAKER_NAME_TO_ID.get(normalize_bookmaker_key(bookmaker_name))
    if not bookmaker_id:
        return rows

    fixture_id = int(fixture["fixture_id"])
    home_team_id = fixture.get("home_team_id")
    away_team_id = fixture.get("away_team_id")

    player_map = load_fixture_player_map(session, fixture_id)
    team_player_map = load_team_player_map(session, [home_team_id, away_team_id])
    fuzzy_candidates = build_fuzzy_candidates(session, [home_team_id, away_team_id])

    for market in markets or []:
        market_name = str(market.get("name") or "")
        market_key = resolve_market_key(market_name)
        if not market_key:
            continue
        normalized_key = market_key
        side = None
        if market_key.endswith("_home"):
            normalized_key = market_key.replace("_home", "")
            side = "home"
        elif market_key.endswith("_away"):
            normalized_key = market_key.replace("_away", "")
            side = "away"

        if market_allowlist is not None and normalized_key not in market_allowlist:
            continue

        updated_at = parse_timestamp(market.get("updatedAt"))
        odds_list = market.get("odds") or []

        if normalized_key == "moneyline":
            rows.extend(
                market_moneyline_rows(
                    fixture_id,
                    bookmaker_id,
                    odds_list,
                    updated_at,
                    home_team_id,
                    away_team_id,
                )
            )
            continue

        if normalized_key == "btts":
            rows.extend(market_yes_no_rows(fixture_id, bookmaker_id, normalized_key, odds_list, updated_at))
            continue

        if normalized_key in {"goals_over_under", "match_shots", "match_shots_on_target"}:
            rows.extend(
                market_over_under_rows(
                    fixture_id,
                    bookmaker_id,
                    normalized_key,
                    odds_list,
                    None,
                    None,
                    updated_at,
                )
            )
            continue

        if normalized_key in TEAM_MARKETS:
            team_id = None
            if side == "home":
                team_id = home_team_id
            elif side == "away":
                team_id = away_team_id
            if not team_id:
                continue
            rows.extend(
                market_over_under_rows(
                    fixture_id,
                    bookmaker_id,
                    normalized_key,
                    odds_list,
                    "team",
                    int(team_id),
                    updated_at,
                )
            )
            continue

        if normalized_key.startswith("player_"):
            for odd in odds_list or []:
                label = odd.get("label") or odd.get("name") or ""
                player_name = extract_player_name(str(label))
                if not player_name:
                    continue
                line = parse_float(odd.get("hdp"))
                price = None
                side_key = None
                for key in ("over", "yes", "home"):
                    price = parse_float(odd.get(key))
                    if price is not None:
                        side_key = "over" if key == "over" else key
                        break
                if price is None:
                    continue
                player_id = resolve_player_id(
                    player_name,
                    player_map,
                    team_player_map,
                    team_ids=[home_team_id, away_team_id],
                    fuzzy_candidates=fuzzy_candidates,
                )
                selection_slug = normalize_slug(player_name)
                selection_key = selection_slug if side_key in {"yes", "home", None} else f"{selection_slug}_{side_key}"
                if player_id is None:
                    unmatched_details.append(
                        {
                            "fixture_id": fixture_id,
                            "market_key": normalized_key,
                            "selection_key": selection_key,
                            "raw_name": player_name,
                        }
                    )
                rows.append(
                    {
                        "fixture_id": fixture_id,
                        "bookmaker_id": bookmaker_id,
                        "market_key": normalized_key,
                        "selection_key": selection_key,
                        "participant_type": "player",
                        "participant_id": player_id,
                        "line": line,
                        "price_decimal": price,
                        "price_american": decimal_to_american(price),
                        "last_updated_at": updated_at,
                    }
                )
            continue

    return rows


def parse_double_chance_rows(
    fixture: Dict[str, object],
    bookmaker_id: int,
    odds_list: List[Dict[str, object]],
    updated_at: Optional[datetime],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    fixture_id = int(fixture["fixture_id"])
    home_aliases = fixture.get("home_alias") or []
    away_aliases = fixture.get("away_alias") or []
    for odd in odds_list or []:
        label = normalize_team_name(str(odd.get("label") or ""))
        price = None
        for key in ("under", "over", "price", "odd"):
            price = parse_float(odd.get(key))
            if price is not None:
                break
        if price is None:
            continue
        has_home = any(alias and alias in label for alias in home_aliases)
        has_away = any(alias and alias in label for alias in away_aliases)
        has_draw = "draw" in label or "tie" in label
        selection = None
        if has_home and has_away:
            selection = "home_or_away"
        elif has_draw and has_home:
            selection = "home_or_draw"
        elif has_draw and has_away:
            selection = "draw_or_away"
        if not selection:
            continue
        rows.append(
            {
                "fixture_id": fixture_id,
                "bookmaker_id": bookmaker_id,
                "market_key": "double_chance",
                "selection_key": selection,
                "participant_type": None,
                "participant_id": None,
                "line": None,
                "price_decimal": price,
                "price_american": decimal_to_american(price),
                "last_updated_at": updated_at,
            }
        )
    return rows


def parse_draw_no_bet_rows(
    fixture_id: int,
    bookmaker_id: int,
    odds_list: List[Dict[str, object]],
    updated_at: Optional[datetime],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for odd in odds_list or []:
        home_price = parse_float(odd.get("home"))
        away_price = parse_float(odd.get("away"))
        if home_price is not None and home_team_id:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": "draw_no_bet",
                    "selection_key": "home",
                    "participant_type": "team",
                    "participant_id": home_team_id,
                    "line": None,
                    "price_decimal": home_price,
                    "price_american": decimal_to_american(home_price),
                    "last_updated_at": updated_at,
                }
            )
        if away_price is not None and away_team_id:
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": "draw_no_bet",
                    "selection_key": "away",
                    "participant_type": "team",
                    "participant_id": away_team_id,
                    "line": None,
                    "price_decimal": away_price,
                    "price_american": decimal_to_american(away_price),
                    "last_updated_at": updated_at,
                }
            )
    return rows


def parse_markets_for_fixture_extended(
    fixture: Dict[str, object],
    markets: List[Dict[str, object]],
    bookmaker_name: str,
    market_allowlist: Optional[set],
    session,
    unmatched_details: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    bookmaker_id = BOOKMAKER_NAME_TO_ID.get(normalize_bookmaker_key(bookmaker_name))
    if not bookmaker_id:
        return rows
    fixture_id = int(fixture["fixture_id"])
    home_team_id = fixture.get("home_team_id")
    away_team_id = fixture.get("away_team_id")

    for market in markets or []:
        market_name = str(market.get("name") or "")
        market_key = resolve_market_key(market_name)
        if not market_key:
            continue
        normalized_key = market_key
        side = None
        if market_key.endswith("_home"):
            normalized_key = market_key.replace("_home", "")
            side = "home"
        elif market_key.endswith("_away"):
            normalized_key = market_key.replace("_away", "")
            side = "away"

        if market_allowlist is not None and normalized_key not in market_allowlist:
            continue

        updated_at = parse_timestamp(market.get("updatedAt"))
        odds_list = market.get("odds") or []

        if normalized_key == "moneyline":
            rows.extend(
                market_moneyline_rows(
                    fixture_id,
                    bookmaker_id,
                    odds_list,
                    updated_at,
                    home_team_id,
                    away_team_id,
                )
            )
            continue

        if normalized_key == "double_chance":
            rows.extend(parse_double_chance_rows(fixture, bookmaker_id, odds_list, updated_at))
            continue

        if normalized_key == "draw_no_bet":
            rows.extend(
                parse_draw_no_bet_rows(
                    fixture_id,
                    bookmaker_id,
                    odds_list,
                    updated_at,
                    home_team_id,
                    away_team_id,
                )
            )
            continue

        if normalized_key == "btts":
            rows.extend(market_yes_no_rows(fixture_id, bookmaker_id, normalized_key, odds_list, updated_at))
            continue

        if normalized_key in {"goals_over_under", "match_shots", "match_shots_on_target"}:
            rows.extend(
                market_over_under_rows(
                    fixture_id,
                    bookmaker_id,
                    normalized_key,
                    odds_list,
                    None,
                    None,
                    updated_at,
                )
            )
            continue

        if normalized_key in TEAM_MARKETS:
            team_id = None
            if side == "home":
                team_id = home_team_id
            elif side == "away":
                team_id = away_team_id
            if not team_id:
                continue
            rows.extend(
                market_over_under_rows(
                    fixture_id,
                    bookmaker_id,
                    normalized_key,
                    odds_list,
                    "team",
                    int(team_id),
                    updated_at,
                )
            )
            continue

        if normalized_key.startswith("player_"):
            rows.extend(
                parse_player_market_rows(
                    fixture,
                    normalized_key,
                    odds_list,
                    bookmaker_id,
                    updated_at,
                    session,
                    unmatched_details,
                )
            )
            continue

    return rows


def parse_player_market_rows(
    fixture: Dict[str, object],
    market_key: str,
    odds_list: List[Dict[str, object]],
    bookmaker_id: int,
    updated_at: Optional[datetime],
    session,
    unmatched_details: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    fixture_id = int(fixture["fixture_id"])
    home_team_id = fixture.get("home_team_id")
    away_team_id = fixture.get("away_team_id")

    player_map = load_fixture_player_map(session, fixture_id)
    team_player_map = load_team_player_map(session, [home_team_id, away_team_id])
    fuzzy_candidates = build_fuzzy_candidates(session, [home_team_id, away_team_id])

    rows: List[Dict[str, object]] = []
    for odd in odds_list or []:
        label = odd.get("label") or odd.get("name") or ""
        player_name = extract_player_name(str(label))
        if not player_name:
            continue
        line = parse_float(odd.get("hdp"))
        price = None
        side_key = None
        for key in ("over", "yes", "home"):
            price = parse_float(odd.get(key))
            if price is not None:
                side_key = "over" if key == "over" else key
                break
        if price is None:
            continue
        player_id = resolve_player_id(
            player_name,
            player_map,
            team_player_map,
            team_ids=[home_team_id, away_team_id],
            fuzzy_candidates=fuzzy_candidates,
        )
        selection_slug = normalize_slug(player_name)
        selection_key = selection_slug if side_key in {"yes", "home", None} else f"{selection_slug}_{side_key}"
        if player_id is None:
            unmatched_details.append(
                {
                    "fixture_id": fixture_id,
                    "market_key": market_key,
                    "selection_key": selection_key,
                    "raw_name": player_name,
                }
            )
        rows.append(
            {
                "fixture_id": fixture_id,
                "bookmaker_id": bookmaker_id,
                "market_key": market_key,
                "selection_key": selection_key,
                "participant_type": "player",
                "participant_id": player_id,
                "line": line,
                "price_decimal": price,
                "price_american": decimal_to_american(price),
                "last_updated_at": updated_at,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8,384", help="Comma-separated league IDs")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument(
        "--bookmakers",
        default=",".join(DEFAULT_BOOKMAKERS),
        help="Comma-separated bookmaker names",
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit events processed")
    parser.add_argument("--sport", default="football")
    parser.add_argument(
        "--unmatched-out",
        default="",
        help="Write unmatched player odds to JSON (fixture_id, market_key, selection_key, raw_name)",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="Write sync report JSON",
    )
    parser.add_argument(
        "--debug-events-out",
        default="",
        help="Write raw events JSON",
    )
    parser.add_argument(
        "--debug-odds-out",
        default="",
        help="Write raw odds JSON",
    )
    args = parser.parse_args()

    raw_leagues = args.leagues.replace('"', "").replace("'", "")
    league_ids = [int(x) for x in raw_leagues.split(",") if x.strip()]
    if not league_ids:
        raise SystemExit("No league IDs provided")

    market_allowlist = load_market_allowlist()
    raw_bookmakers = [b.strip() for b in str(args.bookmakers).split(",") if b.strip()]
    if not raw_bookmakers:
        raw_bookmakers = list(DEFAULT_BOOKMAKERS)
    bookmakers, unknown_bookmakers_requested = canonicalize_bookmakers(raw_bookmakers)
    if not bookmakers:
        raise SystemExit("No valid bookmakers provided")
    requested_bookmaker_keys = {normalize_bookmaker_key(b) for b in bookmakers}

    engine = get_engine()
    session = get_session(engine)
    Base.metadata.create_all(engine)

    fixtures = load_fixtures(session, league_ids, args.days_forward)
    if not fixtures:
        log.info("No fixtures found for odds window")
        return

    league_map = load_league_map(Path(__file__).resolve().parent.parent / "config" / "odds_api_leagues.json")

    client = OddsApiClient()

    unmatched_details: List[Dict[str, object]] = []
    event_map: Dict[int, int] = {}
    events_raw: Dict[int, object] = {}
    odds_raw: Dict[int, object] = {}
    events_unmatched_samples: List[Dict[str, object]] = []
    bookmaker_names_seen: set[str] = set()
    bookmaker_names_saved: set[str] = set()
    bookmaker_names_unknown: set[str] = set()
    league_stats: Dict[int, Dict[str, int]] = {}

    for league_id in league_ids:
        odds_league = league_map.get(league_id)
        if not odds_league:
            log.warning("No Odds-API league mapping for league_id=%s (skipping)", league_id)
            continue
        league_fixtures = [f for f in fixtures if f["league_id"] == league_id]
        if not league_fixtures:
            continue
        league_stats[league_id] = {
            "fixtures_in_window": len(league_fixtures),
            "events_returned": 0,
            "events_matched": 0,
        }
        start_dt, end_dt = fixture_window_bounds(args.days_forward)
        params = {
            "sport": args.sport,
            "league": odds_league,
            "status": "pending,live",
            "from": start_dt.isoformat() + "Z",
            "to": end_dt.isoformat() + "Z",
        }
        try:
            events = client.request("events", params=params)
        except OddsApiError as exc:
            log.error("Odds-API events failed for league %s: %s", odds_league, exc)
            continue
        if not isinstance(events, list):
            log.warning("Unexpected events response for league %s", odds_league)
            continue
        events_raw[league_id] = events

        for event in events:
            league_stats[league_id]["events_returned"] += 1
            fixture = match_event_to_fixture(event, league_fixtures)
            if not fixture:
                if len(events_unmatched_samples) < 20:
                    events_unmatched_samples.append(
                        {
                            "league_id": league_id,
                            "event_id": event.get("id"),
                            "home": event.get("home"),
                            "away": event.get("away"),
                            "date": event.get("date"),
                        }
                    )
                continue
            event_id = event.get("id")
            if event_id is None:
                continue
            league_stats[league_id]["events_matched"] += 1
            event_map[int(event_id)] = fixture["fixture_id"]

    if not event_map:
        log.info("No events matched to fixtures")
        return

    event_ids = list(event_map.keys())
    if args.limit and args.limit > 0:
        event_ids = event_ids[: args.limit]
    log.info("Matched %s events to fixtures", len(event_ids))

    outcomes_total = 0
    batches = [event_ids[i : i + 10] for i in range(0, len(event_ids), 10)]
    for batch in batches:
        params = {
            "eventIds": ",".join(str(e) for e in batch),
            "bookmakers": ",".join(bookmakers),
        }
        try:
            odds_batch = client.request("odds/multi", params=params)
        except OddsApiError as exc:
            log.error("Odds-API odds/multi failed for events %s: %s", batch[:3], exc)
            continue
        if not isinstance(odds_batch, list):
            continue
        for odds_event in odds_batch:
            event_id = odds_event.get("id")
            if event_id is None:
                continue
            fixture_id = event_map.get(int(event_id))
            if not fixture_id:
                continue
            fixture = next((f for f in fixtures if f["fixture_id"] == fixture_id), None)
            if not fixture:
                continue
            bookmakers_payload = odds_event.get("bookmakers") or {}
            if args.debug_odds_out:
                odds_raw[int(event_id)] = bookmakers_payload
            for bookmaker_name, markets in bookmakers_payload.items():
                bookmaker_names_seen.add(bookmaker_name)
                book_key = normalize_bookmaker_key(bookmaker_name)
                if book_key not in requested_bookmaker_keys:
                    continue
                if book_key not in BOOKMAKER_NAME_TO_ID:
                    bookmaker_names_unknown.add(bookmaker_name)
                    continue
                canonical_name = BOOKMAKER_CANONICAL.get(book_key, bookmaker_name)
                rows = parse_markets_for_fixture_extended(
                    fixture,
                    markets,
                    canonical_name,
                    market_allowlist,
                    session,
                    unmatched_details,
                )
                if rows:
                    upsert_outcomes(session, rows)
                    outcomes_total += len(rows)
                    bookmaker_names_saved.add(canonical_name)
            session.commit()

    log.info("Odds sync complete: outcomes=%s", outcomes_total)

    if args.unmatched_out:
        try:
            with open(args.unmatched_out, "w", encoding="utf-8") as f:
                json.dump(unmatched_details, f, indent=2)
            log.info("Wrote unmatched player odds to %s (%s rows)", args.unmatched_out, len(unmatched_details))
        except OSError as exc:
            log.warning("Failed to write unmatched output %s: %s", args.unmatched_out, exc)

    if args.debug_events_out:
        try:
            with open(args.debug_events_out, "w", encoding="utf-8") as f:
                json.dump(events_raw, f, indent=2)
        except OSError as exc:
            log.warning("Failed to write debug events %s: %s", args.debug_events_out, exc)

    if args.debug_odds_out:
        try:
            with open(args.debug_odds_out, "w", encoding="utf-8") as f:
                json.dump(odds_raw, f, indent=2)
        except OSError as exc:
            log.warning("Failed to write debug odds %s: %s", args.debug_odds_out, exc)

    if args.report_out:
        report = {
            "league_ids": league_ids,
            "bookmakers": bookmakers,
            "bookmakers_requested": bookmakers,
            "bookmakers_unknown_requested": unknown_bookmakers_requested,
            "bookmakers_seen": sorted(bookmaker_names_seen),
            "bookmakers_saved": sorted(bookmaker_names_saved),
            "bookmakers_unknown_seen": sorted(bookmaker_names_unknown),
            "events_matched": len(event_map),
            "events_unmatched_samples": events_unmatched_samples,
            "league_stats": league_stats,
            "outcomes_written": outcomes_total,
            "api_calls_total": client.stats.total_calls,
            "api_calls_by_endpoint": client.stats.calls_by_endpoint,
            "api_time_seconds": round(client.stats.api_time_seconds, 2),
            "rate_limit_hits": client.stats.rate_limit_hits,
            "rate_limit_sleeps": client.stats.rate_limit_sleeps,
            "last_rate_limit": client.stats.last_rate_limit,
            "unmatched_players": len(unmatched_details),
        }
        Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
