#!/usr/bin/env python3
"""
Fetch Bet365 pre-match odds from SportMonks and store into SQLite.
- Reads fixture IDs from the local SQLite fixtures table for the next N days
- Calls /v3/football/odds/pre-match/fixtures/{fixture_id}?filter=bookmakers:2
- Stores raw snapshots and normalized outcomes in odds_snapshots/odds_outcomes
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import unicodedata
import difflib
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import bindparam, text

from jxd import SportMonksClient
from jxd.sportmonks_client import SportMonksError
from jxd import SyncService
from jxd.db import get_engine, get_session
from jxd.models import Base

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

MARKET_ID_MAP = {
    267: "player_shots_on_target",
    268: "player_shots",
    331: "player_to_score",
    332: "player_to_assist",
    333: "player_to_score_or_assist",
}

CANONICAL_TEAM_MARKETS = {
    "moneyline",
    "double_chance",
    "draw_no_bet",
    "handicap",
    "card_handicap",
    "team_shots",
    "team_shots_on_target",
    "team_cards",
    "team_corners",
    "team_most_cards",
    "team_most_corners",
    "team_most_shots",
    "team_most_shots_on_target",
    "to_score_a_penalty",
}

CANONICAL_MATCH_MARKETS = {
    "goals_over_under",
    "btts",
    "corners_over_under",
    "total_offsides",
    "match_shots",
    "match_shots_on_target",
    "match_cards",
}

CANONICAL_PLAYER_MARKETS = {
    "1st_goal_scorer",
    "player_to_score",
    "player_to_assist",
    "player_to_score_or_assist",
    "player_card",
    "player_shots",
    "player_shots_on_target",
    "player_goalkeeper_saves",
    "multi_scorers",
    "last_goal_scorer",
}

DEFAULT_MARKET_ALLOWLIST = {
    "moneyline",
    "double_chance",
    "draw_no_bet",
    "handicap",
    "card_handicap",
    "goals_over_under",
    "btts",
    "corners_over_under",
    "total_offsides",
    "match_shots",
    "match_shots_on_target",
    "match_cards",
    "team_shots",
    "team_shots_on_target",
    "team_cards",
    "team_corners",
    "team_most_cards",
    "team_most_corners",
    "team_most_shots",
    "team_most_shots_on_target",
    "player_shots",
    "player_shots_on_target",
    "player_to_assist",
    "player_to_score_or_assist",
    "player_card",
    "player_goalkeeper_saves",
    "full_time_result",
}


def load_market_allowlist() -> Optional[set]:
    raw = (os.environ.get("ODDS_MARKET_ALLOWLIST") or "").strip()
    if raw:
        if raw.lower() in {"all", "*"}:
            return None
        items = {normalize_slug(item) for item in raw.split(",") if item.strip()}
        return {item for item in items if item}
    return set(DEFAULT_MARKET_ALLOWLIST)


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


def merge_selection_text(raw_name: str, raw_label: str) -> str:
    name = (raw_name or "").strip()
    label = (raw_label or "").strip()
    if not name:
        return label
    if not label:
        return name
    if normalize_name(name) == normalize_name(label):
        return name
    return f"{name} {label}".strip()


def parse_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        try:
            return float(str(value).replace("%", ""))
        except Exception:
            return None


def parse_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def parse_line(row: Dict) -> Optional[float]:
    for key in ("line", "handicap", "total", "label"):
        raw = row.get(key)
        if raw is None:
            continue
        if isinstance(raw, (int, float)):
            return float(raw)
        text_val = str(raw).strip()
        match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text_val)
        if match:
            return float(match.group(1))
    return None


def parse_line_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    match = re.search(r"([0-9]+(?:[\\._][0-9]+)?)", text)
    if not match:
        return None
    raw = match.group(1)
    if "_" in raw:
        parts = raw.split("_", 1)
        if len(parts) == 2 and parts[0] and parts[1].isdigit():
            return float(f"{parts[0]}.{parts[1]}")
    return float(raw.replace("_", "."))


def parse_line_side_from_selection_key(selection_key: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    if not selection_key:
        return None, None, None
    key = selection_key.lower().strip()
    pattern_1 = re.match(
        r"^(over|under)_([0-9]+)(?:_([0-9]+))?(?:_(?:team_)?(1|2))?$",
        key,
    )
    if pattern_1:
        direction = pattern_1.group(1)
        whole = pattern_1.group(2)
        frac = pattern_1.group(3)
        side = pattern_1.group(4)
        line = float(f"{whole}.{frac}") if frac else float(whole)
        return line, direction, side
    pattern_2 = re.match(
        r"^([0-9]+)(?:_([0-9]+))?_(over|under)(?:_(?:team_)?(1|2))?$",
        key,
    )
    if pattern_2:
        whole = pattern_2.group(1)
        frac = pattern_2.group(2)
        direction = pattern_2.group(3)
        side = pattern_2.group(4)
        line = float(f"{whole}.{frac}") if frac else float(whole)
        return line, direction, side
    return None, None, None


def extract_side_from_tokens(tokens: List[str]) -> Optional[str]:
    if not tokens:
        return None
    token_set = set(tokens)
    if "team1" in token_set or ("team" in token_set and "1" in token_set):
        return "1"
    if "team2" in token_set or ("team" in token_set and "2" in token_set):
        return "2"
    if "home" in token_set or "host" in token_set or "hosts" in token_set:
        return "1"
    if "away" in token_set or "visitor" in token_set or "visitors" in token_set or "guest" in token_set:
        return "2"
    return None


def parse_timestamp(value: Optional[object]) -> Optional[datetime]:
    if not value:
        return None
    text_val = str(value).replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(text_val)
    except Exception:
        return None


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


def resolve_market_key(row: Dict) -> str:
    market_id = parse_int(row.get("market_id"))
    if market_id in MARKET_ID_MAP:
        return MARKET_ID_MAP[market_id]

    desc = str(row.get("market_description") or row.get("market") or "")
    desc_lower = desc.lower()

    if "full time result" in desc_lower or "full-time result" in desc_lower or "fulltime result" in desc_lower:
        return "moneyline"
    if "match result" in desc_lower:
        return "moneyline"
    if "draw no bet" in desc_lower or "dnb" in desc_lower:
        return "draw_no_bet"
    if "double chance" in desc_lower:
        return "double_chance"
    if "both teams to score" in desc_lower or "btts" in desc_lower:
        return "btts"
    if "moneyline" in desc_lower or "1x2" in desc_lower or "match winner" in desc_lower:
        return "moneyline"
    if "win-draw-win" in desc_lower or "1x2 (90" in desc_lower:
        return "moneyline"

    if "handicap" in desc_lower and "card" in desc_lower:
        return "card_handicap"
    if "handicap" in desc_lower:
        return "handicap"

    if "corner" in desc_lower and ("over" in desc_lower or "under" in desc_lower or "total" in desc_lower):
        return "corners_over_under"
    if "corner" in desc_lower and "most" in desc_lower:
        return "team_most_corners"

    if "player" in desc_lower and ("booked" in desc_lower or "card" in desc_lower):
        return "player_card"
    if "card" in desc_lower and "most" in desc_lower:
        return "team_most_cards"
    if "card" in desc_lower and "team" in desc_lower:
        return "team_cards"
    if "card" in desc_lower:
        return "match_cards"

    if "offsides" in desc_lower or "offside" in desc_lower:
        return "total_offsides"

    if "shots on target" in desc_lower:
        if "player" in desc_lower:
            return "player_shots_on_target"
        if "team" in desc_lower:
            return "team_shots_on_target"
        return "match_shots_on_target"

    if "shots" in desc_lower:
        if "player" in desc_lower:
            return "player_shots"
        if "team" in desc_lower:
            return "team_shots"
        if "match" in desc_lower or "total" in desc_lower:
            return "match_shots"

    if "goalkeeper" in desc_lower and "save" in desc_lower:
        return "player_goalkeeper_saves"

    if "score or assist" in desc_lower:
        return "player_to_score_or_assist"
    if "assist" in desc_lower and "player" in desc_lower:
        return "player_to_assist"
    if "to score" in desc_lower and "penalty" in desc_lower:
        return "to_score_a_penalty"
    if "to score" in desc_lower or "anytime goalscorer" in desc_lower or "anytime goal scorer" in desc_lower:
        return "player_to_score"

    if ("first goal" in desc_lower or "1st goal" in desc_lower) and "scorer" in desc_lower:
        return "1st_goal_scorer"
    if "last goal" in desc_lower and "scorer" in desc_lower:
        return "last_goal_scorer"
    if "multi" in desc_lower and "goal" in desc_lower and "scorer" in desc_lower:
        return "multi_scorers"

    if "goal line" in desc_lower:
        return "goals_over_under"
    if "goals" in desc_lower and ("over" in desc_lower or "under" in desc_lower or "total" in desc_lower):
        return "goals_over_under"

    return normalize_slug(desc or "market")


PLAYER_MARKET_KEYS = set(CANONICAL_PLAYER_MARKETS) | {
    "goalscorers",
}

TEAM_MARKET_KEYS = set(CANONICAL_TEAM_MARKETS)

MATCH_MARKET_KEYS = set(CANONICAL_MATCH_MARKETS)

TEAM_TOTAL_MARKETS = {
    "team_shots",
    "team_shots_on_target",
    "team_cards",
    "team_corners",
}


def resolve_participant_type(row: Dict, market_key: str) -> Optional[str]:
    desc = str(row.get("market_description") or row.get("market") or "").lower()
    if market_key in MATCH_MARKET_KEYS:
        return None
    if market_key in TEAM_MARKET_KEYS:
        return "team"
    if "team" in desc or market_key.startswith("team_"):
        return "team"
    if "player" in desc or market_key.startswith("player_") or market_key in PLAYER_MARKET_KEYS:
        return "player"
    if any(
        token in market_key
        for token in (
            "goalscorer",
            "goal_scorer",
            "scorer",
            "to_score",
            "to_assist",
            "assist",
            "shots",
            "shot",
            "sot",
        )
    ):
        return "player"
    return None


def normalize_selection_tokens(value: str) -> List[str]:
    return normalize_name_tokens(value)


def detect_yes_no(tokens: List[str]) -> Optional[str]:
    if "yes" in tokens:
        return "yes"
    if "no" in tokens:
        return "no"
    return None


def detect_over_under(tokens: List[str]) -> Optional[str]:
    if "over" in tokens:
        return "over"
    if "under" in tokens:
        return "under"
    return None


def normalize_team_side_selection(
    text: str,
    home_aliases: Iterable[str],
    away_aliases: Iterable[str],
) -> Optional[str]:
    tokens = normalize_selection_tokens(text)
    if "draw" in tokens or "tie" in tokens or "x" in tokens:
        return "draw"
    if "home" in tokens or "host" in tokens or "hosts" in tokens or "local" in tokens:
        return "home"
    if "away" in tokens or "visitor" in tokens or "visitors" in tokens or "guest" in tokens:
        return "away"
    if "team1" in tokens or "team_1" in tokens or "1" in tokens:
        return "home"
    if "team2" in tokens or "team_2" in tokens or "2" in tokens:
        return "away"

    normalized = normalize_name(text)
    for alias in home_aliases:
        if alias and alias in normalized:
            return "home"
    for alias in away_aliases:
        if alias and alias in normalized:
            return "away"
    return None


def normalize_double_chance_selection(
    text: str,
    home_aliases: Iterable[str],
    away_aliases: Iterable[str],
) -> Optional[str]:
    raw = (text or "").replace(" ", "").upper()
    if "1X" in raw or "X1" in raw:
        return "home_or_draw"
    if "X2" in raw or "2X" in raw:
        return "draw_or_away"
    if "12" in raw or "1-2" in raw or "1&2" in raw:
        return "home_or_away"
    tokens = normalize_selection_tokens(text)
    if "home" in tokens and "draw" in tokens:
        return "home_or_draw"
    if "away" in tokens and "draw" in tokens:
        return "draw_or_away"
    if "home" in tokens and "away" in tokens:
        return "home_or_away"
    normalized = normalize_name(text)
    has_home = any(alias and alias in normalized for alias in home_aliases)
    has_away = any(alias and alias in normalized for alias in away_aliases)
    if has_home and has_away:
        return "home_or_away"
    if has_home and ("draw" in tokens or "tie" in tokens):
        return "home_or_draw"
    if has_away and ("draw" in tokens or "tie" in tokens):
        return "draw_or_away"
    return None


def normalize_selection_key(
    market_key: str,
    raw_name: str,
    raw_label: str,
    home_aliases: Iterable[str],
    away_aliases: Iterable[str],
) -> Optional[str]:
    text = merge_selection_text(raw_name, raw_label)
    tokens = normalize_selection_tokens(text)

    if market_key in {"btts"}:
        return detect_yes_no(tokens)

    if market_key in {
        "goals_over_under",
        "corners_over_under",
        "match_shots",
        "match_shots_on_target",
        "match_cards",
        "total_offsides",
        "team_shots",
        "team_shots_on_target",
        "team_cards",
        "team_corners",
    }:
        return detect_over_under(tokens)

    if market_key == "double_chance":
        return normalize_double_chance_selection(text, home_aliases, away_aliases)

    if market_key in {
        "moneyline",
        "draw_no_bet",
        "handicap",
        "card_handicap",
        "team_most_cards",
        "team_most_corners",
        "team_most_shots",
        "team_most_shots_on_target",
        "to_score_a_penalty",
    }:
        return normalize_team_side_selection(text, home_aliases, away_aliases)

    return None


def fixture_window_bounds(days_forward: int) -> Tuple[str, str]:
    start_dt = datetime.utcnow()
    end_dt = start_dt + timedelta(days=days_forward)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start_dt.strftime(fmt), end_dt.strftime(fmt)


def fetch_fixture_rows(
    session,
    league_ids: List[int],
    start_dt: str,
    end_dt: str,
) -> List[Dict]:
    league_list = ",".join(str(x) for x in league_ids)
    rows = session.execute(
        text(
            f"""
            select id, home_team_id, away_team_id
            from fixtures
            where league_id in ({league_list})
              and datetime(starting_at) >= :start_dt
              and datetime(starting_at) < :end_dt
            """
        ),
        {"start_dt": start_dt, "end_dt": end_dt},
    ).fetchall()
    return [
        {"fixture_id": r[0], "home_team_id": r[1], "away_team_id": r[2]} for r in rows
    ]


def load_team_context(
    session,
    team_ids: Iterable[int],
) -> Tuple[Dict[str, int], Dict[int, List[str]]]:
    ids = [int(x) for x in team_ids if x]
    if not ids:
        return {}, {}
    stmt = text("select id, name, short_code from teams where id in :ids").bindparams(
        bindparam("ids", expanding=True),
    )
    rows = session.execute(stmt, {"ids": ids}).fetchall()
    mapping: Dict[str, int] = {}
    aliases: Dict[int, List[str]] = {}
    for team_id, name, short_code in rows:
        alias_list: List[str] = []
        for label in (name, short_code):
            if not label:
                continue
            normalized = normalize_name(str(label))
            if not normalized:
                continue
            mapping[normalized] = int(team_id)
            alias_list.append(normalized)
        aliases[int(team_id)] = alias_list
    return mapping, aliases


def fetch_team_player_counts(session, team_ids: Iterable[int]) -> Dict[int, int]:
    ids = [int(x) for x in team_ids if x]
    if not ids:
        return {}
    stmt = text(
        """
        select team_id, count(*) as cnt
        from players
        where team_id in :team_ids
        group by team_id
        """
    ).bindparams(bindparam("team_ids", expanding=True))
    rows = session.execute(stmt, {"team_ids": ids}).fetchall()
    return {int(team_id): int(count) for team_id, count in rows}


def load_team_player_names(
    session,
    team_ids: Iterable[int],
    limit: int = 50,
) -> Dict[int, List[str]]:
    ids = [int(x) for x in team_ids if x]
    if not ids:
        return {}
    stmt = text(
        """
        select team_id, name, common_name, short_name
        from players
        where team_id in :team_ids
        order by name
        """
    ).bindparams(bindparam("team_ids", expanding=True))
    rows = session.execute(stmt, {"team_ids": ids}).fetchall()
    names: Dict[int, List[str]] = {}
    for team_id, name, common_name, short_name in rows:
        tid = int(team_id) if team_id else None
        if tid is None:
            continue
        for candidate in (name, common_name, short_name):
            if not candidate:
                continue
            names.setdefault(tid, [])
            if candidate not in names[tid]:
                names[tid].append(candidate)
                if limit and len(names[tid]) >= limit:
                    break
    return names


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


def resolve_player_id(
    raw_name: str,
    fixture_map: Dict[str, List[Tuple[int, Optional[int]]]],
    team_map: Dict[str, List[Tuple[int, Optional[int]]]],
    team_ids: Optional[Iterable[int]] = None,
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
    return None


HOME_ALIASES = {
    "home",
    "hometeam",
    "team1",
    "team_1",
    "host",
    "hosts",
    "local",
    "homeclub",
}
AWAY_ALIASES = {
    "away",
    "awayteam",
    "team2",
    "team_2",
    "visitor",
    "visitors",
    "guest",
    "awayclub",
}


def resolve_team_id(
    raw_name: str,
    selection_key: str,
    team_map: Dict[str, int],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_aliases: Iterable[str],
    away_aliases: Iterable[str],
) -> Optional[int]:
    selection_key = selection_key or ""
    if selection_key in {"1", "team_1", "team1"} and home_team_id:
        return int(home_team_id)
    if selection_key in {"2", "team_2", "team2"} and away_team_id:
        return int(away_team_id)
    tokens = [token for token in selection_key.lower().split("_") if token]
    home_tokens = {"home", "host", "hosts", "local"}
    away_tokens = {"away", "visitor", "visitors", "guest"}
    if home_tokens.intersection(tokens) and not away_tokens.intersection(tokens) and home_team_id:
        return int(home_team_id)
    if away_tokens.intersection(tokens) and not home_tokens.intersection(tokens) and away_team_id:
        return int(away_team_id)
    normalized_name = normalize_name(raw_name)
    if normalized_name and normalized_name in team_map:
        return team_map[normalized_name]
    if normalized_name in HOME_ALIASES and home_team_id:
        return int(home_team_id)
    if normalized_name in AWAY_ALIASES and away_team_id:
        return int(away_team_id)
    normalized_selection = normalize_name(selection_key)
    if normalized_selection in HOME_ALIASES and home_team_id:
        return int(home_team_id)
    if normalized_selection in AWAY_ALIASES and away_team_id:
        return int(away_team_id)
    for alias in home_aliases:
        if alias and alias in normalized_selection and home_team_id:
            return int(home_team_id)
    for alias in away_aliases:
        if alias and alias in normalized_selection and away_team_id:
            return int(away_team_id)
    return None


def resolve_team_id_from_label(
    raw_label: str,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_aliases: Iterable[str],
    away_aliases: Iterable[str],
) -> Optional[int]:
    side = normalize_team_side_selection(raw_label or "", home_aliases, away_aliases)
    if side == "home" and home_team_id:
        return int(home_team_id)
    if side == "away" and away_team_id:
        return int(away_team_id)
    return None


NON_PLAYER_MARKETS = {
    "match_shots",
    "match_shots_on_target",
    "to_score_in_half",
}


def is_non_player_selection(selection_key: str, market_key: str) -> bool:
    selection_key = selection_key or ""
    if market_key in NON_PLAYER_MARKETS:
        return True
    if selection_key.startswith(("no_goalscorer", "no_goal", "no_goals")):
        return True
    if "1st_half" in selection_key or "2nd_half" in selection_key:
        return True
    if selection_key.startswith(
        (
            "home_yes",
            "home_no",
            "away_yes",
            "away_no",
            "draw_yes",
            "draw_no",
            "tie_yes",
            "tie_no",
            "yes_yes",
            "no_no",
        )
    ):
        return True
    return False


def is_generic_team_prop(selection_key: str) -> bool:
    selection_key = selection_key or ""
    if re.fullmatch(r"\d+_\d+", selection_key):
        return True
    if selection_key in {"yes_yes", "no_no"}:
        return True
    if selection_key in {"tie_tie", "draw_draw"}:
        return True
    return False


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


def parse_outcomes(
    fixture_id: int,
    bookmaker_id: int,
    data: List[Dict],
    player_map: Dict[str, List[Tuple[int, Optional[int]]]],
    team_map: Dict[str, int],
    team_player_map: Dict[str, List[Tuple[int, Optional[int]]]],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_aliases: Iterable[str],
    away_aliases: Iterable[str],
    fuzzy_candidates: Optional[List[Dict[str, object]]] = None,
    unmatched_details: Optional[List[Dict]] = None,
    unmatched_counts: Optional[Dict[Tuple[str, str], int]] = None,
    unmatched_team_counts: Optional[Dict[str, int]] = None,
    unmatched_selection_counts: Optional[Dict[str, int]] = None,
    debug_samples: Optional[List[Dict]] = None,
    debug_team_players: Optional[Dict[int, List[str]]] = None,
    debug_limit: int = 0,
    debug_fuzzy_matches: Optional[List[Dict]] = None,
    market_allowlist: Optional[set] = None,
    allowlist_counts: Optional[Dict[str, int]] = None,
) -> List[Dict]:
    outcomes: List[Dict] = []
    line_markets = {
        "team_shots",
        "team_shots_on_target",
        "match_shots",
        "match_shots_on_target",
        "goals_over_under",
        "corners_over_under",
    }

    for row in data:
        market_key = resolve_market_key(row)
        if market_allowlist is not None and market_key not in market_allowlist:
            if allowlist_counts is not None:
                allowlist_counts["dropped"] = allowlist_counts.get("dropped", 0) + 1
            continue
        participant_type = resolve_participant_type(row, market_key)
        name = row.get("name") or row.get("total") or row.get("label") or ""
        label = row.get("label") or row.get("total") or ""
        total = row.get("total")
        selection_text = merge_selection_text(str(name), str(label))
        if total is not None:
            total_text = str(total).strip()
            if total_text and total_text not in selection_text:
                selection_text = f"{selection_text} {total_text}".strip()
        raw_selection_key = normalize_slug(selection_text)
        selection_key = raw_selection_key
        canonical_selection = normalize_selection_key(
            market_key,
            str(name),
            str(label),
            home_aliases,
            away_aliases,
        )
        if canonical_selection:
            selection_key = canonical_selection

        line = parse_line(row)
        if line is None:
            line = parse_line_from_text(selection_text)
        key_line, key_direction, key_side = parse_line_side_from_selection_key(raw_selection_key)
        if key_side is None:
            key_side = extract_side_from_tokens(normalize_selection_tokens(selection_text))
        if key_line is not None and market_key in line_markets:
            line = key_line
        if key_direction in {"over", "under"} and market_key in line_markets:
            selection_key = key_direction
        if (
            market_key in TEAM_TOTAL_MARKETS
            and key_side in {"1", "2"}
            and key_line is None
            and line in (1.0, 2.0)
        ):
            line = None
        price_decimal = parse_float(row.get("value") or row.get("dp3"))
        if price_decimal is None:
            continue
        price_american = parse_int(row.get("american"))
        last_updated_at = parse_timestamp(row.get("latest_bookmaker_update") or row.get("updated_at"))

        participant_id = None
        raw_participant_id = parse_int(row.get("participant_id") or row.get("player_id"))
        non_player_selection = is_non_player_selection(selection_key, market_key)
        generic_team_prop = is_generic_team_prop(selection_key)
        team_id_candidate = resolve_team_id(
            str(name),
            selection_key,
            team_map,
            home_team_id,
            away_team_id,
            home_aliases,
            away_aliases,
        )
        if market_key in TEAM_TOTAL_MARKETS and key_side in {"1", "2"}:
            side_team_id = home_team_id if key_side == "1" else away_team_id
            if side_team_id:
                team_id_candidate = int(side_team_id)
        if team_id_candidate is None and market_key in TEAM_TOTAL_MARKETS:
            team_id_candidate = resolve_team_id_from_label(
                str(label),
                home_team_id,
                away_team_id,
                home_aliases,
                away_aliases,
            )
        neutral_team_selection = selection_key in {"draw", "home_or_away"}
        if neutral_team_selection:
            team_id_candidate = None
        if participant_type == "player" and non_player_selection:
            participant_type = None
        if participant_type == "team":
            if selection_key in {"draw", "home_or_away"}:
                participant_type = None
            if generic_team_prop or team_id_candidate is None:
                participant_type = None
        if market_key.startswith("match_"):
            participant_type = None
        if participant_type is None and team_id_candidate is not None and not generic_team_prop:
            participant_type = "team"
        if participant_type == "player":
            mapped_name = extract_player_name(str(name))
            if raw_participant_id is not None:
                participant_id = raw_participant_id
            else:
                participant_id = resolve_player_id(
                    mapped_name,
                    player_map,
                    team_player_map,
                    team_ids=[home_team_id, away_team_id],
                )
                if participant_id is None and fuzzy_candidates:
                    fuzzy_match = fuzzy_match_player(mapped_name, fuzzy_candidates)
                    if fuzzy_match:
                        candidate, score = fuzzy_match
                        participant_id = int(candidate["player_id"])
                        if debug_fuzzy_matches is not None:
                            debug_fuzzy_matches.append(
                                {
                                    "fixture_id": fixture_id,
                                    "market_key": market_key,
                                    "selection_key": selection_key,
                                    "raw_name": str(name),
                                    "mapped_name": mapped_name,
                                    "matched_name": candidate.get("name"),
                                    "matched_player_id": participant_id,
                                    "matched_team_id": candidate.get("team_id"),
                                    "score": round(float(score), 3),
                                }
                            )
            if participant_id is None:
                if unmatched_counts is not None:
                    key = (market_key, str(name))
                    unmatched_counts[key] = unmatched_counts.get(key, 0) + 1
                if unmatched_selection_counts is not None:
                    unmatched_selection_counts[selection_key] = unmatched_selection_counts.get(selection_key, 0) + 1
                if unmatched_details is not None:
                    unmatched_details.append(
                        {
                            "fixture_id": fixture_id,
                            "market_key": market_key,
                            "selection_key": selection_key,
                            "raw_name": str(name),
                        }
                    )
                if (
                    debug_samples is not None
                    and debug_team_players is not None
                    and debug_limit > 0
                    and len(debug_samples) < debug_limit
                ):
                    debug_samples.append(
                        {
                            "fixture_id": fixture_id,
                            "market_key": market_key,
                            "selection_key": selection_key,
                            "raw_name": str(name),
                            "mapped_name": mapped_name,
                            "home_team_id": home_team_id,
                            "away_team_id": away_team_id,
                            "home_team_players": debug_team_players.get(home_team_id, [])[:20],
                            "away_team_players": debug_team_players.get(away_team_id, [])[:20],
                        }
                    )
        elif participant_type == "team":
            participant_id = team_id_candidate
            if participant_id is None and unmatched_team_counts is not None:
                unmatched_team_counts[selection_key] = unmatched_team_counts.get(selection_key, 0) + 1

        outcomes.append(
            {
                "fixture_id": fixture_id,
                "bookmaker_id": bookmaker_id,
                "market_key": market_key,
                "selection_key": selection_key,
                "participant_type": participant_type,
                "participant_id": participant_id,
                "line": line,
                "price_decimal": price_decimal,
                "price_american": price_american,
                "last_updated_at": last_updated_at,
            }
        )
    return outcomes


def fetch_odds_for_fixture(client: SportMonksClient, fixture_id: int, bookmaker_id: int) -> List[Dict]:
    try:
        payload = client.request(
            "GET",
            f"odds/pre-match/fixtures/{fixture_id}",
            params={"filter": f"bookmakers:{bookmaker_id}"},
        )
    except SportMonksError as exc:
        status = exc.status_code
        if status in {404, 422}:
            log.info("No odds available for fixture %s (status %s)", fixture_id, status)
            return []
        raise
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, list) else []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--leagues",
        default="8,384",
        help="Comma-separated league IDs",
    )
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--bookmaker-id", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=0, help="Limit fixtures processed")
    parser.add_argument(
        "--debug-fixture",
        type=int,
        default=0,
        help="Debug a single fixture's odds payload (no DB writes).",
    )
    parser.add_argument(
        "--refresh-upcoming",
        action="store_true",
        help="Refresh upcoming fixtures for the league ids before fetching odds",
    )
    parser.add_argument(
        "--refresh-squads",
        action="store_true",
        help="Refresh team squads for upcoming fixtures to improve player mapping",
    )
    parser.add_argument(
        "--refresh-squads-missing",
        dest="refresh_squads_missing",
        action="store_true",
        help="Refresh squads for teams missing players (default).",
    )
    parser.add_argument(
        "--no-refresh-squads-missing",
        dest="refresh_squads_missing",
        action="store_false",
        help="Disable auto refresh of squads for missing teams.",
    )
    parser.add_argument(
        "--unmatched-out",
        default="",
        help="Write unmatched player odds to JSON (fixture_id, market_key, selection_key, raw_name)",
    )
    parser.add_argument(
        "--debug-mapping-out",
        default="",
        help="Write mapping debug samples to JSON.",
    )
    parser.add_argument(
        "--debug-mapping-limit",
        type=int,
        default=100,
        help="Max debug samples to capture.",
    )
    parser.add_argument(
        "--debug-parse-examples",
        action="store_true",
        help="Print line/side parsing examples and exit.",
    )
    parser.set_defaults(refresh_squads_missing=True)
    args = parser.parse_args()

    if args.debug_parse_examples:
        examples = [
            "25_5_over",
            "8_5_under",
            "over_10_5_2",
            "under_3_5_1",
            "over_10_5_team_2",
        ]
        for raw in examples:
            normalized = normalize_slug(raw)
            line, direction, side = parse_line_side_from_selection_key(normalized)
            print(
                f"{raw} -> normalized={normalized} line={line} direction={direction} side={side}",
                flush=True,
            )
        raise SystemExit(0)

    raw_leagues = args.leagues.replace('"', "").replace("'", "")
    league_ids = [int(x) for x in raw_leagues.split(",") if x.strip()]
    if not league_ids:
        raise SystemExit("No league IDs provided")

    client = SportMonksClient()
    if args.debug_fixture:
        fixture_id = int(args.debug_fixture)
        data = fetch_odds_for_fixture(client, fixture_id, args.bookmaker_id)
        log.info("Debug fixture %s markets=%s", fixture_id, len(data))
        markets = {}
        shot_rows = []
        shot_total_rows = []
        shot_total_keys = {
            "team_shots",
            "team_shots_on_target",
            "match_shots",
            "match_shots_on_target",
        }
        for row in data:
            market_id = parse_int(row.get("market_id"))
            market_desc = str(row.get("market_description") or row.get("market") or "")
            market_key = resolve_market_key(row)
            markets.setdefault((market_id, market_desc, market_key), 0)
            markets[(market_id, market_desc, market_key)] += 1
            desc_lower = market_desc.lower()
            selection_text = merge_selection_text(str(row.get("name") or ""), str(row.get("label") or ""))
            selection_lower = selection_text.lower()
            if (
                "shot" in desc_lower
                or "on target" in desc_lower
                or "shot" in selection_lower
                or "on target" in selection_lower
            ):
                shot_rows.append(
                    {
                        "market_id": market_id,
                        "market_description": market_desc,
                        "market_key": market_key,
                        "name": row.get("name"),
                        "label": row.get("label"),
                        "total": row.get("total"),
                        "selection_key": normalize_slug(selection_text),
                        "value": row.get("value"),
                        "american": row.get("american"),
                    }
                )
            if market_key in shot_total_keys:
                shot_total_rows.append(
                    {
                        "market_id": market_id,
                        "market_description": market_desc,
                        "market_key": market_key,
                        "name": row.get("name"),
                        "label": row.get("label"),
                        "total": row.get("total"),
                        "selection_key": normalize_slug(selection_text),
                        "value": row.get("value"),
                        "american": row.get("american"),
                    }
                )
        for (market_id, market_desc, market_key), count in sorted(markets.items(), key=lambda item: item[0][1]):
            log.info("market_id=%s market_key=%s desc=%s outcomes=%s", market_id, market_key, market_desc, count)
        if shot_rows:
            log.info("Shot-related markets (first 200 rows):")
            for row in shot_rows[:200]:
                log.info("%s", row)
        else:
            log.info("No shot-related markets found for fixture %s", fixture_id)
        if shot_total_rows:
            log.info("Shot totals markets (first 200 rows):")
            for row in shot_total_rows[:200]:
                log.info("%s", row)
        else:
            log.info("No shot totals markets (team/match) found for fixture %s", fixture_id)
        return

    engine = get_engine()
    session = get_session(engine)
    Base.metadata.create_all(engine)

    svc = None
    if args.refresh_upcoming or args.refresh_squads or args.refresh_squads_missing:
        svc = SyncService(client, session)
        svc.ensure_schema()
        if args.refresh_upcoming:
            log.info("Refreshing upcoming fixtures for odds window (%s days)", args.days_forward)
            svc.sync_upcoming_window(league_ids, days_forward=args.days_forward)
    start_dt, end_dt = fixture_window_bounds(args.days_forward)
    fixtures = fetch_fixture_rows(session, league_ids, start_dt, end_dt)
    if args.limit and args.limit > 0:
        fixtures = fixtures[: args.limit]

    if not fixtures:
        log.info("No fixtures found for odds window")
        return

    log.info("Found %s fixtures for odds window", len(fixtures))

    team_ids = {
        fixture.get("home_team_id")
        for fixture in fixtures
        if fixture.get("home_team_id")
    } | {
        fixture.get("away_team_id")
        for fixture in fixtures
        if fixture.get("away_team_id")
    }
    if (args.refresh_squads or args.refresh_squads_missing) and svc is not None and team_ids:
        team_ids_list = sorted(team_ids)
        if args.refresh_squads:
            log.info("Refreshing squads for %s teams", len(team_ids_list))
            svc.sync_squads_for_teams(team_ids_list)
        else:
            counts = fetch_team_player_counts(session, team_ids_list)
            missing = [team_id for team_id in team_ids_list if counts.get(team_id, 0) == 0]
            if missing:
                log.info(
                    "Refreshing squads for %s/%s teams missing players",
                    len(missing),
                    len(team_ids_list),
                )
                svc.sync_squads_for_teams(missing)

    unmatched_details: List[Dict] = []
    unmatched_counts: Dict[Tuple[str, str], int] = {}
    unmatched_team_counts: Dict[str, int] = {}
    unmatched_selection_counts: Dict[str, int] = {}
    debug_samples: List[Dict] = []
    debug_fuzzy_matches: List[Dict] = []
    raw_allowlist = (os.environ.get("ODDS_MARKET_ALLOWLIST") or "").strip()
    market_allowlist = load_market_allowlist()
    allowlist_enabled = market_allowlist is not None
    if not allowlist_enabled and os.environ.get("ALLOWLIST_BYPASS", "").lower() not in {"1", "true", "yes"}:
        raise SystemExit("Allowlist disabled. Set ALLOWLIST_BYPASS=1 to proceed.")
    if allowlist_enabled:
        log.info("Market allowlist (%s): %s", len(market_allowlist), ", ".join(sorted(market_allowlist)))
    else:
        log.warning("Market allowlist bypassed (ODDS_MARKET_ALLOWLIST=%s)", raw_allowlist or "unset")
    allowlist_counts: Dict[str, int] = {"dropped": 0}

    for idx, fixture in enumerate(fixtures, start=1):
        fixture_id = fixture["fixture_id"]
        home_team_id = fixture.get("home_team_id")
        away_team_id = fixture.get("away_team_id")
        team_map, team_aliases = load_team_context(session, [home_team_id, away_team_id])
        home_aliases = team_aliases.get(home_team_id, []) if home_team_id else []
        away_aliases = team_aliases.get(away_team_id, []) if away_team_id else []
        player_map = load_fixture_player_map(session, fixture_id)
        team_player_map = load_team_player_map(
            session,
            [home_team_id, away_team_id],
        )
        fuzzy_candidates = build_fuzzy_candidates(session, [home_team_id, away_team_id])
        debug_team_players = None
        if args.debug_mapping_out:
            debug_team_players = load_team_player_names(session, [home_team_id, away_team_id], limit=50)

        data = fetch_odds_for_fixture(client, fixture_id, args.bookmaker_id)
        snapshot = {
            "fixture_id": fixture_id,
            "bookmaker_id": args.bookmaker_id,
            "pulled_at": datetime.utcnow(),
            "raw": json.dumps({"data": data}),
        }
        session.execute(
            text(
                """
                insert into odds_snapshots (fixture_id, bookmaker_id, pulled_at, raw)
                values (:fixture_id, :bookmaker_id, :pulled_at, :raw)
                """
            ),
            snapshot,
        )

        outcomes = parse_outcomes(
            fixture_id,
            args.bookmaker_id,
            data,
            player_map,
            team_map,
            team_player_map,
            home_team_id,
            away_team_id,
            home_aliases,
            away_aliases,
            fuzzy_candidates,
            unmatched_details,
            unmatched_counts,
            unmatched_team_counts,
            unmatched_selection_counts,
            debug_samples,
            debug_team_players,
            args.debug_mapping_limit,
            debug_fuzzy_matches,
            market_allowlist,
            allowlist_counts,
        )
        upsert_outcomes(session, outcomes)
        session.commit()
        if idx == 1 or idx % 100 == 0 or idx == len(fixtures):
            log.info(
                "Processed fixture %s (%s/%s) outcomes=%s",
                fixture_id,
                idx,
                len(fixtures),
                len(outcomes),
            )

    log.info("Odds sync complete")
    if market_allowlist is not None:
        log.info(
            "Market allowlist active (size=%s). Dropped outcomes=%s",
            len(market_allowlist),
            allowlist_counts.get("dropped", 0),
        )

    if unmatched_counts:
        by_market: Dict[str, Dict[str, int]] = {}
        for (market_key, raw_name), count in unmatched_counts.items():
            by_market.setdefault(market_key, {})[raw_name] = count
        for market_key, names in sorted(by_market.items()):
            top = sorted(names.items(), key=lambda item: item[1], reverse=True)[:20]
            summary = ", ".join(f"{name}({count})" for name, count in top)
            log.info("Unmatched player names summary %s: %s", market_key, summary)

    if unmatched_team_counts:
        top = sorted(unmatched_team_counts.items(), key=lambda item: item[1], reverse=True)[:20]
        summary = ", ".join(f"{key}({count})" for key, count in top)
        log.info("Unmatched team selection_key summary: %s", summary)

    if unmatched_selection_counts:
        top = sorted(unmatched_selection_counts.items(), key=lambda item: item[1], reverse=True)[:20]
        summary = ", ".join(f"{key}({count})" for key, count in top)
        log.info("Unmatched player selection_key summary: %s", summary)

    if args.unmatched_out:
        try:
            with open(args.unmatched_out, "w", encoding="utf-8") as f:
                json.dump(unmatched_details, f, indent=2)
            log.info("Wrote unmatched player odds to %s (%s rows)", args.unmatched_out, len(unmatched_details))
        except OSError as exc:
            log.warning("Failed to write unmatched output %s: %s", args.unmatched_out, exc)

    if args.debug_mapping_out:
        try:
            payload = {
                "unmatched_player_names": [
                    {"market_key": market_key, "raw_name": raw_name, "count": count}
                    for (market_key, raw_name), count in sorted(
                        unmatched_counts.items(), key=lambda item: item[1], reverse=True
                    )[:200]
                ],
                "unmatched_player_selection_keys": [
                    {"selection_key": key, "count": count}
                    for key, count in sorted(
                        unmatched_selection_counts.items(), key=lambda item: item[1], reverse=True
                    )[:200]
                ],
                "unmatched_team_selection_keys": [
                    {"selection_key": key, "count": count}
                    for key, count in sorted(
                        unmatched_team_counts.items(), key=lambda item: item[1], reverse=True
                    )[:200]
                ],
                "samples": debug_samples,
                "fuzzy_matches": debug_fuzzy_matches,
            }
            with open(args.debug_mapping_out, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            log.info("Wrote debug mapping output to %s", args.debug_mapping_out)
        except OSError as exc:
            log.warning("Failed to write debug mapping output %s: %s", args.debug_mapping_out, exc)


if __name__ == "__main__":
    main()
