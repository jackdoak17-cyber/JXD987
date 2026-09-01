#!/usr/bin/env python3
"""
Sync pre-match odds from Odds-API.io into SQLite.

- Maps Odds-API events to SportMonks fixtures in SQLite (by league + kickoff + team names).
- Fetches odds via /odds/multi for selected bookmakers.
- Normalizes markets into odds_outcomes.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import re
import unicodedata
import difflib
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import bindparam, text

from jxd import SportMonksClient, SyncService
from jxd.db import get_engine, get_session
from jxd.models import Base
from jxd.odds_api_client import OddsApiClient, OddsApiError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

BOOKMAKER_NAME_TO_ID = {
    "bet365": 2,
    "kambi": 3,
    "paddypower": 4,
    "unibet": 5,
    "betmgm": 8,
    "betfairexchange": 9,
}
BOOKMAKER_CANONICAL = {
    "bet365": "Bet365",
    "kambi": "Kambi",
    "paddypower": "Paddy Power",
    "unibet": "Unibet",
    "betmgm": "BetMGM",
    "betfairexchange": "Betfair Exchange",
}
HISTORICAL_ODDS_BASE_URL = os.environ.get("ODDS_API_HISTORICAL_BASE") or "https://api.odds-api.io/v3"
SETTLED_EVENT_STATUSES = {"settled", "finished", "final", "ended", "completed"}
MONEYLINE_MARKET_KEYS = {
    "moneyline",
    "match_result",
    "match_winner",
    "match_winner_90",
    "match_winner_90_min",
    "full_time_result",
    "full_time_result_90",
    "1x2",
    "home_draw_away",
}
MONEYLINE_SIDES = ("home", "draw", "away")

MATCHER_CONTRACT_PATH = Path(
    os.environ.get("ODDS_API_MATCHER_CONTRACT_PATH")
    or Path(__file__).resolve().parent.parent / "config" / "odds_api_matcher_contract.json"
)


def _load_matcher_contract() -> Dict[str, object]:
    try:
        contract = json.loads(MATCHER_CONTRACT_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Unable to load odds API matcher contract: {MATCHER_CONTRACT_PATH}") from exc
    if not isinstance(contract, dict) or contract.get("schemaVersion") != 1:
        raise RuntimeError(f"Unsupported odds API matcher contract: {MATCHER_CONTRACT_PATH}")
    normalization = contract.get("nameNormalization")
    matching = contract.get("matching")
    if not isinstance(normalization, dict) or not isinstance(matching, dict):
        raise RuntimeError(f"Incomplete odds API matcher contract: {MATCHER_CONTRACT_PATH}")
    aliases = normalization.get("teamAliases")
    token_drop = normalization.get("tokenDrop")
    variant_noise = normalization.get("variantNoise")
    if (
        not isinstance(aliases, dict)
        or not all(isinstance(key, str) and isinstance(value, str) and key and value for key, value in aliases.items())
        or not isinstance(token_drop, list)
        or not all(isinstance(value, str) and value for value in token_drop)
        or not isinstance(variant_noise, list)
        or not all(isinstance(value, str) and value for value in variant_noise)
    ):
        raise RuntimeError(f"Invalid name normalization in odds API matcher contract: {MATCHER_CONTRACT_PATH}")

    required_matching = (
        "upstreamEventWindowPadHours",
        "exactNameKickoffDriftHours",
        "placeholderKickoffDriftHours",
        "nearKickoffHours",
        "exactTeamScore",
        "minimumTeamScore",
        "exactCombinedScore",
        "disambiguatedCombinedScore",
        "minimumScoreMargin",
        "prefixSuffixScore",
        "prefixSuffixMinimumLength",
    )
    for key in required_matching:
        value = matching.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise RuntimeError(f"Invalid matching value {key} in odds API matcher contract: {MATCHER_CONTRACT_PATH}")
    return contract


MATCHER_CONTRACT = _load_matcher_contract()
_MATCHER_NORMALIZATION = MATCHER_CONTRACT["nameNormalization"]
_MATCHER_MATCHING = MATCHER_CONTRACT["matching"]
TEAM_NAME_ALIAS = dict(_MATCHER_NORMALIZATION["teamAliases"])
TEAM_TOKEN_DROP = set(_MATCHER_NORMALIZATION["tokenDrop"])
TEAM_VARIANT_NOISE = set(_MATCHER_NORMALIZATION["variantNoise"])
UPSTREAM_EVENT_WINDOW_PAD_HOURS = int(_MATCHER_MATCHING["upstreamEventWindowPadHours"])
EXACT_NAME_KICKOFF_DRIFT_HOURS = float(_MATCHER_MATCHING["exactNameKickoffDriftHours"])
PLACEHOLDER_KICKOFF_DRIFT_HOURS = float(_MATCHER_MATCHING["placeholderKickoffDriftHours"])
NEAR_KICKOFF_HOURS = float(_MATCHER_MATCHING["nearKickoffHours"])
EXACT_TEAM_SCORE = float(_MATCHER_MATCHING["exactTeamScore"])
MINIMUM_TEAM_SCORE = float(_MATCHER_MATCHING["minimumTeamScore"])
EXACT_COMBINED_SCORE = float(_MATCHER_MATCHING["exactCombinedScore"])
DISAMBIGUATED_COMBINED_SCORE = float(_MATCHER_MATCHING["disambiguatedCombinedScore"])
MINIMUM_SCORE_MARGIN = float(_MATCHER_MATCHING["minimumScoreMargin"])
PREFIX_SUFFIX_SCORE = float(_MATCHER_MATCHING["prefixSuffixScore"])
PREFIX_SUFFIX_MINIMUM_LENGTH = int(_MATCHER_MATCHING["prefixSuffixMinimumLength"])

DEFAULT_MARKET_ALLOWLIST = {
    "moneyline",
    "double_chance",
    "draw_no_bet",
    "goals_over_under",
    "goals_over_under_first_half",
    "btts",
    "match_shots",
    "match_shots_on_target",
    "team_shots",
    "team_shots_on_target",
    "team_total_goals",
    "player_shots",
    "player_shots_on_target",
    "player_fouls_committed",
    "player_fouls_drawn",
    "player_to_score",
    "player_to_assist",
    "player_to_score_or_assist",
    "player_card",
    "player_tackles",
    "player_goalkeeper_saves",
}

TEAM_MARKETS = {"team_shots", "team_shots_on_target", "team_total_goals"}
MATCH_MARKETS = {
    "match_shots",
    "match_shots_on_target",
    "goals_over_under",
    "goals_over_under_first_half",
    "btts",
}
BET365_SINGLE_SIDED_POSITIVE_PLAYER_MARKETS = {
    "player_fouls_committed",
    "player_fouls_drawn",
}

MARKET_NAME_MAP = {
    "ml": "moneyline",
    "moneyline": "moneyline",
    "match_result": "moneyline",
    "match_winner": "moneyline",
    "match_winner_90": "moneyline",
    "match_winner_90_min": "moneyline",
    "full_time_result": "moneyline",
    "full_time_result_90": "moneyline",
    "home_draw_away": "moneyline",
    "1x2": "moneyline",
    "h2h": "moneyline",
    "draw_no_bet": "draw_no_bet",
    "double_chance": "double_chance",
    "goals_over_under": "goals_over_under",
    "totals_ht": "goals_over_under_first_half",
    "totals_1st_half": "goals_over_under_first_half",
    "totals_first_half": "goals_over_under_first_half",
    "goals_over_under_1st_half": "goals_over_under_first_half",
    "goals_over_under_first_half": "goals_over_under_first_half",
    "totals": "goals_over_under",
    "alternative_goal_line": "goals_over_under",
    "alternative_goal_lines": "goals_over_under",
    "alternate_goal_line": "goals_over_under",
    "alternate_goal_lines": "goals_over_under",
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
    "team_total_home": "team_total_goals_home",
    "team_total_away": "team_total_goals_away",
    "team_totals_home": "team_total_goals_home",
    "team_totals_away": "team_total_goals_away",
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
    "player_tackles": "player_tackles",
    "player_tackles_over_under": "player_tackles",
    "tackles": "player_tackles",
    "goalkeeper_saves": "player_goalkeeper_saves",
    "goalkeeper_saves_over_under": "player_goalkeeper_saves",
    "goalie_saves": "player_goalkeeper_saves",
    "keeper_saves": "player_goalkeeper_saves",
    "player_saves": "player_goalkeeper_saves",
    "player_saves_over_under": "player_goalkeeper_saves",
    "player_goalkeeper_saves": "player_goalkeeper_saves",
}


@dataclass
class LeagueResult:
    """
    Stage-1 fetch handoff object for Phase 3A.
    Each worker returns one object; Stage-2 writes these sequentially.
    """

    league_id: int
    league_name: str
    fixtures_fetched: int
    odds_records: List[Dict[str, object]]
    api_calls_made: int
    fetch_duration_seconds: float
    error: Exception | None
    events_returned: int = 0
    events_matched: int = 0
    unmatched_samples: List[Dict[str, object]] = field(default_factory=list)
    moneyline_coverage: List[Dict[str, object]] = field(default_factory=list)
    raw_events: List[Dict[str, object]] = field(default_factory=list)
    raw_odds_payloads: Dict[int, object] = field(default_factory=dict)
    api_calls_by_endpoint: Dict[str, int] = field(default_factory=dict)
    api_time_seconds: float = 0.0
    rate_limit_hits: int = 0
    rate_limit_sleeps: int = 0
    last_rate_limit: Optional[Dict[str, object]] = None


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


def normalize_team_variants(value: str) -> List[str]:
    if not value:
        return []
    raw = value.lower().strip()
    raw = raw.replace("&", "and")
    tokens = [token for token in normalize_name_tokens(raw) if token not in TEAM_TOKEN_DROP]
    if not tokens:
        return []
    variants = {"".join(tokens)}
    if len(tokens) > 1:
        prefix = "".join(tokens[:-1])
        suffix = "".join(tokens[1:])
        if prefix:
            variants.add(prefix)
        if suffix and suffix not in TEAM_VARIANT_NOISE:
            variants.add(suffix)
    tokens_no_digits = [token for token in tokens if not token.isdigit()]
    if tokens_no_digits and tokens_no_digits != tokens:
        variants.add("".join(tokens_no_digits))
        if len(tokens_no_digits) > 1:
            prefix = "".join(tokens_no_digits[:-1])
            suffix = "".join(tokens_no_digits[1:])
            if prefix:
                variants.add(prefix)
            if suffix and suffix not in TEAM_VARIANT_NOISE:
                variants.add(suffix)
    expanded = set()
    for alias in variants:
        expanded.add(alias)
        if alias in TEAM_NAME_ALIAS:
            expanded.add(TEAM_NAME_ALIAS[alias])
    return [item for item in expanded if item and item not in TEAM_VARIANT_NOISE]


def team_aliases(value: str, short_code: Optional[str] = None) -> List[str]:
    aliases = set()
    if value:
        aliases.update(normalize_team_variants(value))
        aliases.add(normalize_team_name(value))
        aliases.add(normalize_name(value))
    if short_code:
        aliases.add(normalize_team_name(short_code))
        aliases.add(normalize_name(short_code))
    for item in list(aliases):
        if item in TEAM_NAME_ALIAS:
            aliases.add(TEAM_NAME_ALIAS[item])
    return [alias for alias in aliases if alias]


def name_variants(value: str) -> List[str]:
    tokens = normalize_name_tokens(value)
    if not tokens:
        return []
    variants: List[str] = []
    seen: Set[str] = set()

    def add(variant: str) -> None:
        if not variant or variant in seen:
            return
        seen.add(variant)
        variants.append(variant)

    add(normalize_name(value))
    if len(tokens) >= 2:
        first, last = tokens[0], tokens[-1]
        add(first + last)
        add(last + first)
        add(first[0] + last)
        add(last + first[0])
    if len(tokens) >= 3:
        first = tokens[0]
        second = tokens[1]
        last = tokens[-1]
        second_last = tokens[-2]
        middle = tokens[1:-1]
        tail = "".join(tokens[1:])
        add(first + second)
        add(second + first)
        add(first[0] + second)
        add(second + first[0])
        if second_last != second:
            add(first + second_last)
            add(second_last + first)
            add(first[0] + second_last)
            add(second_last + first[0])
        add(last + "".join([first, *middle]))
        if tail:
            add(tail)
            add(last + "".join(middle))
        if len(tokens[1]) == 1:
            add(first + last)
            add(last + first)
    return variants


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
    if "unibet" in raw:
        return "unibet"
    if "betmgm" in raw:
        return "betmgm"
    if "betfair" in raw and "exchange" in raw:
        return "betfairexchange"
    return raw


def load_default_bookmakers() -> List[str]:
    """Load the bookmaker set supported by the user-facing odds contract."""
    config_path = Path(__file__).resolve().parent.parent / "config" / "odds_api_bookmakers.json"
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Unable to load bookmaker configuration from {config_path}: {exc}") from exc

    if not isinstance(raw, dict) or raw.get("schemaVersion") != 1:
        raise SystemExit(f"Invalid bookmaker configuration schema in {config_path}")
    items = raw.get("bookmakers")
    if not isinstance(items, list) or not items:
        raise SystemExit(f"Bookmaker configuration must contain a non-empty bookmakers list: {config_path}")

    names: List[str] = []
    seen_ids: Set[int] = set()
    seen_names: Set[str] = set()
    known_names = set(BOOKMAKER_CANONICAL.values())
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("id"), int) or not isinstance(item.get("name"), str):
            raise SystemExit(f"Invalid bookmaker entry in {config_path}: {item!r}")
        bookmaker_id = int(item["id"])
        name = item["name"].strip()
        key = normalize_bookmaker_key(name)
        if not name or name not in known_names or key not in BOOKMAKER_NAME_TO_ID:
            raise SystemExit(f"Unsupported bookmaker in {config_path}: {name!r}")
        if bookmaker_id != BOOKMAKER_NAME_TO_ID[key]:
            raise SystemExit(
                f"Bookmaker id mismatch in {config_path}: {name!r} has id {bookmaker_id}, "
                f"expected {BOOKMAKER_NAME_TO_ID[key]}"
            )
        if bookmaker_id in seen_ids or name in seen_names:
            raise SystemExit(f"Duplicate bookmaker in {config_path}: {name!r}")
        seen_ids.add(bookmaker_id)
        seen_names.add(name)
        names.append(name)
    return names


DEFAULT_BOOKMAKERS = load_default_bookmakers()


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


def load_excluded_league_ids() -> Set[int]:
    """Return competitions that must not trigger paid Odds-API requests."""
    path = Path(__file__).resolve().parent.parent / "config" / "odds_api_sync_excluded_leagues.json"
    if not path.exists():
        return set()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit(f"Expected a JSON array in {path}")
    return {int(value) for value in raw}


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


def inspect_upstream_moneyline(
    bookmakers_payload: object,
    requested_bookmaker_keys: Set[str],
) -> Dict[str, List[str]]:
    """Return usable moneyline sides by configured bookmaker.

    This deliberately inspects the same market names and price bounds that
    the writer accepts.  The result is compact evidence for downstream
    validators; raw provider payloads remain an opt-in debug artifact.
    """
    if not isinstance(bookmakers_payload, dict):
        return {}

    sides_by_bookmaker: Dict[str, Set[str]] = {}
    for raw_bookmaker, raw_markets in bookmakers_payload.items():
        bookmaker_key = normalize_bookmaker_key(str(raw_bookmaker))
        canonical_name = BOOKMAKER_CANONICAL.get(bookmaker_key)
        if not canonical_name or bookmaker_key not in requested_bookmaker_keys:
            continue
        if not isinstance(raw_markets, list):
            continue

        available_sides = sides_by_bookmaker.setdefault(canonical_name, set())
        for raw_market in raw_markets:
            if not isinstance(raw_market, dict):
                continue
            if resolve_market_key(str(raw_market.get("name") or "")) != "moneyline":
                continue
            raw_odds = raw_market.get("odds")
            if not isinstance(raw_odds, list):
                continue
            for raw_odd in raw_odds:
                if not isinstance(raw_odd, dict):
                    continue
                for side in MONEYLINE_SIDES:
                    price = parse_float(raw_odd.get(side))
                    if price is not None and 1 < price <= 500:
                        available_sides.add(side)

    return {
        bookmaker: [side for side in MONEYLINE_SIDES if side in sides]
        for bookmaker, sides in sorted(sides_by_bookmaker.items())
        if sides
    }


def extract_player_market_price(
    market_key: str,
    bookmaker_id: int,
    odd: Dict[str, object],
) -> Tuple[Optional[float], Optional[str]]:
    for key in ("over", "yes", "home"):
        price = parse_float(odd.get(key))
        if price is not None:
            side_key = "over" if key == "over" else key
            return price, side_key

    # Odds-API encodes Bet365 foul props as one-sided `under` prices on 0.5 lines.
    # Those prices represent the positive 1+ side we surface alongside the other books.
    if bookmaker_id == 2 and market_key in BET365_SINGLE_SIDED_POSITIVE_PLAYER_MARKETS:
        price = parse_float(odd.get("under"))
        if price is not None:
            return price, "over"

    return None, None


ORDINAL_SUFFIX_RE = re.compile(r"(\d)(st|nd|rd|th)\b", re.IGNORECASE)
LINE_TOKEN_RE = re.compile(r"\d+(?:[._]\d+)?")
LINE_SIDE_RE = re.compile(
    r"(?:(?P<side>over|under)[_\\s-]*)?(?P<line>\d+(?:[._]\d+)?)"
    r"(?:[_\\s-]*(?P<side2>over|under))?",
    re.IGNORECASE,
)


def parse_line_from_text(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return parse_float(value)
    text = str(value).strip()
    if not text:
        return None
    normalized = ORDINAL_SUFFIX_RE.sub("", text.lower())
    tokens = LINE_TOKEN_RE.findall(normalized)
    if not tokens:
        return None
    numbers: List[float] = []
    for token in tokens:
        num = parse_float(token.replace("_", "."))
        if num is not None:
            numbers.append(num)
    if not numbers:
        return None
    if len(numbers) == 1:
        return numbers[0]
    if "," in normalized or "/" in normalized or re.search(r"\d\s*-\s*\d", normalized):
        return sum(numbers) / len(numbers)
    return numbers[-1]


def parse_side_from_text(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).lower()
    if "over" in text:
        return "over"
    if "under" in text:
        return "under"
    return None


def parse_line_side_from_key(key: str) -> Tuple[Optional[float], Optional[str]]:
    if not key:
        return None, None
    match = LINE_SIDE_RE.search(key.replace("__", "_"))
    if not match:
        return None, None
    side = (match.group("side") or match.group("side2") or "").lower()
    line = parse_line_from_text(match.group("line"))
    if side not in {"over", "under"}:
        return None, None
    return line, side


def extract_over_under_line(odd: Dict[str, object]) -> Optional[float]:
    for key in ("hdp", "line", "total", "handicap"):
        line = parse_line_from_text(odd.get(key))
        if line is not None:
            return line
    for key in ("name", "label"):
        line = parse_line_from_text(odd.get(key))
        if line is not None:
            return line
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
    if "alternative_goal_line" in key or "alternate_goal_line" in key:
        if any(token in key for token in ("ht", "1st_half", "first_half", "half_time")):
            return "goals_over_under_first_half"
        return "goals_over_under"
    if "totals" in key and any(token in key for token in ("ht", "1st_half", "first_half", "half_time")):
        return "goals_over_under_first_half"
    if "goals_over_under" in key and any(
        token in key for token in ("ht", "1st_half", "first_half", "half_time")
    ):
        return "goals_over_under_first_half"
    if "tackle" in key and "team" not in key and "match" not in key:
        return "player_tackles"
    if "save" in key and "team" not in key and "match" not in key:
        if any(token in key for token in ("goalkeeper", "keeper", "goalie", "player")):
            return "player_goalkeeper_saves"
    if key in DEFAULT_MARKET_ALLOWLIST:
        return key
    return None


def load_league_map(path: Path) -> Dict[int, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {int(k): str(v) for k, v in raw.items() if v}


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sqlite_utc_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def fixture_window_bounds(
    days_back: int,
    days_forward: int,
    calendar_history: bool = False,
) -> Tuple[datetime, datetime]:
    now_utc = utc_now_naive()
    if calendar_history:
        # Settled history is a calendar contract, not a rolling 48-hour
        # contract. The latter drops the first day's fixtures as soon as the
        # job runs late in the day, which is how visible history loses odds.
        # UTC is the pipeline's declared time basis.
        today_start = datetime.combine(now_utc.date(), datetime.min.time())
        return today_start - timedelta(days=max(0, days_back)), today_start
    start_dt = now_utc - timedelta(days=max(0, days_back))
    end_dt = now_utc + timedelta(days=days_forward)
    return start_dt, end_dt


def load_fixture_moneyline_completeness(session, fixture_ids: List[int]) -> Set[int]:
    """Return fixtures with a complete usable home/draw/away moneyline.

    Historical fetches are intentionally skipped only when the local row set
    already satisfies the same three-side contract used by the public
    validator.  Treating one arbitrary moneyline row as "present" permanently
    stranded partially-ingested settled fixtures.
    """
    if not fixture_ids:
        return set()
    stmt = (
        text(
            """
            select o.fixture_id,
                   f.home_team_id,
                   f.away_team_id,
                   o.participant_type,
                   o.participant_id,
                   o.selection_key,
                   o.price_decimal
            from odds_outcomes o
            join fixtures f on f.id = o.fixture_id
            where o.fixture_id in :fixture_ids
              and o.market_key in :market_keys
            """
        )
        .bindparams(bindparam("fixture_ids", expanding=True))
        .bindparams(bindparam("market_keys", expanding=True))
    )
    rows = session.execute(
        stmt,
        {"fixture_ids": fixture_ids, "market_keys": sorted(MONEYLINE_MARKET_KEYS)},
    ).fetchall()
    sides_by_fixture: Dict[int, Set[str]] = {}
    for row in rows:
        fixture_id = int(row[0])
        price = parse_float(row[6])
        if price is None or price <= 1 or price > 500:
            continue
        participant_type = str(row[3] or "").strip().lower()
        participant_id = int(row[4]) if row[4] is not None else None
        selection_key = str(row[5] or "").strip().lower()
        sides = sides_by_fixture.setdefault(fixture_id, set())
        if participant_type == "team" and participant_id == row[1]:
            sides.add("home")
        elif participant_type == "team" and participant_id == row[2]:
            sides.add("away")
        elif selection_key in {"draw", "x"} or "draw" in selection_key:
            sides.add("draw")
    return {
        fixture_id
        for fixture_id, sides in sides_by_fixture.items()
        if sides == set(MONEYLINE_SIDES)
    }


def is_settled_fixture(
    status: Optional[object],
    status_code: Optional[object],
    home_score: Optional[object],
    away_score: Optional[object],
    starting_at: Optional[datetime],
) -> bool:
    settled_statuses = SETTLED_EVENT_STATUSES | {"ft", "aet", "pen", "ft_pen"}
    normalized_status = str(status or "").strip().lower()
    normalized_code = str(status_code or "").strip().lower()
    if normalized_status in settled_statuses or normalized_code in settled_statuses:
        return True
    return (
        home_score is not None
        and away_score is not None
        and starting_at is not None
        and starting_at <= utc_now_naive()
    )


def load_fixtures(
    session,
    league_ids: List[int],
    days_back: int,
    days_forward: int,
    calendar_history: bool = False,
) -> List[Dict[str, object]]:
    if not league_ids:
        return []
    start_dt, end_dt = fixture_window_bounds(days_back, days_forward, calendar_history)
    start_dt_sql = sqlite_utc_timestamp(start_dt)
    end_dt_sql = sqlite_utc_timestamp(end_dt)
    stmt = text(
        """
        select f.id,
               f.league_id,
               f.starting_at,
               f.status,
               f.status_code,
               f.home_team_id,
               f.away_team_id,
               f.home_score,
               f.away_score,
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
            "start_dt": start_dt_sql,
            "end_dt": end_dt_sql,
        },
    ).fetchall()
    complete_moneyline_fixture_ids = load_fixture_moneyline_completeness(
        session,
        [int(row.id) for row in rows if row.id],
    )

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
                "status": row.status,
                "status_code": row.status_code,
                "home_team_id": row.home_team_id,
                "away_team_id": row.away_team_id,
                "home_score": row.home_score,
                "away_score": row.away_score,
                "home_alias": home_alias,
                "away_alias": away_alias,
                "has_complete_moneyline_odds": int(row.id) in complete_moneyline_fixture_ids,
                "is_settled": is_settled_fixture(
                    row.status,
                    row.status_code,
                    row.home_score,
                    row.away_score,
                    start_val,
                ),
            }
        )
    return fixtures


def fixture_priority_bucket(starting_at: Optional[datetime], now_utc: datetime) -> Optional[str]:
    if starting_at is None:
        return None
    hours_to_kickoff = (starting_at - now_utc).total_seconds() / 3600.0
    if hours_to_kickoff < 0:
        return None
    if hours_to_kickoff <= 2:
        return "p1"
    if hours_to_kickoff <= 24:
        return "p2"
    if hours_to_kickoff <= 14 * 24:
        return "p3"
    return None


def filter_fixtures_by_priority(
    fixtures: List[Dict[str, object]],
    priority: Optional[str],
) -> List[Dict[str, object]]:
    if priority is None:
        return fixtures
    now_utc = utc_now_naive()
    filtered: List[Dict[str, object]] = []
    for fixture in fixtures:
        if priority == "settled-history":
            # The calendar window is already bounded to dates before today.
            # Do not make delayed SportMonks status updates hide a fixture
            # from the historical provider lookup.
            filtered.append(fixture)
            continue
        bucket = fixture_priority_bucket(fixture.get("starting_at"), now_utc)
        if bucket == priority:
            filtered.append(fixture)
    return filtered


def estimate_api_calls_for_scope(
    fixtures: List[Dict[str, object]],
    scoped_league_ids: Iterable[int],
) -> int:
    # One /events call per league plus a conservative /odds/multi batch estimate.
    league_count = len({int(league_id) for league_id in scoped_league_ids})
    odds_batches = math.ceil(len(fixtures) / 10) if fixtures else 0
    return league_count + odds_batches


def fetch_league_odds_payload(
    league_id: int,
    odds_league: str,
    league_fixtures: List[Dict[str, object]],
    sport: str,
    days_back: int,
    days_forward: int,
    bookmakers: List[str],
    per_league_limit: int,
    calendar_history: bool = False,
) -> LeagueResult:
    started = time.time()
    client = OddsApiClient()
    historical_client: Optional[OddsApiClient] = None
    result = LeagueResult(
        league_id=league_id,
        league_name=odds_league,
        fixtures_fetched=len(league_fixtures),
        odds_records=[],
        api_calls_made=0,
        fetch_duration_seconds=0.0,
        error=None,
    )
    event_candidates_by_fixture: Dict[int, List[Dict[str, object]]] = {}
    odds_response_by_event_id: Dict[int, Dict[str, object]] = {}

    try:
        start_dt, end_dt = fixture_window_bounds(days_back, days_forward, calendar_history)
        event_start_dt = start_dt - timedelta(hours=UPSTREAM_EVENT_WINDOW_PAD_HOURS)
        event_end_dt = end_dt + timedelta(hours=UPSTREAM_EVENT_WINDOW_PAD_HOURS)
        params = {
            "sport": sport,
            "league": odds_league,
            "from": event_start_dt.isoformat() + "Z",
            "to": event_end_dt.isoformat() + "Z",
        }
        if days_back <= 0:
            params["status"] = "pending,live"
        events = client.request("events", params=params)
        if not isinstance(events, list):
            raise OddsApiError(f"Unexpected events response for league {odds_league}")

        result.raw_events = events
        result.events_returned = len(events)

        event_to_fixture: Dict[int, int] = {}
        historical_backfill: List[Tuple[int, int]] = []
        for event in events:
            fixture = match_event_to_fixture(event, league_fixtures)
            if not fixture:
                if len(result.unmatched_samples) < 20:
                    result.unmatched_samples.append(
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
            event_id = int(event_id)
            event_candidates_by_fixture.setdefault(int(fixture["fixture_id"]), []).append(
                {
                    "event_id": event_id,
                    "home": event.get("home"),
                    "away": event.get("away"),
                    "date": event.get("date"),
                    "status": event.get("status"),
                }
            )
            event_status = str(event.get("status") or "").strip().lower()
            if calendar_history or event_status in SETTLED_EVENT_STATUSES:
                if not bool(fixture.get("has_complete_moneyline_odds")):
                    historical_backfill.append((event_id, int(fixture["fixture_id"])))
                continue
            event_to_fixture[event_id] = int(fixture["fixture_id"])

        result.events_matched = len(event_to_fixture) + len(historical_backfill)
        if event_to_fixture:
            event_ids = list(event_to_fixture.keys())
            if per_league_limit > 0:
                event_ids = event_ids[:per_league_limit]
            for event_id in event_ids:
                odds_response_by_event_id[event_id] = {
                    "requested": True,
                    "received": False,
                    "valid": None,
                }
            batches = [event_ids[i : i + 10] for i in range(0, len(event_ids), 10)]

            for batch in batches:
                try:
                    odds_batch = client.request(
                        "odds/multi",
                        params={
                            "eventIds": ",".join(str(event_id) for event_id in batch),
                            "bookmakers": ",".join(bookmakers),
                        },
                    )
                except Exception:
                    for event_id in batch:
                        odds_response_by_event_id[event_id]["valid"] = False
                    raise
                if not isinstance(odds_batch, list):
                    for event_id in batch:
                        odds_response_by_event_id[event_id]["valid"] = False
                    continue
                for event_id in batch:
                    odds_response_by_event_id[event_id]["valid"] = True
                for odds_event in odds_batch:
                    event_id = odds_event.get("id")
                    if event_id is None:
                        continue
                    event_id = int(event_id)
                    fixture_id = event_to_fixture.get(int(event_id))
                    if not fixture_id:
                        continue
                    if event_id in odds_response_by_event_id:
                        odds_response_by_event_id[event_id]["received"] = True
                    bookmakers_payload = odds_event.get("bookmakers") or {}
                    result.raw_odds_payloads[event_id] = bookmakers_payload
                    result.odds_records.append(
                        {
                            "event_id": event_id,
                            "fixture_id": int(fixture_id),
                            "bookmakers_payload": bookmakers_payload,
                        }
                    )

        if historical_backfill:
            historical_client = OddsApiClient(base_url=HISTORICAL_ODDS_BASE_URL)
            if per_league_limit > 0:
                historical_backfill = historical_backfill[:per_league_limit]
            for event_id, fixture_id in historical_backfill:
                odds_response_by_event_id[event_id] = {
                    "requested": True,
                    "received": False,
                    "valid": None,
                }
                try:
                    historical_payload = historical_client.request(
                        "historical/odds",
                        params={
                            "eventId": str(event_id),
                            "bookmakers": ",".join(bookmakers),
                        },
                    )
                except Exception:
                    odds_response_by_event_id[event_id]["valid"] = False
                    raise
                if not isinstance(historical_payload, dict):
                    odds_response_by_event_id[event_id]["valid"] = False
                    continue
                odds_response_by_event_id[event_id]["valid"] = True
                odds_response_by_event_id[event_id]["received"] = True
                bookmakers_payload = historical_payload.get("bookmakers") or {}
                result.raw_odds_payloads[event_id] = bookmakers_payload
                if bookmakers_payload:
                    result.odds_records.append(
                        {
                            "event_id": event_id,
                            "fixture_id": int(fixture_id),
                            "bookmakers_payload": bookmakers_payload,
                        }
                    )

        for fixture in league_fixtures:
            fixture_id = int(fixture["fixture_id"])
            candidates = event_candidates_by_fixture.get(fixture_id, [])
            if not candidates:
                result.moneyline_coverage.append(
                    {
                        "fixture_id": fixture_id,
                        "league_id": league_id,
                        "odds_api_league": odds_league,
                        "matching_status": "unmatched",
                        "event_id": None,
                        "event": None,
                        "candidate_event_ids": [],
                        "odds_response_status": "not_applicable",
                        "supported_moneyline_bookmakers": [],
                        "moneyline_sides_by_bookmaker": {},
                    }
                )
                continue

            candidate_evidence: List[Dict[str, object]] = []
            for candidate in candidates:
                event_id = int(candidate["event_id"])
                response = odds_response_by_event_id.get(event_id, {})
                response_received = bool(response.get("received"))
                response_valid = response.get("valid")
                payload = result.raw_odds_payloads.get(event_id, {}) if response_received else {}
                sides_by_bookmaker = inspect_upstream_moneyline(payload, {
                    normalize_bookmaker_key(bookmaker) for bookmaker in bookmakers
                })
                candidate_evidence.append(
                    {
                        "event_id": event_id,
                        "event": candidate,
                        "odds_response_status": (
                            "received"
                            if response_received
                            else "invalid"
                            if response_valid is False
                            else "empty"
                            if response_valid is True
                            else "missing"
                            if response.get("requested")
                            else "not_requested"
                        ),
                        "supported_moneyline_bookmakers": sorted(sides_by_bookmaker),
                        "moneyline_sides_by_bookmaker": sides_by_bookmaker,
                    }
                )

            selected = max(
                candidate_evidence,
                key=lambda item: (
                    len(item["supported_moneyline_bookmakers"]),
                    item["odds_response_status"] == "received",
                    int(item["event_id"]),
                ),
            )
            selected_event = dict(selected["event"])
            result.moneyline_coverage.append(
                {
                    "fixture_id": fixture_id,
                    "league_id": league_id,
                    "odds_api_league": odds_league,
                    "matching_status": "matched",
                    "event_id": int(selected["event_id"]),
                    "event": selected_event,
                    "candidate_event_ids": [int(item["event_id"]) for item in candidate_evidence],
                    "odds_response_status": selected["odds_response_status"],
                    "supported_moneyline_bookmakers": selected["supported_moneyline_bookmakers"],
                    "moneyline_sides_by_bookmaker": selected["moneyline_sides_by_bookmaker"],
                }
            )
    except Exception as exc:
        result.error = exc
    finally:
        stats_clients = [client]
        if historical_client is not None:
            stats_clients.append(historical_client)
        result.fetch_duration_seconds = round(time.time() - started, 2)
        result.api_calls_made = sum(stats.stats.total_calls for stats in stats_clients)
        result.api_calls_by_endpoint = {}
        for api_client in stats_clients:
            for endpoint, count in api_client.stats.calls_by_endpoint.items():
                result.api_calls_by_endpoint[endpoint] = result.api_calls_by_endpoint.get(endpoint, 0) + int(count)
        result.api_time_seconds = round(sum(stats.stats.api_time_seconds for stats in stats_clients), 2)
        result.rate_limit_hits = sum(stats.stats.rate_limit_hits for stats in stats_clients)
        result.rate_limit_sleeps = sum(stats.stats.rate_limit_sleeps for stats in stats_clients)
        result.last_rate_limit = next(
            (stats.stats.last_rate_limit for stats in reversed(stats_clients) if stats.stats.last_rate_limit),
            None,
        )

    return result


def score_name_match(event_name: str, aliases: Iterable[str]) -> float:
    event_variants = normalize_team_variants(event_name)
    if not event_variants:
        return 0.0
    event_variants = [variant for variant in event_variants if variant not in TEAM_VARIANT_NOISE]
    if not event_variants:
        return 0.0
    best = 0.0
    for alias in aliases:
        if not alias or alias in TEAM_VARIANT_NOISE:
            continue
        for event_norm in event_variants:
            if event_norm == alias:
                return 1.0
            # Providers sometimes expand a short local name with a formal
            # club name (for example, SportMonks "Amed SK" versus Odds API
            # "Amed Sportif Faaliyetler"). Treat a distinctive four-character
            # prefix/suffix as a strong match; kickoff and the opposite team
            # are still checked by match_event_to_fixture, preventing broad
            # names from matching unrelated fixtures.
            if (
                len(alias) >= PREFIX_SUFFIX_MINIMUM_LENGTH
                and (event_norm.startswith(alias) or event_norm.endswith(alias))
            ):
                best = max(best, PREFIX_SUFFIX_SCORE)
            elif (
                len(event_norm) >= PREFIX_SUFFIX_MINIMUM_LENGTH
                and (alias.startswith(event_norm) or alias.endswith(event_norm))
            ):
                best = max(best, PREFIX_SUFFIX_SCORE)
            score = difflib.SequenceMatcher(None, event_norm, alias).ratio()
            if score > best:
                best = score
    return best


def fixture_has_placeholder_kickoff(value: Optional[datetime]) -> bool:
    if value is None:
        return False
    return value.hour == 0 and value.minute == 0 and value.second == 0


def kickoff_match_rank(
    event_dt: Optional[datetime],
    fixture_dt: Optional[datetime],
    home_score: float,
    away_score: float,
) -> Optional[Tuple[int, float]]:
    if event_dt is None or fixture_dt is None:
        return 0, 0.0
    delta_hours = abs((fixture_dt - event_dt).total_seconds()) / 3600.0
    if delta_hours <= NEAR_KICKOFF_HOURS:
        return 0, delta_hours
    # SportMonks sometimes leaves upcoming kickoffs at 00:00:00 until a later refresh.
    # When both team names are effectively exact, allow a wider window so those fixtures
    # still ingest odds instead of being dropped on date mismatch alone.
    if (
        fixture_has_placeholder_kickoff(fixture_dt)
        and home_score >= EXACT_TEAM_SCORE
        and away_score >= EXACT_TEAM_SCORE
        and delta_hours <= PLACEHOLDER_KICKOFF_DRIFT_HOURS
    ):
        return 1, delta_hours
    if (
        home_score >= EXACT_TEAM_SCORE
        and away_score >= EXACT_TEAM_SCORE
        and delta_hours <= EXACT_NAME_KICKOFF_DRIFT_HOURS
    ):
        return 2, delta_hours
    return None


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
    best_time_rank = 99
    best_delta_hours = float("inf")
    second_best = 0.0
    for fixture in fixtures:
        fixture_dt = fixture.get("starting_at")
        home_score = score_name_match(event_home, fixture.get("home_alias") or [])
        away_score = score_name_match(event_away, fixture.get("away_alias") or [])
        time_match = kickoff_match_rank(event_dt, fixture_dt, home_score, away_score)
        if time_match is None:
            continue
        time_rank, delta_hours = time_match
        combined = home_score + away_score
        if (
            combined > best_score
            or (
                math.isclose(combined, best_score)
                and (
                    time_rank < best_time_rank
                    or (
                        time_rank == best_time_rank
                        and delta_hours < best_delta_hours
                    )
                )
            )
        ):
            second_best = best_score
            best_score = combined
            best_min = min(home_score, away_score)
            best_time_rank = time_rank
            best_delta_hours = delta_hours
            best = fixture

    if not best:
        return None
    if best_min < MINIMUM_TEAM_SCORE:
        return None
    if best_score >= EXACT_COMBINED_SCORE:
        return best
    if best_score >= DISAMBIGUATED_COMBINED_SCORE and (best_score - second_best) >= MINIMUM_SCORE_MARGIN:
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
            first = tokens[0]
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
                    "first": first,
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
    raw_tokens = normalize_name_tokens(raw_name)
    raw_first = raw_tokens[0] if len(raw_tokens) >= 2 else ""
    require_first_match = len(raw_first) > 1
    team_id_set = {int(x) for x in team_ids or [] if x}
    candidate_first: Dict[int, str] = {}
    for candidate in fuzzy_candidates or []:
        player_id = candidate.get("player_id")
        if player_id is None:
            continue
        first = str(candidate.get("first") or "")
        existing = candidate_first.get(int(player_id), "")
        if not existing or (len(existing) <= 1 and len(first) > 1):
            candidate_first[int(player_id)] = first

    def apply_first_name_filter(
        candidates: List[Tuple[int, Optional[int]]],
    ) -> List[Tuple[int, Optional[int]]]:
        if not require_first_match:
            return candidates
        filtered: List[Tuple[int, Optional[int]]] = []
        for pid, tid in candidates:
            first = candidate_first.get(pid, "")
            if not first or len(first) <= 1:
                continue
            if first == raw_first:
                filtered.append((pid, tid))
        return filtered

    variants = name_variants(raw_name)
    if not variants:
        return None
    saw_candidates = False
    for variant in variants:
        candidates = fixture_map.get(variant)
        if candidates:
            saw_candidates = True
            filtered = [
                (pid, tid)
                for pid, tid in candidates
                if not team_id_set or (tid and tid in team_id_set)
            ]
            if filtered:
                candidates = filtered
            candidates = apply_first_name_filter(candidates)
            if not candidates:
                continue
            unique = {pid for pid, _ in candidates}
            if len(unique) == 1:
                return next(iter(unique))
            continue
    for variant in variants:
        candidates = team_map.get(variant)
        if candidates:
            saw_candidates = True
            filtered = [
                (pid, tid)
                for pid, tid in candidates
                if not team_id_set or (tid and tid in team_id_set)
            ]
            if filtered:
                candidates = filtered
            candidates = apply_first_name_filter(candidates)
            if not candidates:
                continue
            unique = {pid for pid, _ in candidates}
            if len(unique) == 1:
                return next(iter(unique))
            continue
    if fuzzy_candidates and not saw_candidates:
        fuzzy_match = fuzzy_match_player(raw_name, fuzzy_candidates)
        if fuzzy_match:
            candidate, _score = fuzzy_match
            return int(candidate["player_id"])
    return None


def upsert_outcomes(session, rows: List[Dict], preserve_existing: bool = False) -> None:
    if not rows:
        return
    rows = dedupe_outcome_rows(rows)
    if not rows:
        return
    conflict_sql = (
        "do nothing"
        if preserve_existing
        else """
        do update set
          participant_type = coalesce(excluded.participant_type, odds_outcomes.participant_type),
          participant_id = excluded.participant_id,
          price_decimal = excluded.price_decimal,
          price_american = excluded.price_american,
          last_updated_at = excluded.last_updated_at
        """
    )
    sql = text(
        f"""
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
        {conflict_sql}
        """
    )
    session.execute(sql, rows)


def normalize_outcome_line(value: object) -> Optional[float]:
    parsed = parse_float(value)
    if parsed is None:
        return None
    return round(parsed, 6)


def outcome_write_key(row: Dict[str, object]) -> Tuple[int, int, str, str, Optional[float]]:
    return (
        int(row.get("fixture_id") or 0),
        int(row.get("bookmaker_id") or 0),
        str(row.get("market_key") or ""),
        str(row.get("selection_key") or ""),
        normalize_outcome_line(row.get("line")),
    )


def should_replace_outcome_row(current: Dict[str, object], candidate: Dict[str, object]) -> bool:
    current_updated = current.get("last_updated_at")
    candidate_updated = candidate.get("last_updated_at")
    if candidate_updated and (not current_updated or candidate_updated > current_updated):
        return True
    if current_updated and candidate_updated and current_updated > candidate_updated:
        return False
    current_participant_id = current.get("participant_id")
    candidate_participant_id = candidate.get("participant_id")
    if current_participant_id is None and candidate_participant_id is not None:
        return True
    current_participant_type = current.get("participant_type")
    candidate_participant_type = candidate.get("participant_type")
    if not current_participant_type and candidate_participant_type:
        return True
    return False


def dedupe_outcome_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    deduped: Dict[Tuple[int, int, str, str, Optional[float]], Dict[str, object]] = {}
    for row in rows:
        key = outcome_write_key(row)
        current = deduped.get(key)
        if current is None or should_replace_outcome_row(current, row):
            deduped[key] = row
    collapsed = len(rows) - len(deduped)
    if collapsed > 0:
        log.warning(
            "Collapsed %s duplicate odds_outcomes rows before SQLite upsert for fixture=%s bookmaker=%s",
            collapsed,
            rows[0].get("fixture_id"),
            rows[0].get("bookmaker_id"),
        )
    return list(deduped.values())


def delete_fixture_market_rows(
    session,
    fixture_id: int,
    bookmaker_id: int,
    market_keys: Iterable[str],
) -> None:
    keys = [key for key in market_keys if key]
    if not keys:
        return
    stmt = (
        text(
            """
            delete from odds_outcomes
            where fixture_id = :fixture_id
              and bookmaker_id = :bookmaker_id
              and market_key in :market_keys
            """
        )
        .bindparams(bindparam("market_keys", expanding=True))
    )
    session.execute(
        stmt,
        {"fixture_id": fixture_id, "bookmaker_id": bookmaker_id, "market_keys": keys},
    )


def delete_invalid_goals_over_under(
    session,
    league_ids: Iterable[int],
    days_back: int,
    days_forward: int,
    calendar_history: bool = False,
) -> int:
    league_list = [int(value) for value in league_ids if value]
    if not league_list:
        return 0
    start_dt, end_dt = fixture_window_bounds(days_back, days_forward, calendar_history)
    start_dt_sql = sqlite_utc_timestamp(start_dt)
    end_dt_sql = sqlite_utc_timestamp(end_dt)
    stmt = (
        text(
            """
            delete from odds_outcomes
            where fixture_id in (
                select id
                from fixtures
                where league_id in :league_ids
                  and starting_at >= :start_dt
                  and starting_at < :end_dt
            )
              and market_key in ('goals_over_under','goals_over_under_first_half')
              and (selection_key not in ('over','under') or line is null)
            """
        )
        .bindparams(bindparam("league_ids", expanding=True))
    )
    result = session.execute(
        stmt,
        {"league_ids": league_list, "start_dt": start_dt_sql, "end_dt": end_dt_sql},
    )
    return int(result.rowcount or 0)


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
        entries: Dict[Tuple[str, float], float] = {}
        line = extract_over_under_line(odd)
        over_price = parse_float(odd.get("over"))
        under_price = parse_float(odd.get("under"))
        if line is not None:
            line = round(line, 2)
            if over_price is not None:
                entries[("over", line)] = over_price
            if under_price is not None:
                entries[("under", line)] = under_price

        if not entries:
            side = parse_side_from_text(odd.get("label") or odd.get("name"))
            price = parse_float(odd.get("price") or odd.get("odd"))
            if side and price is not None:
                line = extract_over_under_line(odd)
                if line is not None:
                    line = round(line, 2)
                    entries[(side, line)] = price

        if not entries:
            for key, value in odd.items():
                if key in {"hdp", "line", "total", "handicap", "over", "under", "label", "name"}:
                    continue
                line_from_key, side_from_key = parse_line_side_from_key(str(key))
                if line_from_key is None or side_from_key is None:
                    continue
                price = parse_float(value)
                if price is None:
                    continue
                line_from_key = round(line_from_key, 2)
                entries[(side_from_key, line_from_key)] = price

        for (side, line_value), price in entries.items():
            sel = side if not selection_prefix else f"{selection_prefix}_{side}"
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "bookmaker_id": bookmaker_id,
                    "market_key": market_key,
                    "selection_key": sel,
                    "participant_type": participant_type,
                    "participant_id": participant_id,
                    "line": line_value,
                    "price_decimal": price,
                    "price_american": decimal_to_american(price),
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

        if normalized_key in {
            "goals_over_under",
            "goals_over_under_first_half",
            "match_shots",
            "match_shots_on_target",
        }:
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
                    selection_prefix=side,
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
                price, side_key = extract_player_market_price(normalized_key, bookmaker_id, odd)
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
    row_map: Dict[Tuple[int, int, str, str, Optional[float]], Dict[str, object]] = {}
    priority_map: Dict[Tuple[int, int, str, str, Optional[float]], int] = {}
    bookmaker_id = BOOKMAKER_NAME_TO_ID.get(normalize_bookmaker_key(bookmaker_name))
    if not bookmaker_id:
        return []
    fixture_id = int(fixture["fixture_id"])
    home_team_id = fixture.get("home_team_id")
    away_team_id = fixture.get("away_team_id")

    def merge_rows(new_rows: List[Dict[str, object]], priority: int) -> None:
        for row in new_rows:
            key = (
                int(row.get("fixture_id") or 0),
                int(row.get("bookmaker_id") or 0),
                str(row.get("market_key") or ""),
                str(row.get("selection_key") or ""),
                row.get("line"),
            )
            existing_priority = priority_map.get(key)
            if existing_priority is None or priority > existing_priority:
                row_map[key] = row
                priority_map[key] = priority
                continue
            if existing_priority > priority:
                continue
            current = row_map.get(key)
            if not current:
                row_map[key] = row
                priority_map[key] = priority
                continue
            current_updated = current.get("last_updated_at")
            next_updated = row.get("last_updated_at")
            if next_updated and (not current_updated or next_updated > current_updated):
                row_map[key] = row

    for market in markets or []:
        market_name = str(market.get("name") or "")
        market_source_key = normalize_market_key(market_name)
        if "alternative_goal_line" in market_source_key or "alternate_goal_line" in market_source_key:
            # Exclude alternative goal lines; keep only main goals over/under markets.
            continue
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
        priority = 0
        if normalized_key in {"goals_over_under", "goals_over_under_first_half"}:
            if "alternative_goal_line" in market_source_key or "alternate_goal_line" in market_source_key:
                priority = 1
            else:
                priority = 2

        if normalized_key == "moneyline":
            merge_rows(
                market_moneyline_rows(
                    fixture_id,
                    bookmaker_id,
                    odds_list,
                    updated_at,
                    home_team_id,
                    away_team_id,
                ),
                priority,
            )
            continue

        if normalized_key == "double_chance":
            merge_rows(
                parse_double_chance_rows(fixture, bookmaker_id, odds_list, updated_at),
                priority,
            )
            continue

        if normalized_key == "draw_no_bet":
            merge_rows(
                parse_draw_no_bet_rows(
                    fixture_id,
                    bookmaker_id,
                    odds_list,
                    updated_at,
                    home_team_id,
                    away_team_id,
                ),
                priority,
            )
            continue

        if normalized_key == "btts":
            merge_rows(
                market_yes_no_rows(
                    fixture_id,
                    bookmaker_id,
                    normalized_key,
                    odds_list,
                    updated_at,
                ),
                priority,
            )
            continue

        if normalized_key in {"goals_over_under", "match_shots", "match_shots_on_target"}:
            merge_rows(
                market_over_under_rows(
                    fixture_id,
                    bookmaker_id,
                    normalized_key,
                    odds_list,
                    None,
                    None,
                    updated_at,
                ),
                priority,
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
            merge_rows(
                market_over_under_rows(
                    fixture_id,
                    bookmaker_id,
                    normalized_key,
                    odds_list,
                    "team",
                    int(team_id),
                    updated_at,
                    selection_prefix=side,
                ),
                priority,
            )
            continue

        if normalized_key.startswith("player_"):
            merge_rows(
                parse_player_market_rows(
                    fixture,
                    normalized_key,
                    odds_list,
                    bookmaker_id,
                    updated_at,
                    session,
                    unmatched_details,
                ),
                priority,
            )
            continue

    return list(row_map.values())


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
        price, side_key = extract_player_market_price(market_key, bookmaker_id, odd)
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
        if market_key == "player_to_score_or_assist":
            label_lower = str(label).lower()
            if "score or assist" in label_lower or "goal or assist" in label_lower:
                goa_tag = "score_or_assist"
            elif "assist" in label_lower:
                goa_tag = "assist"
            elif "score" in label_lower or "goal" in label_lower:
                goa_tag = "score"
            else:
                goa_tag = "score_or_assist"
            selection_slug = f"{selection_slug}_{goa_tag}"
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
    parser.add_argument(
        "--include-excluded",
        action="store_true",
        help="Explicitly allow leagues listed in odds_api_sync_excluded_leagues.json.",
    )
    # Keep a short settled window so callers that omit --days-back still
    # retrieve historical pre-kickoff odds for fixtures that finished earlier
    # on the same day.
    parser.add_argument("--days-back", type=int, default=int(os.environ.get("ODDS_SYNC_DAYS_BACK", "1")))
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument(
        "--bookmakers",
        default=",".join(DEFAULT_BOOKMAKERS),
        help="Comma-separated bookmaker names",
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit events processed")
    parser.add_argument("--sport", default="football")
    parser.add_argument(
        "--priority",
        choices=["p1", "p2", "p3", "settled-history"],
        default=None,
        help="Optional scope: p1<=2h, p2=2-24h, p3=24h-14d, or settled-history for the previous calendar days.",
    )
    parser.add_argument(
        "--refresh-upcoming",
        action="store_true",
        help="Refresh upcoming fixtures from SportMonks before fetching odds.",
    )
    parser.add_argument(
        "--refresh-squads",
        action="store_true",
        help="Refresh squads for all teams in the odds window.",
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
        "--refresh-sidelined-window",
        dest="refresh_sidelined_window",
        action="store_true",
        help="Refresh sidelined status for all teams in the odds window (default).",
    )
    parser.add_argument(
        "--no-refresh-sidelined-window",
        dest="refresh_sidelined_window",
        action="store_false",
        help="Disable auto refresh of sidelined status for teams in the odds window.",
    )
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
    parser.add_argument(
        "--refresh-only",
        action="store_true",
        help="Run SportMonks refresh steps only and skip odds fetch/write.",
    )
    parser.set_defaults(refresh_squads_missing=True, refresh_sidelined_window=True)
    args = parser.parse_args()
    calendar_history = args.priority == "settled-history"
    window_start, window_end = fixture_window_bounds(
        args.days_back,
        args.days_forward,
        calendar_history,
    )
    window_kind = "settled_history_calendar" if calendar_history else "rolling"
    window_start_iso = window_start.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    window_end_iso = window_end.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    raw_leagues = args.leagues.replace('"', "").replace("'", "")
    league_ids = [int(x) for x in raw_leagues.split(",") if x.strip()]
    if not league_ids:
        raise SystemExit("No league IDs provided")
    excluded_ids = load_excluded_league_ids()
    blocked_ids = sorted(set(league_ids).intersection(excluded_ids))
    if blocked_ids and not args.include_excluded:
        raise SystemExit(
            "Refusing paid Odds-API sync for excluded cup league IDs "
            f"{blocked_ids}; pass --include-excluded only for an intentional override."
        )

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

    refresh_upcoming = bool(args.refresh_upcoming)
    refresh_squads = bool(args.refresh_squads)
    refresh_squads_missing = bool(args.refresh_squads_missing)
    refresh_sidelined_window = bool(args.refresh_sidelined_window)
    if args.priority:
        if refresh_upcoming or refresh_squads or refresh_squads_missing or refresh_sidelined_window:
            log.info("Priority mode %s active: skipping SportMonks refresh steps.", args.priority)
        refresh_upcoming = False
        refresh_squads = False
        refresh_squads_missing = False
        refresh_sidelined_window = False

    svc: Optional[SyncService] = None
    if refresh_upcoming or refresh_squads or refresh_squads_missing or refresh_sidelined_window:
        if not os.environ.get("SPORTMONKS_API_TOKEN"):
            log.warning("SPORTMONKS_API_TOKEN missing; skipping SportMonks refresh steps.")
        else:
            client_sm = SportMonksClient()
            svc = SyncService(client_sm, session)
            svc.ensure_schema()
            if refresh_upcoming:
                log.info("Refreshing upcoming fixtures for odds window (%s days)", args.days_forward)
                svc.sync_upcoming_window(league_ids, days_forward=args.days_forward)

    fixtures = load_fixtures(
        session,
        league_ids,
        args.days_back,
        args.days_forward,
        calendar_history,
    )
    if args.priority:
        before = len(fixtures)
        fixtures = filter_fixtures_by_priority(fixtures, args.priority)
        log.info("Priority filter=%s fixtures_in_scope=%s/%s", args.priority, len(fixtures), before)
    if not fixtures:
        log.info("No fixtures found for odds window")
        if args.report_out:
            report = {
                "generated_at": utc_now_iso(),
                "league_ids": league_ids,
                "priority": args.priority,
                "window_kind": window_kind,
                "window_start": window_start_iso,
                "window_end": window_end_iso,
                "refresh_only": args.refresh_only,
                "fixtures_in_scope": 0,
                "events_matched": 0,
                "outcomes_written": 0,
                "moneyline_coverage": [],
                "errors": [],
            }
            Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        return

    teams_in_window = {
        fixture.get("home_team_id")
        for fixture in fixtures
        if fixture.get("home_team_id")
    } | {
        fixture.get("away_team_id")
        for fixture in fixtures
        if fixture.get("away_team_id")
    }
    window_team_ids = sorted(int(tid) for tid in teams_in_window if tid)
    missing_team_ids: List[int] = []
    refreshed_team_ids: List[int] = []
    if svc and (refresh_squads or refresh_squads_missing) and teams_in_window:
        if refresh_squads:
            refreshed_team_ids = window_team_ids
        else:
            counts = fetch_team_player_counts(session, teams_in_window)
            missing_team_ids = [int(tid) for tid in teams_in_window if counts.get(int(tid), 0) == 0]
            refreshed_team_ids = sorted(missing_team_ids)
        if refreshed_team_ids:
            log.info(
                "Refreshing squads for %s/%s teams",
                len(refreshed_team_ids),
                len(teams_in_window),
            )
            svc.sync_squads_for_teams(refreshed_team_ids)
    sidelined_refreshed_team_ids: List[int] = []
    if svc and refresh_sidelined_window and window_team_ids:
        sidelined_refreshed_team_ids = window_team_ids
        log.info(
            "Refreshing sidelined status for %s teams in odds window",
            len(sidelined_refreshed_team_ids),
        )
        svc.sync_sidelined_for_teams(sidelined_refreshed_team_ids)

    if args.refresh_only:
        log.info("Refresh-only mode complete; skipping odds fetch/write stage.")
        if args.report_out:
            report = {
                "generated_at": utc_now_iso(),
                "league_ids": league_ids,
                "priority": args.priority,
                "window_kind": window_kind,
                "window_start": window_start_iso,
                "window_end": window_end_iso,
                "refresh_only": True,
                "fixtures_in_scope": len(fixtures),
                "teams_in_window": len(teams_in_window),
                "teams_missing_players": len(missing_team_ids),
                "teams_squads_refreshed": len(refreshed_team_ids),
                "teams_sidelined_refreshed": len(sidelined_refreshed_team_ids),
                "events_matched": 0,
                "outcomes_written": 0,
                "moneyline_coverage": [],
                "errors": [],
            }
            Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        return

    league_map = load_league_map(Path(__file__).resolve().parent.parent / "config" / "odds_api_leagues.json")

    unmatched_details: List[Dict[str, object]] = []
    events_raw: Dict[int, object] = {}
    odds_raw: Dict[int, object] = {}
    events_unmatched_samples: List[Dict[str, object]] = []
    bookmaker_names_seen: set[str] = set()
    bookmaker_names_saved: set[str] = set()
    bookmaker_names_unknown: set[str] = set()
    league_stats: Dict[int, Dict[str, int]] = {}
    moneyline_coverage: List[Dict[str, object]] = []
    fetch_errors: List[str] = []

    fixtures_by_id: Dict[int, Dict[str, object]] = {int(fixture["fixture_id"]): fixture for fixture in fixtures}
    leagues_with_fixtures: List[Tuple[int, str, List[Dict[str, object]]]] = []
    for league_id in league_ids:
        odds_league = league_map.get(league_id)
        if not odds_league:
            log.warning("No Odds-API league mapping for league_id=%s (skipping)", league_id)
            continue
        league_fixtures = [fixture for fixture in fixtures if fixture["league_id"] == league_id]
        if not league_fixtures:
            continue
        leagues_with_fixtures.append((league_id, odds_league, league_fixtures))

    scoped_league_ids = [league_id for league_id, _, _ in leagues_with_fixtures]
    predicted_calls = estimate_api_calls_for_scope(fixtures, scoped_league_ids)
    configured_rate_limit = 0
    configured_raw = (os.environ.get("ODDS_API_RATE_LIMIT_PER_HOUR") or "").strip()
    if configured_raw:
        try:
            configured_rate_limit = int(configured_raw)
        except ValueError:
            configured_rate_limit = 0
            log.warning("Invalid ODDS_API_RATE_LIMIT_PER_HOUR=%s (ignored)", configured_raw)
    log.info(
        "Sync scope priority=%s fixtures=%s leagues=%s predicted_api_calls=%s configured_rate_limit=%s",
        args.priority or "all",
        len(fixtures),
        len(scoped_league_ids),
        predicted_calls,
        configured_rate_limit or "unset",
    )
    if configured_rate_limit and predicted_calls > int(configured_rate_limit * 0.8):
        log.warning(
            "Predicted API calls (%s) exceed 80%% of configured hourly limit (%s).",
            predicted_calls,
            configured_rate_limit,
        )

    max_concurrent = max(1, int(os.environ.get("ODDS_SYNC_MAX_CONCURRENT", "3")))
    results_by_league: Dict[int, LeagueResult] = {}
    if leagues_with_fixtures:
        workers = min(max_concurrent, len(leagues_with_fixtures))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    fetch_league_odds_payload,
                    league_id,
                    odds_league,
                    league_fixtures,
                    args.sport,
                    args.days_back,
                    args.days_forward,
                    bookmakers,
                    args.limit,
                    calendar_history,
                ): league_id
                for league_id, odds_league, league_fixtures in leagues_with_fixtures
            }
            for future in as_completed(futures):
                league_id = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = LeagueResult(
                        league_id=league_id,
                        league_name=league_map.get(league_id, ""),
                        fixtures_fetched=0,
                        odds_records=[],
                        api_calls_made=0,
                        fetch_duration_seconds=0.0,
                        error=exc,
                    )
                results_by_league[league_id] = result

    outcomes_total = 0
    api_calls_total = 0
    api_calls_by_endpoint: Dict[str, int] = {}
    api_time_seconds = 0.0
    rate_limit_hits = 0
    rate_limit_sleeps = 0
    last_rate_limit: Optional[Dict[str, object]] = None
    events_matched_total = 0

    for league_id in league_ids:
        result = results_by_league.get(league_id)
        if result is None:
            continue
        league_stats[league_id] = {
            "fixtures_in_window": result.fixtures_fetched,
            "events_returned": result.events_returned,
            "events_matched": result.events_matched,
            "moneyline_evidence": len(result.moneyline_coverage),
        }
        events_matched_total += result.events_matched
        moneyline_coverage.extend(result.moneyline_coverage)
        api_calls_total += result.api_calls_made
        api_time_seconds += result.api_time_seconds
        rate_limit_hits += result.rate_limit_hits
        rate_limit_sleeps += result.rate_limit_sleeps
        if result.last_rate_limit:
            last_rate_limit = result.last_rate_limit
        for endpoint, count in result.api_calls_by_endpoint.items():
            api_calls_by_endpoint[endpoint] = api_calls_by_endpoint.get(endpoint, 0) + int(count)

        if args.debug_events_out:
            events_raw[league_id] = result.raw_events
        for sample in result.unmatched_samples:
            if len(events_unmatched_samples) >= 20:
                break
            events_unmatched_samples.append(sample)

        if result.error is not None:
            message = f"league {league_id} fetch failed: {result.error}"
            log.error(message)
            fetch_errors.append(message)
            continue

        for odds_record in result.odds_records:
            fixture_id = int(odds_record["fixture_id"])
            event_id = int(odds_record["event_id"])
            fixture = fixtures_by_id.get(fixture_id)
            if fixture is None:
                continue
            bookmakers_payload = odds_record.get("bookmakers_payload") or {}
            if args.debug_odds_out:
                odds_raw[event_id] = bookmakers_payload
            bookmaker_groups: Dict[str, List[List[Dict[str, object]]]] = {}
            for bookmaker_name, markets in bookmakers_payload.items():
                bookmaker_names_seen.add(bookmaker_name)
                book_key = normalize_bookmaker_key(bookmaker_name)
                if book_key not in requested_bookmaker_keys:
                    continue
                if book_key not in BOOKMAKER_NAME_TO_ID:
                    bookmaker_names_unknown.add(bookmaker_name)
                    continue
                bookmaker_groups.setdefault(book_key, []).append(list(markets or []))

            for book_key, market_lists in bookmaker_groups.items():
                canonical_name = BOOKMAKER_CANONICAL.get(book_key, book_key)
                merged_markets: List[Dict[str, object]] = []
                for market_list in market_lists:
                    merged_markets.extend(market_list)
                rows = parse_markets_for_fixture_extended(
                    fixture,
                    merged_markets,
                    canonical_name,
                    market_allowlist,
                    session,
                    unmatched_details,
                )
                if not rows:
                    continue
                market_keys = {row.get("market_key") for row in rows if row.get("market_key")}
                if not calendar_history:
                    delete_fixture_market_rows(
                        session,
                        fixture_id,
                        BOOKMAKER_NAME_TO_ID[book_key],
                        market_keys,
                    )
                upsert_outcomes(session, rows, preserve_existing=calendar_history)
                outcomes_total += len(rows)
                bookmaker_names_saved.add(canonical_name)
            session.commit()

    removed_invalid = 0
    if args.priority is None:
        removed_invalid = delete_invalid_goals_over_under(
            session,
            league_ids,
            args.days_back,
            args.days_forward,
            calendar_history,
        )
    if removed_invalid:
        log.warning("Removed %s invalid goals_over_under rows after sync.", removed_invalid)
        session.commit()

    log.info(
        "Odds sync complete: outcomes=%s events_matched=%s actual_api_calls=%s",
        outcomes_total,
        events_matched_total,
        api_calls_total,
    )

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
            "generated_at": utc_now_iso(),
            "league_ids": league_ids,
            "bookmakers": bookmakers,
            "bookmakers_requested": bookmakers,
            "bookmakers_unknown_requested": unknown_bookmakers_requested,
            "bookmakers_seen": sorted(bookmaker_names_seen),
            "bookmakers_saved": sorted(bookmaker_names_saved),
            "bookmakers_unknown_seen": sorted(bookmaker_names_unknown),
            "teams_in_window": len(teams_in_window),
            "teams_missing_players": len(missing_team_ids),
            "teams_squads_refreshed": len(refreshed_team_ids),
            "teams_sidelined_refreshed": len(sidelined_refreshed_team_ids),
            "priority": args.priority,
            "window_kind": window_kind,
            "window_start": window_start_iso,
            "window_end": window_end_iso,
            "refresh_only": False,
            "days_back": args.days_back,
            "days_forward": args.days_forward,
            "fixtures_in_scope": len(fixtures),
            "leagues_in_scope": len(scoped_league_ids),
            "predicted_api_calls": predicted_calls,
            "configured_rate_limit_per_hour": configured_rate_limit or None,
            "events_matched": events_matched_total,
            "events_unmatched_samples": events_unmatched_samples,
            "moneyline_coverage": moneyline_coverage,
            "league_stats": league_stats,
            "outcomes_written": outcomes_total,
            "api_calls_total": api_calls_total,
            "api_calls_by_endpoint": api_calls_by_endpoint,
            "api_time_seconds": round(api_time_seconds, 2),
            "rate_limit_hits": rate_limit_hits,
            "rate_limit_sleeps": rate_limit_sleeps,
            "last_rate_limit": last_rate_limit,
            "errors": fetch_errors,
            "unmatched_players": len(unmatched_details),
        }
        Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")

    if fetch_errors:
        raise SystemExit("One or more league fetches failed.")


if __name__ == "__main__":
    main()
