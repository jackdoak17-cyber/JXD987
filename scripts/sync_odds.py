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
import re
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import bindparam, text

from jxd import SportMonksClient
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


def normalize_slug(value: str) -> str:
    text_val = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text_val or "unknown"


def normalize_name(value: str) -> str:
    text_val = unicodedata.normalize("NFKD", value)
    text_val = "".join(ch for ch in text_val if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", text_val.lower())


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
    for key in ("label", "handicap", "line"):
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


def parse_timestamp(value: Optional[object]) -> Optional[datetime]:
    if not value:
        return None
    text_val = str(value).replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(text_val)
    except Exception:
        return None


def resolve_market_key(row: Dict) -> str:
    market_id = parse_int(row.get("market_id"))
    if market_id in MARKET_ID_MAP:
        return MARKET_ID_MAP[market_id]
    desc = row.get("market_description") or row.get("market") or "market"
    return normalize_slug(str(desc))


PLAYER_MARKET_KEYS = {
    "goalscorers",
    "1st_goal_scorer",
    "last_goal_scorer",
    "multi_scorers",
    "player_to_score",
    "player_to_score_or_assist",
    "player_shots",
    "player_shots_on_target",
}


def resolve_participant_type(row: Dict, market_key: str) -> Optional[str]:
    desc = str(row.get("market_description") or "").lower()
    if "player" in desc or market_key.startswith("player_") or market_key in PLAYER_MARKET_KEYS:
        return "player"
    if "team" in desc:
        return "team"
    return None


def fetch_fixture_rows(session, league_ids: List[int], start: datetime, end: datetime) -> List[Dict]:
    league_list = ",".join(str(x) for x in league_ids)
    rows = session.execute(
        text(
            f"""
            select id, home_team_id, away_team_id
            from fixtures
            where league_id in ({league_list})
              and starting_at >= :start
              and starting_at <= :end
            """
        ),
        {"start": start, "end": end},
    ).fetchall()
    return [
        {"fixture_id": r[0], "home_team_id": r[1], "away_team_id": r[2]} for r in rows
    ]


def load_team_map(session, team_ids: Iterable[int]) -> Dict[str, int]:
    ids = [int(x) for x in team_ids if x]
    if not ids:
        return {}
    stmt = text("select id, name, short_code from teams where id in :ids").bindparams(
        bindparam("ids", expanding=True),
    )
    rows = session.execute(stmt, {"ids": ids}).fetchall()
    mapping: Dict[str, int] = {}
    for team_id, name, short_code in rows:
        for label in (name, short_code):
            if not label:
                continue
            mapping[normalize_name(str(label))] = int(team_id)
    return mapping


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
            normalized = normalize_name(str(candidate))
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
            normalized = normalize_name(str(candidate))
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
) -> Optional[int]:
    normalized = normalize_name(raw_name)
    if not normalized:
        return None
    candidates = fixture_map.get(normalized)
    if candidates:
        unique = {pid for pid, _ in candidates}
        if len(unique) == 1:
            return next(iter(unique))
        return candidates[0][0]
    candidates = team_map.get(normalized)
    if candidates:
        unique = {pid for pid, _ in candidates}
        if len(unique) == 1:
            return next(iter(unique))
        return candidates[0][0]
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
) -> List[Dict]:
    outcomes: List[Dict] = []
    for row in data:
        market_key = resolve_market_key(row)
        participant_type = resolve_participant_type(row, market_key)
        name = row.get("name") or row.get("total") or row.get("label") or ""
        label = row.get("label") or row.get("total") or ""
        selection_key = normalize_slug(f"{name} {label}".strip())

        line = parse_line(row)
        price_decimal = parse_float(row.get("value") or row.get("dp3"))
        if price_decimal is None:
            continue
        price_american = parse_int(row.get("american"))
        last_updated_at = parse_timestamp(row.get("latest_bookmaker_update") or row.get("updated_at"))

        participant_id = None
        normalized_name = normalize_name(str(name))
        if participant_type is None and normalized_name in team_map:
            participant_type = "team"
        if participant_type == "player":
            participant_id = resolve_player_id(str(name), player_map, team_player_map)
            if participant_id is None:
                log.warning(
                    "Unmatched player odds name (fixture %s, market %s, selection %s): %s",
                    fixture_id,
                    market_key,
                    selection_key,
                    name,
                )
        elif participant_type == "team":
            participant_id = team_map.get(normalized_name)

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
    payload = client.request(
        "GET",
        f"odds/pre-match/fixtures/{fixture_id}",
        params={"filter": f"bookmakers:{bookmaker_id}"},
    )
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, list) else []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8", help="Comma-separated league IDs")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--bookmaker-id", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=0, help="Limit fixtures processed")
    args = parser.parse_args()

    league_ids = [int(x) for x in args.leagues.split(",") if x.strip()]
    if not league_ids:
        raise SystemExit("No league IDs provided")

    engine = get_engine()
    session = get_session(engine)
    Base.metadata.create_all(engine)

    client = SportMonksClient()
    now = datetime.utcnow()
    end = now + timedelta(days=args.days_forward)

    fixtures = fetch_fixture_rows(session, league_ids, now, end)
    if args.limit and args.limit > 0:
        fixtures = fixtures[: args.limit]

    if not fixtures:
        log.info("No fixtures found for odds window")
        return

    for idx, fixture in enumerate(fixtures, start=1):
        fixture_id = fixture["fixture_id"]
        team_map = load_team_map(session, [fixture.get("home_team_id"), fixture.get("away_team_id")])
        player_map = load_fixture_player_map(session, fixture_id)
        team_player_map = load_team_player_map(
            session,
            [fixture.get("home_team_id"), fixture.get("away_team_id")],
        )

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
        )
        upsert_outcomes(session, outcomes)
        session.commit()
        log.info(
            "Processed fixture %s (%s/%s) outcomes=%s",
            fixture_id,
            idx,
            len(fixtures),
            len(outcomes),
        )

    log.info("Odds sync complete")


if __name__ == "__main__":
    main()
