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


HOME_ALIASES = {"home", "hometeam", "team1"}
AWAY_ALIASES = {"away", "awayteam", "team2"}


def resolve_team_id(
    raw_name: str,
    selection_key: str,
    team_map: Dict[str, int],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_aliases: Iterable[str],
    away_aliases: Iterable[str],
) -> Optional[int]:
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
    unmatched_details: Optional[List[Dict]] = None,
    unmatched_counts: Optional[Dict[Tuple[str, str], int]] = None,
    unmatched_team_counts: Optional[Dict[str, int]] = None,
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
        team_id_candidate = resolve_team_id(
            str(name),
            selection_key,
            team_map,
            home_team_id,
            away_team_id,
            home_aliases,
            away_aliases,
        )
        if participant_type is None and team_id_candidate is not None:
            participant_type = "team"
        if participant_type == "player":
            participant_id = resolve_player_id(str(name), player_map, team_player_map)
            if participant_id is None:
                if unmatched_counts is not None:
                    key = (market_key, str(name))
                    unmatched_counts[key] = unmatched_counts.get(key, 0) + 1
                if unmatched_details is not None:
                    unmatched_details.append(
                        {
                            "fixture_id": fixture_id,
                            "market_key": market_key,
                            "selection_key": selection_key,
                            "raw_name": str(name),
                        }
                    )
        elif participant_type == "team":
            participant_id = team_id_candidate or team_map.get(normalized_name)
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
    parser.set_defaults(refresh_squads_missing=True)
    args = parser.parse_args()

    raw_leagues = args.leagues.replace('"', "").replace("'", "")
    league_ids = [int(x) for x in raw_leagues.split(",") if x.strip()]
    if not league_ids:
        raise SystemExit("No league IDs provided")

    engine = get_engine()
    session = get_session(engine)
    Base.metadata.create_all(engine)

    client = SportMonksClient()
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
            unmatched_details,
            unmatched_counts,
            unmatched_team_counts,
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

    if args.unmatched_out:
        try:
            with open(args.unmatched_out, "w", encoding="utf-8") as f:
                json.dump(unmatched_details, f, indent=2)
            log.info("Wrote unmatched player odds to %s (%s rows)", args.unmatched_out, len(unmatched_details))
        except OSError as exc:
            log.warning("Failed to write unmatched output %s: %s", args.unmatched_out, exc)


if __name__ == "__main__":
    main()
