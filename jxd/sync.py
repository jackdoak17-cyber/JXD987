from __future__ import annotations

import logging
import hashlib
import json
from datetime import datetime, timedelta, date
from typing import Dict, Iterable, Optional, Sequence, Set, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .sportmonks_client import SportMonksClient, SportMonksError
from .models import (
    Base,
    Season,
    Round,
    Team,
    Player,
    PlayerTeamHistory,
    TeamSquadMembership,
    TeamSquadSnapshot,
    Fixture,
    FixtureParticipant,
    FixtureStatistic,
    FixturePlayerStatistic,
    FixturePlayer,
    SidelinedPlayer,
)

log = logging.getLogger(__name__)


def parse_dt(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    txt = str(raw).replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(txt)
    except Exception:
        return None


def _parse_date(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    try:
        return date.fromisoformat(str(raw))
    except Exception:
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(val)
    except Exception:
        try:
            return int(float(val))
        except Exception:
            return None


def _fixture_status_values(raw: Dict) -> Tuple[Optional[str], Optional[str]]:
    state = raw.get("state") or {}
    state_short = state.get("short_name") if isinstance(state, dict) else None
    state_name = state.get("state") if isinstance(state, dict) else None
    state_developer = state.get("developer_name") if isinstance(state, dict) else None

    status = raw.get("status") or state_short or state_name or state_developer
    status_code = raw.get("status_code") or state_name or state_developer or state_short

    return (
        str(status).upper() if status else None,
        str(status_code).upper() if status_code else None,
    )


def _fixture_lineup_confirmed(raw: Dict) -> Tuple[bool, Optional[bool]]:
    metadata = raw.get("metadata")
    if not isinstance(metadata, list):
        return False, None
    for item in metadata:
        if not isinstance(item, dict):
            continue
        if item.get("type_id") != 572:
            continue
        values = item.get("values")
        if not isinstance(values, dict):
            return True, None
        confirmed = values.get("confirmed")
        if isinstance(confirmed, bool):
            return True, confirmed
        return True, None
    return True, None


def _ensure_fixture_columns(engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        cols = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(fixtures)").fetchall()}
        desired = {
            "lineup_confirmed": "INTEGER",
        }
        for name, ddl_type in desired.items():
            if name not in cols:
                conn.exec_driver_sql(f"ALTER TABLE fixtures ADD COLUMN {name} {ddl_type}")


def _ensure_fixture_player_columns(engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        cols = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(fixture_players)").fetchall()}
        desired = {
            "detailed_position_id": "INTEGER",
            "detailed_position_name": "TEXT",
            "detailed_position_code": "TEXT",
            "formation_field": "TEXT",
            "formation_position": "INTEGER",
            "lineup_detailed_position_id": "INTEGER",
            "lineup_detailed_position_name": "TEXT",
            "lineup_detailed_position_code": "TEXT",
            "position_abbr": "TEXT",
        }
        for name, ddl_type in desired.items():
            if name not in cols:
                conn.exec_driver_sql(f"ALTER TABLE fixture_players ADD COLUMN {name} {ddl_type}")


def _ensure_team_player_columns(engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        for table in ("teams", "players"):
            cols = {row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()}
            if "image_path" not in cols:
                conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN image_path TEXT")
            if table == "players":
                for col in ("common_name", "short_name", "display_name", "team_updated_at"):
                    if col not in cols:
                        col_type = "TEXT" if col != "team_updated_at" else "DATETIME"
                        conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def _ensure_team_squad_columns(engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        tables = {
            row[0]
            for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "team_squad_memberships" not in tables:
            return
        cols = {
            row[1]
            for row in conn.exec_driver_sql("PRAGMA table_info(team_squad_memberships)").fetchall()
        }
        if "provider_started_at" not in cols:
            conn.exec_driver_sql(
                "ALTER TABLE team_squad_memberships ADD COLUMN provider_started_at DATETIME"
            )


def _extract_stat_value(data) -> Optional[int]:
    """Pull a numeric value from lineup.detail or statistic payloads."""
    if data is None:
        return None
    if isinstance(data, (int, float, str)):
        return _safe_int(data)
    if isinstance(data, dict):
        for key in (
            "value",
            "total",
            "goals",
            "shots_on_target",
            "shotson_target",
            "in",
            "out",
            "home",
            "away",
            "penalties",
        ):
            if key in data:
                v = _safe_int(data.get(key))
                if v is not None:
                    return v
        for v in data.values():
            parsed = _safe_int(v)
            if parsed is not None:
                return parsed
    return None


def _sum_stat_values(details: Iterable[Dict], type_id: int) -> Optional[int]:
    total = 0
    found = False
    for d in details or []:
        type_info = d.get("type") or {}
        stat_type_id = d.get("type_id") or type_info.get("id")
        if stat_type_id != type_id:
            continue
        value = _extract_stat_value(d.get("data") or d.get("value") or d.get("stat"))
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def _is_starter(lineup_type: Optional[object]) -> Optional[bool]:
    if lineup_type is None:
        return None
    text = str(lineup_type).lower()
    if text in {"11", "lineup", "starting", "starter", "1"}:
        return True
    if text in {"12", "substitute", "sub", "bench"}:
        return False
    return None


# SportMonks minutes played type id.
MINUTES_TYPE_IDS = {119}
MINUTES_NAME_HINTS = ("minute", "minutes")

GOALS_TYPE_ID = 52
ASSISTS_TYPE_ID = 79
GOAL_CONTRIBUTIONS_TYPE_ID = 200001
GOAL_CONTRIBUTIONS_CODE = "goal_contributions"
GOAL_CONTRIBUTIONS_NAME = "Goal Contributions"

SHOTS_TOTAL_TYPE_ID = 42
SHOTS_ON_TARGET_TYPE_ID = 86
SHOTS_INSIDEBOX_TYPE_ID = 49
TOTAL_CROSSES_TYPE_ID = 98
ACCURATE_CROSSES_TYPE_ID = 99
TACKLES_TYPE_ID = 78
INTERCEPTIONS_TYPE_ID = 100
CLEARANCES_TYPE_ID = 101
BLOCKED_SHOTS_TYPE_ID = 97
BALL_RECOVERY_TYPE_ID = 27271

SHOT_ACCURACY_PERCENT_TYPE_ID = 200010
SHOT_ACCURACY_PERCENT_CODE = "shot_accuracy_percent"
SHOT_ACCURACY_PERCENT_NAME = "Shot Accuracy %"

INSIDE_BOX_SHOT_SHARE_PERCENT_TYPE_ID = 200011
INSIDE_BOX_SHOT_SHARE_PERCENT_CODE = "inside_box_shot_share_percent"
INSIDE_BOX_SHOT_SHARE_PERCENT_NAME = "Inside Box Shot Share %"

CROSS_ACCURACY_PERCENT_TYPE_ID = 200012
CROSS_ACCURACY_PERCENT_CODE = "cross_accuracy_percent"
CROSS_ACCURACY_PERCENT_NAME = "Cross Accuracy %"

DEFENSIVE_INVOLVEMENT_TYPE_ID = 200013
DEFENSIVE_INVOLVEMENT_CODE = "defensive_involvement"
DEFENSIVE_INVOLVEMENT_NAME = "Defensive Involvement"

DERIVED_PLAYER_RATIO_STATS = (
    {
        "type_id": SHOT_ACCURACY_PERCENT_TYPE_ID,
        "code": SHOT_ACCURACY_PERCENT_CODE,
        "name": SHOT_ACCURACY_PERCENT_NAME,
        "numerator_type_id": SHOTS_ON_TARGET_TYPE_ID,
        "denominator_type_id": SHOTS_TOTAL_TYPE_ID,
    },
    {
        "type_id": CROSS_ACCURACY_PERCENT_TYPE_ID,
        "code": CROSS_ACCURACY_PERCENT_CODE,
        "name": CROSS_ACCURACY_PERCENT_NAME,
        "numerator_type_id": ACCURATE_CROSSES_TYPE_ID,
        "denominator_type_id": TOTAL_CROSSES_TYPE_ID,
    },
)

DERIVED_TEAM_RATIO_STATS = (
    {
        "type_id": SHOT_ACCURACY_PERCENT_TYPE_ID,
        "code": SHOT_ACCURACY_PERCENT_CODE,
        "name": SHOT_ACCURACY_PERCENT_NAME,
        "numerator_type_id": SHOTS_ON_TARGET_TYPE_ID,
        "denominator_type_id": SHOTS_TOTAL_TYPE_ID,
    },
    {
        "type_id": INSIDE_BOX_SHOT_SHARE_PERCENT_TYPE_ID,
        "code": INSIDE_BOX_SHOT_SHARE_PERCENT_CODE,
        "name": INSIDE_BOX_SHOT_SHARE_PERCENT_NAME,
        "numerator_type_id": SHOTS_INSIDEBOX_TYPE_ID,
        "denominator_type_id": SHOTS_TOTAL_TYPE_ID,
    },
    {
        "type_id": CROSS_ACCURACY_PERCENT_TYPE_ID,
        "code": CROSS_ACCURACY_PERCENT_CODE,
        "name": CROSS_ACCURACY_PERCENT_NAME,
        "numerator_type_id": ACCURATE_CROSSES_TYPE_ID,
        "denominator_type_id": TOTAL_CROSSES_TYPE_ID,
    },
)

DEFENSIVE_INVOLVEMENT_COMPONENT_TYPE_IDS = (
    TACKLES_TYPE_ID,
    INTERCEPTIONS_TYPE_ID,
    CLEARANCES_TYPE_ID,
    BALL_RECOVERY_TYPE_ID,
    BLOCKED_SHOTS_TYPE_ID,
)


def _safe_ratio_percent(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if denominator is None or denominator <= 0:
        return None
    numerator_value = float(numerator or 0)
    return round((numerator_value / float(denominator)) * 100.0, 4)


def _upsert_fixture_player_derived_stat(
    session: Session,
    fixture_id: int,
    player_id: int,
    team_id: Optional[int],
    type_id: int,
    code: str,
    name: str,
    value: Optional[float],
    extra: Dict,
) -> None:
    pk = (fixture_id, player_id, type_id, code)
    obj = session.get(FixturePlayerStatistic, pk)
    if value is None:
        if obj:
            session.delete(obj)
        return
    payload = {
        "fixture_id": fixture_id,
        "player_id": player_id,
        "team_id": team_id,
        "type_id": type_id,
        "code": code,
        "name": name,
        "value": value,
        "extra": extra,
    }
    if obj:
        for k, v in payload.items():
            setattr(obj, k, v)
    else:
        session.add(FixturePlayerStatistic(**payload))


def _upsert_fixture_team_derived_stat(
    session: Session,
    fixture_id: int,
    team_id: int,
    type_id: int,
    code: str,
    name: str,
    value: Optional[float],
    extra: Dict,
) -> None:
    pk = (fixture_id, team_id, type_id, code, None)
    obj = session.get(FixtureStatistic, pk)
    if value is None:
        if obj:
            session.delete(obj)
        return
    payload = {
        "fixture_id": fixture_id,
        "team_id": team_id,
        "type_id": type_id,
        "code": code,
        "name": name,
        "location": None,
        "value": value,
        "extra": extra,
    }
    if obj:
        for k, v in payload.items():
            setattr(obj, k, v)
    else:
        session.add(FixtureStatistic(**payload))


POSITION_ABBR_MAP = {
    "right-wing": "RW",
    "left-wing": "LW",
    "right-winger": "RW",
    "left-winger": "LW",
    "right-midfield": "RM",
    "left-midfield": "LM",
    "central-midfield": "CM",
    "defensive-midfield": "DM",
    "attacking-midfield": "AM",
    "right-back": "RB",
    "left-back": "LB",
    "centre-back": "CB",
    "central-defender": "CB",
    "goalkeeper": "GK",
    "striker": "ST",
    "centre-forward": "ST",
}


def map_position_code_to_abbr(code: str, name: str | None = None, fallback: str | None = None) -> str | None:
    if code:
        abbr = POSITION_ABBR_MAP.get(str(code).lower())
        if abbr:
            return abbr
    if name:
        lower_name = name.lower()
        for key, abbr in POSITION_ABBR_MAP.items():
            if key in lower_name:
                return abbr
    return fallback

def _extract_minutes(lineup: Dict, details: Iterable[Dict]) -> Optional[int]:
    # Direct fields on the lineup
    for key in ("minutes_played", "minutes", "played_minutes"):
        val = _safe_int(lineup.get(key))
        if val is not None:
            return val

    # Look through details for minute-related entries
    for d in details or []:
        type_info = d.get("type") or {}
        name = str(type_info.get("name") or type_info.get("code") or "").lower()
        type_id = d.get("type_id") or type_info.get("id")
        val = _extract_stat_value(d.get("data") or d.get("value"))
        if val is None:
            continue
        if type_id in MINUTES_TYPE_IDS:
            return val
        if any(hint in name for hint in MINUTES_NAME_HINTS):
            return val
    return None


def _upsert(session: Session, model, data: Dict) -> None:
    obj = session.get(model, data.get("id"))
    if obj:
        for k, v in data.items():
            setattr(obj, k, v)
    else:
        session.add(model(**data))


def choose_keep_seasons_per_league(session: Session) -> Set[int]:
    """
    Keep current + previous (by end_date desc) per league.
    """
    keep: Set[int] = set()
    league_ids = [row[0] for row in session.execute(select(Season.league_id).distinct())]
    for league_id in league_ids:
        seasons = (
            session.query(Season)
            .filter(Season.league_id == league_id)
            .order_by(Season.is_current.desc(), Season.end_date.desc().nullslast())
            .all()
        )
        if not seasons:
            continue
        current = next((s for s in seasons if s.is_current), None)
        if current:
            keep.add(current.id)
        for s in seasons:
            if current and s.id == current.id:
                continue
            keep.add(s.id)
            break
    return keep


class SyncService:
    def __init__(self, client: SportMonksClient, session: Session) -> None:
        self.client = client
        self.session = session

    def ensure_schema(self) -> None:
        Base.metadata.create_all(self.session.get_bind())
        _ensure_fixture_columns(self.session.get_bind())
        _ensure_fixture_player_columns(self.session.get_bind())
        _ensure_team_player_columns(self.session.get_bind())
        _ensure_team_squad_columns(self.session.get_bind())

    def _track_player_team_history(self, player_id: int, team_id: int, sync_run_at: datetime) -> None:
        if not player_id or not team_id:
            return
        latest = (
            self.session.query(PlayerTeamHistory)
            .filter(PlayerTeamHistory.player_id == player_id)
            .order_by(PlayerTeamHistory.effective_from.desc(), PlayerTeamHistory.id.desc())
            .first()
        )
        if latest and latest.team_id == team_id and (latest.effective_to is None):
            latest.updated_at = sync_run_at
            return
        if latest and latest.effective_to is None:
            latest.effective_to = sync_run_at
        entry = PlayerTeamHistory(
            player_id=player_id,
            team_id=team_id,
            source="squad_sync",
            effective_from=sync_run_at,
            effective_to=None,
            created_at=sync_run_at,
            updated_at=sync_run_at,
        )
        self.session.add(entry)

    def _end_player_team_assignment(self, player_id: int, team_id: int, sync_run_at: datetime) -> None:
        """Close a current squad assignment when the provider no longer lists it."""
        latest = (
            self.session.query(PlayerTeamHistory)
            .filter(PlayerTeamHistory.player_id == player_id)
            .order_by(PlayerTeamHistory.effective_from.desc(), PlayerTeamHistory.id.desc())
            .first()
        )
        if latest and latest.team_id == team_id and latest.effective_to is None:
            latest.effective_to = sync_run_at
            latest.updated_at = sync_run_at

    # --- seasons & teams ---
    def sync_seasons(self, league_ids: Sequence[int]) -> int:
        count = 0
        for item in self.client.fetch_collection("seasons", includes=["league"], per_page=200):
            league_id = item.get("league_id") or (item.get("league") or {}).get("id")
            if league_ids and league_id not in league_ids:
                continue
            data = {
                "id": item.get("id"),
                "league_id": league_id,
                "name": item.get("name"),
                "start_date": parse_dt(item.get("start_date") or item.get("starting_at")),
                "end_date": parse_dt(item.get("end_date") or item.get("ending_at")),
                "is_current": bool(item.get("is_current") or item.get("current")),
                "extra": item,
            }
            _upsert(self.session, Season, data)
            count += 1
        self.session.commit()
        log.info("Synced seasons: %s", count)
        return count

    def sync_rounds_for_leagues(self, league_ids: Sequence[int]) -> int:
        if not league_ids:
            return 0
        seasons = self.session.query(Season).filter(Season.league_id.in_(league_ids)).all()
        count = 0
        for season in seasons:
            endpoint = f"rounds/seasons/{season.id}"
            for item in self.client.fetch_collection(endpoint, per_page=200):
                payload = {
                    "id": item.get("id"),
                    "league_id": item.get("league_id") or season.league_id,
                    "season_id": item.get("season_id") or season.id,
                    "stage_id": _safe_int(item.get("stage_id")),
                    "name": item.get("name"),
                    "starting_at": _parse_date(item.get("starting_at")),
                    "ending_at": _parse_date(item.get("ending_at")),
                    "is_current": bool(item.get("is_current")),
                    "games_in_current_week": bool(item.get("games_in_current_week")),
                    "finished": bool(item.get("finished")),
                    "extra": item,
                }
                _upsert(self.session, Round, payload)
                count += 1
        self.session.commit()
        log.info("Synced rounds: %s", count)
        return count

    def sync_teams_for_leagues(self, league_ids: Sequence[int]) -> int:
        if not league_ids:
            return 0
        seasons = self.session.query(Season).filter(Season.league_id.in_(league_ids)).all()
        seen_team_ids: Set[int] = set()
        count = 0
        for season in seasons:
            endpoint = f"teams/seasons/{season.id}"
            for item in self.client.fetch_collection(endpoint, includes=["venue"], per_page=200):
                team_id = item.get("id")
                if team_id in seen_team_ids:
                    continue
                image_path = item.get("image_path") or item.get("logo_path")
                data = {
                    "id": team_id,
                    "name": item.get("name"),
                    "short_code": item.get("short_code"),
                    "extra": item,
                }
                if image_path:
                    data["image_path"] = image_path
                _upsert(self.session, Team, data)
                seen_team_ids.add(team_id)
                count += 1
        self.session.commit()
        log.info("Synced teams: %s", count)
        return count

    def sync_squads_for_teams(self, team_ids: Sequence[int]) -> int:
        if not team_ids:
            return 0
        count = 0
        seen: Set[int] = set()
        sync_run_at = datetime.utcnow()
        for team_id in team_ids:
            if not team_id or team_id in seen:
                continue
            seen.add(team_id)
            endpoint = f"squads/teams/{team_id}"
            try:
                # Do not mutate current membership while paging.  Only a fully
                # successful, non-empty response is authoritative enough to remove
                # somebody from a squad.
                squad_rows = list(self.client.fetch_collection(endpoint, includes=["player"], per_page=200))
                squad_player_ids: Set[int] = {
                    int((item.get("player") or {}).get("id") or item.get("player_id"))
                    for item in squad_rows
                    if (item.get("player") or {}).get("id") or item.get("player_id")
                }
                if not squad_player_ids:
                    snapshot = TeamSquadSnapshot(
                        team_id=team_id,
                        source="sportmonks",
                        status="empty",
                        observed_at=sync_run_at,
                        completed_at=datetime.utcnow(),
                        player_count=0,
                        error="Provider returned no valid player IDs; existing membership preserved.",
                        created_at=sync_run_at,
                    )
                    self.session.add(snapshot)
                    self.session.commit()
                    log.warning("Squad lookup returned no players for team %s; preserving existing assignment", team_id)
                    continue

                snapshot = TeamSquadSnapshot(
                    team_id=team_id,
                    source="sportmonks",
                    status="success",
                    observed_at=sync_run_at,
                    completed_at=datetime.utcnow(),
                    player_count=len(squad_player_ids),
                    payload_hash=hashlib.sha256(
                        json.dumps(sorted(squad_player_ids), separators=(",", ":")).encode("utf-8")
                    ).hexdigest(),
                    created_at=sync_run_at,
                )
                self.session.add(snapshot)
                self.session.flush()

                for item in squad_rows:
                    player = item.get("player") or {}
                    player_id = player.get("id") or item.get("player_id")
                    if not player_id:
                        continue
                    player_id = int(player_id)
                    payload = {
                        "id": player_id,
                        "name": player.get("name") or player.get("display_name"),
                        "display_name": player.get("display_name") or player.get("name"),
                        "common_name": player.get("common_name"),
                        "short_name": player.get("short_name"),
                        "image_path": player.get("image_path"),
                        "extra": player,
                    }
                    provider_started_at = parse_dt(item.get("start") or item.get("joined_at")) or sync_run_at
                    # Some provider records overlap while a transfer is being
                    # processed.  The newer effective squad record wins; the
                    # older membership is closed so one player cannot appear in
                    # two current teams just because endpoint order changed.
                    assignment_is_current = True
                    other_memberships = (
                        self.session.query(TeamSquadMembership)
                        .filter(TeamSquadMembership.player_id == player_id)
                        .filter(TeamSquadMembership.team_id != team_id)
                        .filter(TeamSquadMembership.is_active.is_(True))
                        .all()
                    )
                    for other_membership in other_memberships:
                        other_started_at = other_membership.provider_started_at or other_membership.last_seen_at
                        if other_started_at and other_started_at > provider_started_at:
                            assignment_is_current = False
                            continue
                        other_membership.is_active = False
                        other_membership.updated_at = sync_run_at
                        self._end_player_team_assignment(other_membership.player_id, other_membership.team_id, sync_run_at)

                    membership = self.session.get(TeamSquadMembership, (team_id, player_id))
                    if membership:
                        membership.is_active = assignment_is_current
                        membership.last_seen_at = sync_run_at
                        membership.provider_started_at = provider_started_at
                        membership.last_snapshot_id = snapshot.id
                        membership.source = "sportmonks"
                        membership.updated_at = sync_run_at
                    else:
                        self.session.add(
                            TeamSquadMembership(
                                team_id=team_id,
                                player_id=player_id,
                                is_active=assignment_is_current,
                                first_seen_at=sync_run_at,
                                last_seen_at=sync_run_at,
                                provider_started_at=provider_started_at,
                                last_snapshot_id=snapshot.id,
                                source="sportmonks",
                                created_at=sync_run_at,
                                updated_at=sync_run_at,
                            )
                        )
                    existing = self.session.get(Player, player_id)
                    payload["team_id"] = team_id if assignment_is_current else (existing.team_id if existing else None)
                    track_history = assignment_is_current
                    if track_history:
                        payload["team_updated_at"] = sync_run_at
                    elif existing is not None:
                        payload["team_updated_at"] = existing.team_updated_at
                    _upsert(self.session, Player, payload)
                    if track_history and payload.get("team_id"):
                        self._track_player_team_history(player_id, payload["team_id"], sync_run_at)
                    count += 1

                stale_memberships = (
                    self.session.query(TeamSquadMembership)
                    .filter(TeamSquadMembership.team_id == team_id)
                    .filter(TeamSquadMembership.is_active.is_(True))
                    .filter(~TeamSquadMembership.player_id.in_(squad_player_ids))
                    .all()
                )
                for membership in stale_memberships:
                    membership.is_active = False
                    membership.updated_at = sync_run_at
                    self._end_player_team_assignment(membership.player_id, team_id, sync_run_at)

                # Maintain the legacy denormalised player.team_id for existing
                # consumers, but treat snapshot membership as the source of truth.
                stale_players = (
                    self.session.query(Player)
                    .filter(Player.team_id == team_id)
                    .filter(~Player.id.in_(squad_player_ids))
                    .all()
                )
                for stale_player in stale_players:
                    self._end_player_team_assignment(stale_player.id, team_id, sync_run_at)
                    stale_player.team_id = None
                    stale_player.team_updated_at = sync_run_at
                self.session.commit()
            except SportMonksError as exc:
                self.session.rollback()
                self.session.add(
                    TeamSquadSnapshot(
                        team_id=team_id,
                        source="sportmonks",
                        status="failed",
                        observed_at=sync_run_at,
                        completed_at=datetime.utcnow(),
                        player_count=0,
                        error=f"Provider response failed ({exc.status_code}): {exc}",
                        created_at=sync_run_at,
                    )
                )
                self.session.commit()
                if exc.status_code in {404, 422}:
                    log.info("No squad data for team %s (status %s)", team_id, exc.status_code)
                    continue
                raise
        log.info("Synced squads for %s teams (%s players)", len(seen), count)
        return count

    def sync_sidelined_for_teams(self, team_ids: Sequence[int]) -> int:
        if not team_ids:
            return 0
        count = 0
        stale_marked_complete = 0
        seen: Set[int] = set()
        sync_run_at = datetime.utcnow()
        for team_id in team_ids:
            if not team_id or team_id in seen:
                continue
            seen.add(team_id)
            endpoint = f"teams/{team_id}"
            try:
                team_data = self.client.fetch_single(endpoint, includes=["sidelined"])
            except SportMonksError as exc:
                if exc.status_code in {404, 422}:
                    log.info("No sidelined data for team %s (status %s)", team_id, exc.status_code)
                    continue
                raise
            sidelined = team_data.get("sidelined") or []
            seen_ids: Set[int] = set()
            for entry in sidelined:
                sidelined_id = entry.get("id")
                player_id = entry.get("player_id")
                if not sidelined_id or not player_id:
                    continue
                seen_ids.add(int(sidelined_id))
                payload = {
                    "id": sidelined_id,
                    "player_id": player_id,
                    "team_id": entry.get("team_id") or team_id,
                    "category": entry.get("category"),
                    "type_id": _safe_int(entry.get("type_id")),
                    "season_id": _safe_int(entry.get("season_id")),
                    "start_date": _parse_date(entry.get("start_date")),
                    "end_date": _parse_date(entry.get("end_date")),
                    "games_missed": _safe_int(entry.get("games_missed")),
                    "completed": entry.get("completed"),
                    "updated_at": sync_run_at,
                    "extra": entry,
                }
                _upsert(self.session, SidelinedPlayer, payload)
                count += 1

            # If an entry disappears from the provider list, mark it complete so it
            # no longer appears in sidelined_active.
            stale_query = self.session.query(SidelinedPlayer).filter(
                SidelinedPlayer.team_id == team_id,
            ).filter(
                (SidelinedPlayer.completed.is_(None)) | (SidelinedPlayer.completed.is_(False)),
            )
            if seen_ids:
                stale_query = stale_query.filter(~SidelinedPlayer.id.in_(list(seen_ids)))
            stale_rows = stale_query.all()
            for stale_row in stale_rows:
                stale_row.completed = True
                if stale_row.end_date is None:
                    stale_row.end_date = sync_run_at.date()
                stale_row.updated_at = sync_run_at
            stale_marked_complete += len(stale_rows)
        self.session.commit()
        log.info(
            "Synced sidelined entries for %s teams (%s rows, %s marked complete)",
            len(seen),
            count,
            stale_marked_complete,
        )
        return count

    # --- fixtures ---
    def _store_fixture_season(self, raw: Dict) -> None:
        """Persist season metadata discovered through a team fixture feed.

        Team-scoped fixture history can cross competitions that are not part of
        the menu's normal league allowlist.  Keeping the provider's season row
        alongside the fixture lets the normal core exporter retain those
        confirmed score records without inventing a league or season.
        """
        season_raw = raw.get("season") or {}
        if isinstance(season_raw, dict) and isinstance(season_raw.get("data"), dict):
            season_raw = season_raw["data"]
        if not isinstance(season_raw, dict):
            season_raw = {}
        season_id = raw.get("season_id") or season_raw.get("id")
        league_raw = raw.get("league") or {}
        if isinstance(league_raw, dict) and isinstance(league_raw.get("data"), dict):
            league_raw = league_raw["data"]
        if not isinstance(league_raw, dict):
            league_raw = {}
        league_id = raw.get("league_id") or league_raw.get("id") or season_raw.get("league_id")
        if season_id is None or league_id is None:
            return

        existing = self.session.get(Season, season_id)
        payload = {
            "id": season_id,
            "league_id": league_id,
            "name": season_raw.get("name"),
            "start_date": parse_dt(season_raw.get("start_date") or season_raw.get("starting_at")),
            "end_date": parse_dt(season_raw.get("end_date") or season_raw.get("ending_at")),
            "is_current": bool(season_raw.get("is_current") or season_raw.get("current")),
            "extra": season_raw or raw,
        }
        if existing is None:
            self.session.add(Season(**payload))
            return
        # Do not erase richer metadata previously synced by the league feed
        # when a team-scoped response omits one of the nested fields.
        for key, value in payload.items():
            if key in {"id", "league_id"} or value is not None:
                setattr(existing, key, value)

    def _map_fixture(self, raw: Dict) -> Dict:
        home_score, away_score = self._extract_scores(raw.get("scores") or raw.get("score"))
        status, status_code = _fixture_status_values(raw)
        has_lineup_confirmed, lineup_confirmed = _fixture_lineup_confirmed(raw)
        # Team-scoped fixture feeds often provide season/league only through
        # their included relation objects. Preserve those provider IDs so the
        # core exporter can retain the cross-competition history rows instead
        # of silently dropping them when it filters by season_id.
        season_raw = raw.get("season") or {}
        if isinstance(season_raw, dict) and isinstance(season_raw.get("data"), dict):
            season_raw = season_raw["data"]
        if not isinstance(season_raw, dict):
            season_raw = {}
        league_raw = raw.get("league") or {}
        if isinstance(league_raw, dict) and isinstance(league_raw.get("data"), dict):
            league_raw = league_raw["data"]
        if not isinstance(league_raw, dict):
            league_raw = {}
        season_id = raw.get("season_id") or season_raw.get("id")
        league_id = raw.get("league_id") or league_raw.get("id") or season_raw.get("league_id")
        payload = {
            "id": raw.get("id"),
            "league_id": league_id,
            "season_id": season_id,
            "starting_at": parse_dt(raw.get("starting_at")),
            "status": status,
            "status_code": status_code,
            "home_team_id": raw.get("home_team_id"),
            "away_team_id": raw.get("away_team_id"),
            "home_score": home_score,
            "away_score": away_score,
            "extra": raw,
        }
        if has_lineup_confirmed:
            payload["lineup_confirmed"] = lineup_confirmed
        return payload

    def _extract_scores(self, scores_raw) -> Tuple[Optional[int], Optional[int]]:
        """
        Prefer CURRENT (type_id 1525) scores when present; otherwise first home/away entries.
        Handles zero scores without dropping them.
        """
        home_score = away_score = None
        if isinstance(scores_raw, list):
            # CURRENT scores
            for s in scores_raw:
                if s.get("type_id") != 1525:
                    continue
                score_obj = s.get("score") or {}
                participant = score_obj.get("participant") or s.get("participant")
                goals_val = score_obj.get("goals")
                if goals_val is None:
                    goals_val = s.get("goals")
                goals = _safe_int(goals_val)
                if participant == "home" and goals is not None:
                    home_score = goals
                if participant == "away" and goals is not None:
                    away_score = goals
            # Fallback to first occurrences
            if home_score is None or away_score is None:
                for s in scores_raw:
                    score_obj = s.get("score") if isinstance(s, dict) else {}
                    participant = (score_obj or {}).get("participant") or s.get("participant")
                    goals_val = (score_obj or {}).get("goals")
                    if goals_val is None:
                        goals_val = s.get("goals")
                    goals = _safe_int(goals_val)
                    if participant == "home" and home_score is None:
                        home_score = goals
                    if participant == "away" and away_score is None:
                        away_score = goals
        elif isinstance(scores_raw, dict):
            home_score = _safe_int(scores_raw.get("localteam_score") or scores_raw.get("home"))
            away_score = _safe_int(scores_raw.get("visitorteam_score") or scores_raw.get("away"))
        return home_score, away_score

    def _store_participants(self, fixture_id: int, participants: Iterable[Dict]) -> Dict[str, Dict]:
        loc_map: Dict[str, Dict] = {}
        for p in participants or []:
            team_id = p.get("id") or p.get("team_id")
            if team_id is None:
                continue
            self._upsert_team_from_participant(p)
            meta = p.get("meta") or {}
            location = (meta.get("location") or meta.get("venue") or meta.get("side") or "").lower()
            score_val = _safe_int(meta.get("score") or meta.get("outcome"))
            data = {
                "fixture_id": fixture_id,
                "team_id": team_id,
                "location": location or None,
                "score": score_val,
                "extra": p,
            }
            obj = self.session.get(FixtureParticipant, (fixture_id, team_id))
            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                self.session.add(FixtureParticipant(**data))
            if location in ("home", "away"):
                loc_map[location] = {"team_id": team_id, "score": score_val}
        return loc_map

    def _upsert_team_from_participant(self, participant: Dict) -> None:
        team_id = participant.get("id") or participant.get("team_id")
        if team_id is None:
            return
        data: Dict[str, object] = {"id": team_id, "extra": participant}
        name = participant.get("name") or participant.get("team_name") or participant.get("display_name")
        short_code = participant.get("short_code") or participant.get("code")
        image_path = participant.get("image_path") or participant.get("logo_path") or participant.get("logo")
        if name:
            data["name"] = name
        if short_code:
            data["short_code"] = short_code
        if image_path:
            data["image_path"] = image_path
        _upsert(self.session, Team, data)

    def _store_statistics(
        self,
        fixture_id: int,
        stats: Iterable[Dict],
        log_changes: bool = False,
    ) -> None:
        team_stat_values: Dict[int, Dict[int, float]] = {}
        for s in stats or []:
            type_info = s.get("type") or {}
            type_id = s.get("type_id") or type_info.get("id")
            code = type_info.get("code") or (type_id and str(type_id))
            name = type_info.get("name")
            location = (s.get("location") or "").lower() or "unknown"
            data = s.get("data") or {}
            value = _extract_stat_value(data)
            team_id = s.get("participant_id") or s.get("team_id")

            if not team_id:
                continue
            if type_id is not None and value is not None:
                bucket = team_stat_values.setdefault(team_id, {})
                numeric_value = float(value)
                current = bucket.get(type_id)
                if current is None or numeric_value > current:
                    bucket[type_id] = numeric_value
            pk = (fixture_id, team_id, type_id, code, location)
            obj = self.session.get(FixtureStatistic, pk)
            payload = {
                "fixture_id": fixture_id,
                "team_id": team_id,
                "type_id": type_id,
                "code": code,
                "name": name,
                "location": location,
                "value": value,
                "extra": s,
            }
            if log_changes and value is not None:
                old_value = obj.value if obj else None
                if old_value != value:
                    log.info(
                        "RECONCILED: fixture %s | team_stat | type_id %s | team %s | old=%s new=%s",
                        fixture_id,
                        type_id,
                        team_id,
                        old_value,
                        value,
                    )
                if type_id is not None:
                    dupes = (
                        self.session.query(FixtureStatistic)
                        .filter(
                            FixtureStatistic.fixture_id == fixture_id,
                            FixtureStatistic.team_id == team_id,
                            FixtureStatistic.type_id == type_id,
                            FixtureStatistic.location == location,
                            FixtureStatistic.code != code,
                        )
                        .all()
                    )
                    for dupe in dupes:
                        if dupe.value != value:
                            log.info(
                                "RECONCILED: fixture %s | team_stat | type_id %s | team %s | old=%s new=%s",
                                fixture_id,
                                type_id,
                                team_id,
                                dupe.value,
                                value,
                            )
                        dupe.value = value
                        if name and not dupe.name:
                            dupe.name = name
            if obj:
                for k, v in payload.items():
                    setattr(obj, k, v)
            else:
                self.session.add(FixtureStatistic(**payload))

        for team_id, stat_values in team_stat_values.items():
            for meta in DERIVED_TEAM_RATIO_STATS:
                numerator = stat_values.get(meta["numerator_type_id"])
                denominator = stat_values.get(meta["denominator_type_id"])
                _upsert_fixture_team_derived_stat(
                    self.session,
                    fixture_id,
                    team_id,
                    meta["type_id"],
                    meta["code"],
                    meta["name"],
                    _safe_ratio_percent(numerator, denominator),
                    {
                        "source": "derived",
                        "formula": "ratio_percent",
                        "numerator_type_id": meta["numerator_type_id"],
                        "denominator_type_id": meta["denominator_type_id"],
                        "numerator_value": numerator,
                        "denominator_value": denominator,
                    },
                )

    def _store_lineups(
        self,
        fixture_id: int,
        lineups: Iterable[Dict],
        log_changes: bool = False,
    ) -> None:
        for l in lineups or []:
            player_id = l.get("player_id") or (l.get("player") or {}).get("id")
            if not player_id:
                continue
            team_id = l.get("team_id") or (l.get("team") or {}).get("id") or l.get("participant_id")
            player = l.get("player") or {}
            player_image = (
                player.get("image_path")
                or player.get("image")
                or l.get("player_image")
                or l.get("image_path")
            )
            # Upsert player master record
            player_payload = {
                "id": player_id,
                "name": player.get("fullname") or player.get("name") or l.get("player_name"),
                "display_name": player.get("display_name") or player.get("name"),
                "short_name": player.get("short_name") or player.get("short_code"),
                "common_name": player.get("common_name"),
                "team_id": team_id,
                "extra": player or l,
            }
            if player_image:
                player_payload["image_path"] = player_image
            _upsert(self.session, Player, player_payload)

            details = l.get("details") or []
            self._store_lineup_details_stats(
                fixture_id,
                player_id,
                team_id,
                details,
                log_changes=log_changes,
            )
            minutes_played = _extract_minutes(
                l,
                details,
            )
            position_obj = l.get("position") or {}
            position_name_raw = (
                position_obj.get("name")
                or l.get("position_name")
                or player.get("position_name")
                or player.get("position")
                or l.get("position")
            )
            position_name = str(position_name_raw) if position_name_raw is not None else None
            starter_flag = _is_starter(l.get("type") or l.get("type_id") or l.get("lineup_type"))
            position_raw = l.get("position") or player.get("position") or player.get("position_name")
            detailed_position_id = (
                l.get("detailed_position_id")
                or player.get("detailed_position_id")
                or position_obj.get("detailed_position_id")
            )
            detailed_position_name = (
                l.get("detailed_position_name")
                or player.get("detailed_position_name")
                or position_obj.get("detailed_position_name")
                or position_obj.get("name")
            )
            detailed_position_code = (
                l.get("detailed_position_code")
                or player.get("detailed_position_code")
                or position_obj.get("detailed_position_code")
                or position_obj.get("code")
            )
            dp_obj = l.get("detailedposition") or l.get("detailed_position") or {}
            dp_id = dp_obj.get("id")
            dp_name = dp_obj.get("name")
            dp_code = dp_obj.get("code")
            position_abbr = map_position_code_to_abbr(dp_code, dp_name or detailed_position_name or position_name, fallback=None)
            formation_field_val = l.get("formation_field")
            formation_position_val = _safe_int(l.get("formation_position"))
            payload = {
                "fixture_id": fixture_id,
                "player_id": player_id,
                "team_id": team_id,
                "name": l.get("player_name") or player.get("fullname") or player.get("name"),
                "position": str(position_raw) if position_raw is not None else None,
                "position_name": position_name,
                "lineup_type": (l.get("type") or l.get("type_id") or "").__str__() if (l.get("type") or l.get("type_id")) else None,
                "formation_field": str(formation_field_val) if formation_field_val is not None else None,
                "formation_position": formation_position_val,
                "jersey_number": str(l.get("jersey_number") or l.get("number") or "") or None,
                "is_starter": starter_flag,
                "minutes_played": minutes_played,
                "detailed_position_id": _safe_int(detailed_position_id),
                "detailed_position_name": str(detailed_position_name) if detailed_position_name is not None else None,
                "detailed_position_code": str(detailed_position_code) if detailed_position_code is not None else None,
                "lineup_detailed_position_id": _safe_int(dp_id),
                "lineup_detailed_position_name": str(dp_name) if dp_name is not None else None,
                "lineup_detailed_position_code": str(dp_code) if dp_code is not None else None,
                "position_abbr": position_abbr,
                "extra": l,
            }
            obj = self.session.get(FixturePlayer, (fixture_id, player_id))
            if obj:
                for k, v in payload.items():
                    setattr(obj, k, v)
            else:
                self.session.add(FixturePlayer(**payload))

            if not log_changes:
                for d in l.get("details") or []:
                    type_info = d.get("type") or {}
                    type_id = d.get("type_id") or type_info.get("id")
                    code = type_info.get("code") or (type_id and str(type_id))
                    name = type_info.get("name")
                    value = _extract_stat_value(d.get("data") or d.get("value"))
                    pk = (fixture_id, player_id, type_id, code)
                    obj_stat = self.session.get(FixturePlayerStatistic, pk)
                    payload_stat = {
                        "fixture_id": fixture_id,
                        "player_id": player_id,
                        "team_id": team_id,
                        "type_id": type_id,
                        "code": code,
                        "name": name,
                        "value": value,
                        "extra": d,
                    }
                    if obj_stat:
                        for k, v in payload_stat.items():
                            setattr(obj_stat, k, v)
                    else:
                        self.session.add(FixturePlayerStatistic(**payload_stat))

            goals = _sum_stat_values(details, GOALS_TYPE_ID)
            assists = _sum_stat_values(details, ASSISTS_TYPE_ID)
            if goals is not None or assists is not None:
                derived_value = (goals or 0) + (assists or 0)
                _upsert_fixture_player_derived_stat(
                    self.session,
                    fixture_id,
                    player_id,
                    team_id,
                    GOAL_CONTRIBUTIONS_TYPE_ID,
                    GOAL_CONTRIBUTIONS_CODE,
                    GOAL_CONTRIBUTIONS_NAME,
                    derived_value,
                    {"source": "derived", "goals": goals or 0, "assists": assists or 0},
                )

            for meta in DERIVED_PLAYER_RATIO_STATS:
                numerator = _sum_stat_values(details, meta["numerator_type_id"])
                denominator = _sum_stat_values(details, meta["denominator_type_id"])
                _upsert_fixture_player_derived_stat(
                    self.session,
                    fixture_id,
                    player_id,
                    team_id,
                    meta["type_id"],
                    meta["code"],
                    meta["name"],
                    _safe_ratio_percent(numerator, denominator),
                    {
                        "source": "derived",
                        "formula": "ratio_percent",
                        "numerator_type_id": meta["numerator_type_id"],
                        "denominator_type_id": meta["denominator_type_id"],
                        "numerator_value": numerator,
                        "denominator_value": denominator,
                    },
                )

            defensive_components = []
            for component_type_id in DEFENSIVE_INVOLVEMENT_COMPONENT_TYPE_IDS:
                component_value = _sum_stat_values(details, component_type_id)
                if component_value is not None:
                    defensive_components.append((component_type_id, component_value))
            defensive_total = (
                float(sum(component_value for _, component_value in defensive_components))
                if defensive_components
                else None
            )
            _upsert_fixture_player_derived_stat(
                self.session,
                fixture_id,
                player_id,
                team_id,
                DEFENSIVE_INVOLVEMENT_TYPE_ID,
                DEFENSIVE_INVOLVEMENT_CODE,
                DEFENSIVE_INVOLVEMENT_NAME,
                defensive_total,
                {
                    "source": "derived",
                    "formula": "sum",
                    "component_type_ids": [type_id for type_id, _ in defensive_components],
                    "component_values": {
                        str(type_id): value for type_id, value in defensive_components
                    },
                },
            )

    def _store_lineup_details_stats(
        self,
        fixture_id: int,
        player_id: int,
        team_id: Optional[int],
        details: Iterable[Dict],
        log_changes: bool = False,
    ) -> None:
        for d in details or []:
            type_info = d.get("type") or {}
            type_id = d.get("type_id") or type_info.get("id")
            if not type_id:
                continue
            code = type_info.get("code") or (type_id and str(type_id))
            name = type_info.get("name")
            value = _extract_stat_value(d.get("data") or d.get("value") or d.get("stat"))
            if value is None:
                continue
            pk = (fixture_id, player_id, type_id, code)
            obj = self.session.get(FixturePlayerStatistic, pk)
            payload = {
                "fixture_id": fixture_id,
                "player_id": player_id,
                "team_id": team_id,
                "type_id": type_id,
                "code": code,
                "name": name,
                "value": value,
                "extra": d,
            }
            if log_changes:
                old_value = obj.value if obj else None
                if old_value != value:
                    log.info(
                        "RECONCILED: fixture %s | player_stat | type_id %s | player %s | old=%s new=%s",
                        fixture_id,
                        type_id,
                        player_id,
                        old_value,
                        value,
                    )
            if obj:
                for k, v in payload.items():
                    setattr(obj, k, v)
            else:
                self.session.add(FixturePlayerStatistic(**payload))

    def _apply_participant_derivations(self, fixture: Fixture, loc_map: Dict[str, Dict]) -> None:
        if fixture.home_team_id is None and loc_map.get("home"):
            fixture.home_team_id = loc_map["home"].get("team_id")
        if fixture.away_team_id is None and loc_map.get("away"):
            fixture.away_team_id = loc_map["away"].get("team_id")

        home_part = loc_map.get("home")
        away_part = loc_map.get("away")
        if fixture.home_score is None and home_part:
            fixture.home_score = _safe_int(home_part.get("score"))
        if fixture.away_score is None and away_part:
            fixture.away_score = _safe_int(away_part.get("score"))

    def _store_fixture_raw(self, raw: Dict, log_changes: bool = False) -> None:
        self._store_fixture_season(raw)
        data = self._map_fixture(raw)
        fixture = self.session.get(Fixture, data["id"])
        if fixture:
            for k, v in data.items():
                setattr(fixture, k, v)
        else:
            fixture = Fixture(**data)
            self.session.add(fixture)

        loc_map = self._store_participants(fixture.id, raw.get("participants") or [])
        self._apply_participant_derivations(fixture, loc_map)
        self._store_statistics(
            fixture.id,
            raw.get("statistics") or [],
            log_changes=log_changes,
        )
        self._store_lineups(
            fixture.id,
            raw.get("lineups") or [],
            log_changes=log_changes,
        )

    def _chunks_newest_first(self, start: date, end: date, span_days: int = 90):
        cursor = end
        while cursor >= start:
            chunk_start = max(start, cursor - timedelta(days=span_days - 1))
            yield chunk_start, cursor
            cursor = chunk_start - timedelta(days=1)

    def sync_fixtures_between(
        self,
        start: date,
        end: date,
        league_ids: Optional[Sequence[int]] = None,
        includes: Optional[Sequence[str]] = None,
    ) -> int:
        params: Dict[str, object] = {}
        if league_ids:
            params["filters"] = f"fixtureLeagues:{','.join(str(l) for l in league_ids)}"
        if includes is None:
            includes = [
                "participants",
                "scores",
                "state",
                "statistics",
                "statistics.type",
                "lineups.details",
                "lineups.position",
                "lineups.detailedposition",
                "lineups.player",
            ]
        count = 0
        for chunk_start, chunk_end in self._chunks_newest_first(start, end):
            endpoint = f"fixtures/between/{chunk_start.isoformat()}/{chunk_end.isoformat()}"
            for item in self.client.fetch_collection(endpoint, params=params, includes=includes, per_page=50):
                self._store_fixture_raw(item)
                count += 1
        self.session.commit()
        log.info("Synced fixtures between %s and %s: %s", start, end, count)
        return count

    def sync_fixtures_for_season(self, season: Season) -> int:
        season_start = (season.start_date or datetime.utcnow() - timedelta(days=365)).date()
        today = datetime.utcnow().date()
        season_end = season.end_date.date() if season.end_date else today + timedelta(days=1)
        if season.is_current and season_end > today:
            season_end = today + timedelta(days=1)
        params: Dict[str, object] = {"filters": f"fixtureSeasons:{season.id}"}
        includes = [
            "participants",
            "scores",
            "state",
            "statistics",
            "statistics.type",
            "lineups.details",
            "lineups.position",
            "lineups.detailedposition",
            "lineups.player",
        ]
        count = 0
        for chunk_start, chunk_end in self._chunks_newest_first(season_start, season_end):
            endpoint = f"fixtures/between/{chunk_start.isoformat()}/{chunk_end.isoformat()}"
            for item in self.client.fetch_collection(endpoint, params=params, includes=includes, per_page=50):
                self._store_fixture_raw(item)
                count += 1
        self.session.commit()
        log.info("Synced fixtures for season %s (%s - %s): %s", season.id, season_start, season_end, count)
        return count

    def sync_recent_window(self, league_ids: Sequence[int], days: int = 120) -> int:
        today = datetime.utcnow().date()
        start = today - timedelta(days=days)
        end = today + timedelta(days=1)
        return self.sync_fixtures_between(start, end, league_ids=league_ids)

    def reconcile_fixtures(
        self,
        fixture_ids: Sequence[int],
        includes: Optional[Sequence[str]] = None,
    ) -> int:
        if includes is None:
            includes = [
                "participants",
                "scores",
                "statistics",
                "statistics.type",
                "lineups.details",
                "lineups.position",
                "lineups.detailedposition",
                "lineups.player",
            ]
        count = 0
        for fixture_id in fixture_ids:
            endpoint = f"fixtures/{fixture_id}"
            try:
                payload = self.client.request(
                    "GET",
                    endpoint,
                    params={"include": ";".join(includes)},
                )
            except SportMonksError as exc:
                log.warning("Reconcile failed for fixture %s: %s", fixture_id, exc)
                continue
            data = payload.get("data") or {}
            if not data:
                log.warning("Reconcile missing data for fixture %s", fixture_id)
                continue
            self._store_fixture_raw(data, log_changes=True)
            count += 1
        self.session.commit()
        log.info("Reconciled %s fixtures by ID", count)
        return count

    def reconcile_recent_fixtures(self, league_ids: Sequence[int], days: int = 7) -> int:
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = (
            self.session.query(Fixture)
            .filter(Fixture.starting_at >= cutoff)
            .filter(Fixture.home_score.isnot(None))
            .filter(Fixture.away_score.isnot(None))
        )
        if league_ids:
            query = query.filter(Fixture.league_id.in_(league_ids))
        fixture_ids = [row.id for row in query.order_by(Fixture.starting_at.desc()).all()]
        if not fixture_ids:
            log.info("No recent fixtures found for reconciliation (last %s days).", days)
            return 0
        return self.reconcile_fixtures(fixture_ids)

    def sync_upcoming_window(self, league_ids: Sequence[int], days_forward: int = 14) -> int:
        today = datetime.utcnow().date()
        end = today + timedelta(days=days_forward)
        includes = ["participants", "scores", "state"]
        return self.sync_fixtures_between(today, end, league_ids=league_ids, includes=includes)

    def sync_team_history_for_recent_fixtures(
        self,
        league_ids: Sequence[int],
        history_days: int = 365,
        minimum_completed_matches: int = 5,
        batch_size: int = 25,
        batch_index: Optional[int] = None,
        upcoming_days: int = 14,
    ) -> Dict[str, int]:
        """Backfill confirmed team history across competitions in bounded batches.

        The fixture menu contains cup participants whose recent matches are in
        a different domestic competition (or have not been synced at all). A
        league-filtered refresh cannot discover those rows. This method uses
        SportMonks' team-scoped date-range endpoint and stores only provider
        responses; no derived or substituted values are written.

        The local SQLite database is recreated by GitHub Actions on each run,
        so the batch rotates deterministically by UTC day. Supabase retains
        each exported batch, allowing the complete candidate set to converge
        without making one scheduled run unbounded.
        """
        now = datetime.utcnow()
        end = now + timedelta(days=max(upcoming_days, 0))
        upcoming_query = (
            self.session.query(Fixture.home_team_id, Fixture.away_team_id)
            .filter(Fixture.starting_at >= now)
            .filter(Fixture.starting_at <= end)
        )
        if league_ids:
            upcoming_query = upcoming_query.filter(Fixture.league_id.in_(list(league_ids)))
        upcoming_team_ids: Set[int] = set()
        for home_id, away_id in upcoming_query.all():
            if home_id:
                upcoming_team_ids.add(int(home_id))
            if away_id:
                upcoming_team_ids.add(int(away_id))
        if not upcoming_team_ids:
            log.info("Team history backfill skipped: no upcoming fixture teams")
            return {"teams_considered": 0, "teams_selected": 0, "fixtures_synced": 0, "teams_failed": 0}

        team_id_list = sorted(upcoming_team_ids)
        completed_base = (
            self.session.query(Fixture.home_team_id, func.count(Fixture.id))
            .filter(Fixture.home_team_id.in_(team_id_list))
            .filter(Fixture.starting_at < now)
            .filter(Fixture.home_score.isnot(None), Fixture.away_score.isnot(None))
            .group_by(Fixture.home_team_id)
            .all()
        )
        away_base = (
            self.session.query(Fixture.away_team_id, func.count(Fixture.id))
            .filter(Fixture.away_team_id.in_(team_id_list))
            .filter(Fixture.starting_at < now)
            .filter(Fixture.home_score.isnot(None), Fixture.away_score.isnot(None))
            .group_by(Fixture.away_team_id)
            .all()
        )
        completed_counts = {team_id: 0 for team_id in team_id_list}
        for team_id, count in completed_base:
            completed_counts[int(team_id)] += int(count or 0)
        for team_id, count in away_base:
            completed_counts[int(team_id)] += int(count or 0)

        minimum = max(0, int(minimum_completed_matches))
        candidates = [team_id for team_id in team_id_list if completed_counts[team_id] < minimum]
        candidates.sort(key=lambda team_id: (completed_counts[team_id], team_id))
        if not candidates:
            log.info(
                "Team history backfill skipped: all %s upcoming teams have at least %s completed matches",
                len(team_id_list),
                minimum,
            )
            return {
                "teams_considered": len(team_id_list),
                "teams_selected": 0,
                "fixtures_synced": 0,
                "teams_failed": 0,
            }

        size = max(1, int(batch_size))
        batch_count = (len(candidates) + size - 1) // size
        if batch_index is None:
            batch_index = now.date().toordinal() % batch_count
        selected = candidates[(int(batch_index) % batch_count) * size : (int(batch_index) % batch_count + 1) * size]
        start = (now - timedelta(days=max(1, int(history_days)))).date()
        end_date = now.date()
        includes = ["participants", "scores", "state", "season", "league"]
        synced = 0
        failed = 0
        for team_id in selected:
            try:
                endpoint = f"fixtures/between/{start.isoformat()}/{end_date.isoformat()}/{team_id}"
                team_count = 0
                for item in self.client.fetch_collection(endpoint, includes=includes, per_page=50):
                    # The endpoint is authoritative, but keep the history pass
                    # bounded to completed fixtures before the current UTC day.
                    starting_at = parse_dt(item.get("starting_at"))
                    if starting_at is not None and starting_at > now:
                        continue
                    self._store_fixture_raw(item)
                    team_count += 1
                self.session.commit()
                synced += team_count
                log.info("Team history backfill team=%s fixtures=%s", team_id, team_count)
            except SportMonksError as exc:
                self.session.rollback()
                failed += 1
                log.warning("Team history backfill failed team=%s: %s", team_id, exc)

        log.info(
            "Team history backfill complete: considered=%s selected=%s fixtures=%s failed=%s batch=%s/%s",
            len(team_id_list),
            len(selected),
            synced,
            failed,
            int(batch_index) % batch_count,
            batch_count,
        )
        return {
            "teams_considered": len(team_id_list),
            "teams_selected": len(selected),
            "fixtures_synced": synced,
            "teams_failed": failed,
        }

    def sync_history_window(self, league_ids: Sequence[int], keep_season_ids: Set[int]) -> int:
        seasons = (
            self.session.query(Season)
            .filter(Season.league_id.in_(league_ids), Season.id.in_(keep_season_ids))
            .all()
        )
        total = 0
        for season in seasons:
            total += self.sync_fixtures_for_season(season)
        return total
