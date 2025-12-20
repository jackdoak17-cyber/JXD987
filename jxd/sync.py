from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Dict, Iterable, Optional, Sequence, Set, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from .sportmonks_client import SportMonksClient
from .models import (
    Base,
    Season,
    Team,
    Player,
    Fixture,
    FixtureParticipant,
    FixtureStatistic,
    FixturePlayerStatistic,
    FixturePlayer,
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


def _safe_int(val) -> Optional[int]:
    try:
        return int(val)
    except Exception:
        try:
            return int(float(val))
        except Exception:
            return None


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
                for col in ("common_name", "short_name"):
                    if col not in cols:
                        conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")


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


def _is_starter(lineup_type: Optional[object]) -> Optional[bool]:
    if lineup_type is None:
        return None
    text = str(lineup_type).lower()
    if text in {"11", "lineup", "starting", "starter", "1"}:
        return True
    if text in {"12", "substitute", "sub", "bench"}:
        return False
    return None


MINUTES_TYPE_IDS = {119, 1584}
MINUTES_NAME_HINTS = ("minute", "minutes")



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
        _ensure_fixture_player_columns(self.session.get_bind())
        _ensure_team_player_columns(self.session.get_bind())

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

    # --- fixtures ---
    def _map_fixture(self, raw: Dict) -> Dict:
        home_score, away_score = self._extract_scores(raw.get("scores") or raw.get("score"))
        return {
            "id": raw.get("id"),
            "league_id": raw.get("league_id"),
            "season_id": raw.get("season_id"),
            "starting_at": parse_dt(raw.get("starting_at")),
            "status": raw.get("status"),
            "status_code": raw.get("status_code") or (raw.get("state") or {}).get("state"),
            "home_team_id": raw.get("home_team_id"),
            "away_team_id": raw.get("away_team_id"),
            "home_score": home_score,
            "away_score": away_score,
            "extra": raw,
        }

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

    def _store_statistics(self, fixture_id: int, stats: Iterable[Dict]) -> None:
        for s in stats or []:
            type_info = s.get("type") or {}
            type_id = s.get("type_id") or type_info.get("id")
            code = type_info.get("code") or (type_id and str(type_id))
            name = type_info.get("name")
            location = (s.get("location") or "").lower() or None
            data = s.get("data") or {}
            value = _extract_stat_value(data)
            team_id = s.get("participant_id") or s.get("team_id")

            if not team_id:
                continue
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
            if obj:
                for k, v in payload.items():
                    setattr(obj, k, v)
            else:
                self.session.add(FixtureStatistic(**payload))

    def _store_lineups(self, fixture_id: int, lineups: Iterable[Dict]) -> None:
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
                "short_name": player.get("short_name") or player.get("short_code"),
                "common_name": player.get("common_name"),
                "team_id": team_id,
                "extra": player or l,
            }
            if player_image:
                player_payload["image_path"] = player_image
            _upsert(self.session, Player, player_payload)

            details = l.get("details") or []
            self._store_lineup_details_stats(fixture_id, player_id, team_id, details)
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

    def _store_lineup_details_stats(
        self,
        fixture_id: int,
        player_id: int,
        team_id: Optional[int],
        details: Iterable[Dict],
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

    def _store_fixture_raw(self, raw: Dict) -> None:
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
        self._store_statistics(fixture.id, raw.get("statistics") or [])
        self._store_lineups(fixture.id, raw.get("lineups") or [])

    def _chunks_newest_first(self, start: date, end: date, span_days: int = 90):
        cursor = end
        while cursor >= start:
            chunk_start = max(start, cursor - timedelta(days=span_days - 1))
            yield chunk_start, cursor
            cursor = chunk_start - timedelta(days=1)

    def sync_fixtures_between(self, start: date, end: date, league_ids: Optional[Sequence[int]] = None) -> int:
        params: Dict[str, object] = {}
        if league_ids:
            params["filters"] = f"fixtureLeagues:{','.join(str(l) for l in league_ids)}"
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
