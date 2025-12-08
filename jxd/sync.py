from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from .api import SportMonksClient
from .models import Country, Fixture, HeadToHead, League, Player, Season, Team, Venue
from .utils import parse_dt

log = logging.getLogger(__name__)


def _upsert(session: Session, model, data: Dict):
    obj = session.get(model, data["id"]) if "id" in data else None
    if obj:
        for key, value in data.items():
            setattr(obj, key, value)
    else:
        obj = model(**data)
        session.add(obj)


def _merge_raw(model_name: str, raw: Dict) -> Dict:
    return {"extra": raw}


def _country_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "code": raw.get("code") or raw.get("iso2"),
        "continent": raw.get("continent"),
        **_merge_raw("country", raw),
    }


def _league_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "type": raw.get("type"),
        "country_id": (raw.get("country") or {}).get("id") or raw.get("country_id"),
        "logo_path": raw.get("logo"),
        **_merge_raw("league", raw),
    }


def _season_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "league_id": (raw.get("league") or {}).get("id") or raw.get("league_id"),
        "start_date": parse_dt(raw.get("start_date")),
        "end_date": parse_dt(raw.get("end_date")),
        "is_current": raw.get("is_current") or raw.get("current"),
        **_merge_raw("season", raw),
    }


def _venue_mapper(raw: Dict) -> Dict:
    coordinates = raw.get("coordinates") or {}
    return {
        "id": raw.get("id"),
        "name": raw.get("name") or raw.get("city"),
        "city": raw.get("city"),
        "country_id": raw.get("country_id"),
        "capacity": raw.get("capacity"),
        "latitude": coordinates.get("latitude") or raw.get("latitude"),
        "longitude": coordinates.get("longitude") or raw.get("longitude"),
        "image_path": raw.get("image_path"),
        **_merge_raw("venue", raw),
    }


def _team_mapper(raw: Dict) -> Dict:
    venue = raw.get("venue") or {}
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "short_code": raw.get("short_code"),
        "country_id": raw.get("country_id"),
        "founded": raw.get("founded"),
        "venue_id": raw.get("venue_id") or venue.get("id"),
        "logo_path": raw.get("logo_path") or raw.get("logo"),
        "is_national": raw.get("national_team") or raw.get("is_national"),
        **_merge_raw("team", raw),
    }


def _player_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "first_name": raw.get("firstname") or raw.get("first_name"),
        "last_name": raw.get("lastname") or raw.get("last_name"),
        "display_name": raw.get("display_name") or raw.get("name"),
        "nationality_id": raw.get("nationality_id") or (raw.get("nationality") or {}).get("id"),
        "birth_date": parse_dt(raw.get("birthdate") or raw.get("birth_date")),
        "height": raw.get("height"),
        "weight": raw.get("weight"),
        "position_id": (raw.get("position") or {}).get("id") or raw.get("position_id"),
        "position_name": (raw.get("position") or {}).get("name"),
        "image_path": raw.get("image_path") or raw.get("image"),
        "current_team_id": raw.get("team_id") or (raw.get("team") or {}).get("id"),
        **_merge_raw("player", raw),
    }


def _fixture_mapper(raw: Dict) -> Dict:
    scores = raw.get("scores") or raw.get("score") or {}
    weather = raw.get("weather_report") or raw.get("weather")
    return {
        "id": raw.get("id"),
        "league_id": raw.get("league_id"),
        "season_id": raw.get("season_id"),
        "round_id": raw.get("round_id") or (raw.get("round") or {}).get("id"),
        "group_id": raw.get("group_id") or (raw.get("group") or {}).get("id"),
        "stage_id": raw.get("stage_id") or (raw.get("stage") or {}).get("id"),
        "referee_id": raw.get("referee_id") or (raw.get("referee") or {}).get("id"),
        "venue_id": raw.get("venue_id") or (raw.get("venue") or {}).get("id"),
        "starting_at": parse_dt(raw.get("starting_at")),
        "status": raw.get("status"),
        "status_code": raw.get("status_code") or raw.get("time") or raw.get("state"),
        "home_team_id": raw.get("home_team_id"),
        "away_team_id": raw.get("away_team_id"),
        "home_score": scores.get("localteam_score") or scores.get("home"),
        "away_score": scores.get("visitorteam_score") or scores.get("away"),
        "scores": scores,
        "weather_report": weather,
        **_merge_raw("fixture", raw),
    }


class SyncService:
    def __init__(self, client: SportMonksClient, session: Session) -> None:
        self.client = client
        self.session = session

    def sync_countries(self) -> int:
        log.info("Syncing countries")
        count = 0
        for item in self.client.fetch_collection("countries"):
            mapped = _country_mapper(item)
            _upsert(self.session, Country, mapped)
            count += 1
        self.session.commit()
        log.info("Countries synced: %s", count)
        return count

    def sync_leagues(self) -> int:
        log.info("Syncing leagues")
        count = 0
        for item in self.client.fetch_collection("leagues", includes=["country"]):
            mapped = _league_mapper(item)
            _upsert(self.session, League, mapped)
            count += 1
        self.session.commit()
        log.info("Leagues synced: %s", count)
        return count

    def sync_seasons(self) -> int:
        log.info("Syncing seasons")
        count = 0
        for item in self.client.fetch_collection("seasons", includes=["league"]):
            mapped = _season_mapper(item)
            _upsert(self.session, Season, mapped)
            count += 1
        self.session.commit()
        log.info("Seasons synced: %s", count)
        return count

    def sync_venues(self) -> int:
        log.info("Syncing venues")
        count = 0
        for item in self.client.fetch_collection("venues"):
            mapped = _venue_mapper(item)
            _upsert(self.session, Venue, mapped)
            count += 1
        self.session.commit()
        log.info("Venues synced: %s", count)
        return count

    def sync_teams(self, season_id: Optional[int] = None) -> int:
        log.info("Syncing teams%s", f" for season {season_id}" if season_id else "")
        path = "teams"
        params: Dict[str, object] = {}
        if season_id:
            path = f"teams/seasons/{season_id}"
        count = 0
        for item in self.client.fetch_collection(path, params=params, includes=["venue"]):
            mapped = _team_mapper(item)
            _upsert(self.session, Team, mapped)
            count += 1
        self.session.commit()
        log.info("Teams synced: %s", count)
        return count

    def sync_players(self, season_id: Optional[int] = None, team_id: Optional[int] = None) -> int:
        log.info(
            "Syncing players%s%s",
            f" for season {season_id}" if season_id else "",
            f" and team {team_id}" if team_id else "",
        )
        path = "players"
        if season_id:
            path = f"players/seasons/{season_id}"
        params: Dict[str, object] = {}
        if team_id:
            params["team_id"] = team_id

        count = 0
        for item in self.client.fetch_collection(path, params=params, includes=["team", "position"]):
            mapped = _player_mapper(item)
            _upsert(self.session, Player, mapped)
            count += 1
        self.session.commit()
        log.info("Players synced: %s", count)
        return count

    def sync_fixtures(
        self, season_id: Optional[int] = None, team_ids: Optional[List[int]] = None
    ) -> int:
        log.info(
            "Syncing fixtures%s%s",
            f" for season {season_id}" if season_id else "",
            f" filtered by teams {team_ids}" if team_ids else "",
        )
        path = "fixtures"
        params: Dict[str, object] = {}
        if season_id:
            path = f"fixtures/seasons/{season_id}"
        if team_ids:
            params["team_ids"] = ",".join(str(t) for t in team_ids)

        count = 0
        for item in self.client.fetch_collection(
            path,
            params=params,
            includes=["league", "season", "scores", "weather_report", "venue"],
        ):
            mapped = _fixture_mapper(item)
            _upsert(self.session, Fixture, mapped)
            count += 1
        self.session.commit()
        log.info("Fixtures synced: %s", count)
        return count

    def sync_h2h(self, team_a_id: int, team_b_id: int) -> Dict:
        """
        Fetch head-to-head fixtures and store summary + fixture list as JSON.
        """
        log.info("Syncing H2H %s vs %s", team_a_id, team_b_id)
        data = self.client.fetch_single(
            f"head-to-head/{team_a_id}/{team_b_id}",
            params={"per_page": 50},
        )
        summary = data.get("summary") or {}
        fixtures = data.get("fixtures") or data.get("data") or data
        h2h_row = {
            "id": None,
            "team_a_id": team_a_id,
            "team_b_id": team_b_id,
            "summary": summary,
            "fixtures": fixtures,
        }
        existing = (
            self.session.query(HeadToHead)
            .filter(
                HeadToHead.team_a_id == team_a_id,
                HeadToHead.team_b_id == team_b_id,
            )
            .one_or_none()
        )
        if existing:
            for k, v in h2h_row.items():
                if k == "id":
                    continue
                setattr(existing, k, v)
        else:
            self.session.add(HeadToHead(**h2h_row))
        self.session.commit()
        log.info("Stored H2H for %s vs %s", team_a_id, team_b_id)
        return data


def bootstrap_schema(session: Session) -> None:
    from .db import Base

    Base.metadata.create_all(session.get_bind())

