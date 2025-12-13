from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from .api import SportMonksClient
from .models import (
    SyncState,
    Bookmaker,
    Country,
    Fixture,
    FixtureParticipant,
    HeadToHead,
    League,
    Market,
    OddsOutcome,
    Player,
    PlayerStatLine,
    PlayerOdds,
    Season,
    Team,
    TeamStatLine,
    Venue,
    Type,
)
from .utils import parse_dt, to_float, dict_hash

log = logging.getLogger(__name__)
FOOTBALL = "football/"
CORE = "core/"
# Use fully-qualified football odds prefix to avoid missing sport path.
ODDS = "football/odds/"


def _upsert(session: Session, model, data: Dict):
    obj = session.get(model, data["id"]) if "id" in data else None
    if obj:
        for key, value in data.items():
            setattr(obj, key, value)
    else:
        obj = model(**data)
        session.add(obj)


def _merge_raw(raw: Dict) -> Dict:
    return {"extra": raw}

def _normalize_name(val: str) -> str:
    return "".join(ch for ch in (val or "").lower() if ch.isalnum() or ch.isspace()).strip()


def _country_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "code": raw.get("code") or raw.get("iso2"),
        "continent": raw.get("continent"),
        **_merge_raw(raw),
    }


def _league_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "type": raw.get("type"),
        "country_id": (raw.get("country") or {}).get("id") or raw.get("country_id"),
        "logo_path": raw.get("logo"),
        **_merge_raw(raw),
    }


def _season_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "league_id": (raw.get("league") or {}).get("id") or raw.get("league_id"),
        "start_date": parse_dt(raw.get("start_date") or raw.get("starting_at")),
        "end_date": parse_dt(raw.get("end_date") or raw.get("ending_at")),
        "is_current": raw.get("is_current") or raw.get("current"),
        **_merge_raw(raw),
    }


def _venue_mapper(raw: Dict) -> Dict:
    coordinates = raw.get("coordinates") or {}
    return {
        "id": raw.get("id"),
        "name": raw.get("name") or raw.get("city"),
        "city": raw.get("city"),
        "country_id": raw.get("country_id"),
        "capacity": raw.get("capacity"),
        "latitude": to_float(coordinates.get("latitude") or raw.get("latitude")),
        "longitude": to_float(coordinates.get("longitude") or raw.get("longitude")),
        "image_path": raw.get("image_path"),
        **_merge_raw(raw),
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
        **_merge_raw(raw),
    }


def _type_mapper(raw: Dict) -> Dict:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "code": raw.get("code") or raw.get("short_code"),
        "entity": raw.get("entity") or raw.get("model") or raw.get("resource"),
        **_merge_raw(raw),
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
        **_merge_raw(raw),
    }


def _fixture_mapper(raw: Dict) -> Dict:
    scores_raw = raw.get("scores") or raw.get("score") or {}
    home_score = None
    away_score = None
    if isinstance(scores_raw, list):
        for s in scores_raw:
            score_obj = s.get("score") or {}
            participant = score_obj.get("participant")
            goals = score_obj.get("goals")
            if participant == "home" and home_score is None:
                home_score = goals
            if participant == "away" and away_score is None:
                away_score = goals
    elif isinstance(scores_raw, dict):
        home_score = scores_raw.get("localteam_score") or scores_raw.get("home")
        away_score = scores_raw.get("visitorteam_score") or scores_raw.get("away")
    scores = scores_raw if isinstance(scores_raw, dict) else {"list": scores_raw}
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
        "home_score": home_score,
        "away_score": away_score,
        "scores": scores,
        "weather_report": weather,
        **_merge_raw(raw),
    }


def _safe_int(val) -> Optional[int]:
    try:
        return int(val)
    except Exception:
        try:
            return int(float(val))
        except Exception:
            return None


def _group_team_stats(stats: Iterable[Dict]) -> Dict[int, List[Dict]]:
    grouped: Dict[int, List[Dict]] = {}
    for stat in stats or []:
        tid = stat.get("team_id") or stat.get("participant_id")
        if tid is None:
            continue
        grouped.setdefault(int(tid), []).append(stat)
    return grouped


class SyncService:
    def __init__(self, client: SportMonksClient, session: Session) -> None:
        self.client = client
        self.session = session

    # ---- sync state helpers ----
    def _get_state(self, key: str) -> Optional[str]:
        row = self.session.get(SyncState, key)
        return row.value if row else None

    def _set_state(self, key: str, value: str) -> None:
        row = self.session.get(SyncState, key)
        if row:
            row.value = value
        else:
            self.session.add(SyncState(key=key, value=value))
        self.session.commit()

    # ---- reference data ----
    def sync_countries(self) -> int:
        log.info("Syncing countries")
        count = 0
        for item in self.client.fetch_collection(f"{CORE}countries"):
            _upsert(self.session, Country, _country_mapper(item))
            count += 1
        self.session.commit()
        log.info("Countries synced: %s", count)
        return count

    def sync_leagues(self) -> int:
        log.info("Syncing leagues")
        count = 0
        for item in self.client.fetch_collection(f"{FOOTBALL}leagues", includes=["country"]):
            _upsert(self.session, League, _league_mapper(item))
            count += 1
        self.session.commit()
        log.info("Leagues synced: %s", count)
        return count

    def sync_seasons(self) -> int:
        log.info("Syncing seasons")
        count = 0
        for item in self.client.fetch_collection(f"{FOOTBALL}seasons", includes=["league"]):
            _upsert(self.session, Season, _season_mapper(item))
            count += 1
        self.session.commit()
        log.info("Seasons synced: %s", count)
        return count

    def sync_types(self, entity: Optional[str] = None) -> int:
        """
        Sync stat/event type reference (helps decode player_stats.type_id rows).
        """
        log.info("Syncing types (entity=%s)", entity or "any")
        params: Dict[str, object] = {}
        if entity:
            params["entity"] = entity
        count = 0
        try:
            for item in self.client.fetch_collection(f"{FOOTBALL}types", params=params, per_page=500):
                _upsert(self.session, Type, _type_mapper(item))
                count += 1
            self.session.commit()
            log.info("Types synced: %s", count)
        except Exception as exc:
            # Some plans might not expose the types endpoint; fail soft.
            log.warning("Types sync failed (ignored): %s", exc)
        return count

    def sync_venues(self) -> int:
        log.info("Syncing venues")
        count = 0
        for item in self.client.fetch_collection(f"{FOOTBALL}venues"):
            _upsert(self.session, Venue, _venue_mapper(item))
            count += 1
        self.session.commit()
        log.info("Venues synced: %s", count)
        return count

    def sync_league_teams(self, league_ids: List[int]) -> int:
        """
        Convenience helper: sync teams for the given league IDs using their current seasons.
        """
        if not league_ids:
            return 0
        # Ensure seasons exist first
        if not self.session.query(Season).count():
            self.sync_seasons()
        seasons = self.session.query(Season).filter(Season.league_id.in_(league_ids)).all()
        if not seasons:
            log.info("No seasons found for leagues %s; run sync_seasons first", league_ids)
            return 0
        total = 0
        for s in seasons:
            total += self.sync_teams(season_id=s.id)
        return total

    # ---- entities ----
    def sync_teams(self, season_id: Optional[int] = None) -> int:
        log.info("Syncing teams%s", f" for season {season_id}" if season_id else "")
        path = f"{FOOTBALL}teams"
        if season_id:
            path = f"{FOOTBALL}teams/seasons/{season_id}"
        count = 0
        id_after = None
        if not season_id:
            state_key = "teams_id_after"
            try:
                id_after_val = self._get_state(state_key)
                id_after = int(id_after_val) if id_after_val else None
            except Exception:
                id_after = None
        for item in self.client.fetch_collection(path, includes=["venue"], id_after=id_after):
            _upsert(self.session, Team, _team_mapper(item))
            count += 1
            if not season_id:
                self._set_state("teams_id_after", str(item.get("id")))
        self.session.commit()
        log.info("Teams synced: %s", count)
        return count

    def sync_players(self, season_id: Optional[int] = None, team_id: Optional[int] = None) -> int:
        log.info(
            "Syncing players%s%s",
            f" for season {season_id}" if season_id else "",
            f" and team {team_id}" if team_id else "",
        )
        path = f"{FOOTBALL}players"
        if season_id:
            path = f"{FOOTBALL}players/seasons/{season_id}"
        params: Dict[str, object] = {}
        if team_id:
            params["team_id"] = team_id

        count = 0
        id_after = None
        if season_id and not team_id:
            state_key = f"players_id_after_season_{season_id}"
            try:
                id_after_val = self._get_state(state_key)
                id_after = int(id_after_val) if id_after_val else None
            except Exception:
                id_after = None

        for item in self.client.fetch_collection(path, params=params, includes=["team", "position"], id_after=id_after):
            _upsert(self.session, Player, _player_mapper(item))
            count += 1
            if season_id and not team_id:
                self._set_state(f"players_id_after_season_{season_id}", str(item.get("id")))
        self.session.commit()
        log.info("Players synced: %s", count)
        return count

    # ---- fixtures & details ----
    def sync_fixtures(
        self, season_id: Optional[int] = None, team_ids: Optional[List[int]] = None, league_ids: Optional[List[int]] = None
    ) -> int:
        log.info(
            "Syncing fixtures%s%s%s",
            f" for season {season_id}" if season_id else "",
            f" filtered by teams {team_ids}" if team_ids else "",
            f" filtered by leagues {league_ids}" if league_ids else "",
        )
        path = f"{FOOTBALL}fixtures"
        params: Dict[str, object] = {}
        filters = None
        if season_id:
            path = f"{FOOTBALL}fixtures/seasons/{season_id}"
        if team_ids:
            params["team_ids"] = ",".join(str(t) for t in team_ids)
        if league_ids:
            filters = f"fixtureLeagues:{','.join(str(l) for l in league_ids)}"

        count = 0
        for item in self.client.fetch_collection(
            path,
            params=params,
            includes=["scores", "participants"],
            filters=filters,
            per_page=50,
        ):
            _upsert(self.session, Fixture, _fixture_mapper(item))
            self._store_participants(item)
            count += 1
        self.session.commit()
        log.info("Fixtures synced: %s", count)
        return count

    def sync_fixtures_between(
        self,
        start_date: str,
        end_date: str,
        league_ids: Optional[List[int]] = None,
        with_details: bool = False,
        limit: Optional[int] = None,
    ) -> int:
        """
        Fetch fixtures between dates (inclusive). Dates are YYYY-MM-DD.
        with_details=True pulls statistics/lineups (heavier).
        """
        log.info(
            "Syncing fixtures between %s and %s%s%s",
            start_date,
            end_date,
            " with details" if with_details else "",
            f" leagues {league_ids}" if league_ids else "",
        )
        path = f"{FOOTBALL}fixtures/between/{start_date}/{end_date}"
        filters = None
        if league_ids:
            filters = f"fixtureLeagues:{','.join(str(l) for l in league_ids)}"
        includes = ["participants", "scores"]
        if with_details:
            includes.extend(["statistics.type", "lineups.player", "lineups.details", "lineups.details.type"])

        count = 0
        for item in self.client.fetch_collection(
            path,
            filters=filters,
            includes=includes,
            per_page=50,
        ):
            _upsert(self.session, Fixture, _fixture_mapper(item))
            self._store_participants(item)
            if with_details:
                self._store_team_stats(item)
                self._store_player_stats(item)
            count += 1
            if limit and count >= limit:
                break
        self.session.commit()
        log.info("Fixtures between synced: %s", count)
        return count

    def sync_history_window(
        self,
        days_back: int = 400,
        days_forward: int = 14,
        league_ids: Optional[List[int]] = None,
        with_details: bool = True,
        limit: Optional[int] = None,
    ) -> int:
        """
        Convenience wrapper: fetch fixtures for a relative window (past/future) with optional details.
        """
        today = datetime.utcnow().date()
        start = (today - timedelta(days=days_back)).isoformat()
        end = (today + timedelta(days=days_forward)).isoformat()
        log.info(
            "Syncing history window %s to %s (back=%s forward=%s) with_details=%s limit=%s",
            start,
            end,
            days_back,
            days_forward,
            with_details,
            limit,
        )
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
        max_span = 95  # SportMonks caps date range at 100 days
        total = 0
        cursor = start_dt
        while cursor <= end_dt:
            chunk_end = min(cursor + timedelta(days=max_span - 1), end_dt)
            remaining = None if limit is None else max(limit - total, 0)
            if remaining == 0:
                break
            chunk_limit = remaining if remaining else None
            count = self.sync_fixtures_between(
                start_date=cursor.date().isoformat(),
                end_date=chunk_end.date().isoformat(),
                league_ids=league_ids,
                with_details=with_details,
                limit=chunk_limit,
            )
            total += count
            cursor = chunk_end + timedelta(days=1)
        log.info("History window complete: %s fixtures stored", total)
        return total

    def sync_fixture_details(
        self,
        season_id: Optional[int] = None,
        league_ids: Optional[List[int]] = None,
        limit: Optional[int] = None,
    ) -> int:
        """
        Fetch fixtures with heavy includes (participants, statistics, lineups) to persist team/player stats.
        """
        log.info(
            "Syncing fixture details%s%s%s",
            f" for season {season_id}" if season_id else "",
            f" filtered by leagues {league_ids}" if league_ids else "",
            f" limit {limit}" if limit else "",
        )
        path = f"{FOOTBALL}fixtures"
        filters = None
        params: Dict[str, object] = {}
        if season_id:
            path = f"{FOOTBALL}fixtures/seasons/{season_id}"
        if league_ids:
            filters = f"fixtureLeagues:{','.join(str(l) for l in league_ids)}"

        count = 0
        for item in self.client.fetch_collection(
            path,
            params=params,
            includes=[
                "participants",
                "scores",
                "statistics.type",
                "lineups.player",
                "lineups.details",
                "lineups.details.type",
            ],
            filters=filters,
            per_page=50,
        ):
            _upsert(self.session, Fixture, _fixture_mapper(item))
            self._store_participants(item)
            self._store_team_stats(item)
            self._store_player_stats(item)
            count += 1
            if limit and count >= limit:
                break
        self.session.commit()
        log.info("Fixture details synced: %s", count)
        return count

    def sync_livescores(self, limit: Optional[int] = None) -> int:
        """
        Fetch live scores (15-min pre-kickoff through 15-min post-game) and upsert fixtures + participants.
        """
        log.info("Syncing livescores%s", f" limit {limit}" if limit else "")
        path = f"{FOOTBALL}livescores"
        count = 0
        for item in self.client.fetch_collection(
            path,
            includes=["participants", "scores"],
            per_page=50,
        ):
            _upsert(self.session, Fixture, _fixture_mapper(item))
            self._store_participants(item)
            count += 1
            if limit and count >= limit:
                break
        self.session.commit()
        log.info("Livescores synced: %s", count)
        return count

    # ---- odds ----
    def sync_bookmakers(self) -> int:
        log.info("Syncing bookmakers")
        count = 0
        for row in self.client.fetch_collection(f"{ODDS}bookmakers", per_page=1000):
            if not row:
                continue
            data = {
                "id": row.get("id"),
                "name": row.get("name") or row.get("bookmaker"),
                "slug": row.get("slug"),
                "extra": row,
            }
            _upsert(self.session, Bookmaker, data)
            count += 1
        self.session.commit()
        log.info("Bookmakers synced: %s", count)
        return count

    def sync_odds(
        self,
        fixture_ids: Optional[List[int]] = None,
        bookmaker_id: Optional[int] = None,
        league_ids: Optional[List[int]] = None,
        limit: Optional[int] = None,
    ) -> int:
        bookmaker_id = bookmaker_id or 2
        ids = fixture_ids or self._fixture_ids_for_leagues(league_ids, limit)
        if not ids:
            log.warning("No fixtures found to fetch odds for.")
            return 0

        log.info("Syncing odds for %s fixtures (bookmaker %s)", len(ids), bookmaker_id)
        processed = 0
        for fid in ids:
            payload = self.client.get_raw(
                f"{ODDS}pre-match/fixtures/{fid}",
                params={"filter": f"bookmakers:{bookmaker_id}"} if bookmaker_id else {},
            )
            rows = payload.get("data") if isinstance(payload, dict) else None
            if rows is None:
                rows = payload.get("odds") if isinstance(payload, dict) else None
            if rows is None and isinstance(payload, list):
                rows = payload
            if rows is None:
                continue
            self._store_odds_rows(fid, rows)
            processed += 1
            if limit and processed >= limit:
                break
        self.session.commit()
        log.info("Odds synced for %s fixtures", processed)
        return processed

    def sync_player_odds(
        self,
        days_forward: int = 7,
        bookmaker_id: int = 2,
        league_ids: Optional[List[int]] = None,
        limit: Optional[int] = 200,
    ) -> int:
        """
        Fetch player prop odds (shots/SOT) for upcoming fixtures in the next N days.
        """
        now = datetime.utcnow()
        future_cutoff = now + timedelta(days=days_forward)
        query = self.session.query(Fixture.id)
        query = query.filter(Fixture.starting_at != None)  # noqa: E711
        query = query.filter(Fixture.starting_at >= now, Fixture.starting_at <= future_cutoff)
        if league_ids:
            query = query.filter(Fixture.league_id.in_(league_ids))
        query = query.order_by(Fixture.starting_at.asc())
        if limit:
            query = query.limit(limit)
        fixture_ids = [row[0] for row in query.all()]
        if not fixture_ids:
            log.info("No upcoming fixtures found for player odds.")
            return 0

        # Build name lookup for player matching
        players = self.session.query(Player.id, Player.display_name, Player.first_name, Player.last_name).all()
        name_to_id: Dict[str, int] = {}
        for pid, dname, fname, lname in players:
            for nm in (dname, fname, lname, f"{fname or ''} {lname or ''}".strip()):
                key = _normalize_name(nm)
                if key:
                    name_to_id.setdefault(key, pid)

        processed_rows = 0
        for fid in fixture_ids:
            payload = self.client.get_raw(
                f"{ODDS}pre-match/fixtures/{fid}",
                params={"filter": f"bookmakers:{bookmaker_id}"} if bookmaker_id else {},
            )
            rows = payload.get("data") if isinstance(payload, dict) else None
            if rows is None and isinstance(payload, list):
                rows = payload
            if not rows:
                continue
            for row in rows:
                market_name = (row.get("market_name") or row.get("market_description") or "").lower()
                # crude filter for player shots/SOT markets
                if not any(k in market_name for k in ["player", "shot"]):
                    continue
                label = _normalize_name(str(row.get("label") or row.get("name") or ""))
                player_id = name_to_id.get(label)
                if not player_id:
                    continue
                data = {
                    "fixture_id": fid,
                    "player_id": player_id,
                    "market_id": row.get("market_id"),
                    "market_name": row.get("market_name") or row.get("market_description"),
                    "selection": row.get("label") or row.get("name"),
                    "line": to_float(row.get("handicap") or row.get("line") or row.get("total")),
                    "decimal_odds": to_float(row.get("value")),
                    "american_odds": row.get("american"),
                    "fractional_odds": row.get("fractional"),
                    "extra": row,
                }
                # Upsert manually because UniqueConstraint is composite
                obj = (
                    self.session.query(PlayerOdds)
                    .filter_by(
                        fixture_id=data["fixture_id"],
                        player_id=data["player_id"],
                        market_id=data["market_id"],
                        line=data["line"],
                        selection=data["selection"],
                    )
                    .one_or_none()
                )
                if obj:
                    for k, v in data.items():
                        setattr(obj, k, v)
                else:
                    self.session.add(PlayerOdds(**data))
                processed_rows += 1
        self.session.commit()
        log.info("Player odds synced rows=%s fixtures=%s", processed_rows, len(fixture_ids))
        return processed_rows

    def sync_inplay_odds(
        self,
        bookmaker_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> int:
        """
        Fetch latest inplay odds snapshot (default Bet365).
        """
        bookmaker_id = bookmaker_id or 2
        params = {"per_page": limit or 200}
        if bookmaker_id:
            params["filter"] = f"bookmakers:{bookmaker_id}"
        payload = self.client.get_raw(f"{ODDS}inplay/latest", params=params)
        rows = payload.get("data") if isinstance(payload, dict) else None
        if rows is None and isinstance(payload, list):
            rows = payload
        if not rows:
            log.info("No inplay odds rows returned")
            return 0
        # Group by fixture to reuse odds storing
        by_fixture: Dict[int, List[Dict]] = {}
        for row in rows:
            fid = row.get("fixture_id")
            if fid is None:
                continue
            by_fixture.setdefault(int(fid), []).append(row)
        for fid, subset in by_fixture.items():
            self._store_odds_rows(fid, subset)
        self.session.commit()
        total = sum(len(v) for v in by_fixture.values())
        log.info("Inplay odds synced: fixtures=%s rows=%s", len(by_fixture), total)
        return len(by_fixture)

    # ---- H2H ----
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

    # ---- helpers ----
    def _fixture_ids_for_leagues(self, league_ids: Optional[List[int]], limit: Optional[int]) -> List[int]:
        query = self.session.query(Fixture.id)
        if league_ids:
            query = query.filter(Fixture.league_id.in_(league_ids))
        query = query.order_by(Fixture.starting_at.desc().nulls_last())
        if limit:
            query = query.limit(limit)
        return [row[0] for row in query.all()]

    def _store_participants(self, fixture_raw: Dict) -> None:
        participants = fixture_raw.get("participants") or []
        fixture_id = fixture_raw.get("id")
        if not fixture_id:
            return
        for p in participants:
            team_id = p.get("id") or p.get("team_id")
            if not team_id:
                continue
            meta = p.get("meta") or {}
            obj = (
                self.session.query(FixtureParticipant)
                .filter_by(fixture_id=fixture_id, team_id=team_id)
                .one_or_none()
            )
            data = {
                "fixture_id": fixture_id,
                "team_id": team_id,
                "location": meta.get("location"),
                "result": meta.get("result"),
                "score": meta.get("score") or meta.get("outcome"),
                "extra": p,
            }
            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                self.session.add(FixtureParticipant(**data))

    def _store_team_stats(self, fixture_raw: Dict) -> None:
        fixture_id = fixture_raw.get("id")
        stats_grouped = _group_team_stats(fixture_raw.get("statistics") or [])
        for team_id, rows in stats_grouped.items():
            obj = (
                self.session.query(TeamStatLine)
                .filter_by(fixture_id=fixture_id, team_id=team_id)
                .one_or_none()
            )
            data = {
                "fixture_id": fixture_id,
                "team_id": team_id,
                "location": (rows[0].get("location") if rows else None),
                "stats": rows,
            }
            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                self.session.add(TeamStatLine(**data))

    def _store_player_stats(self, fixture_raw: Dict) -> None:
        fixture_id = fixture_raw.get("id")
        for lu in fixture_raw.get("lineups") or []:
            player = lu.get("player") or {}
            player_id = player.get("id") or lu.get("player_id")
            team_id = lu.get("team_id") or (lu.get("team") or {}).get("id")
            if not (fixture_id and player_id and team_id):
                continue
            _upsert(self.session, Player, _player_mapper(player))
            stats_rows = lu.get("details") or []
            minutes = None
            for d in stats_rows:
                if d.get("type_id") in (119,):
                    minutes = minutes or _safe_int(d.get("value") or d.get("data"))
            data = {
                "fixture_id": fixture_id,
                "team_id": team_id,
                "player_id": player_id,
                "position": lu.get("position"),
                "jersey_number": lu.get("number"),
                "is_starting": lu.get("is_starting") or lu.get("formation_position") is not None,
                "minutes": minutes,
                "stats": stats_rows,
                "extra": lu,
            }
            obj = (
                self.session.query(PlayerStatLine)
                .filter_by(fixture_id=fixture_id, player_id=player_id)
                .one_or_none()
            )
            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                self.session.add(PlayerStatLine(**data))

    def _store_odds_rows(self, fixture_id: int, rows: List[Dict]) -> None:
        for row in rows:
            if not isinstance(row, dict):
                continue
            bookmaker_id = row.get("bookmaker_id")
            market_id = row.get("market_id")
            provider_outcome_id = row.get("id")
            raw_hash = dict_hash(row)

            if bookmaker_id and not self.session.get(Bookmaker, bookmaker_id):
                bm = {
                    "id": bookmaker_id,
                    "name": row.get("bookmaker_name") or row.get("bookmaker") or f"Bookmaker {bookmaker_id}",
                    "slug": None,
                    "extra": {"source": "odds_row"},
                }
                self.session.add(Bookmaker(**bm))

            if market_id and not self.session.get(Market, market_id):
                mk = {
                    "id": market_id,
                    "name": row.get("market_description") or row.get("market_name") or str(market_id),
                    "grouping": None,
                    "extra": {"source": "odds_row"},
                }
                self.session.add(Market(**mk))

            obj = None
            if provider_outcome_id:
                obj = (
                    self.session.query(OddsOutcome)
                    .filter_by(provider_outcome_id=provider_outcome_id)
                    .one_or_none()
                )
            if obj is None:
                obj = (
                    self.session.query(OddsOutcome)
                    .filter_by(
                        fixture_id=fixture_id,
                        bookmaker_id=bookmaker_id,
                        market_id=market_id,
                        label=row.get("label"),
                        participant=row.get("name") or row.get("participant") or row.get("total"),
                        handicap=row.get("handicap"),
                        total=str(row.get("total")) if row.get("total") is not None else None,
                    )
                    .one_or_none()
                )
                if obj and obj.raw_hash == raw_hash:
                    # identical payload, skip
                    continue

            data = {
                "provider_outcome_id": provider_outcome_id,
                "fixture_id": fixture_id,
                "bookmaker_id": bookmaker_id,
                "market_id": market_id,
                "market_description": row.get("market_description") or row.get("market_name"),
                "label": row.get("label"),
                "name": row.get("name"),
                "participant": row.get("participant") or row.get("name") or row.get("total"),
                "participant_type": row.get("participant_type"),
                "participant_id": row.get("participant_id"),
                "handicap": row.get("handicap"),
                "total": str(row.get("total")) if row.get("total") is not None else None,
                "decimal_odds": float(row.get("value")) if row.get("value") not in (None, "") else None,
                "american_odds": row.get("american"),
                "fractional_odds": row.get("fractional"),
                "probability": row.get("probability"),
                "stopped": row.get("stopped"),
                "is_winning": row.get("winning"),
                "raw": row,
                "raw_hash": raw_hash,
            }
            if obj:
                for k, v in data.items():
                    setattr(obj, k, v)
            else:
                self.session.add(OddsOutcome(**data))


def bootstrap_schema(session: Session) -> None:
    from .db import Base

    Base.metadata.create_all(session.get_bind())
    ensure_player_form_columns(session)


def ensure_player_form_columns(session: Session) -> None:
    """
    Lightweight, SQLite-friendly migration to add newer player_forms columns if missing.
    """
    needed = {
        "goals_avg": "FLOAT",
        "shots_on_ge_1_pct": "FLOAT",
        "shots_on_ge_2_pct": "FLOAT",
        "goals_ge_1_pct": "FLOAT",
        "goals_ge_2_pct": "FLOAT",
        "assists_ge_1_pct": "FLOAT",
    }
    existing = {
        row[1]
        for row in session.execute(text("PRAGMA table_info(player_forms)")).fetchall()
    }
    for col, ddl in needed.items():
        if col in existing:
            continue
        session.execute(text(f"ALTER TABLE player_forms ADD COLUMN {col} {ddl}"))
    session.commit()
