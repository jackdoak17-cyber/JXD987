from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models import (
    Fixture,
    FixtureParticipant,
    OddsLatest,
    OddsOutcome,
    PlayerForm,
    PlayerStatLine,
    TeamForm,
)
from .utils import parse_dt

log = logging.getLogger(__name__)


# ---- Team form ----
def compute_team_form(session: Session, team_id: int, sample_size: int = 10) -> Optional[TeamForm]:
    """
    Compute simple team form stats over last N fixtures (goals, over/under 2.5).
    """
    query = (
        session.query(Fixture, FixtureParticipant)
        .join(FixtureParticipant, Fixture.id == FixtureParticipant.fixture_id)
        .filter(FixtureParticipant.team_id == team_id)
        .filter(Fixture.home_score != None, Fixture.away_score != None)  # noqa: E711
        .order_by(Fixture.starting_at.desc().nulls_last())
        .limit(sample_size)
    )
    rows = query.all()
    if not rows:
        return None

    fixtures_raw = []
    goals_for = []
    goals_against = []
    over25 = 0
    league_id = None
    season_id = None
    for fx, part in rows:
        league_id = league_id or fx.league_id
        season_id = season_id or fx.season_id
        if fx.home_score is None or fx.away_score is None:
            continue
        if part.location == "home":
            gf, ga = fx.home_score, fx.away_score
        else:
            gf, ga = fx.away_score, fx.home_score
        goals_for.append(gf)
        goals_against.append(ga)
        total_goals = (gf or 0) + (ga or 0)
        if total_goals >= 3:
            over25 += 1
        fixtures_raw.append({"fixture_id": fx.id, "gf": gf, "ga": ga, "date": fx.starting_at.isoformat() if fx.starting_at else None})

    games_played = len(goals_for)
    if games_played == 0:
        return None
    gf_avg = sum(goals_for) / games_played
    ga_avg = sum(goals_against) / games_played
    over25_pct = over25 / games_played
    under25_pct = 1 - over25_pct

    obj = (
        session.query(TeamForm)
        .filter(TeamForm.team_id == team_id, TeamForm.sample_size == sample_size)
        .one_or_none()
    )
    data = {
        "team_id": team_id,
        "league_id": league_id,
        "season_id": season_id,
        "sample_size": sample_size,
        "games_played": games_played,
        "goals_for_avg": gf_avg,
        "goals_against_avg": ga_avg,
        "over_2_5_pct": over25_pct,
        "under_2_5_pct": under25_pct,
        "raw_fixtures": fixtures_raw,
    }
    if obj:
        for k, v in data.items():
            setattr(obj, k, v)
    else:
        obj = TeamForm(**data)
        session.add(obj)
    return obj


# ---- Player form ----
def compute_player_form(session: Session, player_id: int, sample_size: int = 10) -> Optional[PlayerForm]:
    """
    Compute player shooting form over last N appearances.
    """
    query = (
        session.query(PlayerStatLine, Fixture)
        .join(Fixture, PlayerStatLine.fixture_id == Fixture.id)
        .filter(PlayerStatLine.player_id == player_id)
        .filter(Fixture.starting_at != None)  # noqa: E711
        .order_by(Fixture.starting_at.desc())
        .limit(sample_size)
    )
    rows = query.all()
    if not rows:
        return None

    shots_total = []
    shots_on = []
    ge1 = ge2 = ge3 = 0
    fixtures_raw = []
    league_id = season_id = team_id = None

    for stat, fx in rows:
        league_id = league_id or fx.league_id
        season_id = season_id or fx.season_id
        team_id = team_id or stat.team_id
        shots = 0
        shots_on_target = 0
        for d in stat.stats or []:
            t = d.get("type") or {}
            name = t.get("developer_name") or t.get("name") or ""
            val = 0
            data = d.get("data")
            if isinstance(data, dict) and "value" in data:
                val = data.get("value") or 0
            elif isinstance(data, (int, float)):
                val = data
            if name in ("SHOTS_TOTAL", "Shots Total"):
                shots = val
            elif name in ("SHOTS_ON_TARGET", "Shots On Target"):
                shots_on_target = val
        shots_total.append(shots)
        shots_on.append(shots_on_target)
        if shots >= 1:
            ge1 += 1
        if shots >= 2:
            ge2 += 1
        if shots >= 3:
            ge3 += 1
        fixtures_raw.append(
            {"fixture_id": fx.id, "shots": shots, "shots_on": shots_on_target, "date": fx.starting_at.isoformat() if fx.starting_at else None}
        )

    games_played = len(shots_total)
    if games_played == 0:
        return None
    shots_avg = sum(shots_total) / games_played
    shots_on_avg = sum(shots_on) / games_played
    obj = (
        session.query(PlayerForm)
        .filter(PlayerForm.player_id == player_id, PlayerForm.sample_size == sample_size)
        .one_or_none()
    )
    data = {
        "player_id": player_id,
        "team_id": team_id,
        "league_id": league_id,
        "season_id": season_id,
        "sample_size": sample_size,
        "games_played": games_played,
        "shots_total_avg": shots_avg,
        "shots_on_target_avg": shots_on_avg,
        "shots_ge_1_pct": ge1 / games_played,
        "shots_ge_2_pct": ge2 / games_played,
        "shots_ge_3_pct": ge3 / games_played,
        "minutes_avg": None,
        "raw_fixtures": fixtures_raw,
    }
    if obj:
        for k, v in data.items():
            setattr(obj, k, v)
    else:
        obj = PlayerForm(**data)
        session.add(obj)
    return obj


def bulk_compute_forms(session: Session, sample_size: int = 10) -> Tuple[int, int]:
    """
    Recompute team and player forms for all IDs present.
    """
    team_ids = [row[0] for row in session.query(FixtureParticipant.team_id).distinct().all()]
    player_ids = [row[0] for row in session.query(PlayerStatLine.player_id).distinct().all()]
    t_count = p_count = 0
    for tid in team_ids:
        if compute_team_form(session, tid, sample_size):
            t_count += 1
    for pid in player_ids:
        if compute_player_form(session, pid, sample_size):
            p_count += 1
    session.commit()
    log.info("Computed forms: teams=%s players=%s (sample=%s)", t_count, p_count, sample_size)
    return t_count, p_count


# ---- Odds normalization ----

MARKET_MAP = {
    1: "match_result",
    2: "double_chance",
    6: "asian_handicap",
    7: "goal_line",
    10: "draw_no_bet",
    14: "btts",
    268: "player_shots",
}


def normalize_odds(session: Session) -> int:
    """
    Snapshot latest odds per fixture/market/selection/line into odds_latest.
    """
    rows = (
        session.query(OddsOutcome)
        .order_by(OddsOutcome.fixture_id, OddsOutcome.market_id, OddsOutcome.updated_at.desc())
        .all()
    )
    count = 0
    for row in rows:
        sel = row.participant or row.label or row.name
        line = row.handicap or row.total or row.label
        market_name = MARKET_MAP.get(row.market_id) or row.market_description or str(row.market_id)
        updated_at_source = None
        raw = row.raw or {}
        if raw:
            updated_at_source = parse_dt(raw.get("latest_bookmaker_update"))
        if updated_at_source is None:
            updated_at_source = row.updated_at

        obj = (
            session.query(OddsLatest)
            .filter_by(
                fixture_id=row.fixture_id,
                bookmaker_id=row.bookmaker_id,
                market_id=row.market_id,
                selection=sel,
                line=str(line) if line is not None else None,
            )
            .one_or_none()
        )
        data = {
            "fixture_id": row.fixture_id,
            "bookmaker_id": row.bookmaker_id,
            "market_id": row.market_id,
            "market_name": market_name,
            "selection": sel,
            "line": str(line) if line is not None else None,
            "decimal_odds": row.decimal_odds,
            "updated_at_source": updated_at_source,
            "raw": raw,
        }
        if obj:
            for k, v in data.items():
                setattr(obj, k, v)
        else:
            session.add(OddsLatest(**data))
        count += 1
    session.commit()
    log.info("Normalized odds rows: %s", count)
    return count
