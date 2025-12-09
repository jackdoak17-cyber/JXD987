from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional, Tuple

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
    PlayerAvailability,
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
    wins = draws = losses = 0
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
        # approximate W/D/L from goals
        if (gf or 0) > (ga or 0):
            wins += 1
        elif (gf or 0) == (ga or 0):
            draws += 1
        else:
            losses += 1
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
        "win_pct": wins / games_played,
        "draw_pct": draws / games_played,
        "loss_pct": losses / games_played,
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
    minutes_list = []
    ge1 = ge2 = ge3 = 0
    assists = []
    fixtures_raw = []
    league_id = season_id = team_id = None

    for stat, fx in rows:
        league_id = league_id or fx.league_id
        season_id = season_id or fx.season_id
        team_id = team_id or stat.team_id
        shots = 0
        shots_on_target = 0
        assists_val = 0
        minutes_val = stat.minutes or 0
        minutes_list.append(minutes_val)
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
            elif name in ("ASSISTS", "Assists"):
                assists_val = val
        shots_total.append(shots)
        shots_on.append(shots_on_target)
        assists.append(assists_val)
        if shots >= 1:
            ge1 += 1
        if shots >= 2:
            ge2 += 1
        if shots >= 3:
            ge3 += 1
        fixtures_raw.append(
            {
                "fixture_id": fx.id,
                "shots": shots,
                "shots_on": shots_on_target,
                "assists": assists_val,
                "minutes": minutes_val,
                "date": fx.starting_at.isoformat() if fx.starting_at else None,
            }
        )

    games_played = len(shots_total)
    if games_played == 0:
        return None
    shots_avg = sum(shots_total) / games_played
    shots_on_avg = sum(shots_on) / games_played
    assists_avg = sum(assists) / games_played if assists else 0
    minutes_avg = sum(minutes_list) / games_played if minutes_list else None
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
        "assists_avg": assists_avg,
        "minutes_avg": minutes_avg,
        "raw_fixtures": fixtures_raw,
    }
    if obj:
        for k, v in data.items():
            setattr(obj, k, v)
    else:
        obj = PlayerForm(**data)
        session.add(obj)
    return obj


def bulk_compute_forms(session: Session, sample_sizes: Iterable[int] | None = None, availability_sample: int = 2) -> Tuple[int, int, int]:
    """
    Recompute team and player forms for all IDs present for the given sample sizes.
    Also computes availability.
    """
    sample_sizes = sorted({int(s) for s in (sample_sizes or [10]) if s})
    team_ids = [row[0] for row in session.query(FixtureParticipant.team_id).distinct().all()]
    player_ids = [row[0] for row in session.query(PlayerStatLine.player_id).distinct().all()]
    t_total = p_total = 0
    for sample_size in sample_sizes:
        t_count = p_count = 0
        for tid in team_ids:
            if compute_team_form(session, tid, sample_size):
                t_count += 1
        for pid in player_ids:
            if compute_player_form(session, pid, sample_size):
                p_count += 1
        session.commit()
        t_total += t_count
        p_total += p_count
        log.info("Computed forms sample=%s: teams=%s players=%s", sample_size, t_count, p_count)

    avail_count = compute_availability(session, sample_size=availability_sample)
    log.info(
        "Availability sample=%s entries=%s across form samples=%s",
        availability_sample,
        avail_count,
        sample_sizes,
    )
    return t_total, p_total, avail_count


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


def compute_availability(session: Session, sample_size: int = 2) -> int:
    """
    Mark likely starters based on appearances in last N fixtures.
    """
    players = session.query(PlayerStatLine.player_id, PlayerStatLine.team_id).distinct().all()
    total = 0
    for pid, tid in players:
        rows = (
            session.query(PlayerStatLine, Fixture)
            .join(Fixture, PlayerStatLine.fixture_id == Fixture.id)
            .filter(PlayerStatLine.player_id == pid, PlayerStatLine.team_id == tid)
            .order_by(Fixture.starting_at.desc())
            .limit(sample_size)
            .all()
        )
        if not rows:
            continue
        appearances = 0
        starts = 0
        for stat, _ in rows:
            appearances += 1
            if stat.is_starting:
                starts += 1
        likely = starts / sample_size >= 0.5
        obj = (
            session.query(PlayerAvailability)
            .filter(PlayerAvailability.player_id == pid, PlayerAvailability.sample_size == sample_size)
            .one_or_none()
        )
        data = {
            "player_id": pid,
            "team_id": tid,
            "likely_starter": likely,
            "confidence": starts / sample_size,
            "reason": f"started {starts}/{sample_size} recent",
            "sample_size": sample_size,
        }
        if obj:
            for k, v in data.items():
                setattr(obj, k, v)
        else:
            session.add(PlayerAvailability(**data))
        total += 1
    session.commit()
    log.info("Computed availability entries: %s", total)
    return total
