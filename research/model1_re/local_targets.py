from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class LocalTrainingTarget:
    """
    A locally-labeled training target derived from our own odds + final stats.

    This replaces the Statshub dependency: we use our own database to decide
    whether the player went over the line in the fixture.
    """

    group_key: str
    market_key: str
    public_market_type: str
    fixture_id: int
    fixture_timestamp: datetime
    league_id: int
    season_id: int | None
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    player_id: int
    player_name_db: str
    team_id: int
    opponent_team_id: int
    team_is_home: bool
    line: float
    best_over_odds: float
    actual_value: float
    hit: bool
    raw_row: dict[str, Any] | None = None

