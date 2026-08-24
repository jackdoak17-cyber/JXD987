from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    JSON,
    PrimaryKeyConstraint,
    Numeric,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Season(Base):
    __tablename__ = "seasons"

    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, nullable=False)
    name = Column(String, nullable=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    is_current = Column(Boolean, default=False)
    extra = Column(JSON, nullable=True)


class Round(Base):
    __tablename__ = "rounds"

    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, nullable=False)
    season_id = Column(Integer, nullable=False)
    stage_id = Column(Integer, nullable=True)
    name = Column(String, nullable=True)
    starting_at = Column(Date, nullable=True)
    ending_at = Column(Date, nullable=True)
    is_current = Column(Boolean, default=False)
    games_in_current_week = Column(Boolean, default=False)
    finished = Column(Boolean, default=False)
    extra = Column(JSON, nullable=True)


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)
    short_code = Column(String, nullable=True)
    image_path = Column(String, nullable=True)
    extra = Column(JSON, nullable=True)


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)
    display_name = Column(String, nullable=True)
    short_name = Column(String, nullable=True)
    common_name = Column(String, nullable=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    team_updated_at = Column(DateTime, nullable=True)
    image_path = Column(String, nullable=True)
    extra = Column(JSON, nullable=True)


class SidelinedPlayer(Base):
    __tablename__ = "sidelined_players"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    category = Column(String, nullable=True)
    type_id = Column(Integer, nullable=True)
    season_id = Column(Integer, nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    games_missed = Column(Integer, nullable=True)
    completed = Column(Boolean, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    extra = Column(JSON, nullable=True)


class PlayerTeamHistory(Base):
    __tablename__ = "player_team_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    source = Column(String, nullable=True)
    effective_from = Column(DateTime, nullable=False)
    effective_to = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TeamSquadSnapshot(Base):
    """A provider response used to establish a team's current squad."""

    __tablename__ = "team_squad_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    source = Column(String, nullable=False, default="sportmonks")
    status = Column(String, nullable=False)  # success, empty, or failed
    observed_at = Column(DateTime, nullable=False, index=True)
    completed_at = Column(DateTime, nullable=True)
    player_count = Column(Integer, nullable=False, default=0)
    payload_hash = Column(String, nullable=True)
    error = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class TeamSquadMembership(Base):
    """Current squad membership derived only from successful snapshots."""

    __tablename__ = "team_squad_memberships"
    __table_args__ = (PrimaryKeyConstraint("team_id", "player_id"),)

    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    first_seen_at = Column(DateTime, nullable=False)
    last_seen_at = Column(DateTime, nullable=False, index=True)
    provider_started_at = Column(DateTime, nullable=True, index=True)
    last_snapshot_id = Column(Integer, ForeignKey("team_squad_snapshots.id"), nullable=True)
    source = Column(String, nullable=False, default="sportmonks")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Fixture(Base):
    __tablename__ = "fixtures"

    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, nullable=True)
    season_id = Column(Integer, nullable=True)
    starting_at = Column(DateTime, nullable=True, index=True)
    status = Column(String, nullable=True)
    status_code = Column(String, nullable=True)
    home_team_id = Column(Integer, nullable=True)
    away_team_id = Column(Integer, nullable=True)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    lineup_confirmed = Column(Boolean, nullable=True)
    extra = Column(JSON, nullable=True)


class FixtureParticipant(Base):
    __tablename__ = "fixture_participants"
    __table_args__ = (PrimaryKeyConstraint("fixture_id", "team_id"),)

    fixture_id = Column(Integer, ForeignKey("fixtures.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    location = Column(String, nullable=True)  # home/away
    score = Column(Integer, nullable=True)
    extra = Column(JSON, nullable=True)


class FixtureStatistic(Base):
    __tablename__ = "fixture_statistics"
    __table_args__ = (PrimaryKeyConstraint("fixture_id", "team_id", "type_id", "code", "location"),)

    fixture_id = Column(Integer, ForeignKey("fixtures.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    type_id = Column(Integer, nullable=True)
    code = Column(String, nullable=True)
    name = Column(String, nullable=True)
    location = Column(String, nullable=True)
    value = Column(Integer, nullable=True)
    extra = Column(JSON, nullable=True)


class FixturePlayer(Base):
    __tablename__ = "fixture_players"
    __table_args__ = (PrimaryKeyConstraint("fixture_id", "player_id"),)

    fixture_id = Column(Integer, ForeignKey("fixtures.id"), nullable=False)
    player_id = Column(Integer, nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    name = Column(String, nullable=True)
    position = Column(String, nullable=True)
    lineup_type = Column(String, nullable=True)  # lineup/substitute
    formation_position = Column(Integer, nullable=True)
    jersey_number = Column(String, nullable=True)
    is_starter = Column(Boolean, nullable=True)
    minutes_played = Column(Integer, nullable=True)
    position_name = Column(String, nullable=True)
    detailed_position_id = Column(Integer, nullable=True)
    detailed_position_name = Column(String, nullable=True)
    detailed_position_code = Column(String, nullable=True)
    formation_field = Column(String, nullable=True)
    lineup_detailed_position_id = Column(Integer, nullable=True)
    lineup_detailed_position_name = Column(String, nullable=True)
    lineup_detailed_position_code = Column(String, nullable=True)
    position_abbr = Column(String, nullable=True)
    extra = Column(JSON, nullable=True)


class FixturePlayerStatistic(Base):
    __tablename__ = "fixture_player_statistics"
    __table_args__ = (PrimaryKeyConstraint("fixture_id", "player_id", "type_id", "code"),)

    fixture_id = Column(Integer, ForeignKey("fixtures.id"), nullable=False)
    player_id = Column(Integer, nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    type_id = Column(Integer, nullable=True)
    code = Column(String, nullable=True)
    name = Column(String, nullable=True)
    # Player ratings and a small number of provider metrics are decimal-valued.
    # Keep the source model numeric so the ingestion path cannot silently
    # truncate values such as 7.85 to 7 before export.
    value = Column(Numeric(12, 4), nullable=True)
    extra = Column(JSON, nullable=True)


class SyncState(Base):
    __tablename__ = "sync_state"

    key = Column(String, primary_key=True)
    value = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class OddsSnapshot(Base):
    __tablename__ = "odds_snapshots"

    id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, nullable=False, index=True)
    bookmaker_id = Column(Integer, nullable=False, default=2)
    pulled_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    raw = Column(JSON, nullable=False)


class OddsOutcome(Base):
    __tablename__ = "odds_outcomes"
    __table_args__ = (
        UniqueConstraint(
            "fixture_id", "bookmaker_id", "market_key", "selection_key", "line"
        ),
    )

    id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, nullable=False, index=True)
    bookmaker_id = Column(Integer, nullable=False, default=2)
    market_key = Column(String, nullable=False, index=True)
    selection_key = Column(String, nullable=False)
    participant_type = Column(String, nullable=True)
    participant_id = Column(Integer, nullable=True, index=True)
    line = Column(Numeric, nullable=True)
    price_decimal = Column(Numeric, nullable=False)
    price_american = Column(Integer, nullable=True)
    last_updated_at = Column(DateTime, nullable=True)
