from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, JSON, PrimaryKeyConstraint
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
    short_name = Column(String, nullable=True)
    common_name = Column(String, nullable=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    image_path = Column(String, nullable=True)
    extra = Column(JSON, nullable=True)


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
    value = Column(Integer, nullable=True)
    extra = Column(JSON, nullable=True)


class SyncState(Base):
    __tablename__ = "sync_state"

    key = Column(String, primary_key=True)
    value = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
