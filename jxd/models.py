from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    UniqueConstraint,
    Index,
    BigInteger,
)

from .db import Base


class Country(Base):
    __tablename__ = "countries"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    code = Column(String(10))
    continent = Column(String(50))
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class League(Base):
    __tablename__ = "leagues"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50))
    country_id = Column(Integer, ForeignKey("countries.id"), index=True)
    logo_path = Column(String(500))
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Season(Base):
    __tablename__ = "seasons"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    league_id = Column(Integer, ForeignKey("leagues.id"), index=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    is_current = Column(Boolean)
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Venue(Base):
    __tablename__ = "venues"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    city = Column(String(255))
    country_id = Column(Integer, ForeignKey("countries.id"), index=True)
    capacity = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    image_path = Column(String(500))
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Team(Base):
    __tablename__ = "teams"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    short_code = Column(String(50))
    country_id = Column(Integer, ForeignKey("countries.id"), index=True)
    founded = Column(Integer)
    venue_id = Column(Integer, ForeignKey("venues.id"), index=True)
    logo_path = Column(String(500))
    is_national = Column(Boolean)
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Player(Base):
    __tablename__ = "players"
    id = Column(Integer, primary_key=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    display_name = Column(String(255))
    nationality_id = Column(Integer, ForeignKey("countries.id"), index=True)
    birth_date = Column(DateTime)
    height = Column(Float)
    weight = Column(Float)
    position_id = Column(Integer)
    position_name = Column(String(100))
    image_path = Column(String(500))
    current_team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Fixture(Base):
    __tablename__ = "fixtures"
    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey("leagues.id"), index=True)
    season_id = Column(Integer, ForeignKey("seasons.id"), index=True)
    round_id = Column(Integer)
    group_id = Column(Integer)
    stage_id = Column(Integer)
    referee_id = Column(Integer)
    venue_id = Column(Integer, ForeignKey("venues.id"), index=True)
    starting_at = Column(DateTime, index=True)
    status = Column(String(100))
    status_code = Column(String(10))
    home_team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    away_team_id = Column(Integer, ForeignKey("teams.id"), index=True)
    home_score = Column(Integer)
    away_score = Column(Integer)
    scores = Column(JSON)
    weather_report = Column(JSON)
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class HeadToHead(Base):
    __tablename__ = "head_to_head"
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_a_id = Column(Integer, ForeignKey("teams.id"), index=True)
    team_b_id = Column(Integer, ForeignKey("teams.id"), index=True)
    fetched_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    summary = Column(JSON)
    fixtures = Column(JSON)
    __table_args__ = (
        UniqueConstraint("team_a_id", "team_b_id", name="uq_head_to_head_pair"),
        Index("idx_head_to_head_pair", "team_a_id", "team_b_id"),
    )


class FixtureParticipant(Base):
    __tablename__ = "fixture_participants"
    fixture_id = Column(Integer, ForeignKey("fixtures.id"), primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), primary_key=True)
    location = Column(String(10))
    result = Column(String(20))
    score = Column(String(20))
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TeamStatLine(Base):
    """
    Team-level statistics for a fixture (raw stats JSON preserved).
    """

    __tablename__ = "team_stats"
    id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(Integer, ForeignKey("fixtures.id"), index=True, nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), index=True, nullable=False)
    location = Column(String(10))
    stats = Column(JSON)  # raw list of statistics rows from SportMonks
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    __table_args__ = (UniqueConstraint("fixture_id", "team_id", name="uq_team_stats_fixture"),)


class PlayerStatLine(Base):
    """
    Player-level statistics for a fixture (lineups + details preserved).
    """

    __tablename__ = "player_stats"
    id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(Integer, ForeignKey("fixtures.id"), index=True, nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), index=True, nullable=False)
    player_id = Column(Integer, ForeignKey("players.id"), index=True, nullable=False)
    position = Column(String(100))
    jersey_number = Column(Integer)
    is_starting = Column(Boolean)
    minutes = Column(Integer)
    stats = Column(JSON)  # raw list of detail rows from SportMonks
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    __table_args__ = (
        UniqueConstraint("fixture_id", "player_id", name="uq_player_stats_fixture"),
        Index("idx_player_stats_team_fixture", "fixture_id", "team_id"),
    )


class Bookmaker(Base):
    __tablename__ = "bookmakers"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    slug = Column(String(255))
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Market(Base):
    __tablename__ = "markets"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    grouping = Column(String(255))
    extra = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class OddsOutcome(Base):
    __tablename__ = "odds_outcomes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    provider_outcome_id = Column(BigInteger, index=True)
    fixture_id = Column(Integer, ForeignKey("fixtures.id"), index=True, nullable=False)
    bookmaker_id = Column(Integer, ForeignKey("bookmakers.id"), index=True, nullable=False)
    market_id = Column(Integer, ForeignKey("markets.id"), index=True, nullable=False)
    market_description = Column(String(255))
    label = Column(String(255))
    name = Column(String(255))
    participant = Column(String(255))
    participant_type = Column(String(50))
    participant_id = Column(Integer)
    handicap = Column(String(64))
    total = Column(String(64))
    decimal_odds = Column(Float)
    american_odds = Column(String(20))
    fractional_odds = Column(String(50))
    probability = Column(String(50))
    stopped = Column(Boolean)
    is_winning = Column(Boolean)
    raw = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    __table_args__ = (
        Index("idx_odds_fixture_market", "fixture_id", "market_id"),
        UniqueConstraint(
            "fixture_id",
            "bookmaker_id",
            "market_id",
            "label",
            "participant",
            "handicap",
            "total",
            name="uq_odds_outcome_key",
        ),
    )
