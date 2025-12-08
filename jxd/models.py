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

