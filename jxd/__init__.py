"""
Lightweight SportMonks ingestion package.

Exports the client, database helpers, ORM models, and sync service used by
the command entrypoints under ``scripts/``.
"""

from .sportmonks_client import SportMonksClient
from .db import get_engine, get_session
from .models import Base, Season, Team, Fixture, FixtureParticipant, SyncState
from .sync import SyncService, choose_keep_seasons_per_league

__all__ = [
    "SportMonksClient",
    "get_engine",
    "get_session",
    "Base",
    "Season",
    "Team",
    "Fixture",
    "FixtureParticipant",
    "SyncState",
    "SyncService",
    "choose_keep_seasons_per_league",
]
