from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT / "research" / "model1_re"
ARTIFACTS_DIR = WORKSPACE / "artifacts"
REPORTS_DIR = WORKSPACE / "reports"

load_dotenv(ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    supabase_db_url: str
    public_value_bets_url: str = "https://www.statshub.com/api/value-bets"
    request_timeout_seconds: int = 30
    player_window: int = 60
    player_windows: tuple[int, ...] = (5, 10, 20, 40, 60)
    minimum_matches: int = 5
    started_only: bool = True
    opponent_windows: tuple[int, ...] = (10, 20, 40, 60)


def get_settings() -> Settings:
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL in repo .env")
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return Settings(supabase_db_url=db_url)
