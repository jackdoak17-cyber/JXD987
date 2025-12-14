import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


def _db_path() -> str:
    """
    Resolve the SQLite path from env (JXD_DB_PATH) or default to data/jxd.sqlite.
    Ensures parent directory exists.
    """
    default = Path("data") / "jxd.sqlite"
    path = Path(os.environ.get("JXD_DB_PATH", str(default)))
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def get_engine(echo: bool = False):
    return create_engine(f"sqlite:///{_db_path()}", echo=echo, future=True)


def get_session(engine=None) -> Session:
    if engine is None:
        engine = get_engine()
    SessionLocal = sessionmaker(bind=engine, future=True)
    return SessionLocal()
