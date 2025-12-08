from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

log = logging.getLogger(__name__)

Base = declarative_base()


def make_engine(database_url: str):
    if database_url.startswith("sqlite:///"):
        db_path = Path(database_url.replace("sqlite:///", ""))
        db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(database_url, future=True)
    return engine


def make_session(database_url: str) -> Session:
    engine = make_engine(database_url)
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)()


def session_scope(session: Session) -> Iterator[Session]:
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

