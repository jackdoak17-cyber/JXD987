from __future__ import annotations

from datetime import datetime
import fcntl
import os
from pathlib import Path
import sqlite3
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jxd.models import Base, Fixture, Player, TeamSquadMembership, TeamSquadSnapshot
from jxd import shared_writer_lock
from jxd.shared_writer_lock import SharedWriterLockUnavailable, release_shared_writer_lock
from jxd.sync import SyncService
from scripts import reconcile_stats_provider_queue as stats_queue


def _is_locked(path: Path) -> bool:
    fd = os.open(path, os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _hold_lock(path: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    monkeypatch.setenv("ODDS_SYNC_LOCK_FD", str(fd))
    return fd


def _sqlite_session(path: Path):
    engine = create_engine(f"sqlite:///{path}", future=True)
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine, future=True)()


def test_release_shared_writer_lock_only_yields_inside_context(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    try:
        assert _is_locked(lock_path)
        with release_shared_writer_lock():
            assert not _is_locked(lock_path)
        assert _is_locked(lock_path)
    finally:
        os.close(fd)


def test_failed_reacquire_is_a_controlled_handoff(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    monkeypatch.setenv("ODDS_SYNC_LOCK_REACQUIRE_WAIT_SECONDS", "0")
    competitor_fd = None
    try:
        with pytest.raises(SharedWriterLockUnavailable) as error:
            with release_shared_writer_lock():
                competitor_fd = os.open(lock_path, os.O_RDWR)
                fcntl.flock(competitor_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert error.value.code == 75
    finally:
        if competitor_fd is not None:
            fcntl.flock(competitor_fd, fcntl.LOCK_UN)
            os.close(competitor_fd)
        os.close(fd)


def test_zero_wait_still_reacquires_an_available_lock(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    monkeypatch.setenv("ODDS_SYNC_LOCK_REACQUIRE_WAIT_SECONDS", "0")
    try:
        with release_shared_writer_lock():
            assert not _is_locked(lock_path)
        assert _is_locked(lock_path)
    finally:
        os.close(fd)


def test_normal_writer_recognizes_active_settlement_lock(tmp_path, monkeypatch):
    settlement_path = tmp_path / "settlement.lock"
    settlement_fd = os.open(settlement_path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(settlement_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    monkeypatch.setenv("ODDS_SYNC_SETTLEMENT_PRIORITY_FILE", str(settlement_path))
    monkeypatch.setenv("ODDS_SYNC_JOB_PRIORITY", "normal")
    try:
        assert shared_writer_lock._settlement_writer_is_waiting()
        monkeypatch.setenv("ODDS_SYNC_JOB_PRIORITY", "settlement")
        assert not shared_writer_lock._settlement_writer_is_waiting()
    finally:
        fcntl.flock(settlement_fd, fcntl.LOCK_UN)
        os.close(settlement_fd)


def test_settlement_provider_fetch_is_unlocked_but_apply_remains_locked(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    engine, session = _sqlite_session(tmp_path / "jxd.sqlite")
    session.add(Fixture(id=701, status="FT", home_score=0, away_score=0))
    session.commit()

    class Client:
        def request(self, method, endpoint, params=None):
            assert not _is_locked(lock_path)
            return {"data": {"id": 701}}

    service = SyncService(Client(), session)
    service.ensure_schema()

    def apply(*args, **kwargs):
        assert _is_locked(lock_path)

    service._store_fixture_raw = apply
    try:
        assert service.reconcile_fixtures([701], includes=["participants", "scores", "state"]) == 1
    finally:
        session.close()
        engine.dispose()
        os.close(fd)


def test_settlement_discards_provider_response_if_source_changed_during_fetch(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    engine, session = _sqlite_session(tmp_path / "jxd.sqlite")
    session.add(Fixture(id=702, status="FT", home_score=0, away_score=0))
    session.commit()

    class Client:
        def request(self, method, endpoint, params=None):
            assert not _is_locked(lock_path)
            row = session.get(Fixture, 702)
            row.home_score = 1
            session.commit()
            return {"data": {"id": 702, "home_score": 2}}

    service = SyncService(Client(), session)
    service.ensure_schema()
    applied = []
    service._store_fixture_raw = lambda *args, **kwargs: applied.append(args[0])
    try:
        assert service.reconcile_fixtures([702], includes=["participants", "scores", "state"]) == 0
        assert applied == []
    finally:
        session.close()
        engine.dispose()
        os.close(fd)


def test_squad_provider_fetch_is_unlocked_but_mutation_remains_locked(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    engine, session = _sqlite_session(tmp_path / "jxd.sqlite")
    session.add(Player(id=81, name="Before"))
    session.commit()

    class Client:
        def fetch_collection(self, endpoint, includes=None, per_page=200):
            assert not _is_locked(lock_path)
            return [{"player": {"id": 81, "name": "Player 81"}, "start": "2026-01-01"}]

    service = SyncService(Client(), session)
    service.ensure_schema()
    original_track = service._track_player_team_history

    def track_while_locked(player_id, team_id, sync_run_at):
        assert _is_locked(lock_path)
        original_track(player_id, team_id, sync_run_at)

    service._track_player_team_history = track_while_locked
    try:
        assert service.sync_squads_for_teams([42]) == 1
        assert session.get(TeamSquadMembership, (42, 81)).is_active
    finally:
        session.close()
        engine.dispose()
        os.close(fd)


def test_squad_discards_response_if_team_state_changes_during_fetch(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    engine, session = _sqlite_session(tmp_path / "jxd.sqlite")

    class Client:
        def fetch_collection(self, endpoint, includes=None, per_page=200):
            assert not _is_locked(lock_path)
            session.add(
                TeamSquadSnapshot(
                    team_id=42,
                    source="sportmonks",
                    status="success",
                    observed_at=datetime(2026, 9, 24),
                    player_count=1,
                    payload_hash="concurrent-state",
                )
            )
            session.commit()
            return [{"player": {"id": 82, "name": "Player 82"}, "start": "2026-01-01"}]

    service = SyncService(Client(), session)
    service.ensure_schema()
    try:
        assert service.sync_squads_for_teams([42]) == 0
        assert service.skipped_stale_squad_team_ids == [42]
        assert session.get(TeamSquadMembership, (42, 82)) is None
    finally:
        session.close()
        engine.dispose()
        os.close(fd)


def test_stats_provider_fetch_runs_outside_shared_writer_lock(tmp_path, monkeypatch):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)

    def fake_fetch(fixture_ids, fetch_concurrency, bulk_size):
        assert not _is_locked(lock_path)
        return ({fixture_id: {"id": fixture_id} for fixture_id in fixture_ids}, {}, 1)

    try:
        with patch.object(stats_queue, "fetch_provider_fixtures", side_effect=fake_fetch):
            fetched, errors, calls = stats_queue.fetch_provider_fixtures_without_shared_lock([1], 2, 50)
        assert fetched == {1: {"id": 1}}
        assert errors == {}
        assert calls == 1
        assert _is_locked(lock_path)
    finally:
        os.close(fd)


def test_stats_source_fingerprint_changes_when_fixture_detail_changes(tmp_path):
    db_path = tmp_path / "jxd.sqlite"
    engine, session = _sqlite_session(db_path)
    session.add(Fixture(id=703, status="FT", home_score=0, away_score=0))
    session.commit()
    conn = sqlite3.connect(db_path)
    try:
        before = stats_queue.fixture_source_fingerprint(conn, 703)
        conn.execute(
            "insert into fixture_statistics(fixture_id,team_id,type_id,code,name,location,value) "
            "values(703,42,1,'shots','Shots','home',4)"
        )
        conn.commit()
        after = stats_queue.fixture_source_fingerprint(conn, 703)
        assert after != before
        eligible, discarded = stats_queue.revalidate_provider_fetch(
            conn,
            [703],
            {703: before},
            {703},
        )
        assert eligible == []
        assert discarded == [
            {"fixture_id": 703, "reason": "shared source state changed during provider fetch"}
        ]
    finally:
        conn.close()
        session.close()
        engine.dispose()
