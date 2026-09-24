from __future__ import annotations

from datetime import datetime
import fcntl
import os
from pathlib import Path
import sqlite3
import subprocess
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jxd.models import (
    Base,
    Fixture,
    FixturePlayer,
    Player,
    PlayerTeamHistory,
    Team,
    TeamSquadMembership,
    TeamSquadSnapshot,
)
from jxd import shared_writer_lock
from jxd.shared_writer_lock import SharedWriterLockUnavailable, release_shared_writer_lock
from jxd.sync import SyncService
from scripts import reconcile_stats_provider_queue as stats_queue
from scripts import sync_sparse_squads as squad_sync
from scripts.reconcile_stats_provider_queue import (
    fetch_and_assess_provider_fixtures_without_shared_lock as fetch_provider_batch,
)


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


def test_normal_reacquire_respects_settlement_priority_gate(
    tmp_path, monkeypatch
):
    lock_path = tmp_path / "shared.lock"
    settlement_path = tmp_path / "settlement.lock"
    writer_fd = _hold_lock(lock_path, monkeypatch)
    settlement_fd = os.open(settlement_path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(settlement_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    monkeypatch.setenv(
        "ODDS_SYNC_SETTLEMENT_PRIORITY_FILE", str(settlement_path)
    )
    monkeypatch.setenv("ODDS_SYNC_JOB_PRIORITY", "normal")
    try:
        fcntl.flock(writer_fd, fcntl.LOCK_UN)
        assert not shared_writer_lock._try_reacquire_shared_writer_lock(
            writer_fd
        )
        assert not _is_locked(lock_path)
        fcntl.flock(settlement_fd, fcntl.LOCK_UN)
        assert shared_writer_lock._try_reacquire_shared_writer_lock(writer_fd)
        assert _is_locked(lock_path)
    finally:
        try:
            fcntl.flock(settlement_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(settlement_fd)
        os.close(writer_fd)


def test_normal_wrapper_skips_while_settlement_priority_is_active(tmp_path):
    lock_path = tmp_path / "shared.lock"
    settlement_path = tmp_path / "settlement.lock"
    settlement_fd = os.open(settlement_path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(settlement_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    environment = os.environ.copy()
    environment.update(
        {
            "ODDS_SYNC_LOCK_FILE": str(lock_path),
            "ODDS_SYNC_SETTLEMENT_PRIORITY_FILE": str(settlement_path),
            "ODDS_SYNC_JOB_PRIORITY": "normal",
            "ODDS_SYNC_LIVE_SCHEDULE_ENABLED": "false",
            "ODDS_SYNC_P3_MAX_DURATION_SECONDS": "5",
        }
    )
    common_path = Path(__file__).parents[1] / "scripts/vps/common.sh"
    try:
        result = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; run_with_global_lock_and_timeout "true"',
                "test",
                str(common_path),
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 2
        assert "settlement shared-lock priority is active" in result.stdout
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

            class Payload(dict):
                def get(self, key, default=None):
                    assert not _is_locked(lock_path)
                    return super().get(key, default)

            return Payload(data={"id": 701})

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


@pytest.mark.parametrize("changed_state", ["player", "player_team_history"])
def test_settlement_discards_provider_response_if_player_state_changes(
    tmp_path, monkeypatch, changed_state
):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    engine, session = _sqlite_session(tmp_path / "jxd.sqlite")
    session.add_all(
        [
            Team(id=42, name="Home"),
            Fixture(
                id=702,
                status="FT",
                home_score=0,
                away_score=0,
                home_team_id=42,
            ),
            Player(id=83, name="Before", team_id=42),
            FixturePlayer(
                fixture_id=702, player_id=83, team_id=42, name="Before"
            ),
            PlayerTeamHistory(
                player_id=83,
                team_id=42,
                source="test",
                effective_from=datetime(2025, 1, 1),
            ),
        ]
    )
    session.commit()

    class Client:
        def request(self, method, endpoint, params=None):
            assert not _is_locked(lock_path)
            if changed_state == "player":
                session.get(Player, 83).extra = {"during_fetch": True}
            else:
                history = (
                    session.query(PlayerTeamHistory)
                    .filter_by(player_id=83)
                    .one()
                )
                history.effective_to = datetime(2025, 6, 1)
            session.commit()
            return {"data": {"id": 702, "home_score": 2}}

    service = SyncService(Client(), session)
    service.ensure_schema()
    applied = []
    service._store_fixture_raw = lambda *args, **kwargs: applied.append(args[0])
    try:
        assert service.reconcile_fixtures([702], includes=["statistics"]) == 0
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

            class UnlockedMapping(dict):
                def get(self, key, default=None):
                    assert not _is_locked(lock_path)
                    return super().get(key, default)

            return [
                UnlockedMapping(
                    player=UnlockedMapping(id=81, name="Player 81"),
                    start="2026-01-01",
                )
            ]

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


def test_squad_only_serving_table_writes_yield_shared_lock(
    tmp_path, monkeypatch
):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    calls = []

    def upsert(table, rows, conflict_columns, dry_run):
        assert not _is_locked(lock_path)
        calls.append((table, conflict_columns))
        return len(rows), []

    def deactivate(team_ids, memberships, dry_run):
        assert not _is_locked(lock_path)
        calls.append(("deactivate", tuple(team_ids)))
        return {"42": 1}

    monkeypatch.setattr(squad_sync, "upsert_table", upsert)
    monkeypatch.setattr(
        squad_sync,
        "deactivate_remote_squad_memberships_missing",
        deactivate,
    )
    try:
        result = squad_sync.export_independent_squad_tables(
            [42],
            [{"id": 7}],
            [{"team_id": 42, "player_id": 81, "is_active": True}],
            False,
        )
        assert _is_locked(lock_path)
        assert result == (1, 1, {"42": 1})
        assert calls == [
            ("team_squad_snapshots", "id"),
            ("team_squad_memberships", "team_id,player_id"),
            ("deactivate", (42,)),
        ]
    finally:
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


def test_squad_discards_response_if_incoming_player_assignment_changes_during_fetch(
    tmp_path, monkeypatch
):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)
    engine, session = _sqlite_session(tmp_path / "jxd.sqlite")
    session.add_all(
        [
            Team(id=7, name="Prior team"),
            Team(id=42, name="Requested team"),
            Player(id=83, name="Transferred player", team_id=7),
        ]
    )
    session.add(
        TeamSquadMembership(
            team_id=7,
            player_id=83,
            is_active=True,
            first_seen_at=datetime(2025, 1, 1),
            last_seen_at=datetime(2026, 1, 1),
            provider_started_at=datetime(2025, 1, 1),
            source="sportmonks",
        )
    )
    session.add(
        PlayerTeamHistory(
            player_id=83,
            team_id=7,
            source="test",
            effective_from=datetime(2025, 1, 1),
        )
    )
    session.commit()

    class Client:
        def fetch_collection(self, endpoint, includes=None, per_page=200):
            assert not _is_locked(lock_path)
            membership = session.get(TeamSquadMembership, (7, 83))
            membership.last_seen_at = datetime(2026, 9, 24)
            session.add(
                PlayerTeamHistory(
                    player_id=83,
                    team_id=7,
                    source="concurrent-transfer",
                    effective_from=datetime(2026, 9, 24),
                )
            )
            session.commit()
            return [{"player": {"id": 83, "name": "Transferred player"}, "start": "2025-01-01"}]

    service = SyncService(Client(), session)
    service.ensure_schema()
    try:
        assert service.sync_squads_for_teams([42]) == 0
        assert service.skipped_stale_squad_team_ids == [42]
        assert session.get(TeamSquadMembership, (42, 83)) is None
        assert session.get(TeamSquadMembership, (7, 83)).is_active
        assert session.get(Player, 83).team_id == 7
    finally:
        session.close()
        engine.dispose()
        os.close(fd)


def test_stats_provider_fetch_runs_unlocked_and_due_recheck_runs_locked(
    tmp_path, monkeypatch
):
    lock_path = tmp_path / "shared.lock"
    fd = _hold_lock(lock_path, monkeypatch)

    def fake_fetch(fixture_ids, fetch_concurrency, bulk_size):
        assert not _is_locked(lock_path)
        return ({fixture_id: {"id": fixture_id} for fixture_id in fixture_ids}, {}, 1)

    def fake_assess(data):
        assert not _is_locked(lock_path)
        return "assessment"

    def fake_hash(data):
        assert not _is_locked(lock_path)
        return "hash"

    def fake_due_candidates(fixture_ids):
        assert _is_locked(lock_path)
        assert fixture_ids == [1]
        return [1]

    try:
        with (
            patch.object(
                stats_queue,
                "fetch_provider_fixtures",
                side_effect=fake_fetch,
            ),
            patch.object(
                stats_queue,
                "assess_provider_payload",
                side_effect=fake_assess,
            ),
            patch.object(
                stats_queue,
                "provider_payload_hash",
                side_effect=fake_hash,
            ),
            patch.object(
                stats_queue,
                "normalized_provider_hash",
                side_effect=fake_hash,
            ),
        ):
            fetched, errors, calls, assessments, still_due = (
                fetch_provider_batch([1], 2, 50, fake_due_candidates)
            )
        assert fetched == {1: {"id": 1}}
        assert errors == {}
        assert calls == 1
        assert assessments == {1: ("assessment", "hash", "hash")}
        assert still_due == {1}
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


def test_stats_source_fingerprint_covers_player_metadata_and_team_history(
    tmp_path,
):
    db_path = tmp_path / "jxd.sqlite"
    engine, session = _sqlite_session(db_path)
    session.add_all(
        [
            Team(id=42, name="Home"),
            Fixture(id=704, status="FT", home_team_id=42),
            Player(
                id=84,
                name="Player",
                team_id=42,
                extra={"source": "before"},
            ),
            FixturePlayer(fixture_id=704, player_id=84, team_id=42),
            PlayerTeamHistory(
                player_id=84,
                team_id=42,
                source="test",
                effective_from=datetime(2025, 1, 1),
            ),
        ]
    )
    session.commit()
    conn = sqlite3.connect(db_path)
    try:
        before = stats_queue.fixture_source_fingerprint(conn, 704)
        conn.execute(
            "update fixtures set extra = ? where id = ?",
            ('{"source":"changed"}', 704),
        )
        conn.commit()
        fixture_changed = stats_queue.fixture_source_fingerprint(conn, 704)
        assert fixture_changed != before

        conn.execute(
            "update players set extra = ? where id = ?",
            ('{"source":"after"}', 84),
        )
        conn.commit()
        player_changed = stats_queue.fixture_source_fingerprint(conn, 704)
        assert player_changed != fixture_changed

        conn.execute(
            "insert into player_team_history "
            "(player_id, team_id, source, effective_from, "
            "created_at, updated_at) "
            "values (?, ?, ?, ?, ?, ?)",
            (
                84,
                42,
                "test",
                "2026-01-01 00:00:00",
                "2026-01-01 00:00:00",
                "2026-01-01 00:00:00",
            ),
        )
        conn.commit()
        history_changed = stats_queue.fixture_source_fingerprint(conn, 704)
        assert history_changed != player_changed
    finally:
        conn.close()
        session.close()
        engine.dispose()


def test_stats_stale_provider_batch_hands_off_after_safe_survivors():
    stale = [
        {
            "fixture_id": 1,
            "reason": "shared source state changed during provider fetch",
        }
    ]
    assert stats_queue.should_handoff_after_stale_provider_batch(stale)
    assert not stats_queue.should_handoff_after_stale_provider_batch([])
