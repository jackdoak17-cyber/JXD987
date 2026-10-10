"""Offline lifecycle replays using real discovery, staging and atomic publication.

The PostgreSQL cluster is disposable, Unix-socket-only, and created by this
suite. No supplied database URL or project credentials are used.
"""
from __future__ import annotations

import copy
import getpass
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import tempfile
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock

import psycopg2
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from jxd.models import Base, Fixture, FixturePlayer, FixturePlayerStatistic, FixtureStatistic
from jxd.sync import SyncService
from scripts import postmatch_fixture_detail_delivery as delivery
from scripts.export_to_supabase import atomic_fixture_detail_publish, upsert_fixture_core
from scripts.refresh_fixture_delivery import is_hidden

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = 19745046


@pytest.fixture(scope="module")
def postgres(tmp_path_factory):
    initdb = shutil.which("initdb")
    pg_ctl = shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("Local PostgreSQL binaries are required for disposable lifecycle replay")
    root = tmp_path_factory.mktemp("postponed-pg")
    data = root / "db"
    socket_dir = tempfile.TemporaryDirectory(prefix="jxd-pg-", dir="/tmp")
    sockets = Path(socket_dir.name)
    subprocess.run([initdb, "-D", str(data), "-A", "trust", "--no-locale"],
                   check=True, capture_output=True)
    subprocess.run([pg_ctl, "-D", str(data), "-l", str(root / "postgres.log"),
                    "-o", f"-F -h '' -k {sockets}", "-w", "start"],
                   check=True, capture_output=True)
    url = f"host={sockets} dbname=postgres user={getpass.getuser()}"
    try:
        yield url
    finally:
        subprocess.run([pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop"],
                       check=True, capture_output=True)
        socket_dir.cleanup()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    original = socket.socket.connect

    def connect(sock, address):
        if sock.family != socket.AF_UNIX:
            raise AssertionError("Network access prohibited during lifecycle replay")
        return original(sock, address)

    monkeypatch.setattr(socket.socket, "connect", connect)


@pytest.fixture
def replay(postgres, tmp_path):
    conn = psycopg2.connect(postgres)
    with conn.cursor() as cur:
        cur.execute("drop schema public cascade; create schema public")
        cur.execute("""
          do $$ begin
            if not exists(select from pg_roles where rolname='service_role') then create role service_role; end if;
            if not exists(select from pg_roles where rolname='anon') then create role anon; end if;
            if not exists(select from pg_roles where rolname='authenticated') then create role authenticated; end if;
          end $$;
          create table teams(id bigint primary key);
          create table fixtures(id bigint primary key, home_team_id bigint, away_team_id bigint,
            league_id bigint, season_id bigint, starting_at timestamptz, status text, status_code text,
            home_score int, away_score int);
          create table players(id bigint primary key, name text, display_name text, short_name text,
            common_name text, team_id bigint, team_updated_at timestamptz, image_path text);
          create table fixture_players(fixture_id bigint, player_id bigint, team_id bigint, is_starter bool,
            minutes_played int, position_name text, detailed_position_id bigint, detailed_position_name text,
            detailed_position_code text, formation_field text, lineup_detailed_position_id bigint,
            lineup_detailed_position_name text, lineup_detailed_position_code text, formation_position int,
            position_abbr text, provider_snapshot_id bigint, primary key(fixture_id,player_id));
          create table fixture_statistics(fixture_id bigint, team_id bigint, type_id bigint,
            value numeric, provider_snapshot_id bigint, primary key(fixture_id,team_id,type_id));
          create table fixture_player_statistics(fixture_id bigint, player_id bigint, team_id bigint,
            type_id bigint, value numeric, provider_snapshot_id bigint,
            primary key(fixture_id,player_id,type_id));
          create table fixture_detail_snapshots(id bigint generated always as identity primary key,
            fixture_id bigint, league_id bigint, season_id bigint, payload_hash text, normalized_hash text,
            provider_status text, quality_status text, payload jsonb, release_id text, error text,
            fetched_at timestamptz, last_seen_at timestamptz, accepted_at timestamptz,
            unique(fixture_id,payload_hash));
          create table fixture_stats_quality_exclusions(fixture_id bigint primary key references fixtures(id),
            league_id bigint, season_id bigint, exclusion_type text, reason text, evidence jsonb,
            first_identified_at timestamptz default now(), last_checked_at timestamptz,
            updated_at timestamptz, next_review_at timestamptz);
          create table fixture_detail_delivery_status(fixture_id bigint primary key, status text,
            next_attempt_at timestamptz, updated_at timestamptz, last_attempted_at timestamptz,
            first_seen_at timestamptz, reason_code text, stable_fetch_count int default 0, last_error text,
            next_revalidation_at timestamptz, last_successful_at timestamptz, delivery_contract_version int,
            accepted_snapshot_id bigint, player_stat_parity bool, lineup_parity bool,
            target_player_stat_count int, target_lineup_count int);
          create table projection_replays(fixture_id bigint primary key, calls int);
          create function refresh_player_stats_for_fixture(fid bigint) returns int language plpgsql as $$
          begin
            insert into projection_replays values(fid,1) on conflict(fixture_id) do update set calls=projection_replays.calls+1;
            return 1;
          end $$;
          create table preserved_bets(id bigint primary key, fixture_id bigint, player_id bigint,
            status text, threshold numeric);
          insert into teams values(101),(202);
        """)
    conn.commit()
    for name, kind in (
        ("league_id", "bigint"), ("season_id", "bigint"), ("attempts", "int"),
        ("last_checked_at", "timestamptz"), ("provider_status", "text"), ("provider_finished", "bool"),
        ("provider_team_stat_count", "int"), ("provider_player_stat_count", "int"), ("provider_lineup_count", "int"),
        ("provider_team_stat_types", "jsonb"), ("provider_missing_type_ids", "jsonb"),
        ("provider_player_stat_types", "jsonb"), ("provider_missing_player_type_ids", "jsonb"),
        ("source_snapshot", "jsonb"), ("target_snapshot", "jsonb"), ("release_id", "text"),
        ("last_payload_hash", "text"), ("last_normalized_hash", "text"),
        ("target_team_stat_count", "int"), ("parity_checked_at", "timestamptz"),
    ):
        with conn.cursor() as cur:
            cur.execute(f"alter table fixture_detail_delivery_status add column {name} {kind}")
    conn.commit()
    with conn.cursor() as cur:
        cur.execute((ROOT / "supabase/migrations/20260831191000_fixture_detail_atomic_publish_v2.sql").read_text())
    conn.commit()
    engine = create_engine(f"sqlite:///{tmp_path / 'spool.sqlite'}")
    Base.metadata.create_all(engine)
    client = Mock()
    client.fetch_collection.side_effect = AssertionError("Unconfigured provider mock")
    try:
        yield Replay(conn, postgres, engine, client)
    finally:
        conn.close()
        engine.dispose()


def payload(status="FT", *, fixture_id=FIXTURE, shots=2, complete=True):
    return {
        "id": fixture_id, "league_id": 8, "season_id": 10,
        "starting_at": "2026-10-09 12:00:00", "state": {"short_name": status},
        "participants": [{"id": 101, "name": "Home", "meta": {"location": "home"}},
                         {"id": 202, "name": "Away", "meta": {"location": "away"}}],
        "scores": [{"description": "CURRENT", "type_id": 1525, "participant_id": 101, "score": {"participant": "home", "goals": 1}},
                   {"description": "CURRENT", "type_id": 1525, "participant_id": 202, "score": {"participant": "away", "goals": 0}}],
        "statistics": [{"participant_id": team, "type_id": stat, "data": {"value": 1}}
                       for team in (101, 202) for stat in delivery.TRACKED_TEAM_STAT_TYPES],
        "lineups": [{"team_id": team, "player_id": player, "type_id": 11,
                     "player": {"id": player, "name": f"Player {player}"},
                     "details": [{"type_id": 119, "data": {"value": 90}},
                                 {"type_id": 42, "data": {"value": shots if player == 11 else 0}},
                                 {"type_id": 86, "data": {"value": 1 if player == 11 else 0}}]}
                    for team, players in ((101, range(11, 22)), (202, range(22, 33)))
                    for player in players] if complete else [],
    }


class Replay:
    def __init__(self, conn, url, engine, client):
        self.conn, self.url, self.engine, self.client = conn, url, engine, client

    def sql(self, query, params=()):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchall() if cur.description else []
        self.conn.commit()
        return result

    def discover(self, data):
        self.client.fetch_collection.side_effect = None
        self.client.fetch_collection.return_value = [copy.deepcopy(data)]
        with Session(self.engine) as session:
            assert SyncService(self.client, session).sync_fixtures_between(
                date(2026, 10, 9), date(2026, 10, 9), [8], ["participants", "scores", "state"]) == 1
            fixture = session.get(Fixture, data["id"])
            columns = ("id", "home_team_id", "away_team_id", "league_id", "season_id",
                       "starting_at", "status", "status_code", "home_score", "away_score")
            upsert_fixture_core(self.conn, [{key: getattr(fixture, key) for key in columns}])

    def quarantine(self, fixture_id=FIXTURE):
        self.sql("""insert into fixture_stats_quality_exclusions
          (fixture_id,exclusion_type,evidence,last_checked_at,next_review_at)
          values(%s,'provider_unavailable','{"provider_status":"POST"}',now()-interval '2 days',now()+interval '7 days')""", (fixture_id,))
        self.sql("""insert into fixture_detail_delivery_status
          (fixture_id,status,next_attempt_at,updated_at,first_seen_at)
          values(%s,'excluded',now()+interval '7 days',now(),now())""", (fixture_id,))

    def publish(self, data):
        assessment = delivery.assess_provider_payload(data)
        snapshot_id = delivery.persist_provider_snapshot(self.url, data["id"], 8, 10, data, assessment,
                    delivery.provider_payload_hash(data), delivery.normalized_provider_hash(data))
        if assessment.status != "ready":
            return False
        delivery.store_provider_detail(self.engine, self.client, data["id"], data, assessment)
        self.export(data, snapshot_id)
        delivery.activate_provider_snapshot(self.url, data["id"], snapshot_id)
        with self.engine.connect() as source:
            raw = source.connection.driver_connection
            assert not delivery.compare_snapshots(delivery.source_snapshot(raw, data["id"]),
                                                  delivery.target_snapshot(self.conn, data["id"]))
        return True

    def export(self, data, snapshot_id):
        with Session(self.engine) as session:
            def rows(model):
                return [{column.name: getattr(row, column.name) for column in model.__table__.columns}
                        for row in session.query(model).filter(model.fixture_id == data["id"]).all()]
            atomic_fixture_detail_publish(self.conn, fixture_id=data["id"], snapshot_id=snapshot_id,
                fixture_players=rows(FixturePlayer), fixture_statistics=rows(FixtureStatistic),
                fixture_player_statistics=rows(FixturePlayerStatistic),
                player_dimensions=[{"id": row["player_id"], "name": f"Player {row['player_id']}"} for row in data["lineups"]])

    def shots(self, fixture_id=FIXTURE, player=11):
        return self.sql("select value from fixture_player_statistics where fixture_id=%s and player_id=%s and type_id=42", (fixture_id, player))


def test_normal_fixture_to_played_complete(replay):
    replay.discover(payload("NS"))
    assert not is_hidden({"status": "NS"})
    replay.discover(payload())
    assert replay.publish(payload())
    assert replay.shots() == [(2,)]
    assert replay.shots(player=22) == [(0,)]  # Only an explicit provider zero.


def test_postponed_rescheduled_same_identity_preserves_bets(replay):
    replay.discover(payload("POST"))
    replay.quarantine()
    replay.sql("insert into preserved_bets values(1,%s,11,'pending',1)", (FIXTURE,))
    assert is_hidden({"status": "POST"})
    assert delivery.candidate_target_fixture_ids(replay.url, [8], 10, False) == []
    rescheduled = payload("NS")
    rescheduled["starting_at"] = "2026-10-10 12:00:00"
    replay.discover(rescheduled)
    assert delivery.candidate_target_fixture_ids(replay.url, [8], 10, False) == []
    replay.discover(payload())
    assert delivery.candidate_target_fixture_ids(replay.url, [8], 10, False) == [FIXTURE]
    assert delivery.excluded_target_fixture_ids(replay.url, [FIXTURE]) == set()
    assert replay.publish(payload())
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(0,)]
    assert replay.sql("select fixture_id,status from preserved_bets") == [(FIXTURE, "pending")]
    assert replay.sql("select count(*) from fixtures") == [(1,)]


def test_replacement_id_is_independent_never_transfers_bets(replay):
    replay.discover(payload("POST"))
    replay.quarantine()
    replay.sql("insert into preserved_bets values(1,%s,11,'pending',1)", (FIXTURE,))
    replacement = payload(fixture_id=FIXTURE + 1)
    replay.discover(replacement)
    assert replay.publish(replacement)
    assert replay.shots() == []
    assert replay.shots(FIXTURE + 1) == [(2,)]
    assert replay.sql("select fixture_id,status from preserved_bets") == [(FIXTURE, "pending")]
    assert replay.sql("select fixture_id from fixture_stats_quality_exclusions") == [(FIXTURE,)]
    assert delivery.candidate_target_fixture_ids(replay.url, [8], 10, False) == [FIXTURE + 1]


def test_old_postponement_quarantine_incomplete_then_complete(replay):
    replay.discover(payload())
    replay.quarantine()
    assert not replay.publish(payload(complete=False))
    assert replay.shots() == []
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(1,)]
    assert replay.publish(payload())
    assert replay.shots() == [(2,)]
    assert replay.sql("select quality_status from fixture_detail_snapshots order by id") == [("provider_pending",), ("accepted",)]


def test_revisions_duplicates_and_accepted_snapshot_preservation(replay):
    replay.discover(payload())
    assert replay.publish(payload())
    assert replay.publish(payload())
    assert replay.sql("select count(*) from fixture_detail_snapshots") == [(1,)]
    assert replay.sql("select quality_status from fixture_detail_snapshots") == [("accepted",)]
    assert replay.publish(payload(shots=3))
    assert replay.shots() == [(3,)]
    assert replay.sql("select count(*) from fixture_detail_snapshots where quality_status='accepted'") == [(2,)]
    assert replay.sql("select count(*) from fixture_players") == [(22,)]


def test_missing_player_shots_never_becomes_zero(replay):
    data = payload()
    data["lineups"][0]["details"] = data["lineups"][0]["details"][:1]
    replay.discover(data)
    assert replay.publish(data)
    assert replay.shots() == []
    assert replay.shots(player=22) == [(0,)]


@pytest.mark.parametrize("failure", ["projection", "activation", "replacement"])
def test_recovery_failure_rolls_back_facts_quarantine_and_acceptance(replay, failure):
    replay.discover(payload())
    assert replay.publish(payload(shots=1))
    replay.quarantine()
    if failure == "projection":
        replay.sql("""create or replace function refresh_player_stats_for_fixture(fid bigint) returns int language plpgsql as $$
                      begin raise exception 'injected projection failure'; end $$""")
    else:
        table = "fixture_detail_snapshots" if failure == "activation" else "fixture_player_statistics"
        event = "update" if failure == "activation" else "insert"
        replay.sql(f"""create function fail_recovery() returns trigger language plpgsql as $$
                      begin raise exception 'injected {failure} failure'; end $$;
                      create trigger fail_recovery before {event} on {table} for each row execute function fail_recovery()""")
    with pytest.raises(psycopg2.Error, match=f"injected {failure} failure"):
        replay.publish(payload(shots=3))
    assert replay.shots() == [(1,)]
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(1,)]
    assert replay.sql("select count(*) from fixture_detail_snapshots where quality_status='accepted'") == [(1,)]
    if failure == "projection":
        replay.sql("""create or replace function refresh_player_stats_for_fixture(fid bigint) returns int language sql as $$ select 1 $$""")
    else:
        replay.sql(f"drop trigger fail_recovery on {table}")
    assert replay.publish(payload(shots=3))
    assert replay.shots() == [(3,)]
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(0,)]


@pytest.mark.parametrize("invalid", ["contradiction", "changed_quarantine", "stale", "sparse", "identity"])
def test_invalid_recovery_cannot_replace_or_unquarantine(replay, invalid):
    replay.discover(payload())
    assert replay.publish(payload(shots=1))
    replay.quarantine()
    data = payload(shots=3)
    if invalid == "contradiction":
        data["status"] = "POST"
    elif invalid == "changed_quarantine":
        replay.sql("update fixture_stats_quality_exclusions set exclusion_type='duplicate'")
    elif invalid == "stale":
        replay.sql("update fixture_stats_quality_exclusions set last_checked_at=now()+interval '1 hour'")
    elif invalid == "identity":
        data["participants"][0]["id"] = 999
    elif invalid == "sparse":
        data["statistics"] = data["statistics"][:1]
    if invalid in {"sparse", "identity"}:
        assert not replay.publish(data)
    else:
        with pytest.raises((ValueError, delivery.ProviderDetailIncompleteError)):
            replay.publish(data)
    assert replay.shots() == [(1,)]
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(1,)]


def test_local_queue_rediscovery_and_retry_spacing(replay):
    replay.discover(payload())
    with replay.engine.connect() as connection:
        conn = connection.connection.driver_connection
        delivery.ensure_ledger(conn)
        conn.execute("""insert into fixture_detail_deliveries(fixture_id,status,provider_status,first_seen_at,
                      updated_at,next_attempt_at) values(?,'excluded','POST',datetime('now'),datetime('now'),datetime('now','+7 days'))""", (FIXTURE,))
        conn.commit()
        assert delivery.candidate_fixture_ids(conn, [8], 72, 10) == [FIXTURE]
        conn.execute("update fixture_detail_deliveries set status='provider_pending',stable_fetch_count=0")
        conn.commit()
        assert delivery.candidate_fixture_ids(conn, [8], 72, 10) == []


@pytest.mark.parametrize("sparse", [False, True])
def test_real_queue_command_completes_recovery_and_records_eligibility(replay, monkeypatch, tmp_path, sparse):
    from scripts import reconcile_stats_provider_queue as queue

    replay.discover(payload())
    replay.quarantine()
    data = payload()
    if sparse:
        data["statistics"] = [row for row in data["statistics"] if row["type_id"] not in {83, 85}]
        with sqlite3.connect(replay.engine.url.database) as conn:
            delivery.ensure_ledger(conn)
            conn.execute("""insert into fixture_detail_deliveries(fixture_id,status,provider_status,
                          first_seen_at,updated_at,last_normalized_hash,stable_fetch_count)
                          values(?,'excluded','POST',datetime('now'),datetime('now'),?,3)""",
                         (FIXTURE, delivery.normalized_provider_hash(data)))
    # Only provider transport and the exporter subprocess boundary are replaced;
    # the queue's SQL, ledger, assessment, staging, export and parity are real.
    monkeypatch.setenv("JXD_DB_PATH", str(replay.engine.url.database))
    monkeypatch.setattr(delivery, "SOURCE_DB", str(replay.engine.url.database))
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", replay.url)
    monkeypatch.setattr(queue, "SportMonksClient", lambda: replay.client)
    monkeypatch.setattr(queue, "acquire_process_lock", lambda: 0)
    monkeypatch.setattr(queue, "fetch_provider_fixtures", lambda *args: ({FIXTURE: data}, {}, 1))

    def export_batch(ids, leagues, report_path, snapshot_ids=None):
        assert ids == [FIXTURE]
        replay.export(data, snapshot_ids[FIXTURE])
        return queue.ExportBatchResult((queue.ExportCommandResult.success(ids),))

    monkeypatch.setattr(queue, "export_batch", export_batch)
    replay.sql("""create function refresh_player_stats_season_eligible(league int, season int,
                 player bigint, fixtures bigint[]) returns int language sql as $$ select 1 $$""")
    monkeypatch.setattr(sys, "argv", ["reconcile_stats_provider_queue", "--leagues", "8",
                        "--batch-size", "1", "--max-batches", "1", "--report-json", str(tmp_path / "queue.json")])
    assert queue.main() == 0
    if sparse:
        assert replay.shots() == []
        assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(1,)]
        assert replay.sql("select status,stable_fetch_count from fixture_detail_delivery_status") == [("provider_pending", 1)]
        # Advance the confirmation schedule using isolated test state only.
        with sqlite3.connect(replay.engine.url.database) as conn:
            conn.execute("update fixture_detail_deliveries set next_attempt_at=datetime('now','-1 minute'),last_attempted_at=datetime('now','-16 minutes')")
        replay.sql("update fixture_detail_delivery_status set next_attempt_at=now()-interval '1 minute',last_attempted_at=now()-interval '16 minutes'")
        assert queue.main() == 0
    assert replay.sql("select status,player_stat_parity,lineup_parity from fixture_detail_delivery_status") == [("provider_sparse" if sparse else "verified", True, True)]
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(0,)]
    assert replay.shots() == [(2,)]


def test_postponed_discovery_does_not_replace_existing_staging_detail(replay):
    replay.discover(payload())
    assert replay.publish(payload(shots=3))
    postponed = payload("POST", shots=0)
    with Session(replay.engine) as session:
        service = SyncService(replay.client, session)
        service._store_fixture_raw(postponed, full_detail=True)
        session.commit()
        assert session.get(Fixture, FIXTURE).status == "POST"
        values = session.query(FixturePlayerStatistic.value).filter_by(fixture_id=FIXTURE, player_id=11, type_id=42).all()
        assert values == [(3,)]
        assert session.query(FixturePlayer).filter_by(fixture_id=FIXTURE).count() == 22


def test_reobserved_accepted_revision_can_recover_new_quarantine(replay, monkeypatch):
    replay.discover(payload())
    monkeypatch.setenv("RUNTIME_RELEASE_ID", "original-release")
    assert replay.publish(payload())
    replay.quarantine()
    replay.sql("update fixture_stats_quality_exclusions set last_checked_at=now()")
    monkeypatch.setenv("RUNTIME_RELEASE_ID", "new-release")
    assert replay.publish(payload())
    assert replay.sql("select quality_status,release_id from fixture_detail_snapshots") == [("accepted", "original-release")]
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(0,)]


def test_partial_starting_lineup_cannot_release_quarantine(replay):
    replay.discover(payload())
    replay.quarantine()
    data = payload()
    data["lineups"] = [data["lineups"][0], data["lineups"][-1]]
    with pytest.raises(ValueError, match="complete starting lineups"):
        replay.publish(data)
    assert replay.shots() == []
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(1,)]


def test_postponed_lineup_queue_and_publication_validators_are_consistent(replay, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    from scripts import sync_confirmed_lineups as lineups
    from scripts.validate_moneyline_coverage import HIDDEN_FIXTURE_STATUSES
    from scripts.verify_fixture_delivery_parity import is_hidden as parity_hidden

    class Clock(datetime):
        @classmethod
        def utcnow(cls):
            return datetime(2026, 10, 10, 12)

    monkeypatch.setattr(lineups, "datetime", Clock)
    monkeypatch.setattr(lineups, "get_engine", lambda: replay.engine)
    replay.discover(payload("POST"))
    assert lineups.fetch_candidate_fixture_ids(48, 48, [8], 10) == []
    assert "POST" in HIDDEN_FIXTURE_STATUSES
    assert parity_hidden({"status": "POST"})
    replay.discover(payload("NS"))
    assert lineups.fetch_candidate_fixture_ids(48, 48, [8], 10) == [FIXTURE]


def test_provider_still_postponed_restores_the_normal_discovery_gate(replay):
    replay.discover(payload())
    replay.quarantine()
    assert delivery.candidate_target_fixture_ids(replay.url, [8], 10, False) == [FIXTURE]
    with replay.engine.connect() as connection:
        conn = connection.connection.driver_connection
        conn.row_factory = sqlite3.Row
        delivery.ensure_ledger(conn)
        delivery.ledger_attempt_start(conn, FIXTURE, delivery.utc_now(), 8, 10)
        assessment = delivery.assess_provider_payload(payload("POST"))
        delivery.mark_provider_unavailable(replay.url, conn, FIXTURE, 1, "still postponed",
                                          evidence={"provider_status": "POST"}, assessment=assessment)
        assert delivery.candidate_fixture_ids(conn, [8], 72, 10) == []
    assert delivery.candidate_target_fixture_ids(replay.url, [8], 10, False) == []
    assert replay.sql("select status,home_score,away_score from fixtures") == [("POST", None, None)]


def test_recovery_rejects_invented_statistic_even_if_staged(replay):
    replay.discover(payload())
    replay.quarantine()
    data = payload()
    data["lineups"][0]["details"] = data["lineups"][0]["details"][:1]
    assessment = delivery.assess_provider_payload(data)
    snapshot_id = delivery.persist_provider_snapshot(replay.url, FIXTURE, 8, 10, data, assessment,
                  delivery.provider_payload_hash(data), delivery.normalized_provider_hash(data))
    delivery.store_provider_detail(replay.engine, replay.client, FIXTURE, data, assessment)
    with Session(replay.engine) as session:
        session.add(FixturePlayerStatistic(fixture_id=FIXTURE, player_id=11, team_id=101,
                    type_id=42, code="42", value=0))
        session.commit()
    with pytest.raises(ValueError, match="differs from the captured provider"):
        replay.export(data, snapshot_id)
    assert replay.shots() == []
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(1,)]


def test_missing_postponement_provenance_does_not_bypass_retry_gate(replay):
    replay.discover(payload())
    replay.quarantine()
    replay.sql("update fixture_stats_quality_exclusions set evidence='{}'")
    assert delivery.candidate_target_fixture_ids(replay.url, [8], 10, False) == []
    assert delivery.excluded_target_fixture_ids(replay.url, [FIXTURE]) == {FIXTURE}


def test_publication_holds_quarantine_identity_until_commit(replay, monkeypatch):
    from scripts import export_to_supabase as exporter

    replay.discover(payload())
    replay.quarantine()
    locked, finish = threading.Event(), threading.Event()
    original = exporter.lock_fixture_recovery
    errors = []

    def pause_under_actual_locks(*args, **kwargs):
        result = original(*args, **kwargs)
        locked.set()
        assert finish.wait(5)
        return result

    def worker():
        try:
            assert replay.publish(payload())
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(exporter, "lock_fixture_recovery", pause_under_actual_locks)
    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert locked.wait(5)
        with psycopg2.connect(replay.url) as competing:
            with competing.cursor() as cur:
                cur.execute("set local lock_timeout='150ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    cur.execute("update fixture_stats_quality_exclusions set exclusion_type='duplicate' where fixture_id=%s", (FIXTURE,))
            competing.rollback()
    finally:
        finish.set()
        thread.join(10)
    assert not thread.is_alive() and not errors
    assert replay.shots() == [(2,)]
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(0,)]


def test_fixture_core_cannot_fall_back_to_unguarded_rest(monkeypatch):
    from scripts import export_to_supabase as exporter

    monkeypatch.setattr(exporter, "SUPABASE_DB_URL", None)
    monkeypatch.setattr(exporter, "filter_rows_for_remote_schema", lambda table, rows: rows)
    with pytest.raises(RuntimeError, match="requires PostgreSQL"):
        exporter.upsert_table("fixtures", [{"id": FIXTURE, "home_score": 1, "away_score": 0}], "id", False)
    assert exporter.upsert_table("fixtures", [{"id": FIXTURE}], "id", True) == (1, 0)


def test_core_batch_preserves_normal_results_and_withholds_only_recovery(replay):
    replay.discover(payload())
    replay.quarantine()
    replacement = payload(fixture_id=FIXTURE + 1)
    replay.discover(replacement)
    rows = [{"id": fixture_id, "status": "FT", "status_code": "FT", "home_score": 2, "away_score": 1}
            for fixture_id in (FIXTURE + 1, FIXTURE)]
    upsert_fixture_core(replay.conn, rows)
    assert replay.sql("select id,home_score,away_score from fixtures order by id") == [(FIXTURE, None, None), (FIXTURE + 1, 2, 1)]
    assert rows[1]["home_score"] == 2  # No mutation of source evidence.


@pytest.mark.parametrize("invalid", ["decimal_team", "foreign_lineup", "decimal_stat_type"])
def test_recovery_requires_exact_detail_identities(replay, invalid):
    replay.discover(payload())
    replay.quarantine()
    data = payload()
    if invalid == "decimal_team":
        data["lineups"][0]["team_id"] = 101.0
    elif invalid == "foreign_lineup":
        data["lineups"][0]["fixture_id"] = FIXTURE + 1
    else:
        data["lineups"][0]["details"][1]["type_id"] = 42.0
    with pytest.raises(ValueError, match="identity"):
        replay.publish(data)
    assert replay.shots() == []
    assert replay.sql("select count(*) from fixture_stats_quality_exclusions") == [(1,)]
