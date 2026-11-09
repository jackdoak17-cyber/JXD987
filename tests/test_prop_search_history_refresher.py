"""Database-backed bounds for the unscheduled Prop Search refresh candidate."""

from __future__ import annotations

import os
import pwd
import shutil
import subprocess
import tempfile
from pathlib import Path

import psycopg2
import pytest

from scripts import refresh_prop_search_history as worker


@pytest.fixture(scope="module")
def local_database():
    if not shutil.which("initdb") or not shutil.which("pg_ctl"):
        pytest.skip("PostgreSQL server binaries are unavailable")
    with tempfile.TemporaryDirectory(prefix="oddssearch-prop-refresh-") as root:
        path = Path(root)
        data = path / "data"
        socket = path / "socket"
        socket.mkdir()
        username = pwd.getpwuid(os.getuid()).pw_name
        port = 55438
        subprocess.run(
            ["initdb", "-D", str(data), "-A", "trust", "-U", username],
            check=True, capture_output=True, text=True,
        )
        subprocess.run(
            ["pg_ctl", "-D", str(data), "-o", f"-k {socket} -h '' -p {port}",
             "-w", "start"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            conn = psycopg2.connect(
                dbname="postgres", user=username, host=str(socket), port=port
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute("select inet_server_addr() is null, current_setting('data_directory')")
                    socket_only, actual_data = cur.fetchone()
                    assert socket_only and actual_data == str(data)
                    cur.execute(
                        """create schema prop_search_history_v1;
                        create table public.fixtures(id bigint primary key);
                        create table public.fixture_player_statistics(fixture_id bigint);
                        create table public.fixture_players(fixture_id bigint);
                        create table prop_search_history_v1.dirty_fixtures(
                          fixture_id bigint primary key,
                          last_dirty_at timestamptz default clock_timestamp()
                        );
                        create table prop_search_history_v1.refreshed_fixtures(
                          fixture_id bigint primary key
                        );
                        create table prop_search_history_v1.readiness(
                          singleton boolean primary key, backfill_verified boolean
                        );
                        insert into public.fixtures values (1),(2),(3);
                        insert into public.fixture_player_statistics values (1),(2),(3);
                        insert into public.fixture_players values (1),(2),(3);
                        insert into prop_search_history_v1.dirty_fixtures(fixture_id)
                          values (2);
                        create function prop_search_history_v1.refresh_fixture(bigint)
                        returns integer language plpgsql as $$
                        begin
                          insert into prop_search_history_v1.refreshed_fixtures
                            values ($1)
                            on conflict do nothing;
                          delete from prop_search_history_v1.dirty_fixtures
                            where fixture_id=$1;
                          return (select count(*)::integer from public.fixture_players
                                  where fixture_id=$1);
                        end $$;"""
                    )
            yield conn
            conn.close()
        finally:
            subprocess.run(
                ["pg_ctl", "-D", str(data), "-m", "fast", "stop"],
                check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )


def arguments(*parts: str):
    return worker.parser().parse_args(list(parts))


def function_md5(conn) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "select md5(pg_get_functiondef(" 
            "to_regprocedure('prop_search_history_v1.refresh_fixture(bigint)')))"
        )
        value = cur.fetchone()[0]
    conn.rollback()
    return value


def test_argument_bounds_reject_unbudgeted_apply():
    with pytest.raises(ValueError, match="max-wal-bytes"):
        worker.validate_args(arguments("--apply"))
    with pytest.raises(ValueError, match="max-fixtures"):
        worker.validate_args(arguments("--max-fixtures", "21"))
    with pytest.raises(ValueError, match="max-seconds"):
        worker.validate_args(arguments("--max-seconds", "61"))


def test_dry_run_is_read_only_and_hash_mismatch_fails(local_database):
    conn = local_database
    report = worker.run(arguments(), conn)
    assert report["status"] == "dry_run"
    assert (report["dirty_before"], report["unrefreshed_before"]) == (1, 3)
    with conn.cursor() as cur:
        cur.execute("select count(*) from prop_search_history_v1.refreshed_fixtures")
        assert cur.fetchone()[0] == 0
    conn.rollback()
    with pytest.raises(RuntimeError, match="expected SQL revision"):
        worker.run(arguments("--apply", "--max-wal-bytes", "1000000",
                             "--expected-refresh-md5", "0" * 32), conn)


def test_apply_prioritizes_dirty_and_stops_at_fixture_ceiling(local_database):
    conn = local_database
    args = arguments(
        "--apply", "--max-fixtures", "2", "--max-wal-bytes", "1000000",
        "--expected-refresh-md5", function_md5(conn),
    )
    report = worker.run(args, conn)
    assert report["status"] == "fixture_ceiling"
    assert report["processed_fixtures"] == 2
    assert report["appearance_rows"] == 2
    assert report["source_kinds"] == {"dirty": 1, "backfill": 1}
    assert 0 < report["accounted_wal_bytes"] <= 1000000
    with conn.cursor() as cur:
        cur.execute("select fixture_id from prop_search_history_v1.refreshed_fixtures order by 1")
        assert [row[0] for row in cur.fetchall()] == [1, 2]
        cur.execute("select count(*) from prop_search_history_v1.dirty_fixtures")
        assert cur.fetchone()[0] == 0
    conn.rollback()


def test_wal_ceiling_rolls_back_selected_fixture(local_database):
    conn = local_database
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "insert into prop_search_history_v1.dirty_fixtures(fixture_id) values (3)"
            )
    report = worker.run(
        arguments("--apply", "--max-wal-bytes", "8193",
                  "--expected-refresh-md5", function_md5(conn)), conn,
    )
    assert report["status"] == "guard_stopped"
    assert report["processed_fixtures"] == 0
    with conn.cursor() as cur:
        cur.execute("select count(*) from prop_search_history_v1.dirty_fixtures where fixture_id=3")
        assert cur.fetchone()[0] == 1
        cur.execute("select count(*) from prop_search_history_v1.refreshed_fixtures where fixture_id=3")
        assert cur.fetchone()[0] == 0
    conn.rollback()


def test_session_lock_defers_a_second_worker(local_database):
    conn = local_database
    with psycopg2.connect(conn.dsn) as holder:
        assert worker.acquire_lock(holder)
        report = worker.run(arguments(), conn)
        assert report["status"] == "busy"
        worker.release_lock(holder)
    with conn.cursor() as cur:
        cur.execute("select count(*) from prop_search_history_v1.dirty_fixtures where fixture_id=3")
        assert cur.fetchone()[0] == 1
    conn.rollback()


def test_statistic_row_ceiling_preserves_dirty_work(local_database):
    conn = local_database
    with conn:
        with conn.cursor() as cur:
            cur.execute("insert into public.fixture_player_statistics values (3)")
    report = worker.run(
        arguments("--apply", "--max-stats-per-fixture", "1",
                  "--max-wal-bytes", "1000000",
                  "--expected-refresh-md5", function_md5(conn)), conn,
    )
    assert report["status"] == "guard_stopped"
    assert report["processed_fixtures"] == 0
    with conn.cursor() as cur:
        cur.execute("select count(*) from prop_search_history_v1.dirty_fixtures where fixture_id=3")
        assert cur.fetchone()[0] == 1
    conn.rollback()
