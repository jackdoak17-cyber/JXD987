import sqlite3

from scripts.export_to_supabase import fetch_players
from scripts.sync_sparse_squads import select_team_batch


def test_select_team_batch_is_bounded_and_resumable():
    team_ids = [11, 22, 33, 44, 55]

    assert select_team_batch(team_ids, offset=0, max_teams=2) == [11, 22]
    assert select_team_batch(team_ids, offset=2, max_teams=2) == [33, 44]
    assert select_team_batch(team_ids, offset=4, max_teams=2) == [55]


def test_select_team_batch_zero_max_means_all_remaining():
    assert select_team_batch([11, 22, 33], offset=1, max_teams=0) == [22, 33]


def test_select_team_batch_rejects_invalid_bounds():
    for offset, max_teams in [(-1, 1), (0, -1)]:
        try:
            select_team_batch([11], offset=offset, max_teams=max_teams)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid batch bounds must raise ValueError")


def test_player_export_uses_latest_active_squad_assignment():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        create table players (
            id integer primary key,
            name text,
            display_name text,
            short_name text,
            common_name text,
            team_id integer,
            team_updated_at text,
            image_path text
        );
        create table team_squad_memberships (
            team_id integer,
            player_id integer,
            is_active integer,
            provider_started_at text,
            last_seen_at text,
            last_snapshot_id integer
        );
        insert into players values (1, 'Player One', null, null, null, 999, 'old', null);
        insert into players values (2, 'Player Two', null, null, null, 888, 'old', null);
        insert into team_squad_memberships values (42, 1, 1, '2026-08-01', 'newer', 2);
        insert into team_squad_memberships values (7, 1, 1, '2026-07-01', 'older', 1);
        """
    )

    rows = {row["id"]: row for row in fetch_players(conn, [1, 2])}

    assert rows[1]["team_id"] == 42
    assert rows[1]["team_updated_at"] == "newer"
    assert rows[2]["team_id"] is None

    conn.close()
