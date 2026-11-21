"""Compare the player-stats read rewrite with its shipped SQL on disposable Postgres."""

from pathlib import Path
import os
import shutil
import socket
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
ORIGINAL = ROOT / "supabase/migrations/20260824_player_stats_read_model.sql"
CANDIDATE = ROOT / "supabase/migrations/20261121232000_player_stats_season_page_before_metrics.sql"


def original_function():
    script = ORIGINAL.read_text()
    start = script.index("create or replace function public.player_stats_season_table(")
    end = script.index("\n$$;", start) + len("\n$$;")
    return script[start:end]


SCHEMA = """
create table public.players (
  id bigint primary key, name text, display_name text, common_name text,
  short_name text, image_path text
);
create table public.teams (
  id bigint primary key, name text, short_code text, image_path text
);
create table public.player_stats_metric_contract (
  metric_key text primary key, supports_per90 boolean not null
);
create table public.player_stats_season_summary (
  league_id bigint, season_id bigint, player_id bigint, team_id bigint,
  position_group text, appearances integer, starts integer, minutes numeric,
  last_appearance_at timestamptz
);
create table public.player_stats_season_metric (
  league_id bigint, season_id bigint, player_id bigint, team_id bigint,
  metric_key text, total_value numeric, eligible_appearances integer,
  reported_appearances integer, zero_safe_appearances integer,
  unknown_appearances integer, not_applicable_appearances integer
);
create index on public.player_stats_season_metric
  (league_id, season_id, player_id, team_id, metric_key);
create index on public.player_stats_season_metric
  (league_id, season_id, metric_key, player_id);
insert into public.teams values
  (10, 'Team A', 'A', null), (20, 'Team B', 'B', null);
insert into public.player_stats_metric_contract values
  ('goals', true), ('assists', true), ('rating', false);
insert into public.players
select n, 'Player ' || n,
       case when n % 12 = 0 then 'Special ' || n else null end,
       null, null, null
from generate_series(1, 120) n;
insert into public.player_stats_season_summary
select 8, 28, n, 10,
       case when n % 3 = 0 then 'DEF' else 'FWD' end,
       n % 8 + 1, n % 6, (n % 8 + 1) * 90,
       '2026-11-01'::timestamptz + n * interval '1 minute'
from generate_series(1, 120) n;
insert into public.player_stats_season_summary
select 8, 28, n, 20, 'FWD', 2, 1, 150,
       '2026-11-05'::timestamptz + n * interval '1 minute'
from generate_series(10, 120, 10) n;
insert into public.player_stats_season_metric
select 8, 28, s.player_id, s.team_id, k.metric_key,
       case when k.metric_key = 'rating' then (s.player_id % 6 + 5)::numeric
            when k.metric_key = 'goals' then (s.player_id % 5)::numeric
            else (s.player_id % 7)::numeric end,
       s.appearances,
       case when s.player_id % 11 = 0 then 0 else s.appearances end,
       case when s.player_id % 11 = 0 then 0 else 0 end,
       case when s.player_id % 11 = 0 then 1 else 0 end,
       0
from public.player_stats_season_summary s
cross join public.player_stats_metric_contract k
where k.metric_key <> 'goals' or s.player_id % 9 <> 0;
-- A player with no contracted metric must remain omitted.
delete from public.player_stats_season_metric where player_id = 119;
update public.player_stats_season_metric
set reported_appearances = 0, zero_safe_appearances = eligible_appearances
where metric_key = 'assists' and player_id % 13 = 0;
update public.player_stats_season_metric
set eligible_appearances = 0, reported_appearances = 0
where metric_key = 'rating' and player_id % 17 = 0;
"""


CASES = [
    "p_sort_key => 'goals', p_sort_measure => 'total'",
    "p_sort_key => 'goals', p_sort_measure => 'per90'",
    "p_sort_key => 'assists', p_sort_measure => 'average'",
    "p_sort_key => 'rating', p_sort_measure => 'per90'",
    "p_sort_key => 'absent', p_sort_measure => 'total'",
    "p_sort_key => 'appearances', p_sort_measure => 'base'",
    "p_sort_key => 'minutes', p_sort_measure => 'base', p_sort_desc => false",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_sort_desc => false",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_team_id => 20",
    "p_sort_key => 'goals', p_sort_measure => 'per90', p_position_group => 'DEF'",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_search => 'special'",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_min_minutes => 720",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_limit => 7, p_offset => 35",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_limit => 150, p_offset => 90",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_limit => 0, p_offset => -2",
    "p_sort_key => 'goals', p_sort_measure => null, p_sort_desc => null",
    "p_sort_key => 'goals', p_sort_measure => 'total', p_team_id => 20, p_search => 'special', p_limit => 3",
    "p_sort_key => 'assists', p_sort_measure => 'total', p_search => ''",
    "p_sort_key => 'rating', p_sort_measure => 'average', p_team_id => 10, p_position_group => 'FWD'",
    "p_sort_key => 'goals', p_sort_measure => 'per90', p_offset => 200",
]


class PlayerStatsReadModelSqlTest(unittest.TestCase):
    def test_candidate_matches_original(self):
        binaries = {name: shutil.which(name) for name in ("initdb", "pg_ctl", "psql")}
        if not all(binaries.values()):
            self.skipTest("Local PostgreSQL binaries are required")

        with tempfile.TemporaryDirectory(prefix="os-stats-") as tmp:
            data = Path(tmp) / "data"
            socket_dir = Path(tmp) / "socket"
            socket_dir.mkdir()
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            subprocess.run([binaries["initdb"], "-D", str(data), "-A", "trust", "-U", "postgres"],
                           check=True, capture_output=True, text=True)
            subprocess.run([binaries["pg_ctl"], "-D", str(data), "-o",
                            f"-c listen_addresses='' -c unix_socket_directories={socket_dir} -p {port}",
                            "-w", "start"], check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=20)
            try:
                env = {**os.environ, "PGHOST": str(socket_dir), "PGPORT": str(port),
                       "PGUSER": "postgres", "PGDATABASE": "postgres"}

                def run(sql):
                    completed = subprocess.run([binaries["psql"], "-X", "-v", "ON_ERROR_STOP=1",
                                                "-At", "-F", "|"], input=sql, env=env,
                                               capture_output=True, text=True)
                    if completed.returncode:
                        raise AssertionError(completed.stderr)
                    return completed.stdout.strip()

                run(SCHEMA + original_function())
                run("alter function public.player_stats_season_table(integer,integer,bigint,text,numeric,text,text,text,boolean,integer,integer) rename to player_stats_season_table_original;")
                run(CANDIDATE.read_text())
                def compare(case):
                    query = f"""
                        select
                          (select coalesce(jsonb_agg(to_jsonb(x) order by x.rank), '[]'::jsonb)
                           from public.player_stats_season_table_original(
                             p_league_id => 8, p_season_id => 28, {case}) x)
                          =
                          (select coalesce(jsonb_agg(to_jsonb(x) order by x.rank), '[]'::jsonb)
                           from public.player_stats_season_table(
                             p_league_id => 8, p_season_id => 28, {case}) x);
                        """
                    self.assertEqual(run(query), "t")

                for case in CASES:
                    with self.subTest(case=case):
                        compare(case)

                run("""
                    insert into public.player_stats_metric_contract
                      select 'extra_' || n, true from generate_series(1, 17) n;
                    insert into public.players
                      select n, 'Player ' || n, null, null, null, null
                      from generate_series(121, 2000) n;
                    insert into public.player_stats_season_summary
                      select 8, 28, n, 10, 'FWD', 10, 8, 900,
                             '2026-11-01'::timestamptz
                      from generate_series(121, 2000) n;
                    insert into public.player_stats_season_metric
                      select 8, 28, s.player_id, s.team_id, c.metric_key,
                             (s.player_id % 9)::numeric, 10, 10, 0, 0, 0
                      from public.player_stats_season_summary s
                      cross join public.player_stats_metric_contract c
                      where s.player_id > 120 or c.metric_key like 'extra_%';
                    analyze public.player_stats_season_metric;
                    analyze public.player_stats_season_summary;
                """)
                for case in (
                    "p_sort_key => 'goals', p_sort_measure => 'total', p_limit => 50",
                    "p_sort_key => 'goals', p_sort_measure => 'per90', p_limit => 50",
                    "p_sort_key => 'rating', p_sort_measure => 'average', p_offset => 35",
                    "p_sort_key => 'goals', p_sort_measure => 'total', p_team_id => 20",
                ):
                    with self.subTest(large_case=case):
                        compare(case)
            finally:
                subprocess.run([binaries["pg_ctl"], "-D", str(data), "-m", "immediate",
                                "-w", "stop"], check=True, capture_output=True, text=True)


if __name__ == "__main__":
    unittest.main()
