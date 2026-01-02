create table if not exists public.team_standings_cache (
  league_id integer not null,
  season_id integer not null,
  team_id integer not null,
  rank integer not null,
  points integer not null,
  played integer not null,
  goals_for integer not null,
  goals_against integer not null,
  goal_diff integer not null,
  updated_at timestamptz not null default now(),
  primary key (league_id, season_id, team_id)
);

create index if not exists team_standings_cache_league_season_rank_idx
  on public.team_standings_cache (league_id, season_id, rank);
