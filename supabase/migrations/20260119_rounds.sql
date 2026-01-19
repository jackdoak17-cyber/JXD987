create table if not exists public.rounds (
  id bigint primary key,
  league_id integer not null,
  season_id integer not null,
  stage_id integer,
  name text,
  starting_at date,
  ending_at date,
  is_current boolean default false,
  games_in_current_week boolean default false,
  finished boolean default false
);

create index if not exists rounds_league_id_idx on public.rounds(league_id);
create index if not exists rounds_season_id_idx on public.rounds(season_id);
create index if not exists rounds_is_current_idx on public.rounds(is_current);
