create table if not exists public.sidelined_players (
  id bigint primary key,
  player_id bigint not null,
  team_id bigint,
  category text,
  type_id integer,
  season_id integer,
  start_date date,
  end_date date,
  games_missed integer,
  completed boolean,
  updated_at timestamptz not null default now(),
  extra jsonb
);

create index if not exists sidelined_players_player_id_idx on public.sidelined_players (player_id);
create index if not exists sidelined_players_team_id_idx on public.sidelined_players (team_id);
create index if not exists sidelined_players_active_idx on public.sidelined_players (completed, end_date);

create or replace view public.sidelined_active as
select *
from public.sidelined_players
where coalesce(completed, false) = false
  and (end_date is null or end_date >= (now() at time zone 'utc')::date);
