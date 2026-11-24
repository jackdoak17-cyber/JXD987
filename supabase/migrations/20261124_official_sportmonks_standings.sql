-- Canonical official league tables supplied by SportMonks.
--
-- Match-derived standings remain useful as an independent audit, but they do
-- not contain federation corrections or competition-specific tie breakers.

create table if not exists public.official_team_standings (
  league_id integer not null,
  season_id integer not null,
  stage_id bigint not null,
  group_id bigint null,
  team_id integer not null,
  provider_standing_id bigint not null,
  standing_rule_id bigint null,
  round_id bigint null,
  position integer not null,
  points integer not null,
  played integer not null,
  wins integer not null,
  draws integer not null,
  losses integer not null,
  goals_for integer not null,
  goals_against integer not null,
  goal_diff integer not null,
  result text null,
  team_name text not null,
  team_short_code text null,
  team_image_path text null,
  source text not null default 'sportmonks',
  source_payload_hash text not null,
  fetched_at timestamptz not null,
  published_at timestamptz not null default now(),
  primary key (league_id, season_id, team_id),
  constraint official_team_standings_position_positive check (position > 0),
  constraint official_team_standings_played_nonnegative check (played >= 0),
  constraint official_team_standings_record_consistent check (played = wins + draws + losses),
  constraint official_team_standings_goals_consistent check (goal_diff = goals_for - goals_against),
  constraint official_team_standings_source_check check (source = 'sportmonks')
);

create unique index if not exists official_team_standings_scope_position_idx
  on public.official_team_standings (
    league_id,
    season_id,
    stage_id,
    coalesce(group_id, 0),
    position
  );

create index if not exists official_team_standings_season_rank_idx
  on public.official_team_standings (league_id, season_id, position);

create table if not exists public.official_standing_corrections (
  provider_correction_id bigint primary key,
  league_id integer not null,
  season_id integer not null,
  stage_id bigint not null,
  group_id bigint null,
  team_id integer not null,
  type_id bigint null,
  value integer not null,
  calc_type text null,
  active boolean not null,
  source text not null default 'sportmonks',
  source_payload_hash text not null,
  fetched_at timestamptz not null,
  published_at timestamptz not null default now(),
  constraint official_standing_corrections_source_check check (source = 'sportmonks')
);

create index if not exists official_standing_corrections_scope_idx
  on public.official_standing_corrections (league_id, season_id, stage_id, team_id);

create table if not exists public.official_standings_sync_runs (
  id uuid primary key default gen_random_uuid(),
  league_id integer not null,
  season_id integer not null,
  status text not null,
  rows_received integer not null default 0,
  corrections_received integer not null default 0,
  selected_stage_id bigint null,
  source_payload_hash text null,
  evidence jsonb not null default '{}'::jsonb,
  error text null,
  started_at timestamptz not null default now(),
  finished_at timestamptz null,
  constraint official_standings_sync_runs_status_check
    check (status in ('running', 'succeeded', 'failed'))
);

create index if not exists official_standings_sync_runs_scope_idx
  on public.official_standings_sync_runs (league_id, season_id, started_at desc);

alter table public.official_team_standings enable row level security;
alter table public.official_standing_corrections enable row level security;
alter table public.official_standings_sync_runs enable row level security;

revoke all on table public.official_team_standings from public, anon, authenticated;
revoke all on table public.official_standing_corrections from public, anon, authenticated;
revoke all on table public.official_standings_sync_runs from public, anon, authenticated;

grant select, insert, update, delete on table public.official_team_standings to service_role;
grant select, insert, update, delete on table public.official_standing_corrections to service_role;
grant select, insert, update, delete on table public.official_standings_sync_runs to service_role;

do $$
begin
  if exists (select 1 from pg_roles where rolname = 'statsweb_pool') then
    grant select on table public.official_team_standings to statsweb_pool;
  end if;
end
$$;

comment on table public.official_team_standings is
  'Validated SportMonks official table rows. This is the canonical website source for standard league standings.';
comment on table public.official_standing_corrections is
  'SportMonks federation correction evidence. Corrections are audited here but are not applied twice to official points.';
