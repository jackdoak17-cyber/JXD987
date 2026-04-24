create table if not exists public.oddssearcher_opponent_tier_multipliers (
  league_id integer not null,
  season_id integer null,
  position_group text not null,
  tier integer not null check (tier >= 1 and tier <= 6),
  shots_multiplier numeric not null default 1.0,
  sot_multiplier numeric not null default 1.0,
  computed_at timestamptz not null default now(),
  primary key (league_id, season_id, position_group, tier)
);

create index if not exists oddssearcher_opponent_tier_multipliers_league_idx
on public.oddssearcher_opponent_tier_multipliers (league_id);

