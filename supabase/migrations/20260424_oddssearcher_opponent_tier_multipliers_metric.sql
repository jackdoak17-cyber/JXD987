alter table public.oddssearcher_opponent_tier_multipliers
add column if not exists opponent_metric text not null default 'league_position';

do $$
begin
  if exists (
    select 1
    from information_schema.table_constraints
    where constraint_schema = 'public'
      and table_name = 'oddssearcher_opponent_tier_multipliers'
      and constraint_type = 'PRIMARY KEY'
      and constraint_name = 'oddssearcher_opponent_tier_multipliers_pkey'
  ) then
    alter table public.oddssearcher_opponent_tier_multipliers
    drop constraint oddssearcher_opponent_tier_multipliers_pkey;
  end if;
end $$;

alter table public.oddssearcher_opponent_tier_multipliers
add constraint oddssearcher_opponent_tier_multipliers_pkey
primary key (league_id, season_id, opponent_metric, position_group, tier);

create index if not exists oddssearcher_opponent_tier_multipliers_metric_idx
on public.oddssearcher_opponent_tier_multipliers (opponent_metric);

