do $$
begin
  if to_regprocedure('public.player_streak(integer, integer, integer, numeric, integer, numeric, boolean, integer, integer, integer)') is not null then
    execute 'alter function public.player_streak(integer, integer, integer, numeric, integer, numeric, boolean, integer, integer, integer) rename to player_streak_base';
  end if;
end $$;

create or replace function public.player_streak(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_min_avg numeric default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_limit integer default 20,
  p_team_id integer default null,
  p_result_limit integer default 200
)
returns table (
  player_id integer,
  player_name text,
  team_id integer,
  team_name text,
  team_short_code text,
  games integer,
  games_hit integer,
  avg_value numeric,
  last_values numeric[]
)
language sql
stable
as $$
  with base as (
    select *
    from public.player_streak_base(
      p_league_id,
      p_type_id,
      p_n,
      p_threshold,
      p_required,
      p_min_avg,
      p_started_only,
      p_min_minutes,
      p_season_id,
      p_result_limit
    )
  )
  select
    base.player_id::integer,
    base.player_name::text,
    base.team_id::integer,
    base.team_name::text,
    base.team_short_code::text,
    base.games::integer,
    base.games_hit::integer,
    base.avg_value::numeric,
    base.last_values::numeric[] as last_values
  from base
  where p_team_id is null or base.team_id = p_team_id
  limit p_limit;
$$;
