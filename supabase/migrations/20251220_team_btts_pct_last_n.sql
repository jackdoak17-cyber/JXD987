create or replace function public.team_btts_pct_last_n(
  p_league_id integer,
  p_n integer,
  p_season_id integer default null,
  p_limit integer default 20
)
returns table (
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  btts_games integer,
  btts_pct numeric,
  last_flags boolean[]
)
language sql
stable
as $$
  with appearances as (
    select
      f.id as fixture_id,
      f.starting_at,
      f.home_team_id as team_id,
      (f.home_score > 0 and f.away_score > 0) as btts
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    union all
    select
      f.id as fixture_id,
      f.starting_at,
      f.away_team_id as team_id,
      (f.home_score > 0 and f.away_score > 0) as btts
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  ranked as (
    select
      appearances.*,
      row_number() over (
        partition by appearances.team_id
        order by appearances.starting_at desc, appearances.fixture_id desc
      ) as rn
    from appearances
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      windowed.team_id,
      count(*) as games,
      sum(case when windowed.btts then 1 else 0 end) as btts_games,
      sum(case when windowed.btts then 1 else 0 end)::numeric / p_n as btts_pct,
      array_agg(
        windowed.btts
        order by windowed.starting_at desc, windowed.fixture_id desc
      ) as last_flags
    from windowed
    group by windowed.team_id
  )
  select
    agg.team_id::integer,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games::integer as games,
    agg.btts_games::integer as btts_games,
    agg.btts_pct::numeric as btts_pct,
    agg.last_flags::boolean[] as last_flags
  from agg
  join teams t on t.id = agg.team_id
  where agg.games = p_n
  order by agg.btts_pct desc, agg.btts_games desc
  limit p_limit;
$$;
