create or replace function public.team_btts_pct_season(
  p_league_id integer,
  p_season_id integer default null,
  p_team_id integer default null,
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
  last_fixture_at timestamptz
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
  filtered as (
    select *
    from appearances
    where p_team_id is null or team_id = p_team_id
  ),
  agg as (
    select
      team_id,
      count(*) as games,
      sum(case when btts then 1 else 0 end) as btts_games,
      max(starting_at) as last_fixture_at
    from filtered
    group by team_id
  )
  select
    agg.team_id::integer,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games::integer as games,
    agg.btts_games::integer as btts_games,
    case when agg.games > 0
      then agg.btts_games::numeric / agg.games
      else 0
    end as btts_pct,
    agg.last_fixture_at as last_fixture_at
  from agg
  join teams t on t.id = agg.team_id
  order by btts_pct desc, agg.btts_games desc
  limit p_limit;
$$;
