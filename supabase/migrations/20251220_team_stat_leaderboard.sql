create or replace function public.team_stat_leaderboard(
  p_league_id integer,
  p_type_id integer,
  p_season_id integer default null,
  p_limit integer default 20
)
returns table (
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  total_value numeric
)
language sql
stable
as $$
  with agg as (
    select
      fs.team_id,
      sum(fs.value)::numeric as total_value
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id
  )
  select
    agg.team_id::integer,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.total_value::numeric as total_value
  from agg
  left join teams t on t.id = agg.team_id
  order by agg.total_value desc
  limit p_limit;
$$;
