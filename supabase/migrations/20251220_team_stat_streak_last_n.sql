create or replace function public.team_stat_streak_last_n(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
  p_limit integer default 20,
  p_recent_days integer default null
)
returns table (
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  games_hit integer,
  avg_value numeric,
  last_values numeric[]
)
language sql
stable
as $$
  with base as (
    select
      fs.team_id,
      fs.fixture_id,
      max(fs.value)::numeric as stat_value,
      f.starting_at
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at
  ),
  ranked as (
    select
      *,
      row_number() over (
        partition by team_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from base
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      team_id,
      count(*) as games_total,
      count(stat_value) as games_with_stat,
      sum(case when stat_value >= p_threshold then 1 else 0 end) as games_hit,
      avg(stat_value) as avg_value,
      array_agg(stat_value order by starting_at desc, fixture_id desc) as last_values,
      max(starting_at) as latest_starting_at
    from windowed
    group by team_id
  )
  select
    agg.team_id::integer,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games_total::integer as games,
    agg.games_hit::integer,
    agg.avg_value::numeric,
    agg.last_values::numeric[] as last_values
  from agg
  left join teams t on t.id = agg.team_id
  where agg.games_total = p_n
    and agg.games_with_stat = p_n
    and agg.games_hit >= p_required
    and (
      p_recent_days is null
      or agg.latest_starting_at >= (now() - (p_recent_days || ' days')::interval)
    )
  order by agg.games_hit desc, agg.avg_value desc
  limit p_limit;
$$;
