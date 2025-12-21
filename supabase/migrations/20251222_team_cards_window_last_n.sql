create or replace function public.team_cards_window_last_n(
  p_league_id integer,
  p_n integer,
  p_season_id integer default null,
  p_team_id integer default null,
  p_limit integer default 20,
  p_recent_days integer default null
)
returns table (
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  total_value numeric,
  avg_value numeric,
  last_values numeric[],
  last_fixture_at timestamptz
)
language sql
stable
as $$
  with base as (
    select
      fs.team_id,
      fs.fixture_id,
      sum(
        case
          when fs.type_id in (83, 84, 85) then fs.value::numeric
          else 0
        end
      ) as cards_total,
      f.starting_at
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at
  ),
  filtered as (
    select *
    from base
    where p_team_id is null or team_id = p_team_id
  ),
  ranked as (
    select
      filtered.*,
      row_number() over (
        partition by team_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from filtered
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      team_id,
      count(*) as games_total,
      sum(cards_total) as total_value,
      avg(cards_total) as avg_value,
      array_agg(cards_total order by starting_at desc, fixture_id desc) as last_values,
      max(starting_at) as last_fixture_at
    from windowed
    group by team_id
  )
  select
    agg.team_id::integer,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games_total::integer as games,
    agg.total_value::numeric as total_value,
    agg.avg_value::numeric as avg_value,
    agg.last_values::numeric[] as last_values,
    agg.last_fixture_at as last_fixture_at
  from agg
  left join teams t on t.id = agg.team_id
  where agg.games_total = p_n
    and (
      p_recent_days is null
      or agg.last_fixture_at >= (now() - (p_recent_days || ' days')::interval)
    )
  order by agg.avg_value desc, agg.total_value desc
  limit p_limit;
$$;
