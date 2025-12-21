create or replace function public.team_derived_stat_window_last_n(
  p_league_id integer,
  p_type_id integer,
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
  with base_stats as (
    select
      fs.fixture_id,
      fs.team_id,
      max(fs.value)::numeric as stat_value,
      f.starting_at
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at
  ),
  derived as (
    select
      a.team_id,
      a.fixture_id,
      a.starting_at,
      b.stat_value as derived_value
    from base_stats a
    join base_stats b
      on b.fixture_id = a.fixture_id
     and b.team_id <> a.team_id
  ),
  filtered as (
    select *
    from derived
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
      count(derived_value) as games_with_stat,
      sum(derived_value) as total_value,
      avg(derived_value) as avg_value,
      array_agg(derived_value order by starting_at desc, fixture_id desc) as last_values,
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
    and agg.games_with_stat = p_n
    and (
      p_recent_days is null
      or agg.last_fixture_at >= (now() - (p_recent_days || ' days')::interval)
    )
  order by agg.avg_value desc, agg.total_value desc
  limit p_limit;
$$;

create or replace function public.team_derived_stat_leaderboard_last_n(
  p_league_id integer,
  p_type_id integer,
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
  select *
  from public.team_derived_stat_window_last_n(
    p_league_id,
    p_type_id,
    p_n,
    p_season_id,
    p_team_id,
    p_limit,
    p_recent_days
  );
$$;

create or replace function public.team_derived_stat_streak_last_n(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
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
  games_hit integer,
  avg_value numeric,
  last_values numeric[],
  last_fixture_at timestamptz
)
language sql
stable
as $$
  with base_stats as (
    select
      fs.fixture_id,
      fs.team_id,
      max(fs.value)::numeric as stat_value,
      f.starting_at
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at
  ),
  derived as (
    select
      a.team_id,
      a.fixture_id,
      a.starting_at,
      b.stat_value as derived_value
    from base_stats a
    join base_stats b
      on b.fixture_id = a.fixture_id
     and b.team_id <> a.team_id
  ),
  filtered as (
    select *
    from derived
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
      count(derived_value) as games_with_stat,
      sum(case when derived_value >= p_threshold then 1 else 0 end) as games_hit,
      avg(derived_value) as avg_value,
      array_agg(derived_value order by starting_at desc, fixture_id desc) as last_values,
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
    agg.games_hit::integer as games_hit,
    agg.avg_value::numeric as avg_value,
    agg.last_values::numeric[] as last_values,
    agg.last_fixture_at as last_fixture_at
  from agg
  left join teams t on t.id = agg.team_id
  where agg.games_total = p_n
    and agg.games_with_stat = p_n
    and agg.games_hit >= p_required
    and (
      p_recent_days is null
      or agg.last_fixture_at >= (now() - (p_recent_days || ' days')::interval)
    )
  order by agg.games_hit desc, agg.avg_value desc
  limit p_limit;
$$;

create or replace function public.team_derived_stat_streak_meta(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
  p_team_id integer default null,
  p_recent_days integer default null
)
returns table (
  candidate_teams_count integer,
  complete_windows_count integer,
  qualified_teams_count integer
)
language sql
stable
as $$
  with base_stats as (
    select
      fs.fixture_id,
      fs.team_id,
      max(fs.value)::numeric as stat_value,
      f.starting_at
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at
  ),
  derived as (
    select
      a.team_id,
      a.fixture_id,
      a.starting_at,
      b.stat_value as derived_value
    from base_stats a
    join base_stats b
      on b.fixture_id = a.fixture_id
     and b.team_id <> a.team_id
  ),
  filtered as (
    select *
    from derived
    where p_team_id is null or team_id = p_team_id
  ),
  candidate_teams as (
    select team_id
    from filtered
    group by team_id
    having count(*) >= p_n
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
      count(derived_value) as games_with_stat,
      sum(case when derived_value >= p_threshold then 1 else 0 end) as games_hit,
      max(starting_at) as last_fixture_at
    from windowed
    group by team_id
  ),
  complete as (
    select *
    from agg
    where games_total = p_n
      and games_with_stat = p_n
      and (
        p_recent_days is null
        or last_fixture_at >= (now() - (p_recent_days || ' days')::interval)
      )
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where games_hit >= p_required)::integer as qualified_teams_count;
$$;
