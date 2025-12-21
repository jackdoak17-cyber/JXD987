create or replace function public.team_btts_pct_last_n(
  p_league_id integer,
  p_n integer,
  p_season_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
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
      (f.home_score > 0 and f.away_score > 0) as btts,
      true as is_home
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
      (f.home_score > 0 and f.away_score > 0) as btts,
      false as is_home
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  filtered as (
    select *
    from appearances
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
  ),
  ranked as (
    select
      filtered.*,
      row_number() over (
        partition by filtered.team_id
        order by filtered.starting_at desc, filtered.fixture_id desc
      ) as rn
    from filtered
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

create or replace function public.team_btts_pct_season(
  p_league_id integer,
  p_season_id integer default null,
  p_team_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
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
      (f.home_score > 0 and f.away_score > 0) as btts,
      true as is_home
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
      (f.home_score > 0 and f.away_score > 0) as btts,
      false as is_home
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  filtered as (
    select *
    from appearances
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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

create or replace function public.team_btts_last_n_meta(
  p_league_id integer,
  p_n integer,
  p_season_id integer default null,
  p_required integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
  p_team_id integer default null
)
returns table (
  candidate_teams_count integer,
  complete_windows_count integer,
  qualified_teams_count integer
)
language sql
stable
as $$
  with appearances as (
    select
      f.id as fixture_id,
      f.starting_at,
      f.home_team_id as team_id,
      (f.home_score > 0 and f.away_score > 0) as btts,
      true as is_home
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
      (f.home_score > 0 and f.away_score > 0) as btts,
      false as is_home
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  filtered as (
    select *
    from appearances
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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
      count(*) as games,
      sum(case when btts then 1 else 0 end) as btts_games
    from windowed
    group by team_id
  ),
  complete as (
    select *
    from agg
    where games = p_n
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where p_required is null or btts_games >= p_required)::integer
      as qualified_teams_count;
$$;

create or replace function public.team_stat_streak_last_n(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
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
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at, f.home_team_id
  ),
  filtered as (
    select *
    from base
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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

create or replace function public.team_stat_streak_last_n_meta(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
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
  with finished_fixtures as (
    select
      f.id,
      f.starting_at,
      f.home_team_id,
      f.away_team_id
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  appearances as (
    select
      finished_fixtures.id as fixture_id,
      finished_fixtures.starting_at,
      finished_fixtures.home_team_id as team_id,
      true as is_home
    from finished_fixtures
    union all
    select
      finished_fixtures.id as fixture_id,
      finished_fixtures.starting_at,
      finished_fixtures.away_team_id as team_id,
      false as is_home
    from finished_fixtures
  ),
  appearance_filtered as (
    select *
    from appearances
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
  ),
  candidate_teams as (
    select appearance_filtered.team_id
    from appearance_filtered
    group by appearance_filtered.team_id
    having count(*) >= p_n
  ),
  base as (
    select
      fs.team_id,
      fs.fixture_id,
      max(fs.value)::numeric as stat_value,
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at, f.home_team_id
  ),
  base_filtered as (
    select *
    from base
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
  ),
  ranked as (
    select
      base_filtered.team_id,
      base_filtered.fixture_id,
      base_filtered.stat_value,
      base_filtered.starting_at,
      row_number() over (
        partition by base_filtered.team_id
        order by base_filtered.starting_at desc, base_filtered.fixture_id desc
      ) as rn
    from base_filtered
  ),
  windowed as (
    select ranked.*
    from ranked
    where ranked.rn <= p_n
  ),
  agg as (
    select
      windowed.team_id,
      count(*) as games_total,
      count(windowed.stat_value) as games_with_stat,
      sum(case when windowed.stat_value >= p_threshold then 1 else 0 end) as games_hit,
      max(windowed.starting_at) as latest_starting_at
    from windowed
    group by windowed.team_id
  ),
  filtered as (
    select *
    from agg
    where games_total = p_n
      and games_with_stat = p_n
      and (
        p_recent_days is null
        or latest_starting_at >= (now() - (p_recent_days || ' days')::interval)
      )
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from filtered)::integer as complete_windows_count,
    (select count(*) from filtered where games_hit >= p_required)::integer as qualified_teams_count;
$$;

create or replace function public.team_derived_stat_window_last_n(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_season_id integer default null,
  p_team_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
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
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at, f.home_team_id
  ),
  derived as (
    select
      a.team_id,
      a.fixture_id,
      a.starting_at,
      a.is_home,
      b.stat_value as derived_value
    from base_stats a
    join base_stats b
      on b.fixture_id = a.fixture_id
     and b.team_id <> a.team_id
  ),
  filtered as (
    select *
    from derived
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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
  p_home_only boolean default false,
  p_away_only boolean default false,
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
    p_home_only,
    p_away_only,
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
  p_home_only boolean default false,
  p_away_only boolean default false,
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
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at, f.home_team_id
  ),
  derived as (
    select
      a.team_id,
      a.fixture_id,
      a.starting_at,
      a.is_home,
      b.stat_value as derived_value
    from base_stats a
    join base_stats b
      on b.fixture_id = a.fixture_id
     and b.team_id <> a.team_id
  ),
  filtered as (
    select *
    from derived
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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
    agg.games_hit::integer,
    agg.avg_value::numeric,
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
  p_home_only boolean default false,
  p_away_only boolean default false,
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
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at, f.home_team_id
  ),
  derived as (
    select
      a.team_id,
      a.fixture_id,
      a.starting_at,
      a.is_home,
      b.stat_value as derived_value
    from base_stats a
    join base_stats b
      on b.fixture_id = a.fixture_id
     and b.team_id <> a.team_id
  ),
  filtered as (
    select *
    from derived
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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
      max(starting_at) as latest_starting_at
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
        or latest_starting_at >= (now() - (p_recent_days || ' days')::interval)
      )
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where games_hit >= p_required)::integer as qualified_teams_count;
$$;

create or replace function public.team_cards_window_last_n(
  p_league_id integer,
  p_n integer,
  p_season_id integer default null,
  p_team_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
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
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at, f.home_team_id
  ),
  filtered as (
    select *
    from base
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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

create or replace function public.team_cards_window_meta(
  p_league_id integer,
  p_n integer,
  p_season_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
  p_team_id integer default null
)
returns table (
  candidate_teams_count integer,
  complete_windows_count integer,
  qualified_teams_count integer
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
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at, f.home_team_id
  ),
  filtered as (
    select *
    from base
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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
      count(*) as games
    from windowed
    group by team_id
  ),
  complete as (
    select *
    from agg
    where games = p_n
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete)::integer as qualified_teams_count;
$$;

create or replace function public.team_cards_streak_last_n(
  p_league_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
  p_team_id integer default null,
  p_limit integer default 20
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
  with cards_per_team_fixture as (
    select
      fs.fixture_id as fixture_id,
      fs.team_id as team_id,
      f.starting_at as starting_at,
      sum(
        case
          when fs.type_id in (83, 84, 85) then fs.value::numeric
          else 0
        end
      ) as cards,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at, f.home_team_id
  ),
  filtered as (
    select *
    from cards_per_team_fixture
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
  ),
  ranked as (
    select
      filtered.fixture_id,
      filtered.team_id,
      filtered.starting_at,
      filtered.cards,
      row_number() over (
        partition by filtered.team_id
        order by filtered.starting_at desc, filtered.fixture_id desc
      ) as rn
    from filtered
  ),
  windowed as (
    select
      ranked.fixture_id,
      ranked.team_id,
      ranked.starting_at,
      ranked.cards
    from ranked
    where ranked.rn <= p_n
  ),
  agg as (
    select
      windowed.team_id as team_id,
      count(*) as games,
      sum(case when windowed.cards >= p_threshold then 1 else 0 end) as games_hit,
      avg(windowed.cards) as avg_value,
      array_agg(
        windowed.cards
        order by windowed.starting_at desc, windowed.fixture_id desc
      ) as last_values
    from windowed
    group by windowed.team_id
  )
  select
    agg.team_id::integer as team_id,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games::integer as games,
    agg.games_hit::integer as games_hit,
    agg.avg_value::numeric as avg_value,
    agg.last_values::numeric[] as last_values
  from agg
  join teams t on t.id = agg.team_id
  where agg.games = p_n
    and agg.games_hit >= p_required
  order by agg.games_hit desc, agg.avg_value desc
  limit p_limit;
$$;

create or replace function public.team_cards_streak_last_n_meta(
  p_league_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
  p_team_id integer default null
)
returns table (
  candidate_teams_count integer,
  complete_windows_count integer,
  qualified_teams_count integer
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
      f.starting_at,
      (fs.team_id = f.home_team_id) as is_home
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at, f.home_team_id
  ),
  filtered as (
    select *
    from base
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
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
      count(*) as games,
      sum(case when cards_total >= p_threshold then 1 else 0 end) as games_hit
    from windowed
    group by team_id
  ),
  complete as (
    select *
    from agg
    where games = p_n
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where games_hit >= p_required)::integer as qualified_teams_count;
$$;
