create or replace function public.team_over_goals_rate_last_n(
  p_league_id integer,
  p_goals_threshold numeric,
  p_last_n integer,
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
  over_games integer,
  over_pct numeric,
  avg_goals numeric,
  last_values numeric[],
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
      (f.home_score + f.away_score)::numeric as total_goals,
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
      (f.home_score + f.away_score)::numeric as total_goals,
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
        partition by team_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from filtered
  ),
  windowed as (
    select * from ranked where rn <= p_last_n
  ),
  agg as (
    select
      team_id,
      count(*) as games,
      sum(case when total_goals >= p_goals_threshold then 1 else 0 end) as over_games,
      avg(total_goals) as avg_goals,
      array_agg(total_goals order by starting_at desc, fixture_id desc) as last_values,
      max(starting_at) as last_fixture_at
    from windowed
    group by team_id
  )
  select
    agg.team_id::integer,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games::integer as games,
    agg.over_games::integer as over_games,
    case when agg.games > 0
      then agg.over_games::numeric / agg.games
      else 0
    end as over_pct,
    agg.avg_goals::numeric as avg_goals,
    agg.last_values::numeric[] as last_values,
    agg.last_fixture_at as last_fixture_at
  from agg
  left join teams t on t.id = agg.team_id
  where agg.games = p_last_n
    and (
      p_recent_days is null
      or agg.last_fixture_at >= (now() - (p_recent_days || ' days')::interval)
    )
  order by over_pct desc, agg.over_games desc
  limit p_limit;
$$;

create or replace function public.team_over_goals_streak_last_n(
  p_league_id integer,
  p_goals_threshold numeric,
  p_n integer,
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
  avg_goals numeric,
  last_values numeric[],
  last_fixture_at timestamptz
)
language sql
stable
as $$
  with rate as (
    select *
    from public.team_over_goals_rate_last_n(
      p_league_id,
      p_goals_threshold,
      p_n,
      p_season_id,
      p_home_only,
      p_away_only,
      p_team_id,
      p_limit,
      p_recent_days
    )
  )
  select
    rate.team_id::integer,
    rate.team_name::text,
    rate.team_short_code::text,
    rate.team_image_path::text,
    rate.games::integer as games,
    rate.over_games::integer as games_hit,
    rate.avg_goals::numeric as avg_goals,
    rate.last_values::numeric[] as last_values,
    rate.last_fixture_at as last_fixture_at
  from rate
  where rate.over_games >= p_required
  order by rate.over_games desc, rate.avg_goals desc
  limit p_limit;
$$;

create or replace function public.team_over_goals_streak_meta(
  p_league_id integer,
  p_goals_threshold numeric,
  p_n integer,
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
  with appearances as (
    select
      f.id as fixture_id,
      f.starting_at,
      f.home_team_id as team_id,
      (f.home_score + f.away_score)::numeric as total_goals,
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
      (f.home_score + f.away_score)::numeric as total_goals,
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
    select *
    from ranked
    where rn <= p_n
  ),
  agg as (
    select
      team_id,
      count(*) as games,
      sum(case when total_goals >= p_goals_threshold then 1 else 0 end) as games_hit,
      max(starting_at) as last_fixture_at
    from windowed
    group by team_id
  ),
  complete as (
    select *
    from agg
    where games = p_n
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
