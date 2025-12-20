create or replace function public.team_stat_streak_last_n_meta(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
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
      finished_fixtures.home_team_id as team_id
    from finished_fixtures
    union all
    select
      finished_fixtures.id as fixture_id,
      finished_fixtures.starting_at,
      finished_fixtures.away_team_id as team_id
    from finished_fixtures
  ),
  candidate_teams as (
    select appearances.team_id
    from appearances
    group by appearances.team_id
    having count(*) >= p_n
  ),
  base as (
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
      base.team_id,
      base.fixture_id,
      base.stat_value,
      base.starting_at,
      row_number() over (
        partition by base.team_id
        order by base.starting_at desc, base.fixture_id desc
      ) as rn
    from base
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
