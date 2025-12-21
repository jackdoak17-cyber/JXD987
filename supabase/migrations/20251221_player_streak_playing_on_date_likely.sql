drop function if exists public.player_streak_playing_on_date(
  date,
  integer,
  integer,
  integer,
  numeric,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer
);

create or replace function public.player_streak_playing_on_date(
  p_date date,
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_min_avg numeric default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_team_id integer default null,
  p_result_limit integer default 200,
  p_recent_days integer default null,
  p_limit integer default 20
)
returns table (
  player_id integer,
  player_name text,
  player_common_name text,
  player_short_name text,
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  games_hit integer,
  avg_value numeric,
  last_values numeric[],
  last_fixture_at timestamptz,
  window_start_at timestamptz
)
language sql
stable
as $$
  with todays_fixtures as (
    select
      f.id,
      f.home_team_id,
      f.away_team_id
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and (f.starting_at at time zone 'Europe/London')::date = p_date
  ),
  todays_teams as (
    select distinct tf.home_team_id as team_id
    from todays_fixtures tf
    where tf.home_team_id is not null
    union
    select distinct tf.away_team_id as team_id
    from todays_fixtures tf
    where tf.away_team_id is not null
  ),
  candidates as (
    select distinct tlp.player_id
    from team_likely_players tlp
    join todays_teams tt on tt.team_id = tlp.team_id
    where p_team_id is null or tlp.team_id = p_team_id
  ),
  appearances as (
    select
      fp.player_id,
      fp.team_id,
      f.id as fixture_id,
      f.starting_at,
      max(fps.value)::numeric as stat_value
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    left join fixture_player_statistics fps
      on fps.fixture_id = f.id
     and fps.player_id = fp.player_id
     and fps.type_id = p_type_id
    where fp.player_id in (select player_id from candidates)
      and f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
    group by fp.player_id, fp.team_id, f.id, f.starting_at
  ),
  ranked as (
    select
      appearances.*,
      row_number() over (
        partition by appearances.player_id
        order by appearances.starting_at desc, appearances.fixture_id desc
      ) as rn
    from appearances
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      windowed.player_id,
      count(*) as games_total,
      count(windowed.stat_value) as games_with_stat,
      sum(case when coalesce(windowed.stat_value, 0) >= p_threshold then 1 else 0 end) as games_hit,
      avg(windowed.stat_value) as avg_value,
      array_agg(
        windowed.stat_value
        order by windowed.starting_at desc, windowed.fixture_id desc
      ) as last_values,
      max(windowed.starting_at) as last_fixture_at,
      min(windowed.starting_at) as window_start_at
    from windowed
    group by windowed.player_id
  ),
  latest_window_team as (
    select
      windowed.player_id,
      windowed.team_id,
      windowed.starting_at
    from windowed
    where windowed.rn = 1
  ),
  ordered as (
    select
      agg.player_id::integer,
      p.name::text as player_name,
      p.common_name::text as player_common_name,
      p.short_name::text as player_short_name,
      latest_window_team.team_id::integer as team_id,
      t.name::text as team_name,
      t.short_code::text as team_short_code,
      t.image_path::text as team_image_path,
      agg.games_total::integer as games,
      agg.games_hit::integer as games_hit,
      agg.avg_value::numeric as avg_value,
      agg.last_values::numeric[] as last_values,
      agg.last_fixture_at as last_fixture_at,
      agg.window_start_at as window_start_at
    from agg
    join latest_window_team on latest_window_team.player_id = agg.player_id
    join players p on p.id = agg.player_id
    left join teams t on t.id = latest_window_team.team_id
    where agg.games_total = p_n
      and agg.games_with_stat = p_n
      and agg.games_hit >= p_required
      and (p_min_avg is null or agg.avg_value >= p_min_avg)
      and (
        p_recent_days is null
        or agg.last_fixture_at >= (now() - (p_recent_days || ' days')::interval)
      )
    order by agg.games_hit desc, agg.avg_value desc
    limit p_result_limit
  )
  select * from ordered limit p_limit;
$$;

drop function if exists public.player_streak_playing_on_date_counts(
  date,
  integer,
  integer
);

drop function if exists public.player_streak_playing_on_date_counts(
  date,
  integer,
  integer,
  integer,
  numeric,
  integer,
  boolean,
  integer,
  integer,
  integer,
  integer
);

create or replace function public.player_streak_playing_on_date_counts(
  p_date date,
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_team_id integer default null,
  p_recent_days integer default null
)
returns table (
  today_fixtures_count integer,
  today_teams_count integer,
  candidate_players_count integer,
  complete_windows_count integer,
  qualified_players_count integer
)
language sql
stable
as $$
  with todays_fixtures as (
    select
      f.id,
      f.home_team_id,
      f.away_team_id
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and (f.starting_at at time zone 'Europe/London')::date = p_date
  ),
  todays_teams as (
    select distinct tf.home_team_id as team_id
    from todays_fixtures tf
    where tf.home_team_id is not null
    union
    select distinct tf.away_team_id as team_id
    from todays_fixtures tf
    where tf.away_team_id is not null
  ),
  candidates as (
    select distinct tlp.player_id
    from team_likely_players tlp
    join todays_teams tt on tt.team_id = tlp.team_id
    where p_team_id is null or tlp.team_id = p_team_id
  ),
  appearances as (
    select
      fp.player_id,
      f.id as fixture_id,
      f.starting_at,
      max(fps.value)::numeric as stat_value
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    left join fixture_player_statistics fps
      on fps.fixture_id = f.id
     and fps.player_id = fp.player_id
     and fps.type_id = p_type_id
    where fp.player_id in (select player_id from candidates)
      and f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
    group by fp.player_id, f.id, f.starting_at
  ),
  ranked as (
    select
      appearances.*,
      row_number() over (
        partition by appearances.player_id
        order by appearances.starting_at desc, appearances.fixture_id desc
      ) as rn
    from appearances
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      windowed.player_id,
      count(*) as games_total,
      count(windowed.stat_value) as games_with_stat,
      sum(case when coalesce(windowed.stat_value, 0) >= p_threshold then 1 else 0 end) as games_hit,
      max(windowed.starting_at) as last_fixture_at
    from windowed
    group by windowed.player_id
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
    (select count(*) from todays_fixtures)::integer as today_fixtures_count,
    (select count(*) from todays_teams)::integer as today_teams_count,
    (select count(*) from candidates)::integer as candidate_players_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where games_hit >= p_required)::integer as qualified_players_count;
$$;
