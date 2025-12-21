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
  candidate_players as (
    select
      tlp.player_id,
      tlp.team_id
    from team_likely_players tlp
    join todays_teams tt on tt.team_id = tlp.team_id
    where p_team_id is null or tlp.team_id = p_team_id
  ),
  per_player as (
    select
      c.player_id,
      c.team_id,
      stats.games_total,
      stats.games_with_stat,
      stats.games_hit,
      stats.avg_value,
      stats.last_values,
      stats.last_fixture_at,
      stats.window_start_at
    from candidate_players c
    join lateral (
      with last_n as (
        select
          fp.player_id,
          fp.fixture_id,
          f.starting_at
        from fixture_players fp
        join fixtures f on f.id = fp.fixture_id
        where fp.player_id = c.player_id
          and f.league_id = p_league_id
          and (p_season_id is null or f.season_id = p_season_id)
          and f.home_score is not null
          and f.away_score is not null
          and (not p_started_only or fp.is_starter is true)
          and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
        order by f.starting_at desc, f.id desc
        limit p_n
      ),
      stats as (
        select
          ln.player_id,
          ln.fixture_id,
          max(fps.value) as stat_value
        from last_n ln
        left join fixture_player_statistics fps
          on fps.fixture_id = ln.fixture_id
         and fps.player_id = ln.player_id
         and fps.type_id = p_type_id
        group by ln.player_id, ln.fixture_id
      )
      select
        ln.player_id,
        count(*) as games_total,
        count(stats.stat_value) as games_with_stat,
        sum(case when stats.stat_value >= p_threshold then 1 else 0 end) as games_hit,
        avg(stats.stat_value) as avg_value,
        array_agg(
          stats.stat_value
          order by ln.starting_at desc, ln.fixture_id desc
        ) as last_values,
        max(ln.starting_at) as last_fixture_at,
        min(ln.starting_at) as window_start_at
      from last_n ln
      left join stats
        on stats.player_id = ln.player_id
       and stats.fixture_id = ln.fixture_id
      group by ln.player_id
    ) stats on true
  ),
  ordered as (
    select
      per_player.player_id::integer,
      p.name::text as player_name,
      p.common_name::text as player_common_name,
      p.short_name::text as player_short_name,
      per_player.team_id::integer as team_id,
      t.name::text as team_name,
      t.short_code::text as team_short_code,
      t.image_path::text as team_image_path,
      per_player.games_total::integer as games,
      per_player.games_hit::integer as games_hit,
      per_player.avg_value::numeric as avg_value,
      per_player.last_values::numeric[] as last_values,
      per_player.last_fixture_at as last_fixture_at,
      per_player.window_start_at as window_start_at
    from per_player
    join players p on p.id = per_player.player_id
    left join teams t on t.id = per_player.team_id
    where per_player.games_total = p_n
      and per_player.games_with_stat = p_n
      and per_player.games_hit >= p_required
      and (p_min_avg is null or per_player.avg_value >= p_min_avg)
      and (
        p_recent_days is null
        or per_player.last_fixture_at >= (now() - (p_recent_days || ' days')::interval)
      )
    order by per_player.games_hit desc, per_player.avg_value desc
    limit p_result_limit
  )
  select * from ordered limit p_limit;
$$;

drop function if exists public.player_streak_playing_on_date_counts(
  date,
  integer,
  integer
);

create or replace function public.player_streak_playing_on_date_counts(
  p_date date,
  p_league_id integer,
  p_season_id integer default null
)
returns table (
  today_fixtures_count integer,
  today_teams_count integer,
  candidate_players_count integer
)
language sql
stable
as $$
  with todays_fixtures as (
    select f.id, f.home_team_id, f.away_team_id
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
  )
  select
    (select count(*) from todays_fixtures)::integer as today_fixtures_count,
    (select count(*) from todays_teams)::integer as today_teams_count,
    (
      select count(distinct tlp.player_id)
      from team_likely_players tlp
      join todays_teams tt on tt.team_id = tlp.team_id
    )::integer as candidate_players_count;
$$;
