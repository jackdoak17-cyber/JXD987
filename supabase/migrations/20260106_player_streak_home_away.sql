drop function if exists public.player_streak_base(
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
  integer
);

drop function if exists public.player_streak(
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

create or replace function public.player_streak_base(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_min_avg numeric default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_recent_days integer default null,
  p_limit integer default 200,
  p_home_only boolean default false,
  p_away_only boolean default false
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
  with latest_app as (
    select
      fp.player_id,
      fp.team_id,
      f.starting_at,
      f.id as fixture_id,
      row_number() over (
        partition by fp.player_id
        order by f.starting_at desc, f.id desc
      ) as rn
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  latest as (
    select
      player_id,
      team_id as latest_team_id,
      starting_at as latest_starting_at
    from latest_app
    where rn = 1
  ),
  candidates_all as (
    select fp.player_id
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
      and (not p_home_only or fp.team_id = f.home_team_id)
      and (not p_away_only or fp.team_id = f.away_team_id)
    group by fp.player_id
    having count(*) >= p_n
  ),
  candidates as (
    select fp.player_id
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
      and (not p_home_only or fp.team_id = f.home_team_id)
      and (not p_away_only or fp.team_id = f.away_team_id)
      and coalesce(fp.minutes_played, 0) > 0
    group by fp.player_id
    having count(*) >= p_n
  ),
  per_player as (
    select
      c.player_id,
      stats.games_total,
      stats.games_with_stat,
      stats.games_hit,
      stats.avg_value,
      stats.last_values,
      stats.last_fixture_at,
      stats.window_start_at
    from candidates c
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
          and (not p_home_only or fp.team_id = f.home_team_id)
          and (not p_away_only or fp.team_id = f.away_team_id)
          and coalesce(fp.minutes_played, 0) > 0
        order by f.starting_at desc, f.id desc
        limit p_n
      ),
      stats as (
        select
          ln.player_id,
          ln.fixture_id,
          max(fps.value) as raw_value
        from last_n ln
        left join fixture_player_statistics fps
          on fps.fixture_id = ln.fixture_id
         and fps.player_id = ln.player_id
         and fps.type_id = p_type_id
        group by ln.player_id, ln.fixture_id
      ),
      stats_enriched as (
        select
          stats.player_id,
          stats.fixture_id,
          case
            when stats.raw_value is null
              and p_type_id in (42, 86, 52, 56, 96, 78, 57)
              then 0::numeric
            else stats.raw_value::numeric
          end as stat_value,
          case
            when stats.raw_value is null
              and p_type_id in (42, 86, 52, 56, 96, 78, 57)
              then 1
            else 0
          end as zero_filled
        from stats
      )
      select
        ln.player_id,
        count(*) as games_total,
        count(stats_enriched.stat_value) as games_with_stat,
        sum(case when stats_enriched.stat_value >= p_threshold then 1 else 0 end) as games_hit,
        avg(stats_enriched.stat_value) as avg_value,
        array_agg(
          stats_enriched.stat_value
          order by ln.starting_at desc, ln.fixture_id desc
        ) as last_values,
        max(ln.starting_at) as last_fixture_at,
        min(ln.starting_at) as window_start_at
      from last_n ln
      left join stats_enriched
        on stats_enriched.player_id = ln.player_id
       and stats_enriched.fixture_id = ln.fixture_id
      group by ln.player_id
    ) stats on true
  )
  select
    per_player.player_id::integer,
    p.name::text as player_name,
    p.common_name::text as player_common_name,
    p.short_name::text as player_short_name,
    latest.latest_team_id::integer as team_id,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    per_player.games_total::integer as games,
    per_player.games_hit::integer,
    per_player.avg_value::numeric,
    per_player.last_values::numeric[] as last_values,
    per_player.last_fixture_at as last_fixture_at,
    per_player.window_start_at as window_start_at
  from per_player
  join players p on p.id = per_player.player_id
  join latest on latest.player_id = per_player.player_id
  left join teams t on t.id = latest.latest_team_id
  where per_player.games_total = p_n
    and per_player.games_with_stat = p_n
    and per_player.games_hit >= p_required
    and (p_min_avg is null or per_player.avg_value >= p_min_avg)
    and (
      p_recent_days is null
      or (per_player.last_fixture_at at time zone 'Europe/London')::date >=
        ((now() at time zone 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
    )
  order by per_player.games_hit desc, per_player.avg_value desc
  limit p_limit;
$$;

create or replace function public.player_streak(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_min_avg numeric default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_recent_days integer default null,
  p_limit integer default 20,
  p_team_id integer default null,
  p_result_limit integer default 200,
  p_home_only boolean default false,
  p_away_only boolean default false
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
  with base as (
    select *
    from public.player_streak_base(
      p_league_id,
      p_type_id,
      p_n,
      p_threshold,
      p_required,
      p_min_avg,
      p_started_only,
      p_min_minutes,
      p_season_id,
      p_recent_days,
      p_result_limit,
      p_home_only,
      p_away_only
    )
  )
  select
    base.player_id::integer,
    base.player_name::text,
    base.player_common_name::text,
    base.player_short_name::text,
    base.team_id::integer,
    base.team_name::text,
    base.team_short_code::text,
    base.team_image_path::text,
    base.games::integer,
    base.games_hit::integer,
    base.avg_value::numeric,
    base.last_values::numeric[] as last_values,
    base.last_fixture_at as last_fixture_at,
    base.window_start_at as window_start_at
  from base
  where p_team_id is null or base.team_id = p_team_id
  limit p_limit;
$$;
