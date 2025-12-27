drop function if exists public.player_streak_meta(
  integer,
  integer,
  integer,
  numeric,
  integer,
  boolean,
  integer,
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
  p_limit integer default 200
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
            when stats.raw_value is null and p_type_id in (42, 86)
              then 0::numeric
            else stats.raw_value::numeric
          end as stat_value,
          case
            when stats.raw_value is null and p_type_id in (42, 86)
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

create or replace function public.player_streak_meta(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_recent_days integer default null
)
returns table (
  candidate_players_count integer,
  complete_windows_count integer,
  qualified_players_count integer,
  zero_filled_count integer,
  excluded_no_minutes_count integer
)
language sql
stable
as $$
  with base_appearances as (
    select
      fp.player_id,
      fp.minutes_played
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
  ),
  candidates_all as (
    select player_id
    from base_appearances
    group by player_id
    having count(*) >= p_n
  ),
  candidates_played as (
    select player_id
    from base_appearances
    where coalesce(minutes_played, 0) > 0
    group by player_id
    having count(*) >= p_n
  ),
  excluded_no_minutes as (
    select count(*)::integer as excluded_count
    from candidates_all
    where player_id not in (select player_id from candidates_played)
  ),
  per_player as (
    select
      c.player_id,
      stats.games_total,
      stats.games_with_stat,
      stats.games_hit,
      stats.last_fixture_at,
      stats.zero_filled_count
    from candidates_played c
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
            when stats.raw_value is null and p_type_id in (42, 86)
              then 0::numeric
            else stats.raw_value::numeric
          end as stat_value,
          case
            when stats.raw_value is null and p_type_id in (42, 86)
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
        max(ln.starting_at) as last_fixture_at,
        sum(stats_enriched.zero_filled) as zero_filled_count
      from last_n ln
      left join stats_enriched
        on stats_enriched.player_id = ln.player_id
       and stats_enriched.fixture_id = ln.fixture_id
      group by ln.player_id
    ) stats on true
  ),
  complete as (
    select *
    from per_player
    where games_total = p_n
      and games_with_stat = p_n
      and (
        p_recent_days is null
        or (last_fixture_at at time zone 'Europe/London')::date >=
          ((now() at time zone 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
      )
  )
  select
    (select count(*) from candidates_played)::integer as candidate_players_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where games_hit >= p_required)::integer as qualified_players_count,
    (select coalesce(sum(zero_filled_count), 0) from complete)::integer as zero_filled_count,
    (select excluded_count from excluded_no_minutes)::integer as excluded_no_minutes_count;
$$;

create or replace function public.player_stat_window_last_n(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_player_id integer default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
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
  total_value numeric,
  avg_value numeric,
  last_values numeric[],
  last_fixture_at timestamptz,
  window_start_at timestamptz,
  last_fixture_dates timestamptz[]
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
      and (p_player_id is null or fp.player_id = p_player_id)
  ),
  latest as (
    select
      player_id,
      team_id as latest_team_id
    from latest_app
    where rn = 1
  ),
  candidates as (
    select fp.player_id
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (p_player_id is null or fp.player_id = p_player_id)
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
      and coalesce(fp.minutes_played, 0) > 0
    group by fp.player_id
    having count(*) >= p_n
  ),
  per_player as (
    select
      c.player_id,
      stats.games_total,
      stats.games_with_stat,
      stats.total_value,
      stats.avg_value,
      stats.last_values,
      stats.last_fixture_at,
      stats.window_start_at,
      stats.last_fixture_dates
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
            when stats.raw_value is null and p_type_id in (42, 86)
              then 0::numeric
            else stats.raw_value::numeric
          end as stat_value
        from stats
      )
      select
        ln.player_id,
        count(*) as games_total,
        count(stats_enriched.stat_value) as games_with_stat,
        sum(stats_enriched.stat_value) as total_value,
        avg(stats_enriched.stat_value) as avg_value,
        array_agg(
          stats_enriched.stat_value
          order by ln.starting_at desc, ln.fixture_id desc
        ) as last_values,
        max(ln.starting_at) as last_fixture_at,
        min(ln.starting_at) as window_start_at,
        array_agg(
          ln.starting_at
          order by ln.starting_at desc, ln.fixture_id desc
        ) as last_fixture_dates
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
    per_player.total_value::numeric as total_value,
    per_player.avg_value::numeric as avg_value,
    per_player.last_values::numeric[] as last_values,
    per_player.last_fixture_at as last_fixture_at,
    per_player.window_start_at as window_start_at,
    per_player.last_fixture_dates as last_fixture_dates
  from per_player
  join players p on p.id = per_player.player_id
  join latest on latest.player_id = per_player.player_id
  left join teams t on t.id = latest.latest_team_id
  where per_player.games_total = p_n
    and per_player.games_with_stat = p_n
    and (
      p_recent_days is null
      or (per_player.last_fixture_at at time zone 'Europe/London')::date >=
        ((now() at time zone 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
    )
  order by per_player.avg_value desc, per_player.total_value desc
  limit p_limit;
$$;

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
      max(fps.value) as raw_value
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
      and coalesce(fp.minutes_played, 0) > 0
    group by fp.player_id, fp.team_id, f.id, f.starting_at
  ),
  appearances_enriched as (
    select
      appearances.player_id,
      appearances.team_id,
      appearances.fixture_id,
      appearances.starting_at,
      case
        when appearances.raw_value is null and p_type_id in (42, 86)
          then 0::numeric
        else appearances.raw_value::numeric
      end as stat_value,
      case
        when appearances.raw_value is null and p_type_id in (42, 86)
          then 1
        else 0
      end as zero_filled
    from appearances
  ),
  ranked as (
    select
      appearances_enriched.*,
      row_number() over (
        partition by appearances_enriched.player_id
        order by appearances_enriched.starting_at desc, appearances_enriched.fixture_id desc
      ) as rn
    from appearances_enriched
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      windowed.player_id,
      count(*) as games_total,
      count(windowed.stat_value) as games_with_stat,
      sum(case when windowed.stat_value >= p_threshold then 1 else 0 end) as games_hit,
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
      and (p_team_id is null or latest_window_team.team_id = p_team_id)
      and (
        p_recent_days is null
        or (agg.last_fixture_at at time zone 'Europe/London')::date >=
          ((now() at time zone 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
      )
    order by agg.games_hit desc, agg.avg_value desc
    limit p_result_limit
  )
  select * from ordered limit p_limit;
$$;

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
  qualified_players_count integer,
  zero_filled_count integer,
  excluded_no_minutes_count integer
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
  base_appearances as (
    select
      fp.player_id,
      fp.minutes_played
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where fp.player_id in (select player_id from candidates)
      and f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
  ),
  candidates_all as (
    select player_id
    from base_appearances
    group by player_id
    having count(*) >= p_n
  ),
  candidates_played as (
    select player_id
    from base_appearances
    where coalesce(minutes_played, 0) > 0
    group by player_id
    having count(*) >= p_n
  ),
  excluded_no_minutes as (
    select count(*)::integer as excluded_count
    from candidates_all
    where player_id not in (select player_id from candidates_played)
  ),
  appearances as (
    select
      fp.player_id,
      f.id as fixture_id,
      f.starting_at,
      max(fps.value) as raw_value
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    left join fixture_player_statistics fps
      on fps.fixture_id = f.id
     and fps.player_id = fp.player_id
     and fps.type_id = p_type_id
    where fp.player_id in (select player_id from candidates_played)
      and f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
      and coalesce(fp.minutes_played, 0) > 0
    group by fp.player_id, f.id, f.starting_at
  ),
  appearances_enriched as (
    select
      appearances.player_id,
      appearances.fixture_id,
      appearances.starting_at,
      case
        when appearances.raw_value is null and p_type_id in (42, 86)
          then 0::numeric
        else appearances.raw_value::numeric
      end as stat_value,
      case
        when appearances.raw_value is null and p_type_id in (42, 86)
          then 1
        else 0
      end as zero_filled
    from appearances
  ),
  ranked as (
    select
      appearances_enriched.*,
      row_number() over (
        partition by appearances_enriched.player_id
        order by appearances_enriched.starting_at desc, appearances_enriched.fixture_id desc
      ) as rn
    from appearances_enriched
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      windowed.player_id,
      count(*) as games_total,
      count(windowed.stat_value) as games_with_stat,
      sum(case when windowed.stat_value >= p_threshold then 1 else 0 end) as games_hit,
      max(windowed.starting_at) as last_fixture_at,
      sum(windowed.zero_filled) as zero_filled_count
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
        or (last_fixture_at at time zone 'Europe/London')::date >=
          ((now() at time zone 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
      )
  )
  select
    (select count(*) from todays_fixtures)::integer as today_fixtures_count,
    (select count(*) from todays_teams)::integer as today_teams_count,
    (select count(*) from candidates)::integer as candidate_players_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where games_hit >= p_required)::integer as qualified_players_count,
    (select coalesce(sum(zero_filled_count), 0) from complete)::integer as zero_filled_count,
    (select excluded_count from excluded_no_minutes)::integer as excluded_no_minutes_count;
$$;
