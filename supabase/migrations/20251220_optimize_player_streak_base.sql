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
  last_values numeric[]
)
language sql
stable
as $$
  with candidates as (
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
  per_player as (
    select
      c.player_id,
      stats.last_team_id,
      stats.games_total,
      stats.games_with_stat,
      stats.games_hit,
      stats.avg_value,
      stats.last_values
    from candidates c
    join lateral (
      with last_n as (
        select
          fp.player_id,
          fp.team_id,
          f.id as fixture_id,
          f.starting_at,
          row_number() over (
            order by f.starting_at desc, f.id desc
          ) as rn
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
        max(case when ln.rn = 1 then ln.team_id end) as last_team_id,
        count(*) as games_total,
        count(stats.stat_value) as games_with_stat,
        sum(case when stats.stat_value >= p_threshold then 1 else 0 end) as games_hit,
        avg(stats.stat_value) as avg_value,
        array_agg(
          stats.stat_value
          order by ln.starting_at desc, ln.fixture_id desc
        ) as last_values
      from last_n ln
      left join stats
        on stats.player_id = ln.player_id
       and stats.fixture_id = ln.fixture_id
      group by ln.player_id
    ) stats on true
  )
  select
    per_player.player_id::integer,
    p.name::text as player_name,
    p.common_name::text as player_common_name,
    p.short_name::text as player_short_name,
    per_player.last_team_id::integer as team_id,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    per_player.games_total::integer as games,
    per_player.games_hit::integer,
    per_player.avg_value::numeric,
    per_player.last_values::numeric[] as last_values
  from per_player
  join players p on p.id = per_player.player_id
  left join teams t on t.id = per_player.last_team_id
  where per_player.games_total = p_n
    and per_player.games_with_stat = p_n
    and per_player.games_hit >= p_required
    and (p_min_avg is null or per_player.avg_value >= p_min_avg)
  order by per_player.games_hit desc, per_player.avg_value desc
  limit p_limit;
$$;
