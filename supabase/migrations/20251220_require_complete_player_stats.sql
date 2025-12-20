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
  with appearances as (
    select
      fp.player_id,
      fp.team_id,
      f.id as fixture_id,
      f.starting_at,
      fps.value as stat_value
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    left join fixture_player_statistics fps
      on fps.fixture_id = fp.fixture_id
      and fps.player_id = fp.player_id
      and fps.type_id = p_type_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (not p_started_only or fp.is_starter is true)
      and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)
  ),
  ranked as (
    select
      *,
      row_number() over (
        partition by player_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from appearances
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      player_id,
      max(case when rn = 1 then team_id end) as last_team_id,
      count(*) as games_total,
      count(stat_value) as games_with_stat,
      sum(case when stat_value >= p_threshold then 1 else 0 end) as games_hit,
      avg(stat_value) as avg_value,
      array_agg(stat_value order by starting_at desc, fixture_id desc) as last_values
    from windowed
    group by player_id
  )
  select
    agg.player_id::integer,
    p.name::text as player_name,
    p.common_name::text as player_common_name,
    p.short_name::text as player_short_name,
    agg.last_team_id::integer as team_id,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games_total::integer as games,
    agg.games_hit::integer,
    agg.avg_value::numeric,
    agg.last_values::numeric[] as last_values
  from agg
  join players p on p.id = agg.player_id
  left join teams t on t.id = agg.last_team_id
  where agg.games_total = p_n
    and agg.games_with_stat = p_n
    and agg.games_hit >= p_required
    and (p_min_avg is null or agg.avg_value >= p_min_avg)
  order by agg.games_hit desc, agg.avg_value desc
  limit p_limit;
$$;
