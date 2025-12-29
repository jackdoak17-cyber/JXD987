-- Premium Filter: Statistical Average Threshold
-- Filters players based on their statistical average over a custom window
-- Example: Require players to average > 1.0 shots per game over their last 15 games

create or replace function public.filter_players_by_stat_avg(
  p_league_id int,
  p_type_id int,
  p_player_ids int[],
  p_avg_window int,
  p_min_avg numeric,
  p_season_id int default null
)
returns table (
  player_id int,
  avg_value numeric,
  games_counted int
)
language sql
stable
as $$
  with player_stats as (
    select
      fp.player_id,
      f.starting_at,
      f.id as fixture_id,
      coalesce(fps.value, 0) as stat_value
    from public.fixture_players fp
    join public.fixtures f on f.id = fp.fixture_id
    left join public.fixture_player_statistics fps
      on fps.fixture_id = fp.fixture_id
      and fps.player_id = fp.player_id
      and fps.type_id = p_type_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and fp.player_id = any(p_player_ids)
  ),
  last_n_per_player as (
    select
      player_id,
      stat_value,
      row_number() over (
        partition by player_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from player_stats
  ),
  averages as (
    select
      player_id,
      avg(stat_value) as avg_value,
      count(*) as games_counted
    from last_n_per_player
    where rn <= p_avg_window
    group by player_id
    having count(*) = p_avg_window  -- Must have full window of games
  )
  select
    player_id::int,
    avg_value,
    games_counted::int
  from averages
  where avg_value >= p_min_avg;
$$;
