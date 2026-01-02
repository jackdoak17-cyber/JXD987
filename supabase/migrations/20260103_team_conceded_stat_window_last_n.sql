create or replace function public.team_conceded_stat_window_last_n(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_season_id integer default null,
  p_team_id integer default null,
  p_limit integer default 200
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
  with appearances as (
    select
      f.id as fixture_id,
      f.starting_at,
      f.home_team_id as team_id,
      f.away_team_id as opponent_team_id
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (p_team_id is null or f.home_team_id = p_team_id)
    union all
    select
      f.id as fixture_id,
      f.starting_at,
      f.away_team_id as team_id,
      f.home_team_id as opponent_team_id
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
      and (p_team_id is null or f.away_team_id = p_team_id)
  ),
  ranked as (
    select
      a.team_id,
      a.opponent_team_id,
      a.fixture_id,
      a.starting_at,
      row_number() over (
        partition by a.team_id
        order by a.starting_at desc, a.fixture_id desc
      ) as rn
    from appearances a
  ),
  windowed as (
    select *
    from ranked
    where rn <= p_n
  ),
  conceded as (
    select
      w.team_id,
      w.fixture_id,
      w.starting_at,
      coalesce(
        sum(
          case
            when fp.team_id = w.opponent_team_id then fps.value::numeric
            else 0
          end
        ),
        0::numeric
      ) as stat_value
    from windowed w
    left join fixture_players fp
      on fp.fixture_id = w.fixture_id
    left join fixture_player_statistics fps
      on fps.fixture_id = w.fixture_id
     and fps.player_id = fp.player_id
     and fps.type_id = p_type_id
    group by w.team_id, w.fixture_id, w.starting_at
  ),
  agg as (
    select
      team_id,
      count(*) as games_total,
      sum(case when stat_value >= p_threshold then 1 else 0 end) as games_hit,
      avg(stat_value) as avg_value,
      array_agg(stat_value order by starting_at desc, fixture_id desc) as last_values,
      max(starting_at) as latest_starting_at
    from conceded
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
    agg.last_values::numeric[]
  from agg
  left join teams t on t.id = agg.team_id
  where agg.games_total = p_n
  order by agg.games_hit desc, agg.avg_value desc
  limit p_limit;
$$;

grant execute on function public.team_conceded_stat_window_last_n(
  int,
  int,
  int,
  numeric,
  int,
  int,
  int
) to anon, authenticated;
