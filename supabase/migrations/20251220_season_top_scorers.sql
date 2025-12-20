create or replace function public.season_top_scorers(
  p_league_id integer,
  p_season_id integer default null,
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
  goals_total numeric
)
language sql
stable
as $$
  with scoring as (
    select
      fps.player_id,
      sum(fps.value)::numeric as goals_total
    from fixture_player_statistics fps
    join fixtures f on f.id = fps.fixture_id
    where fps.type_id = 52
      and f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fps.player_id
  ),
  latest_app as (
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
    join scoring s on s.player_id = fp.player_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  latest as (
    select
      player_id,
      team_id as latest_team_id
    from latest_app
    where rn = 1
  )
  select
    scoring.player_id::integer,
    p.name::text as player_name,
    p.common_name::text as player_common_name,
    p.short_name::text as player_short_name,
    latest.latest_team_id::integer as team_id,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    scoring.goals_total::numeric as goals_total
  from scoring
  join players p on p.id = scoring.player_id
  left join latest on latest.player_id = scoring.player_id
  left join teams t on t.id = latest.latest_team_id
  order by scoring.goals_total desc
  limit p_limit;
$$;
