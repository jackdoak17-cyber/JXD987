create or replace function public.teams_playing_on_date(
  p_date date,
  p_league_id integer,
  p_season_id integer default null
)
returns table (
  team_id integer,
  fixture_count integer
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
  teams as (
    select tf.home_team_id as team_id
    from todays_fixtures tf
    where tf.home_team_id is not null
    union all
    select tf.away_team_id as team_id
    from todays_fixtures tf
    where tf.away_team_id is not null
  )
  select
    team_id::integer as team_id,
    count(*)::integer as fixture_count
  from teams
  group by team_id;
$$;
