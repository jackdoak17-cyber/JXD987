create or replace function public.team_cards_streak_last_n_meta(
  p_league_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null
)
returns table (
  candidate_teams_count integer,
  complete_windows_count integer,
  qualified_teams_count integer
)
language sql
stable
as $$
  with base as (
    select
      fs.team_id,
      fs.fixture_id,
      sum(
        case
          when fs.type_id in (83, 84, 85) then fs.value::numeric
          else 0
        end
      ) as cards_total,
      f.starting_at
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.team_id, fs.fixture_id, f.starting_at
  ),
  candidate_teams as (
    select team_id
    from base
    group by team_id
    having count(*) >= p_n
  ),
  ranked as (
    select
      base.*,
      row_number() over (
        partition by team_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from base
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      team_id,
      count(*) as games,
      sum(case when cards_total >= p_threshold then 1 else 0 end) as games_hit
    from windowed
    group by team_id
  ),
  complete as (
    select *
    from agg
    where games = p_n
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where games_hit >= p_required)::integer as qualified_teams_count;
$$;
