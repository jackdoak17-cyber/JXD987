create or replace function public.player_stat_leaderboard_meta(
  p_league_id integer,
  p_type_id integer,
  p_season_id integer default null
)
returns table (
  candidate_players_count integer,
  complete_windows_count integer,
  qualified_players_count integer
)
language sql
stable
as $$
  with base as (
    select distinct fps.player_id
    from fixture_player_statistics fps
    join fixtures f on f.id = fps.fixture_id
    where f.league_id = p_league_id
      and fps.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  )
  select
    count(*)::integer as candidate_players_count,
    count(*)::integer as complete_windows_count,
    count(*)::integer as qualified_players_count
  from base;
$$;

create or replace function public.team_stat_leaderboard_meta(
  p_league_id integer,
  p_type_id integer,
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
    select distinct fs.team_id
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and fs.type_id = p_type_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  )
  select
    count(*)::integer as candidate_teams_count,
    count(*)::integer as complete_windows_count,
    count(*)::integer as qualified_teams_count
  from base;
$$;

create or replace function public.team_btts_last_n_meta(
  p_league_id integer,
  p_n integer,
  p_season_id integer default null,
  p_required integer default null
)
returns table (
  candidate_teams_count integer,
  complete_windows_count integer,
  qualified_teams_count integer
)
language sql
stable
as $$
  with appearances as (
    select
      f.id as fixture_id,
      f.starting_at,
      f.home_team_id as team_id,
      (f.home_score > 0 and f.away_score > 0) as btts
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    union all
    select
      f.id as fixture_id,
      f.starting_at,
      f.away_team_id as team_id,
      (f.home_score > 0 and f.away_score > 0) as btts
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  candidate_teams as (
    select team_id
    from appearances
    group by team_id
    having count(*) >= p_n
  ),
  ranked as (
    select
      appearances.*,
      row_number() over (
        partition by team_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from appearances
  ),
  windowed as (
    select * from ranked where rn <= p_n
  ),
  agg as (
    select
      team_id,
      count(*) as games,
      sum(case when btts then 1 else 0 end) as btts_games
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
    (select count(*) from complete where p_required is null or btts_games >= p_required)::integer
      as qualified_teams_count;
$$;

create or replace function public.team_cards_window_meta(
  p_league_id integer,
  p_n integer,
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
      count(*) as games
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
    (select count(*) from complete)::integer as qualified_teams_count;
$$;

create or replace function public.leaderboard_meta(
  p_kind text,
  p_league_id integer,
  p_type_id integer,
  p_season_id integer default null
)
returns table (
  candidate_count integer,
  complete_windows_count integer,
  qualified_count integer
)
language sql
stable
as $$
  select
    case
      when p_kind = 'player' then (select candidate_players_count from public.player_stat_leaderboard_meta(p_league_id, p_type_id, p_season_id))
      else (select candidate_teams_count from public.team_stat_leaderboard_meta(p_league_id, p_type_id, p_season_id))
    end as candidate_count,
    case
      when p_kind = 'player' then (select complete_windows_count from public.player_stat_leaderboard_meta(p_league_id, p_type_id, p_season_id))
      else (select complete_windows_count from public.team_stat_leaderboard_meta(p_league_id, p_type_id, p_season_id))
    end as complete_windows_count,
    case
      when p_kind = 'player' then (select qualified_players_count from public.player_stat_leaderboard_meta(p_league_id, p_type_id, p_season_id))
      else (select qualified_teams_count from public.team_stat_leaderboard_meta(p_league_id, p_type_id, p_season_id))
    end as qualified_count;
$$;
