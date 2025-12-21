create or replace function public.stat_type_counts_by_league(
  p_league_id integer
)
returns table (
  type_id integer,
  player_rows bigint,
  player_players_count bigint,
  team_rows bigint,
  team_teams_count bigint
)
language sql
stable
as $$
  with player_stats as (
    select
      fps.type_id,
      count(*)::bigint as player_rows,
      count(distinct fps.player_id)::bigint as player_players_count
    from fixture_player_statistics fps
    join fixtures f on f.id = fps.fixture_id
    where f.league_id = p_league_id
      and f.home_score is not null
      and f.away_score is not null
    group by fps.type_id
  ),
  team_stats as (
    select
      fs.type_id,
      count(*)::bigint as team_rows,
      count(distinct fs.team_id)::bigint as team_teams_count
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and f.home_score is not null
      and f.away_score is not null
    group by fs.type_id
  )
  select
    coalesce(player_stats.type_id, team_stats.type_id) as type_id,
    player_stats.player_rows,
    player_stats.player_players_count,
    team_stats.team_rows,
    team_stats.team_teams_count
  from player_stats
  full join team_stats on team_stats.type_id = player_stats.type_id;
$$;
