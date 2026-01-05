create or replace function public.player_recent_starts(
  p_league_id integer,
  p_player_ids bigint[],
  p_recent_games integer default 2
)
returns table (
  player_id bigint,
  starts integer,
  games integer
)
language sql
stable
as $$
  with latest_team as (
    select
      fp.player_id,
      fp.team_id,
      row_number() over (
        partition by fp.player_id
        order by f.starting_at desc, f.id desc
      ) as rn
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and fp.player_id = any(p_player_ids)
      and f.home_score is not null
      and f.away_score is not null
  ),
  player_team as (
    select
      p.id as player_id,
      coalesce(p.team_id, lt.team_id) as team_id
    from players p
    left join latest_team lt
      on lt.player_id = p.id
     and lt.rn = 1
    where p.id = any(p_player_ids)
  ),
  team_recent as (
    select
      pt.team_id,
      f.id as fixture_id,
      f.starting_at,
      row_number() over (
        partition by pt.team_id
        order by f.starting_at desc, f.id desc
      ) as rn
    from (
      select distinct team_id
      from player_team
      where team_id is not null
    ) pt
    join fixtures f
      on (f.home_team_id = pt.team_id or f.away_team_id = pt.team_id)
    where f.league_id = p_league_id
      and f.home_score is not null
      and f.away_score is not null
  ),
  player_recent as (
    select
      pt.player_id,
      tr.team_id,
      tr.fixture_id
    from player_team pt
    join team_recent tr on tr.team_id = pt.team_id
    where tr.rn <= greatest(p_recent_games, 1)
  )
  select
    pr.player_id,
    sum(case when fp.is_starter is true then 1 else 0 end)::integer as starts,
    count(*)::integer as games
  from player_recent pr
  left join fixture_players fp
    on fp.fixture_id = pr.fixture_id
   and fp.player_id = pr.player_id
  group by pr.player_id;
$$;
