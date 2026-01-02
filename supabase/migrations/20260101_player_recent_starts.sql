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
  with ranked as (
    select
      fp.player_id,
      fp.is_starter,
      f.starting_at,
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
      and coalesce(fp.minutes_played, 0) > 0
  )
  select
    player_id,
    sum(case when is_starter is true then 1 else 0 end)::integer as starts,
    count(*)::integer as games
  from ranked
  where rn <= greatest(p_recent_games, 1)
  group by player_id;
$$;
