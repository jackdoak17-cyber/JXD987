create or replace function public.player_recent_positions(
  p_player_ids int[],
  p_limit int default 5
)
returns table (
  player_id int,
  position_name text,
  detailed_position_name text,
  lineup_detailed_position_name text,
  detailed_position_id int,
  lineup_detailed_position_id int,
  starting_at timestamptz
)
language sql
stable
as $$
  select
    player_id,
    position_name,
    detailed_position_name,
    lineup_detailed_position_name,
    detailed_position_id,
    lineup_detailed_position_id,
    starting_at
  from (
    select
      fp.player_id,
      fp.position_name,
      fp.detailed_position_name,
      fp.lineup_detailed_position_name,
      fp.detailed_position_id,
      fp.lineup_detailed_position_id,
      f.starting_at,
      row_number() over (
        partition by fp.player_id
        order by f.starting_at desc, fp.fixture_id desc
      ) as rn
    from public.fixture_players fp
    join public.fixtures f on f.id = fp.fixture_id
    where fp.player_id = any(p_player_ids)
  ) ranked
  where rn <= greatest(p_limit, 1);
$$;
