-- Replace one fixture's raw detail in one target-side transaction.
-- The worker calls this function after source validation.  A failed statement
-- rolls back the deletes and inserts together, so an exporter failure cannot
-- leave a previously accepted fixture silently empty.

create or replace function public.publish_fixture_detail_atomic(
  p_fixture_id bigint,
  p_snapshot_id bigint,
  p_fixture_players jsonb,
  p_fixture_statistics jsonb,
  p_fixture_player_statistics jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  fixture_player_count integer;
  fixture_stat_count integer;
  fixture_player_stat_count integer;
begin
  delete from public.fixture_player_statistics where fixture_id = p_fixture_id;
  delete from public.fixture_statistics where fixture_id = p_fixture_id;
  delete from public.fixture_players where fixture_id = p_fixture_id;

  insert into public.fixture_players (
    fixture_id, player_id, team_id, is_starter, minutes_played,
    position_name, detailed_position_id, detailed_position_name,
    detailed_position_code, formation_field, lineup_detailed_position_id,
    lineup_detailed_position_name, lineup_detailed_position_code,
    formation_position, position_abbr, provider_snapshot_id
  )
  select
    p_fixture_id, payload.player_id, payload.team_id, payload.is_starter,
    payload.minutes_played, payload.position_name,
    payload.detailed_position_id, payload.detailed_position_name,
    payload.detailed_position_code, payload.formation_field,
    payload.lineup_detailed_position_id, payload.lineup_detailed_position_name,
    payload.lineup_detailed_position_code, payload.formation_position,
    payload.position_abbr, p_snapshot_id
  from jsonb_to_recordset(coalesce(p_fixture_players, '[]'::jsonb)) as payload(
    player_id bigint,
    team_id bigint,
    name text,
    position text,
    lineup_type text,
    formation_position integer,
    jersey_number text,
    is_starter boolean,
    minutes_played integer,
    position_name text,
    detailed_position_id bigint,
    detailed_position_name text,
    detailed_position_code text,
    formation_field text,
    lineup_detailed_position_id bigint,
    lineup_detailed_position_name text,
    lineup_detailed_position_code text,
    position_abbr text,
    extra jsonb
  );

  insert into public.fixture_statistics (
    fixture_id, team_id, type_id, value, provider_snapshot_id
  )
  select
    p_fixture_id, payload.team_id, payload.type_id, payload.value, p_snapshot_id
  from jsonb_to_recordset(coalesce(p_fixture_statistics, '[]'::jsonb)) as payload(
    team_id bigint,
    type_id bigint,
    code text,
    name text,
    location text,
    value numeric,
    extra jsonb
  );

  insert into public.fixture_player_statistics (
    fixture_id, player_id, team_id, type_id, value, provider_snapshot_id
  )
  select
    p_fixture_id, payload.player_id, payload.team_id, payload.type_id,
    payload.value, p_snapshot_id
  from jsonb_to_recordset(coalesce(p_fixture_player_statistics, '[]'::jsonb)) as payload(
    player_id bigint,
    team_id bigint,
    type_id bigint,
    code text,
    name text,
    value numeric,
    extra jsonb
  );

  select count(*) into fixture_player_count
    from public.fixture_players where fixture_id = p_fixture_id;
  select count(*) into fixture_stat_count
    from public.fixture_statistics where fixture_id = p_fixture_id;
  select count(*) into fixture_player_stat_count
    from public.fixture_player_statistics where fixture_id = p_fixture_id;

  return jsonb_build_object(
    'fixture_id', p_fixture_id,
    'fixture_players', fixture_player_count,
    'fixture_statistics', fixture_stat_count,
    'fixture_player_statistics', fixture_player_stat_count
  );
end;
$$;

revoke all on function public.publish_fixture_detail_atomic(bigint, bigint, jsonb, jsonb, jsonb) from public, anon, authenticated;
grant execute on function public.publish_fixture_detail_atomic(bigint, bigint, jsonb, jsonb, jsonb) to service_role;
