begin;

-- Dependency-aware replacement for the original fixture-detail publisher.
-- Every validation and dimension insert occurs in the same transaction before
-- any active fixture rows are deleted.  A missing parent therefore fails
-- without turning a previously accepted fixture into an empty one.
create or replace function public.publish_fixture_detail_atomic_v2(
  p_fixture_id bigint,
  p_snapshot_id bigint,
  p_players jsonb,
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
  fixture_home_team_id bigint;
  fixture_away_team_id bigint;
  missing_team_id bigint;
  missing_player_id bigint;
  fixture_player_count integer;
  fixture_stat_count integer;
  fixture_player_stat_count integer;
begin
  select home_team_id, away_team_id
    into fixture_home_team_id, fixture_away_team_id
    from public.fixtures
   where id = p_fixture_id;
  if not found then
    raise exception using
      errcode = '23503',
      message = format('fixture-detail dependency missing: fixture id %s', p_fixture_id);
  end if;

  if p_snapshot_id is not null
     and not exists (
       select 1
         from public.fixture_detail_snapshots
        where id = p_snapshot_id
          and fixture_id = p_fixture_id
     ) then
    raise exception using
      errcode = '23503',
      message = format('fixture-detail dependency missing: snapshot id %s for fixture %s', p_snapshot_id, p_fixture_id);
  end if;

  if not exists (select 1 from public.teams where id = fixture_home_team_id) then
    raise exception using
      errcode = '23503',
      message = format('fixture-detail dependency missing: home team id %s', fixture_home_team_id);
  end if;
  if not exists (select 1 from public.teams where id = fixture_away_team_id) then
    raise exception using
      errcode = '23503',
      message = format('fixture-detail dependency missing: away team id %s', fixture_away_team_id);
  end if;

  if exists (
    select 1
      from jsonb_to_recordset(coalesce(p_fixture_players, '[]'::jsonb)) as payload(
        fixture_id bigint, player_id bigint, team_id bigint
      )
     where fixture_id is not null and fixture_id <> p_fixture_id
  ) or exists (
    select 1
      from jsonb_to_recordset(coalesce(p_fixture_statistics, '[]'::jsonb)) as payload(
        fixture_id bigint, team_id bigint, type_id bigint
      )
     where fixture_id is not null and fixture_id <> p_fixture_id
  ) or exists (
    select 1
      from jsonb_to_recordset(coalesce(p_fixture_player_statistics, '[]'::jsonb)) as payload(
        fixture_id bigint, player_id bigint, team_id bigint, type_id bigint
      )
     where fixture_id is not null and fixture_id <> p_fixture_id
  ) then
    raise exception using
      errcode = '22023',
      message = format('fixture-detail payload contains rows for another fixture; expected %s', p_fixture_id);
  end if;

  if exists (
    select 1
      from jsonb_to_recordset(coalesce(p_fixture_players, '[]'::jsonb)) as payload(
        fixture_id bigint, player_id bigint, team_id bigint
      )
     where player_id is null or team_id is null
  ) or exists (
    select 1
      from jsonb_to_recordset(coalesce(p_fixture_statistics, '[]'::jsonb)) as payload(
        fixture_id bigint, team_id bigint, type_id bigint
      )
     where team_id is null or type_id is null
  ) or exists (
    select 1
      from jsonb_to_recordset(coalesce(p_fixture_player_statistics, '[]'::jsonb)) as payload(
        fixture_id bigint, player_id bigint, team_id bigint, type_id bigint
      )
     where player_id is null or team_id is null or type_id is null
  ) then
    raise exception using
      errcode = '22023',
      message = format('fixture-detail payload contains a null required identity for fixture %s', p_fixture_id);
  end if;

  select min(team_id)
    into missing_team_id
    from (
      select team_id
        from jsonb_to_recordset(coalesce(p_fixture_players, '[]'::jsonb)) as payload(
          fixture_id bigint, player_id bigint, team_id bigint
        )
      union all
      select team_id
        from jsonb_to_recordset(coalesce(p_fixture_statistics, '[]'::jsonb)) as payload(
          fixture_id bigint, team_id bigint, type_id bigint
        )
      union all
      select team_id
        from jsonb_to_recordset(coalesce(p_fixture_player_statistics, '[]'::jsonb)) as payload(
          fixture_id bigint, player_id bigint, team_id bigint, type_id bigint
        )
    ) referenced
   where not exists (select 1 from public.teams t where t.id = referenced.team_id);
  if missing_team_id is not null then
    raise exception using
      errcode = '23503',
      message = format('fixture-detail dependency missing: referenced team id %s', missing_team_id);
  end if;

  if exists (
    select 1
      from jsonb_to_recordset(coalesce(p_players, '[]'::jsonb)) as payload(
        id bigint, name text, display_name text, short_name text,
        common_name text, team_id bigint, team_updated_at timestamptz,
        image_path text
      )
     where id is null or nullif(btrim(name), '') is null
  ) then
    raise exception using
      errcode = '22023',
      message = format('fixture-detail player dimension contains a null id/name for fixture %s', p_fixture_id);
  end if;

  -- Insert-only is intentional: fixture delivery may discover a player that is
  -- absent from the serving dimension, but it must never rewrite a canonical
  -- player assignment or display identity owned by another pipeline.
  insert into public.players (
    id, name, display_name, short_name, common_name, team_id,
    team_updated_at, image_path
  )
  select
    payload.id, payload.name, payload.display_name, payload.short_name,
    payload.common_name, payload.team_id, payload.team_updated_at,
    payload.image_path
    from jsonb_to_recordset(coalesce(p_players, '[]'::jsonb)) as payload(
      id bigint, name text, display_name text, short_name text,
      common_name text, team_id bigint, team_updated_at timestamptz,
      image_path text
    )
  on conflict (id) do nothing;

  select min(player_id)
    into missing_player_id
    from (
      select player_id
        from jsonb_to_recordset(coalesce(p_fixture_players, '[]'::jsonb)) as payload(
          fixture_id bigint, player_id bigint, team_id bigint
        )
      union all
      select player_id
        from jsonb_to_recordset(coalesce(p_fixture_player_statistics, '[]'::jsonb)) as payload(
          fixture_id bigint, player_id bigint, team_id bigint, type_id bigint
        )
    ) referenced
   where not exists (select 1 from public.players p where p.id = referenced.player_id);
  if missing_player_id is not null then
    raise exception using
      errcode = '23503',
      message = format('fixture-detail dependency missing: referenced player id %s', missing_player_id);
  end if;

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
    fixture_id bigint, player_id bigint, team_id bigint, name text,
    position text, lineup_type text, formation_position integer,
    jersey_number text, is_starter boolean, minutes_played integer,
    position_name text, detailed_position_id bigint,
    detailed_position_name text, detailed_position_code text,
    formation_field text, lineup_detailed_position_id bigint,
    lineup_detailed_position_name text, lineup_detailed_position_code text,
    position_abbr text, extra jsonb
  );

  insert into public.fixture_statistics (
    fixture_id, team_id, type_id, value, provider_snapshot_id
  )
  select
    p_fixture_id, payload.team_id, payload.type_id, payload.value, p_snapshot_id
  from jsonb_to_recordset(coalesce(p_fixture_statistics, '[]'::jsonb)) as payload(
    fixture_id bigint, team_id bigint, type_id bigint, code text,
    name text, location text, value numeric, extra jsonb
  );

  insert into public.fixture_player_statistics (
    fixture_id, player_id, team_id, type_id, value, provider_snapshot_id
  )
  select
    p_fixture_id, payload.player_id, payload.team_id, payload.type_id,
    payload.value, p_snapshot_id
  from jsonb_to_recordset(coalesce(p_fixture_player_statistics, '[]'::jsonb)) as payload(
    fixture_id bigint, player_id bigint, team_id bigint, type_id bigint,
    code text, name text, value numeric, extra jsonb
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

-- Keep the historical signature callable, but make it fail closed through the
-- preflight-safe implementation.  Existing callers can no longer bypass the
-- dependency boundary merely because they have not been upgraded yet.
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
begin
  return public.publish_fixture_detail_atomic_v2(
    p_fixture_id,
    p_snapshot_id,
    '[]'::jsonb,
    p_fixture_players,
    p_fixture_statistics,
    p_fixture_player_statistics
  );
end;
$$;

revoke all on function public.publish_fixture_detail_atomic_v2(bigint, bigint, jsonb, jsonb, jsonb, jsonb) from public, anon, authenticated;
grant execute on function public.publish_fixture_detail_atomic_v2(bigint, bigint, jsonb, jsonb, jsonb, jsonb) to service_role;
revoke all on function public.publish_fixture_detail_atomic(bigint, bigint, jsonb, jsonb, jsonb) from public, anon, authenticated;
grant execute on function public.publish_fixture_detail_atomic(bigint, bigint, jsonb, jsonb, jsonb) to service_role;

commit;
