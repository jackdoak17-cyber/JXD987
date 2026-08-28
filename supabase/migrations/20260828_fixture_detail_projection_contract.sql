-- Bring already-deployed read-model functions to the explicit delivery
-- eligibility contract without rewriting an applied historical migration.

do $$
declare
  v_definition text;
  v_rewritten text;
begin
  if to_regprocedure('public.refresh_player_stats_season(integer,integer,bigint)') is not null
     and to_regprocedure('public.refresh_player_stats_season_eligible(integer,integer,bigint,bigint[])') is null then
    select pg_get_functiondef(
      'public.refresh_player_stats_season(integer,integer,bigint)'::regprocedure
    ) into v_definition;

    v_rewritten := replace(
      v_definition,
      'public.refresh_player_stats_season(',
      'public.refresh_player_stats_season_eligible('
    );
    v_rewritten := replace(
      v_rewritten,
      'p_player_id bigint DEFAULT NULL::bigint',
      'p_player_id bigint DEFAULT NULL::bigint, p_fixture_ids bigint[] DEFAULT NULL::bigint[]'
    );
    if position('p_fixture_ids' in v_rewritten) = 0 then
      v_rewritten := replace(
        v_rewritten,
        'p_player_id bigint DEFAULT NULL',
        'p_player_id bigint DEFAULT NULL, p_fixture_ids bigint[] DEFAULT NULL'
      );
    end if;
    if position('p_fixture_ids' in v_rewritten) = 0 then
      raise exception 'Could not add explicit fixture eligibility parameter to player projection function';
    end if;
    if position('fixture_detail_delivery_status' in v_rewritten) = 0 then
      v_rewritten := replace(
        v_rewritten,
        E'      and not public.stats_fixture_is_excluded(f.id)\n',
        E'      and not public.stats_fixture_is_excluded(f.id)\n      and (\n        (p_fixture_ids is not null and f.id = any(p_fixture_ids))\n        or exists (\n          select 1\n            from public.fixture_detail_delivery_status d\n           where d.fixture_id = f.id\n             and d.status in (''verified'', ''provider_sparse'')\n        )\n      )\n'
      );
    end if;
    execute v_rewritten;
  elsif to_regprocedure('public.refresh_player_stats_season(integer,integer,bigint,bigint[])') is not null
        and to_regprocedure('public.refresh_player_stats_season_eligible(integer,integer,bigint,bigint[])') is null then
    execute $function$
      create or replace function public.refresh_player_stats_season_eligible(
        p_league_id integer,
        p_season_id integer,
        p_player_id bigint default null,
        p_fixture_ids bigint[] default null
      ) returns integer
      language sql volatile
      set search_path to 'pg_catalog', 'public'
      as $body$
        select public.refresh_player_stats_season(
          p_league_id, p_season_id, p_player_id, p_fixture_ids
        );
      $body$;
    $function$;
  end if;

  if to_regprocedure('public.refresh_player_stats_for_fixture(bigint)') is not null then
    select pg_get_functiondef(
      'public.refresh_player_stats_for_fixture(bigint)'::regprocedure
    ) into v_definition;
    v_rewritten := replace(
      v_definition,
      'public.refresh_player_stats_season(v_league_id, v_season_id, null)',
      'public.refresh_player_stats_season_eligible(v_league_id, v_season_id, null, array[p_fixture_id])'
    );
    if v_rewritten <> v_definition then
      execute v_rewritten;
    end if;
  end if;
end;
$$;

revoke all on function public.refresh_player_stats_season_eligible(integer, integer, bigint, bigint[]) from public, anon;
grant execute on function public.refresh_player_stats_season_eligible(integer, integer, bigint, bigint[]) to service_role;
