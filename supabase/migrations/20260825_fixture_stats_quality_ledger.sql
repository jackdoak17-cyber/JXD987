-- Durable fixture identity and provider-availability ledger for customer stats.
-- Raw provider/fixture rows are retained for auditability; only rows recorded
-- here are prevented from contributing to team/player statistics.

create table if not exists public.fixture_stats_quality_exclusions (
  fixture_id bigint primary key references public.fixtures(id) on delete cascade,
  league_id bigint,
  season_id bigint,
  canonical_fixture_id bigint references public.fixtures(id) on delete restrict,
  exclusion_type text not null check (exclusion_type in ('duplicate', 'provider_unavailable')),
  reason text not null,
  source text not null default 'stats_integrity_audit',
  first_identified_at timestamptz not null default now(),
  last_checked_at timestamptz not null default now(),
  next_review_at timestamptz,
  evidence jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  check (canonical_fixture_id is null or canonical_fixture_id <> fixture_id)
);

create index if not exists fixture_stats_quality_exclusions_scope_idx
  on public.fixture_stats_quality_exclusions (league_id, season_id, exclusion_type);

alter table public.fixture_stats_quality_exclusions enable row level security;
revoke all on table public.fixture_stats_quality_exclusions from public, anon, authenticated;
grant select, insert, update, delete on table public.fixture_stats_quality_exclusions to service_role;

create or replace function public.stats_fixture_is_excluded(p_fixture_id bigint)
returns boolean
language sql
stable
security definer
set search_path = 'pg_catalog', 'public'
as $function$
  select exists (
    select 1
      from public.fixture_stats_quality_exclusions x
     where x.fixture_id = p_fixture_id
       and (x.next_review_at is null or x.next_review_at > now())
  );
$function$;

revoke all on function public.stats_fixture_is_excluded(bigint) from public, anon;
grant execute on function public.stats_fixture_is_excluded(bigint) to authenticated, service_role;
do $$
begin
  if exists (select 1 from pg_roles where rolname = 'statsweb_pool') then
    grant execute on function public.stats_fixture_is_excluded(bigint) to statsweb_pool;
  end if;
end;
$$;

do $$
declare
  v_definition text;
  v_rewritten text;
begin
  if to_regprocedure('public.team_stats_season_table(integer,integer)') is not null then
    select pg_get_functiondef('public.team_stats_season_table(integer,integer)'::regprocedure)
      into v_definition;
    if position('stats_fixture_is_excluded' in v_definition) = 0 then
      v_rewritten := replace(
        v_definition,
        E'      and f.season_id = p_season_id\n',
        E'      and f.season_id = p_season_id\n      and not public.stats_fixture_is_excluded(f.id)\n'
      );
      if v_rewritten = v_definition then
        raise exception 'Could not add fixture-quality predicate to team stats function';
      end if;
      execute v_rewritten;
    end if;
  end if;

  if to_regprocedure('public.refresh_player_stats_season(integer,integer,bigint)') is not null then
    select pg_get_functiondef('public.refresh_player_stats_season(integer,integer,bigint)'::regprocedure)
      into v_definition;
    if position('stats_fixture_is_excluded' in v_definition) = 0 then
      v_rewritten := replace(
        v_definition,
        E'      and fp.team_id is not null\n',
        E'      and fp.team_id is not null\n      and not public.stats_fixture_is_excluded(f.id)\n'
      );
      if v_rewritten = v_definition then
        raise exception 'Could not add fixture-quality predicate to player refresh function';
      end if;
      execute v_rewritten;
    end if;
  end if;
end;
$$;

-- SportMonks exposes 19806634 as the usable canonical identity for this match.
-- The other IDs are retained as raw fixture rows but are excluded from all
-- stats projections to prevent duplicate standings/player contributions.
insert into public.fixture_stats_quality_exclusions (
  fixture_id, league_id, season_id, canonical_fixture_id, exclusion_type,
  reason, evidence, last_checked_at, updated_at
) values
  (19852081, 24, 28020, 19806634, 'duplicate', 'same teams, kickoff and score as canonical provider fixture', '{"canonical":19806634,"duplicate_ids":[19852081,19855030,19859823,19863882,19866210]}'::jsonb, now(), now()),
  (19855030, 24, 28020, 19806634, 'duplicate', 'same teams, kickoff and score as canonical provider fixture', '{"canonical":19806634,"duplicate_ids":[19852081,19855030,19859823,19863882,19866210]}'::jsonb, now(), now()),
  (19859823, 24, 28020, 19806634, 'duplicate', 'same teams, kickoff and score as canonical provider fixture', '{"canonical":19806634,"duplicate_ids":[19852081,19855030,19859823,19863882,19866210]}'::jsonb, now(), now()),
  (19863882, 24, 28020, 19806634, 'duplicate', 'same teams, kickoff and score as canonical provider fixture', '{"canonical":19806634,"duplicate_ids":[19852081,19855030,19859823,19863882,19866210]}'::jsonb, now(), now()),
  (19866210, 24, 28020, 19806634, 'duplicate', 'second provider identity for the same teams, kickoff and score', '{"canonical":19806634,"duplicate_ids":[19852081,19855030,19859823,19863882,19866210]}'::jsonb, now(), now())
on conflict (fixture_id) do update set
  league_id = excluded.league_id,
  season_id = excluded.season_id,
  canonical_fixture_id = excluded.canonical_fixture_id,
  exclusion_type = excluded.exclusion_type,
  reason = excluded.reason,
  evidence = excluded.evidence,
  last_checked_at = excluded.last_checked_at,
  updated_at = excluded.updated_at;

do $$
begin
  if to_regclass('public.fixture_detail_delivery_status') is not null then
    alter table public.fixture_detail_delivery_status
      drop constraint if exists fixture_detail_delivery_status_status_check;
    alter table public.fixture_detail_delivery_status
      add constraint fixture_detail_delivery_status_status_check check (
        status in (
          'new', 'running', 'provider_pending', 'provider_sparse',
          'verified', 'failed', 'export_failed', 'verification_failed',
          'projection_failed', 'excluded'
        )
      );
    update public.fixture_detail_delivery_status
       set status = 'excluded',
           next_attempt_at = '9999-12-31 00:00:00+00'::timestamptz,
           last_error = 'Excluded from stats: duplicate fixture identity; see fixture_stats_quality_exclusions',
           updated_at = now()
     where fixture_id in (19852081, 19855030, 19859823, 19863882, 19866210);
  end if;
end;
$$;
