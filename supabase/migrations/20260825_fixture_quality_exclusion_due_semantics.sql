-- A review date makes a quarantined fixture eligible for a provider retry; it
-- must not make stale raw facts eligible for customer statistics by itself.
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
  );
$function$;
