-- Extend the serving ledger with player-stat coverage and projection failures.
-- This migration is separate because fixture_detail_delivery_status was
-- introduced by the web release and may already exist in production.

alter table if exists public.fixture_detail_delivery_status
  add column if not exists provider_player_stat_types jsonb;
alter table if exists public.fixture_detail_delivery_status
  add column if not exists provider_missing_player_type_ids jsonb;

do $$
begin
  if to_regclass('public.fixture_detail_delivery_status') is not null then
    alter table public.fixture_detail_delivery_status
      drop constraint if exists fixture_detail_delivery_status_status_check;
    alter table public.fixture_detail_delivery_status
      add constraint fixture_detail_delivery_status_status_check check (
        status in (
          'new', 'running', 'provider_pending', 'provider_sparse',
          'verified', 'failed', 'export_failed', 'verification_failed', 'excluded',
          'projection_failed'
        )
      );
  end if;
end;
$$;
