begin;

-- Version the producer/serving evidence contract.  Operations must be able to
-- classify a delivery from durable fields; it must never parse last_error or
-- infer acceptance from the presence of raw public rows.
alter table if exists public.fixture_detail_delivery_status
  add column if not exists delivery_contract_version integer not null default 2,
  add column if not exists reason_code text not null default 'legacy_unclassified',
  add column if not exists player_stat_parity boolean,
  add column if not exists lineup_parity boolean,
  add column if not exists target_player_stat_count integer not null default 0,
  add column if not exists target_lineup_count integer not null default 0,
  add column if not exists target_team_stat_count integer not null default 0,
  add column if not exists parity_checked_at timestamptz;

do $$
begin
  if to_regclass('public.fixture_detail_delivery_status') is not null then
    -- Rows that existed before this contract was installed remain explicitly
    -- legacy until the producer rewrites them with contract version 2.  This
    -- makes a missing parity field a migration/configuration observation, not
    -- an ungrounded data-loss claim.
    update public.fixture_detail_delivery_status
       set delivery_contract_version = 1
     where reason_code = 'legacy_unclassified'
       and delivery_contract_version = 2;

    alter table public.fixture_detail_delivery_status
      drop constraint if exists fixture_detail_delivery_status_reason_code_check;
    alter table public.fixture_detail_delivery_status
      add constraint fixture_detail_delivery_status_reason_code_check check (
        reason_code in (
          'new',
          'running',
          'provider_pending_structure',
          'provider_pending_identity',
          'provider_pending_optional_metrics',
          'provider_pending_shrink',
          'provider_unavailable',
          'accepted',
          'export_failed',
          'dependency_missing',
          'verification_failed',
          'projection_failed',
          'excluded',
          'legacy_unclassified',
          'unknown'
        )
      );

    -- One-time classification for rows written by the pre-contract worker.
    -- Future writes use the structured producer value and never depend on
    -- these text heuristics.
    update public.fixture_detail_delivery_status
       set reason_code = case
         when status = 'new' then 'new'
         when status = 'running' then 'running'
         when status = 'provider_pending'
              and lower(coalesce(last_error, '')) like '%optional provider%' then 'provider_pending_optional_metrics'
         when status = 'provider_pending'
              and lower(coalesce(last_error, '')) like '%shrank%' then 'provider_pending_shrink'
         when status = 'provider_pending'
              and (
                lower(coalesce(last_error, '')) like '%identity%'
                or lower(coalesce(last_error, '')) like '%lineup/player%'
              ) then 'provider_pending_identity'
         when status = 'provider_pending' then 'provider_pending_structure'
         when status in ('provider_sparse', 'verified') then 'accepted'
         when status = 'export_failed'
              and lower(coalesce(last_error, '')) like '%foreign key%' then 'dependency_missing'
         when status = 'export_failed' then 'export_failed'
         when status = 'failed' then 'export_failed'
         when status = 'verification_failed' then 'verification_failed'
         when status = 'projection_failed' then 'projection_failed'
         when status = 'excluded'
              and lower(coalesce(last_error, '')) like '%provider%' then 'provider_unavailable'
         when status = 'excluded' then 'excluded'
         else 'legacy_unclassified'
       end
     where reason_code = 'legacy_unclassified';
  end if;
end;
$$;

-- Do not scan the large fixture-detail tables during this schema migration.
-- Existing rows remain version 1 until the normal producer revalidates them;
-- only v2 producer writes are allowed to claim target counts and parity. This
-- keeps a metadata rollout within the database statement-timeout budget and
-- avoids making historical row presence look like fresh acceptance evidence.

do $$
begin
  if to_regclass('public.fixture_detail_delivery_status') is not null then
    create index if not exists fixture_detail_delivery_status_evidence_idx
      on public.fixture_detail_delivery_status (league_id, status, updated_at desc);
    create index if not exists fixture_detail_delivery_status_parity_idx
      on public.fixture_detail_delivery_status (fixture_id, accepted_snapshot_id, updated_at desc);
  end if;
end;
$$;

commit;
