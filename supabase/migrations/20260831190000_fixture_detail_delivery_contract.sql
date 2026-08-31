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

do $$
begin
  if to_regclass('public.fixture_detail_delivery_status') is not null
     and to_regclass('public.fixtures') is not null
     and to_regclass('public.fixture_players') is not null
     and to_regclass('public.fixture_statistics') is not null
     and to_regclass('public.fixture_player_statistics') is not null then
    -- Backfill cheap scalar serving counts from the existing indexed detail
    -- tables.  These counts are evidence only; they do not make a legacy row
    -- accepted without a snapshot/parity record.
    update public.fixture_detail_delivery_status d
       set target_lineup_count = coalesce((
             select count(distinct fp.player_id)::integer
               from public.fixture_players fp
              where fp.fixture_id = d.fixture_id
           ), 0),
           target_player_stat_count = coalesce((
             select count(distinct (fps.player_id, fps.team_id, fps.type_id))::integer
               from public.fixture_player_statistics fps
              where fps.fixture_id = d.fixture_id
           ), 0),
           target_team_stat_count = coalesce((
             select count(distinct (fs.team_id, fs.type_id))::integer
               from public.fixture_statistics fs
              where fs.fixture_id = d.fixture_id
           ), 0);
  end if;
end;
$$;

do $$
begin
  if to_regclass('public.fixture_detail_delivery_status') is not null then
    -- Accepted snapshots already contain canonical source/target evidence in
    -- the pre-contract ledger.  Carry exact value parity forward where it is
    -- available; producer revalidation fills any remaining legacy nulls.
    update public.fixture_detail_delivery_status
       set player_stat_parity = case
             when source_snapshot->'player_stat_values' is not null
              and target_snapshot->'player_stat_values' is not null
             then source_snapshot->'player_stat_values' = target_snapshot->'player_stat_values'
             else player_stat_parity
           end,
           lineup_parity = case
             when source_snapshot->'lineup_values' is not null
              and target_snapshot->'lineup_values' is not null
             then source_snapshot->'lineup_values' = target_snapshot->'lineup_values'
             else lineup_parity
           end,
           parity_checked_at = case
             when source_snapshot->'player_stat_values' is not null
              and target_snapshot->'player_stat_values' is not null
              and source_snapshot->'lineup_values' is not null
              and target_snapshot->'lineup_values' is not null
             then coalesce(parity_checked_at, updated_at, now())
             else parity_checked_at
           end
     where status in ('provider_sparse', 'verified');
  end if;
end;
$$;

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
