-- Make referee ingestion observable and safe to retry.
-- The production worker uses this state table to distinguish a successful
-- provider response with no appointment from a failed or never-run sync.
create table if not exists public.fixture_referee_sync_state (
  fixture_id bigint primary key references public.fixtures(id) on delete cascade,
  status text not null default 'pending',
  last_attempted_at timestamptz,
  last_successful_at timestamptz,
  next_attempt_at timestamptz not null default now(),
  attempt_count integer not null default 0,
  assignment_count integer not null default 0,
  response_hash text,
  last_error text,
  updated_at timestamptz not null default now(),
  constraint fixture_referee_sync_state_status_check
    check (status in ('pending', 'assigned', 'no_assignment', 'error'))
);

create index if not exists fixture_referee_sync_state_due_idx
  on public.fixture_referee_sync_state(next_attempt_at, status);

create index if not exists fixture_referee_sync_state_status_idx
  on public.fixture_referee_sync_state(status, last_successful_at);

alter table public.fixture_referee_stats
  add column if not exists data_status text not null default 'unknown',
  add column if not exists history_through timestamptz,
  add column if not exists calculation_version text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fixture_referee_stats_data_status_check'
  ) then
    alter table public.fixture_referee_stats
      add constraint fixture_referee_stats_data_status_check
      check (data_status in ('unknown', 'ready', 'limited_history', 'not_available', 'failed'));
  end if;
end
$$;

alter table public.fixture_referee_sync_state enable row level security;
