-- Versioned provider snapshots and non-terminal fixture revalidation.
-- Raw fixture facts remain the compatibility serving tables, while this
-- ledger makes the accepted provider revision explicit and auditable.

create table if not exists public.fixture_detail_snapshots (
  id bigint generated always as identity primary key,
  fixture_id bigint not null references public.fixtures(id) on delete cascade,
  league_id bigint,
  season_id bigint,
  payload_hash text not null,
  normalized_hash text not null,
  provider_status text,
  quality_status text not null,
  payload jsonb not null,
  fetched_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  accepted_at timestamptz,
  release_id text,
  error text,
  unique (fixture_id, payload_hash)
);

create index if not exists fixture_detail_snapshots_fixture_idx
  on public.fixture_detail_snapshots (fixture_id, fetched_at desc);
create index if not exists fixture_detail_snapshots_quality_idx
  on public.fixture_detail_snapshots (quality_status, accepted_at);
create index if not exists fixture_detail_delivery_revalidation_idx
  on public.fixture_detail_delivery_status (next_revalidation_at, status);

alter table public.fixture_detail_snapshots enable row level security;
revoke all on table public.fixture_detail_snapshots from public, anon, authenticated;
grant select, insert, update on table public.fixture_detail_snapshots to service_role;
grant usage, select on sequence public.fixture_detail_snapshots_id_seq to service_role;

alter table if exists public.fixture_detail_delivery_status
  add column if not exists last_checked_at timestamptz;
alter table if exists public.fixture_detail_delivery_status
  add column if not exists next_revalidation_at timestamptz;
alter table if exists public.fixture_detail_delivery_status
  add column if not exists last_payload_hash text;
alter table if exists public.fixture_detail_delivery_status
  add column if not exists last_normalized_hash text;
alter table if exists public.fixture_detail_delivery_status
  add column if not exists stable_fetch_count integer not null default 0;
alter table if exists public.fixture_detail_delivery_status
  add column if not exists accepted_snapshot_id bigint references public.fixture_detail_snapshots(id);

alter table if exists public.fixture_players
  add column if not exists provider_snapshot_id bigint references public.fixture_detail_snapshots(id);
alter table if exists public.fixture_statistics
  add column if not exists provider_snapshot_id bigint references public.fixture_detail_snapshots(id);
alter table if exists public.fixture_player_statistics
  add column if not exists provider_snapshot_id bigint references public.fixture_detail_snapshots(id);

create index if not exists fixture_players_snapshot_idx
  on public.fixture_players (provider_snapshot_id);
create index if not exists fixture_statistics_snapshot_idx
  on public.fixture_statistics (provider_snapshot_id);
create index if not exists fixture_player_statistics_snapshot_idx
  on public.fixture_player_statistics (provider_snapshot_id);
