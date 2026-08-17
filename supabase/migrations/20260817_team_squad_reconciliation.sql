-- Durable provider-snapshot audit trail for current squad reconciliation.
-- Consumers continue to use players.team_id; this table makes the source
-- snapshot, freshness and removals observable and independently verifiable.

create table if not exists public.team_squad_snapshots (
  id bigserial primary key,
  team_id integer not null references public.teams(id) on delete cascade,
  source text not null default 'sportmonks',
  status text not null check (status in ('success', 'empty', 'failed')),
  observed_at timestamptz not null,
  completed_at timestamptz,
  player_count integer not null default 0 check (player_count >= 0),
  payload_hash text,
  error text,
  created_at timestamptz not null default now()
);

create index if not exists team_squad_snapshots_team_observed_idx
  on public.team_squad_snapshots(team_id, observed_at desc);
create index if not exists team_squad_snapshots_status_observed_idx
  on public.team_squad_snapshots(status, observed_at desc);

create table if not exists public.team_squad_memberships (
  team_id integer not null references public.teams(id) on delete cascade,
  player_id integer not null references public.players(id) on delete cascade,
  is_active boolean not null default true,
  first_seen_at timestamptz not null,
  last_seen_at timestamptz not null,
  last_snapshot_id bigint references public.team_squad_snapshots(id) on delete set null,
  source text not null default 'sportmonks',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (team_id, player_id)
);

create index if not exists team_squad_memberships_active_team_idx
  on public.team_squad_memberships(team_id, is_active, last_seen_at desc);
create index if not exists team_squad_memberships_player_active_idx
  on public.team_squad_memberships(player_id, is_active);
