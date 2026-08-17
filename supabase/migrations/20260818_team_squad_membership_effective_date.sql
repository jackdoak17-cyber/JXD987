-- The provider can temporarily return a player in both the old and new squad.
-- Retain the provider's effective date so reconciliation can choose the newer
-- assignment deterministically rather than relying on endpoint processing order.

alter table public.team_squad_memberships
  add column if not exists provider_started_at timestamptz;

create index if not exists team_squad_memberships_player_effective_idx
  on public.team_squad_memberships(player_id, is_active, provider_started_at desc);
