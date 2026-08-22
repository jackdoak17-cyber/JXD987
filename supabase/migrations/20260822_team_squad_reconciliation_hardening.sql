-- Squad snapshots and memberships are written by the service role only.
-- Enable RLS explicitly so a future grant cannot accidentally expose the
-- provider audit tables without a deliberate read policy.

alter table public.team_squad_snapshots enable row level security;
alter table public.team_squad_memberships enable row level security;

