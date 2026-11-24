-- Grouped league tables (for example MLS conferences) need their provider
-- group identity and label preserved alongside the official position.
alter table public.official_team_standings
  add column if not exists group_name text null;
