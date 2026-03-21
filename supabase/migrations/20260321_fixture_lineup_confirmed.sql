alter table if exists public.fixtures
add column if not exists lineup_confirmed boolean;
