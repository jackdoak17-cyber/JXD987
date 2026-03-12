alter table public.fixture_referee_stats
  add column if not exists avg_total_cards numeric,
  add column if not exists games_with_4plus_cards_pct numeric,
  add column if not exists games_with_5plus_cards_pct numeric,
  add column if not exists sample_5 integer not null default 0,
  add column if not exists sample_10 integer not null default 0,
  add column if not exists sample_20 integer not null default 0,
  add column if not exists windows jsonb;

create index if not exists fixture_referee_stats_referee_sample_idx
  on public.fixture_referee_stats(referee_id, sample desc);
