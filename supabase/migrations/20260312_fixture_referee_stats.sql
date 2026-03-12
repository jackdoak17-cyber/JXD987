create table if not exists public.fixture_referee_stats (
  fixture_id bigint primary key references public.fixtures(id) on delete cascade,
  referee_id bigint references public.referees(id) on delete set null,
  referee_name text not null,
  avg_yellow_cards numeric,
  avg_fouls numeric,
  games_with_3plus_cards_pct numeric,
  games_with_red_card_pct numeric,
  avg_corners numeric,
  sample integer not null default 0,
  source text not null default 'sportmonks',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists fixture_referee_stats_referee_idx
  on public.fixture_referee_stats(referee_id);
