alter table public.oddssearcher_opponent_tier_multipliers
add column if not exists shots_multiplier_1 numeric not null default 1.0,
add column if not exists shots_multiplier_2 numeric not null default 1.0,
add column if not exists shots_multiplier_3 numeric not null default 1.0,
add column if not exists sot_multiplier_1 numeric not null default 1.0,
add column if not exists sot_multiplier_2 numeric not null default 1.0,
add column if not exists sot_multiplier_3 numeric not null default 1.0;

