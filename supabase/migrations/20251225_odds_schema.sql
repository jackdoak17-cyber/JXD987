-- Odds tables for Bet365 pre-match markets
create table if not exists public.odds_snapshots (
  id bigserial primary key,
  fixture_id bigint not null,
  bookmaker_id int not null default 2,
  pulled_at timestamptz not null default now(),
  raw jsonb not null
);

create index if not exists odds_snapshots_fixture_id_idx
  on public.odds_snapshots (fixture_id);
create index if not exists odds_snapshots_pulled_at_idx
  on public.odds_snapshots (pulled_at);

create table if not exists public.odds_outcomes (
  id bigserial primary key,
  fixture_id bigint not null,
  bookmaker_id int not null default 2,
  market_key text not null,
  selection_key text not null,
  participant_type text null,
  participant_id bigint null,
  line numeric null,
  price_decimal numeric not null,
  price_american int null,
  last_updated_at timestamptz null,
  unique (fixture_id, bookmaker_id, market_key, selection_key, line)
);

create index if not exists odds_outcomes_fixture_idx
  on public.odds_outcomes (fixture_id);
create index if not exists odds_outcomes_participant_idx
  on public.odds_outcomes (participant_id);
create index if not exists odds_outcomes_market_idx
  on public.odds_outcomes (market_key);
create index if not exists odds_outcomes_price_idx
  on public.odds_outcomes (price_decimal);

create or replace function public.fixture_odds(
  p_fixture_id int,
  p_bookmaker_id int default 2
)
returns table (
  fixture_id bigint,
  bookmaker_id int,
  market_key text,
  selection_key text,
  participant_type text,
  participant_id bigint,
  line numeric,
  price_decimal numeric,
  price_american int,
  last_updated_at timestamptz
)
language sql
stable
as $$
  select
    o.fixture_id,
    o.bookmaker_id,
    o.market_key,
    o.selection_key,
    o.participant_type,
    o.participant_id,
    o.line,
    o.price_decimal,
    o.price_american,
    o.last_updated_at
  from public.odds_outcomes o
  where o.fixture_id = p_fixture_id
    and o.bookmaker_id = p_bookmaker_id
  order by o.market_key, o.selection_key;
$$;

create or replace function public.player_odds_next_days(
  p_league_id int,
  p_days int,
  p_market_key text,
  p_line numeric,
  p_bookmaker_id int default 2
)
returns table (
  fixture_id bigint,
  starting_at timestamptz,
  player_id bigint,
  selection_key text,
  line numeric,
  price_decimal numeric
)
language sql
stable
as $$
  select
    o.fixture_id,
    f.starting_at,
    o.participant_id as player_id,
    o.selection_key,
    o.line,
    o.price_decimal
  from public.odds_outcomes o
  join public.fixtures f on f.id = o.fixture_id
  where f.league_id = p_league_id
    and f.starting_at >= now()
    and f.starting_at < (now() + (p_days || ' days')::interval)
    and o.bookmaker_id = p_bookmaker_id
    and o.market_key = p_market_key
    and (p_line is null or o.line = p_line)
    and o.participant_id is not null
  order by o.price_decimal desc nulls last;
$$;

create or replace function public.filter_players_by_odds(
  p_fixture_date date,
  p_league_id int,
  p_player_ids int[],
  p_market_key text,
  p_line numeric,
  p_min_price numeric,
  p_max_price numeric,
  p_bookmaker_id int default 2,
  p_days int default 1
)
returns table (
  player_id int,
  fixture_id bigint,
  price_decimal numeric,
  line numeric,
  market_key text,
  selection_key text
)
language sql
stable
as $$
  with fixtures_in_range as (
    select id
    from public.fixtures
    where league_id = p_league_id
      and (starting_at at time zone 'Europe/London')::date >= p_fixture_date
      and (starting_at at time zone 'Europe/London')::date < (p_fixture_date + p_days)
  ), filtered as (
    select o.*
    from public.odds_outcomes o
    join fixtures_in_range f on f.id = o.fixture_id
    where o.bookmaker_id = p_bookmaker_id
      and o.market_key = p_market_key
      and (p_line is null or o.line = p_line)
      and (p_min_price is null or o.price_decimal >= p_min_price)
      and (p_max_price is null or o.price_decimal <= p_max_price)
      and o.participant_id = any(p_player_ids)
  )
  select distinct on (participant_id)
    participant_id::int as player_id,
    fixture_id,
    price_decimal,
    line,
    market_key,
    selection_key
  from filtered
  order by participant_id, price_decimal desc nulls last, fixture_id desc;
$$;
