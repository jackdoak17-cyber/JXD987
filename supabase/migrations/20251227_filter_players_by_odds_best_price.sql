drop function if exists public.filter_players_by_odds(
  date,
  integer,
  integer[],
  text,
  numeric,
  numeric,
  numeric,
  integer,
  integer
);

create or replace function public.filter_players_by_odds(
  p_fixture_date date,
  p_league_id int,
  p_player_ids int[],
  p_market_key text,
  p_line numeric,
  p_min_price numeric,
  p_max_price numeric,
  p_bookmaker_id int default null,
  p_days int default 1
)
returns table (
  player_id int,
  fixture_id bigint,
  starting_at timestamptz,
  price_decimal numeric,
  line numeric,
  market_key text,
  selection_key text,
  bookmaker_id int
)
language sql
stable
as $$
  with fixtures_in_range as (
    select id, starting_at
    from public.fixtures
    where league_id = p_league_id
      and (starting_at at time zone 'Europe/London')::date >= p_fixture_date
      and (starting_at at time zone 'Europe/London')::date < (p_fixture_date + p_days)
  ), filtered as (
    select o.*, f.starting_at
    from public.odds_outcomes o
    join fixtures_in_range f on f.id = o.fixture_id
    where (p_bookmaker_id is null or o.bookmaker_id = p_bookmaker_id)
      and o.market_key = p_market_key
      and (p_line is null or o.line = p_line)
      and (p_min_price is null or o.price_decimal >= p_min_price)
      and (p_max_price is null or o.price_decimal <= p_max_price)
      and o.participant_id = any(p_player_ids)
  )
  select distinct on (participant_id)
    participant_id::int as player_id,
    fixture_id,
    starting_at,
    price_decimal,
    line,
    market_key,
    selection_key,
    bookmaker_id
  from filtered
  order by participant_id, price_decimal desc nulls last, fixture_id desc;
$$;
