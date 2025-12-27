create or replace function public.odds_market_line_coverage(
  p_fixture_date date,
  p_league_id int,
  p_market_key text,
  p_days int default 14,
  p_bookmaker_id int default null
)
returns table (
  line numeric,
  distinct_players int,
  odds_rows int
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
  )
  select
    o.line,
    count(distinct o.participant_id)::int as distinct_players,
    count(*)::int as odds_rows
  from public.odds_outcomes o
  join fixtures_in_range f on f.id = o.fixture_id
  where o.market_key = p_market_key
    and o.participant_type = 'player'
    and (p_bookmaker_id is null or o.bookmaker_id = p_bookmaker_id)
  group by o.line
  order by distinct_players desc nulls last, o.line asc nulls last;
$$;
