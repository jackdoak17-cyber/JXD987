-- Premium Filter: Team Moneyline Constraints
-- Filters players based on their team's moneyline odds
-- Example: Exclude players from teams that are big underdogs (moneyline > 400)
--          or big favorites (moneyline < -300)

create or replace function public.filter_players_by_team_moneyline(
  p_fixture_date date,
  p_league_id int,
  p_player_ids int[],
  p_min_moneyline numeric default null,
  p_max_moneyline numeric default null,
  p_bookmaker_id int default 2,
  p_days int default 14
)
returns table (
  player_id int,
  fixture_id bigint,
  team_id bigint,
  team_moneyline numeric,
  is_home boolean
)
language sql
stable
as $$
  with fixtures_in_range as (
    select
      f.id,
      f.home_team_id,
      f.away_team_id
    from public.fixtures f
    where f.league_id = p_league_id
      and (f.starting_at at time zone 'Europe/London')::date >= p_fixture_date
      and (f.starting_at at time zone 'Europe/London')::date < (p_fixture_date + p_days)
  ),
  player_fixtures as (
    select distinct
      fp.player_id,
      fp.fixture_id,
      fp.team_id,
      case when fp.team_id = fir.home_team_id then true else false end as is_home
    from public.fixture_players fp
    join fixtures_in_range fir on fir.id = fp.fixture_id
    where fp.player_id = any(p_player_ids)
  ),
  team_odds as (
    select
      o.fixture_id,
      o.participant_id as team_id,
      o.price_american as moneyline
    from public.odds_outcomes o
    where o.market_key = 'h2h'
      and o.participant_type = 'team'
      and (p_bookmaker_id is null or o.bookmaker_id = p_bookmaker_id)
  )
  select
    pf.player_id::int,
    pf.fixture_id,
    pf.team_id,
    to_.moneyline,
    pf.is_home
  from player_fixtures pf
  join team_odds to_ on to_.fixture_id = pf.fixture_id and to_.team_id = pf.team_id
  where (p_min_moneyline is null or to_.moneyline >= p_min_moneyline)
    and (p_max_moneyline is null or to_.moneyline <= p_max_moneyline);
$$;
