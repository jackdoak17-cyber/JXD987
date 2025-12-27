-- Odds coverage verification (next 14 days, Premier League)
with fixtures_in_range as (
  select id
  from public.fixtures
  where league_id = 8
    and (starting_at at time zone 'Europe/London')::date >= current_date
    and (starting_at at time zone 'Europe/London')::date < (current_date + interval '14 days')
), scoped as (
  select o.*
  from public.odds_outcomes o
  join fixtures_in_range f on f.id = o.fixture_id
  where o.participant_type = 'player'
)
select
  count(distinct case when market_key = 'player_shots_on_target' and line = 0.5 then participant_id end)
    as distinct_sot_05_players,
  round(
    100.0 * count(*) filter (where participant_id is not null) / nullif(count(*), 0),
    2
  ) as mapped_pct_player_outcomes
from scoped;
