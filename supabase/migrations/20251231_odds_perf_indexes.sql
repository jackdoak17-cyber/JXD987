create index if not exists odds_outcomes_fixture_market_idx
  on public.odds_outcomes (fixture_id, market_key);
