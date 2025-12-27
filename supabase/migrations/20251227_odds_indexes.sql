create index if not exists odds_outcomes_fixture_id_idx
  on public.odds_outcomes (fixture_id);

create index if not exists odds_outcomes_fixture_selection_idx
  on public.odds_outcomes (fixture_id, selection_key);

create index if not exists odds_outcomes_participant_idx
  on public.odds_outcomes (participant_type, participant_id);

create index if not exists odds_outcomes_market_idx
  on public.odds_outcomes (market_key);
