begin;

-- The original five-column unique constraint treats NULL lines as distinct,
-- while a later participant-aware index treats NULL and -9999 as the same
-- sentinel.  Keep one canonical public identity for ingestion and preserve
-- the existing one-selection-per-line behavior across both representations.
do $$
begin
  if exists (
    select 1
    from public.odds_outcomes
    group by fixture_id, bookmaker_id, market_key, selection_key, coalesce(line, -9999)
    having count(*) > 1
  ) then
    raise exception
      'Cannot create odds_outcomes_canonical_key: duplicate normalized outcome identities exist';
  end if;
end
$$;

create unique index if not exists odds_outcomes_canonical_key
  on public.odds_outcomes (
    fixture_id,
    bookmaker_id,
    market_key,
    selection_key,
    (coalesce(line, -9999))
  );

commit;
