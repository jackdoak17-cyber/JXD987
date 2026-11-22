create or replace function public.odds_quote_verifications_record(
  p_source_run_id text,
  p_quotes jsonb
)
returns table (supplied_count integer, matched_count integer, written_count integer)
language plpgsql
security definer
set search_path = pg_catalog, public, pg_temp
as $$
declare
  v_supplied integer;
  v_distinct integer;
  v_invalid integer;
begin
  if p_source_run_id is null or length(p_source_run_id) not between 6 and 180
    or p_source_run_id !~ '^[A-Za-z0-9][A-Za-z0-9_.:-]+$'
    or p_quotes is null or jsonb_typeof(p_quotes) <> 'array'
    or jsonb_array_length(p_quotes) > 2000 then
    raise exception 'Invalid verified-quote batch';
  end if;

  -- The ledger serves only current quote freshness. Keep its growth bounded
  -- without tying customer odds publication to a separate cleanup job.
  delete from public.odds_quote_verifications
  where last_verified_at < now() - interval '30 days';

  select count(*)::integer,
    count(*) filter (
      where fixture_id is null or fixture_id < 1
        or bookmaker_id is null or bookmaker_id < 1
        or market_key is null or length(market_key) not between 1 and 100
        or selection_key is null or length(selection_key) not between 1 and 300
        or length(participant_type) not between 1 and 32
        or participant_id < 0
        or price_decimal is null or price_decimal <= 1 or price_decimal > 500
        or observed_at is null
        or observed_at < now() - interval '7 days'
        or observed_at > now() + interval '2 minutes'
    )::integer
  into v_supplied, v_invalid
  from jsonb_to_recordset(p_quotes) as x(
    fixture_id bigint, bookmaker_id integer, market_key text,
    selection_key text, participant_type text, participant_id bigint,
    line numeric, price_decimal numeric, source_last_updated_at timestamptz,
    observed_at timestamptz
  );

  if v_invalid <> 0 then
    raise exception 'Malformed verified quote rows: % of %', v_invalid, v_supplied;
  end if;

  select count(*)::integer into v_distinct
  from (
    select distinct fixture_id, bookmaker_id, market_key, selection_key,
      coalesce(nullif(participant_type, ''), 'team'), coalesce(participant_id, 0),
      coalesce(line, -9999)
    from jsonb_to_recordset(p_quotes) as x(
      fixture_id bigint, bookmaker_id integer, market_key text,
      selection_key text, participant_type text, participant_id bigint,
      line numeric, price_decimal numeric, source_last_updated_at timestamptz,
      observed_at timestamptz
    )
  ) d;
  if v_distinct <> v_supplied then
    raise exception 'Duplicate normalized quote identities in source batch';
  end if;

  return query
  with input as (
    select x.fixture_id, x.bookmaker_id, x.market_key, x.selection_key,
      coalesce(nullif(x.participant_type, ''), 'team') as participant_type,
      coalesce(x.participant_id, 0) as participant_id,
      coalesce(x.line, -9999) as line_key,
      x.price_decimal, x.source_last_updated_at, x.observed_at
    from jsonb_to_recordset(p_quotes) as x(
      fixture_id bigint, bookmaker_id integer, market_key text,
      selection_key text, participant_type text, participant_id bigint,
      line numeric, price_decimal numeric, source_last_updated_at timestamptz,
      observed_at timestamptz
    )
  ), matched as (
    select i.*
    from input i
    join public.odds_outcomes o
      on o.fixture_id = i.fixture_id and o.bookmaker_id = i.bookmaker_id
      and o.market_key = i.market_key and o.selection_key = i.selection_key
      and coalesce(o.participant_type, 'team') = i.participant_type
      and coalesce(o.participant_id, 0) = i.participant_id
      and coalesce(o.line, -9999) = i.line_key
      and o.price_decimal = i.price_decimal
      and o.last_updated_at is not distinct from i.source_last_updated_at
  ), written as (
    insert into public.odds_quote_verifications as v (
      fixture_id, bookmaker_id, market_key, selection_key,
      participant_type, participant_id, line_key,
      price_decimal, source_last_updated_at, last_verified_at, source_run_id
    )
    select fixture_id, bookmaker_id, market_key, selection_key,
      participant_type, participant_id, line_key,
      price_decimal, source_last_updated_at, observed_at, p_source_run_id
    from matched
    on conflict (fixture_id, bookmaker_id, market_key, selection_key,
      participant_type, participant_id, line_key)
    do update set price_decimal = excluded.price_decimal,
      source_last_updated_at = excluded.source_last_updated_at,
      last_verified_at = excluded.last_verified_at,
      source_run_id = excluded.source_run_id
    where excluded.last_verified_at > v.last_verified_at
    returning 1
  )
  select v_supplied, (select count(*)::integer from matched),
    (select count(*)::integer from written);
end;
$$;
