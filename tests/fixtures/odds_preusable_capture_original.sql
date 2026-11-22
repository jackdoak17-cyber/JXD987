-- Exact original SELECT from pinned publisher 711f520; differential oracle.
select o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key,
                       coalesce(o.line, -9999::numeric) as line_key,
                       o.price_decimal, o.last_updated_at,
                       f.starting_at,
                       coalesce(o.participant_type, 'team') as participant_type,
                       coalesce(o.participant_id, 0) as participant_id
                  from public.odds_outcomes o
                  join public.fixtures f on f.id = o.fixture_id
                  join public.odds_quote_verifications v
                    on v.fixture_id = o.fixture_id
                   and v.bookmaker_id = o.bookmaker_id
                   and v.market_key = o.market_key
                   and v.selection_key = o.selection_key
                   and v.participant_type = coalesce(o.participant_type, 'team')
                   and v.participant_id = coalesce(o.participant_id, 0)
                   and v.line_key = coalesce(o.line, -9999::numeric)
                   and v.price_decimal = o.price_decimal
                   and v.source_last_updated_at is not distinct from o.last_updated_at
                 where f.starting_at > (statement_timestamp() at time zone 'utc')
                   and o.market_key = any(%s)
                   and o.price_decimal > 1 and o.price_decimal <= 500
                   and v.last_verified_at <= statement_timestamp() + interval '2 minutes'
                   and v.last_verified_at >= statement_timestamp() -
                     case
                       when f.starting_at <= (statement_timestamp() at time zone 'utc') + interval '2 hours'
                         then interval '2 hours'
                       when f.starting_at <= (statement_timestamp() at time zone 'utc') + interval '1 day'
                         then interval '3 hours'
                       else interval '8 hours'
                     end;
