-- Explicitly quarantine a completed target row that SportMonks does not
-- recognise. The target score is retained for audit but must not count as a
-- real match until the scheduled provider review succeeds.
insert into public.fixture_stats_quality_exclusions (
  fixture_id, league_id, season_id, exclusion_type, reason,
  next_review_at, evidence, last_checked_at, updated_at
) values (
  19739932, 24, 28020, 'provider_unavailable',
  'SportMonks returned no fixture data for a completed target ID',
  now() + interval '7 days',
  '{"confirmedNoDataResponses":2,"targetScore":"0-1","providerState":"absent"}'::jsonb,
  now(), now()
)
on conflict (fixture_id) do update set
  league_id = excluded.league_id,
  season_id = excluded.season_id,
  exclusion_type = case
    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
    then public.fixture_stats_quality_exclusions.exclusion_type
    else excluded.exclusion_type
  end,
  reason = case
    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
    then public.fixture_stats_quality_exclusions.reason
    else excluded.reason
  end,
  next_review_at = case
    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
    then public.fixture_stats_quality_exclusions.next_review_at
    else excluded.next_review_at
  end,
  evidence = case
    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
    then public.fixture_stats_quality_exclusions.evidence
    else excluded.evidence
  end,
  last_checked_at = now(),
  updated_at = now();

update public.fixture_detail_delivery_status
   set status = 'excluded',
       next_attempt_at = now() + interval '7 days',
       last_error = 'Excluded from stats: provider fixture identity unavailable; scheduled review',
       updated_at = now()
 where fixture_id = 19739932;
