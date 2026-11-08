# Fixture-core recovery candidate — 8 November 2026

## Observed failure

Production fixture schedule returned HTTP 409 because the current fixture
release pin expired at 09:56 UTC. The hourly publisher failed its nine-hour
source-freshness check at 13:11. Its default source job was the six-hour VPS
`run_p3_fixture_core_export`, last successful at 18:05 UTC on 7 November.

The primary fixture-core run and website odds delivery both start at minute
four. At 00:04 and 12:04 on 8 November, odds delivery took about seven and
six minutes respectively, while fixture core exhausted its ten-minute lock
retry window and skipped. The later settlement reservation left no safe
lease to start. Historical stats reconciliation was also waiting on the
lock at 12:07–12:09; it was not the demonstrated winner at 12:04.

An independent GitHub failover refreshed and strictly exported the source at
07:56, and published the release then. The failover's source job is
`run_fixture_core_fallback`. The hourly VPS publisher checks only the
primary job, so it does not use that newer verified source to renew the
two-hour release pin. The failover workflow already considers the newest
success of both source IDs. Its scheduled invocations have been delayed by
hours, so it cannot be the only renewal mechanism.

## Candidate change

The hourly publisher selects the latest successful record from exactly the
primary and existing fallback source jobs, then applies the existing
nine-hour age limit. It records the actual selected job ID in publication
evidence. An in-process source refresh still requires its explicit job ID.
The primary fixture-core worker waits up to 40 minutes across subsequent
safe lock windows instead of giving up after ten minutes. It invokes the
provider and export only after acquiring a safe lease, so this does not add
scheduled source runs or bypass the quarter-hour settlement guard.

The proposed production cron adjustment is to move the six-hour fixture-core
entry from minute `4` to minute `17` (`17 */6 * * *`), leaving the same
six-hour frequency. This separates its first lock attempt from website odds
delivery at minute `4`; the longer retry horizon covers a delayed
quarter-hour settlement or another writer at minute `17`. The existing
publisher remains hourly. This cron adjustment has **not** been applied.

## Reviewed production action, pending Jack's approval

1. Recheck the latest successful primary/fallback source and its age. If one
   is within the nine-hour limit, run the existing guarded publisher once
   with that exact source job ID. Do not renew from stale or missing evidence.
2. Verify `fixture_delivery_release_resolve(null::uuid)` returns `current`
   and a signed-in fixture schedule request returns HTTP 200 with expected
   values. Inspect the release's source provenance and pin expiry.
3. Stage an immutable worker release with the existing `.env`, `.venv` and
   SQLite links, deploy the reviewed runtime files into it, and verify its
   manifest before cutover. Repoint only the fixture-core and hourly fixture
   publisher cron entries to that release, and move the former from minute
   `4` to `17`. Verify both crontab entries.
4. Confirm a successful primary source run, an hourly publisher renewal and
   authenticated fixture schedule and detail responses. Watch the next two
   six-hour runs for skipped admissions and fresh publication. Keep the
   source age guard and settlement lease unchanged.

If the new publisher fails, repoint both cron entries to the untouched prior
worker release and restore fixture-core minute `4`; the last valid fixture
release remains in the database and still obeys
its own expiry. The current code changes do not resize Supabase, alter disk
or spend-cap settings, add services, or increase the scheduled job frequency.
Recovery of Prop Search and the leaderboard website release needs separate
end-to-end verification.
