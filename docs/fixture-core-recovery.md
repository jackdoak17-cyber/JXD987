# Fixture-core recovery and admission candidate — 8 November 2026

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
The primary fixture-core worker makes 160 retries separated by 15-second
pauses, providing 40 minutes of retry sleep across subsequent safe lock
windows instead of ten. Actual elapsed time also includes attempt overhead;
this is not a strict 40-minute deadline. It invokes the
provider and export only after acquiring a safe lease, so this does not add
scheduled source runs or bypass the quarter-hour settlement guard.
If an admitted source run reaches its shortened settlement lease, this caller
records a skip and stops instead of repeating provider/export work within the
same invocation. It still retries pre-admission lock and schedule deferrals.

The approved production cron adjustment moved the six-hour fixture-core
entry from minute `4` to minute `17` (`17 */6 * * *`), leaving the same
six-hour frequency. This separates its first lock attempt from website odds
delivery at minute `4`; the longer retry horizon covers a delayed
quarter-hour settlement or another writer at minute `17`. The existing
publisher remains hourly. The cutover was installed at 14:42 UTC with no
new paid capacity. Its one-time immediate publisher invocation safely skipped
under the existing one-hour minimum interval; the next real renewal and a
fresh fixture source are still outstanding.
The quarter-hour settlement can still hold the lock past minute `17`, and
an odds matcher starts at minute `19` every six hours. Recent successful
fixture-core runs took 70–112 seconds, but that history is not a guarantee;
the first two revised runs need observation for knock-on skips or delay.

## Stacked admission-reservation candidate

The next change can restore the fixture-core cron to minute `4` under a
coordinated five-writer admission protocol. The core
caller claims an expiring, unique-owner reservation before waiting for the
canonical writer lock. At the lock admission boundary, newly arriving
normal writers check that reservation under a short guard lock and yield.
Already admitted work drains, and settlement still takes priority. The core
will not begin another admission attempt after its 40-minute retry deadline,
covering the later safe window after a slow minute-four
odds delivery. The reservation expires after at most one hour if the caller
dies; release removes only the matching owner's token. The claim and release
guard waits are bounded at two seconds. The actual admission check confirms
the same owner still holds a reservation long enough for the planned lease,
then rechecks the retry deadline and settlement window before provider work
starts. It recalculates the usable execution lease after the guard wait, so
the timeout cannot consume seconds that the wait already used. The
reservation is released when admission ends, including a deferral before
provider work, before the database heartbeat or healthcheck can stall; exit
cleanup remains a fallback.
The source frequency stays six-hourly. The exact coordinated rollout can
retain minute `17` first, then consider returning to minute `4` only after
measured success; do not combine a schedule change with protocol activation
without recording and verifying it separately.

Every active checkout using the canonical lock must adopt the admission
check together: the odds matcher, fixture core, website odds delivery, squad
reconciliation and leaderboard stats reconciliation. The post-match
settlement runs from the fixture-core checkout and retains its special
priority. The stats reconciliation supervisor is long lived and must be
handed off so it loads the new common functions. Stage and verify each
checkout's own runtime manifest; do not copy the new `common.sh` over an
older release with a different manifest or code shape. The exact five-package
composition, test evidence, forward map and rollback map are in
`docs/fixture-core-81-coordinated-rollout.md`. The reservation remains an
unapplied candidate.

## Production sequence and remaining approval boundaries

1. Jack approved the separate one-time guarded publisher renewal, which ran
   at 14:13 UTC with the verified `run_fixture_core_fallback` source. It
   published release `bda3d455-74bf-455b-aee7-6db158843a77`; the resolver
   returned `current`, and the authenticated schedule returned HTTP 200 with
   all 88 returned fixture identities and team values matching the release.
   Its pin expires at 16:13 UTC. The full source report is preserved in the
   website incident evidence. This approval did not cover deploying #80/#81.
2. Before any future renewal, recheck genuine source freshness and scope. Do
   not renew from stale or missing evidence.
3. Jack separately approved the smaller publisher/retry cutover. It was
   installed at 14:42 UTC with the original release retained for rollback.
   The core schedule is now `17 */6`; the publisher is still hourly at `:11`.
   Its immediate invocation skipped under the one-hour minimum interval.
4. The stacked reservation candidate requires another owner approval. Stage
   and verify the five exact packages in the rollout map, cut all canonical
   writers over together, and hand off the long-lived reconciliation
   supervisor at a verified idle boundary. Preserve `17 */6` initially.
5. Confirm a successful primary source run, an hourly publisher renewal and
   authenticated fixture schedule and detail responses. Watch the next two
   six-hour runs for skipped admissions and fresh publication. Keep the
   source age guard and settlement lease unchanged.

If the new publisher fails, repoint the affected cron entries to the
untouched prior releases; restore fixture-core minute `4` if it was shifted.
The last valid fixture release remains in the database and still obeys its
own expiry. The current code changes do not resize Supabase, alter disk
or spend-cap settings, add services, or increase the scheduled job frequency.
Recovery of Prop Search and the leaderboard website release needs separate
end-to-end verification.
