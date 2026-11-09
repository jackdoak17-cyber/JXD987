# Statistics reconciliation batch export: live source cutover

Date: 2026-11-09. This is a source-only release on the existing VPS. It does
not enable the leaderboard worker or its customer reader, reset resource debt,
change the Supabase disk/compute tier or Spend Cap, or buy a service.

## Exact release and evidence before activation

- Candidate source branch `codex/batched-fixture-detail-export` at `88125da`;
  active prior source archive `3028e0f`.
- The candidate merges `3028e0f` and changes only the bounded statistics
  reconciliation/export path among runtime files. Runtime manifest has 49
  verified entries, SHA-256
  `617534da51cb8f0b63d109067eba176cb853afb6e53d84ebea78295f722b053c`.
  Its new runtime release ID is `368b97679c37`; the prior source release was
  `d025f6607f67`. The separately pinned importer baseline remains
  `375f1f1aaf25`.
- Exact candidate tests: 158 passed, one existing skip, 12 deprecation
  warnings; Python compilation, manifest verification and diff check passed.
  Linux host composition passed in a network-disabled container. See
  [candidate analysis](stats-reconciliation-batched-export-candidate.md) for
  real-source isolated old/batch parity, parent-call, elapsed and WAL evidence
  and its limits.
- The new immutable archive was staged at
  `/opt/odds-sync/JXD987-stats-batch-88125da-release`, with `.env`, `.venv`
  and `data` linked to the same existing runtime dependencies as `3028e0f`.
  It occupied 977,848 directory bytes, excluding those symlink targets. The
  deployment verifier confirmed the full manifest and odds-delivery startup;
  the VPS had 5,370,601,472 free bytes on `/` at 19:27 UTC.
- At the last preflight, the old source supervisor had no reconciliation or
  exporter child. The 19:24 old-source run succeeded with three selected,
  one accepted, two provider-pending and no failure. Its export stage took
  5.342 seconds. The old 19:25 cron entrant skipped because the old
  supervisor still held its lock.

## Activation and rollback boundary

The exact prior crontab is backed up root-only at
`/root/odds-stats-cron-before-88125da-20261109`. The first stop script sent
TERM to the old supervisor and then treated `ps` exit 1 (the expected
already-exited result) as an error; it stopped **before** changing cron.
A readback proved the crontab still matched the backup, with no old supervisor
or source child. A second guarded step replaced only the two archive-path
occurrences in the single statistics cron line and proved exact readback;
all other cron entries were retained. The new supervisor started at 19:26:42
UTC and held PID 1464919. It initially skipped because only 138 seconds of
normal-writer lease remained against its 270-second minimum; this is the
expected protection before the next settlement handoff, not a source failure.

The first genuine new-release batch is recorded below. The cutover was rolled
back because it preserved correctness but did not reduce export time. For any
future rollback, allow any in-flight bounded child to exit or terminate it
under the existing lease, then stop the new supervisor. Restore
**only the statistics cron line** from the saved prior entry after verifying
the current line still points to this candidate; preserve any later unrelated
cron edits. Verify no new child or supervisor remains, restart the prior
manifest-verified archive, and check one ordinary source result and target
delivery status. Do not change the old source pin or resource debt on rollback.

The new release starts its own performance observation sequence. No old
samples are relabeled. Pinning requires the installed sample, freshness and
p95 limits; this source-only cutover does not create a renewable leaderboard
worker allowance or authorize customer reader activation.

## First genuine run and conditional rollback

At 19:37 UTC, candidate release `368b97679c37` completed one genuine
lease-bounded source batch: 20 selected, 13 accepted, seven provider-pending,
zero failed. All 13 accepted target delivery rows had the accepted snapshot
linked, equal source/target snapshots, and true player-stat and lineup parity.
The source-store, export, projection-refresh and activation stages took
21.226, 55.564, 8.354 and 8.811 seconds respectively. The final exporter
report named one explicit fixture and had no batch-outcome field, proving
that the **individual** exporter path ran. The candidate's all-or-nothing
batch gate was disabled by at least one retry in the same accepted set; the
isolated replay had not covered this mixed-attempt production shape. Correct
delivery is established for this batch, but a live speed improvement is not.

After the batch finished and no candidate child remained, the candidate
supervisor was stopped. The current statistics cron entry was checked to
point to this candidate, then only that entry was restored from the root-only
prior backup; unrelated cron entries were retained. The inactive candidate
entry was saved at `/root/odds-stats-cron-candidate-88125da-20261109`. The
prior `3028e0f` supervisor restarted at 19:40:30 UTC; its first checks were
expected lease and settlement deferrals, and PID 1469933 was still running at
19:44:50. No old source pin, resource debt/cap, leaderboard worker, customer
reader, disk/compute tier, Spend Cap or paid service was changed. A later
genuine old-source result remains to be checked.

## Mixed-attempt correction in development

The next candidate groups eligible first attempts into one bounded exporter
call while sending retries from the same source run through the existing
individual commands. If only one first attempt is eligible, all exports stay
individual. It preserves per-fixture outcome order, fails closed on an
ambiguous batch report, and records batch versus individual fixture counts in
the source report. The mixed-success/failure and one-first-attempt cases now
have focused tests; the combined suite passed 160 with one existing skip after
regenerating the 49-entry runtime manifest. Its manifest SHA-256 is
`b5ff7da6e23d0eb546f1087574689fa09fb6edc731bc459c8001e2006f60a4bc`
and release ID is `3df6bb130cb9` (the manifest SHA-1 prefix). The immutable
archive `/opt/odds-sync/JXD987-stats-batch-mixed-24d0a94-release` was staged
and its 49-entry manifest and odds-delivery startup verified at 19:51 UTC.
The corrected candidate was then activated as a source-only trial, as
recorded below. No old sample may be counted toward its source pin.

## Corrected source-only trial at 19:56 UTC

The restored `3028e0f` worker's next ordinary batch started at19:52 and
finished successfully at19:55: 20 selected, 20 accepted, zero pending or
failed. Source-store/export/projection/activation took
30.463/80.556/11.996/17.532 seconds. This is one composition-matched old
run, not a paired controlled benchmark. With no old child left, the old
supervisor was stopped after checking its exact PID and command. The current
crontab was saved root-only as
`/root/odds-stats-cron-before-24d0a94-20261109`; only the statistics entry
was changed to the previously verified `24d0a94` archive, preserving every
other cron entry. Exact cron readback passed. The corrected supervisor started
at19:56:12 as PID1476216, release `3df6bb130cb9`. Its first lease check
deferred because only168 seconds remained against the270-second minimum.
The first genuine corrected-release batch and launch-context correction are
recorded below. Repeated-run and comparable full-workload evidence remain
open.

If this trial fails its source or data checks, first let its child drain or
terminate it under the existing lease. Stop only PID1476216 after verifying
its command, restore only the statistics cron entry from the root-only backup
if the current entry still points to this archive, and restart the verified
old archive. Keep the original source pin/resource debt, paused leaderboard
worker and customer reader unchanged. No paid settings changed in this trial.

## Corrected first run and an action-time launch error

The first manually started corrected supervisor was invoked by absolute script
path from root's working directory. The wrapper derives `REPO_ROOT` for its
runtime files, but its default `data/jxd.sqlite` source path is relative to
the process working directory. At20:07:01 the source worker exited before
candidate selection with `Source database not found: data/jxd.sqlite`.
There was no exporter, accepted fixture, source-ledger mutation or source
report from this attempt. This was a deployment-command error, not evidence
that the staged database symlink was missing. The normal cron command already
includes `cd` to the archive, but the first supervisor would have kept
holding the singleton lock and retrying from the wrong directory. With no
source child in flight, it was stopped and the same exact archive was
restarted **from inside** its directory at20:07:47 as PID1479855. The cron
entry, archive and manifest were not changed. Future manual starts must first
`cd` to the archive, matching cron.

The next lease-bounded corrected run at20:07:48–20:08:42 succeeded: six
selected, six accepted, zero pending or failed. The two-cohort limiter
reduced the nominal100 candidate pool to six. Source report counted **five
batched fixtures and one individual retry**; the final exporter report was
the expected one-fixture retry because the shared report path is last-writer.
Source-store/export/verification/projection/activation took
14.591/16.679/0.525/7.067/8.842 seconds. A direct read of the serving
delivery-status table found all six accepted rows under `3df6bb130cb9` at
20:08:34–20:08:41, with equal non-null source/target snapshots, true player
and lineup parity and accepted snapshot links. Source performance latest
status became success at20:08:42, with only one release sample and latest six
work units. This shows the mixed path now executes and delivers correctly for
one small run. It is not a controlled comparison with the prior20/20 run,
which had export80.556 seconds, or sufficient source-pin evidence.

At20:09:19 the corrected supervisor and exact cron entry remained active.
Read-only serving state still showed capture on, worker/backfill off and
resource paused at `wal_bytes_limit`, with durable WAL3,821,618,701 bytes
and temp444,508,273 bytes. The old source's21 comparable20-unit samples had
total p95 154,667ms against the pinned148,617.4ms ceiling; the corrected
release has not met its20-sample guard. Observe the next ordinary larger
batch and recurring source/Fixtures health before deciding whether to retain
the trial. No paid capacity, disk provisioning or Spend Cap change occurred.

## Second ordinary source batch and Fixtures recurrence

The normal hourly Fixtures publisher finished at20:11:53, published release
`622f1d64-f810-4f44-b99c-c66de5080457` with 1,239 schedule, 1,341
standings, 109,032 metrics and 3,333 odds rows, and pinned it to22:11:06.
Its source freshness identified the genuine fixture-core job verified at
18:18:09, well within the existing540-minute gate. This proves publication
continued while the source-only statistics trial was active; it is not a
browser/customer certification.

The corrected supervisor's next natural batch began at20:22 and succeeded at
20:23:49: 17 selected, 13 accepted, four provider-pending, zero failed.
The existing two-cohort limiter selected17 from the nominal100 pool. Five
accepted fixtures used one batch and eight later-attempt fixtures kept their
individual commands. Candidate selection/provider fetch/source-store/export/
verification/projection/activation took
2.221/1.266/19.988/48.423/1.301/7.451/20.164 seconds. A direct serving
status query found all13 accepted rows under `3df6bb130cb9` at20:23:31–
20:23:48 with equal non-null source/target snapshots, true player and lineup
parity and accepted snapshot links. The source-performance view at20:31 had
one comparable17-unit sample, total p95 108,441ms and processing p95
107,137ms. The old release's21 comparable20-unit runs had total p95
154,667ms against the old pinned148,617.4ms ceiling. Different selected and
accepted fixture sets mean this is a promising but **uncontrolled** speed
comparison; 20 new-release comparable samples and freshness gates remain
unmet.

At20:31 the corrected supervisor and exact cron entry remained active. The
leaderboard pipeline still had old source pin `375f1f1aaf25`, capture on,
worker/backfill off, and resource paused for `wal_bytes_limit`; durable
WAL3,821,618,701 and temp444,508,273 bytes were unchanged by these reads.
Keep the source-only trial for normal recurrence and examine its 20-sample
distribution before pinning. Eight of13 accepted fixtures still used the
individual retry path, limiting this release's possible export gain; do not
broaden batch eligibility without separate retry-safety evidence. JXD `main`
was `aa9304e` at this check, behind the exact active `3028e0f` archive, so a
direct PR from the trial branch would include earlier unmerged host/leaderboard
work. Reconcile that integration boundary before a production-branch merge.

## Third normal source window

At20:37:01–20:37:31, the same corrected supervisor completed another normal
lease-bounded batch: three selected, three accepted, zero pending or failed.
Two fixtures used the batch path and one retry remained individual; export
took10.369 seconds, source-store4.764, projection4.931 and activation4.491.
Direct serving-status read found one `provider_sparse` and two `verified`
rows, all three with non-null equal source/target snapshots, true player and
lineup parity, and accepted snapshot links. The source performance view
reported latest release `3df6bb130cb9`, status success, three work units at
20:37:31, while comparable sample count remained **one**. A later small
success can prove health but cannot satisfy the20-comparable-run pin gate.
Three separate corrected-release batches have now succeeded (6/6, 17/13,
3/3), including mixed batch/retry paths, without target parity failure.
Retain the source-only trial under the unchanged schedule and safeguards;
do not rearm leaderboard processing or switch its source pin yet.

## Fourth normal source window and target parity

The same corrected supervisor next completed a normal 20-unit batch at
20:40:52: 20 selected, 19 accepted, one provider-pending, zero failed.
Source-store/export/projection/activation took 32.750/47.036/9.201/24.296
seconds. The corresponding job row reports 125,731 ms execution and 124,548
ms processing. A direct read-only target query at21:01 found exactly 19
`fixture_detail_delivery_status` rows for release `3df6bb130cb9` with
`last_successful_at` from20:40:30 to20:40:52. All19 had non-null equal
source/target snapshots, true player-stat and lineup parity, and an accepted
snapshot link. All were labelled `provider_sparse`; this is the recorded
provider status, not a delivery failure. The 20-unit run adds a second genuine
comparable source sample. A later successful seven-unit batch at20:52:52
establishes continuing source health but does not increase that comparable
count; the performance view at20:52:55 reported **2/20** and preliminary
normalized total p95 127,480.494 ms. This sample is too small to certify
the source pin or justify leaderboard admission. Keep the source-only archive
and ordinary schedule stable; preserve the old pinned release, resource debt,
paused leaderboard worker, customer reader setting and paid capacity.
