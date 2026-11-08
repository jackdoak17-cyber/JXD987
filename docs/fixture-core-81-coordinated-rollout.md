# Fixture-core #81 coordinated admission rollout candidate — 8 November 2026

This is a prepared, **unapplied** five-checkout change. Jack approved and we
executed the narrower #80 cutover at 14:42 UTC. Jack subsequently authorised
necessary no-added-cost programme work without repeated approval requests.
This rollout still requires the exact package, drain, verification and rollback
checks below; any added spending requires Jack's approval. No additional
infrastructure or spend is proposed.

## Why all five releases must move together

The reservation is a sidecar file derived from `/var/lock/odds-sync.lock`.
The fixture-core caller claims it before attempting the canonical writer
lock; normal writers must check it at admission. An old writer checkout would
ignore the reservation and could overtake fixture core. A partial rollout
therefore cannot establish the intended priority. Settlement retains its
special priority and must run from the compatible core release. The existing
leaderboard stats reconciliation supervisor has held its shell process open
for days; its `common.sh` functions are loaded in memory and changing the
cron path alone cannot update them.

## Exact offline-composed packages

All five packages were composed from read-only copies of the active VPS
releases. The offline composer in `scripts/vps/compose_fixture_core_admission_release.py`
accepts only the inspected baseline `common.sh` hashes and replaces only the
three canonical admission/recording functions plus reservation helpers. It
preserves each checkout's unrelated functions, including the matcher's
always-enabled live-settlement guard and the leaderboard release's database
heavy lock. The core package starts with the installed #80 release and adds
the #81 core wrapper. No archive contains `.env`, `.venv`, tests, SQLite data
or credentials. The existing credential and virtual-environment links must
be recreated to their **unchanged** targets at installation.

Each data-using candidate must also resolve to the same shared SQLite file
as its active predecessor. The core candidate must use the **old settlement**
checkout's `data` target, not #80 core's isolated `data` directory: settlement
and fixture source must see the shared production database.

| Candidate | Required `data` target before cron activation |
| --- | --- |
| Core/settlement | `/opt/odds-sync/JXD987-fixture-core-900d1ca-release/data` |
| Odds matcher | `/opt/odds-sync/JXD987-odds-matcher-48d251e-release/data` |
| Stats reconciliation | Existing link to `/opt/odds-sync/JXD987-runtime-9aa3015-release/data` |
| Squad runtime | `/opt/odds-sync/JXD987-runtime-7e74d46-release/data` |
| Website odds delivery | No local `data` directory; preserve the cron's explicit `ODDS_DELIVERY_DB_PATH`. |

Check the resolved device/inode of each candidate's `data/jxd.sqlite`
against the old settlement's shared file. A new small SQLite file in a
candidate release is a failed preflight even if its runtime manifest passes.

| Writer checkout | Current rollback release | Candidate release | Manifest SHA-1 | Archive SHA-256 |
| --- | --- | --- | --- | --- |
| Fixture core, hourly publisher and post-match settlement | `JXD987-fixture-core-80-aae97d-release` for core/publisher; `JXD987-fixture-core-900d1ca-release` for settlement | `JXD987-fixture-core-81-85450e-release` | `85450ebeada00bd06d67cc8d3b82cb45947481e4` | `8f5e9f13c2898d0f14554b52d363af148769439a9194007b0569e434f8d53ea9` |
| Odds matcher P1/P2/P3 | `JXD987-odds-matcher-48d251e-release` | `JXD987-odds-matcher-81-3148fb-release` | `3148fb66d0652785cd2243c3942e87b3e51f3267` | `2d949c5bcf2ad86c9e7514e40c2d5ad13f9d905b5190c1a5450935c80ace57a2` |
| Leaderboard stats reconciliation | `JXD987-leaderboard-65e105b-release` | `JXD987-leaderboard-81-563d92-release` | `563d92e85772539beb82c9f3e662ca52f61244b5` | `b2f4ba405f494c3aa6facc3e7d80bdb63ab7154cb5f0ce0d073a73197078431b` |
| Website odds delivery | `odds-delivery-5b315fb-release` | `odds-delivery-81-925ae5-release` | `925ae5a0c124669094f9d2c257e4edc033df7f48` | `a4e24b6352e01e59d4a8acab6490a13c1f61fda86acfda3ef06d9fee1d911b59` |
| Squad reconciliation | `JXD987-runtime-7e74d46-release` | `JXD987-runtime-81-700c64-release` | `700c64178cb58512fb05ee9cf4ff593cb2644f71` | `602a691aaf944428f85b04d889633fbc1fbc60a1322b2a0db0569dd38cd90847` |

The five corresponding archives are in the local `release-artifacts/`
directory with names `fixture-core-81-{core,matcher,leaderboard,delivery,runtime}-<manifest-prefix>.tar.gz`.
Each archive was extracted into an empty directory and every manifest entry
was hash-verified. Each composed release's full manifest then passed the
Linux runtime verifier. The executable Linux reservation/settlement,
ownership-expiry, lock-order, bounded-wait, heartbeat-stall and post-wait
lease tests passed on **all five** compositions, as did the admitted-work
handoff test. This is focused admission evidence, not complete end-to-end
certification of each older release's unrelated jobs.

## Forward map for a no-added-cost rollout after the evidence gates

1. Recheck all active releases and manifests, archive hashes, symlink targets,
   cron entries and the currently held locks. Confirm no candidate target
   path already exists. Do not start the rollout during a fixture-core source
   run, settlement batch or data-heavy maintenance hold.
2. Install the five immutable packages side by side, restore each release's
   existing `.env` and `.venv` symlink targets and the exact shared `data`
   links above, and verify their entire manifests on the VPS. Compare all
   data-using candidates' resolved SQLite device/inode to the old settlement
   file before freezing cron. Keep all rollback directories untouched.
3. Back up the complete root crontab, then make a **first atomic crontab
   replacement** that removes every affected old entry: P1, P2, odds P3,
   fixture core, hourly publisher, settlement, stats supervisor, website odds
   delivery and squad reconciliation. Leave unrelated entries exactly as
   found. Read back the resulting crontab and prove none of the affected
   entries remains. This is a temporary admission freeze; record its start
   time and keep it as short as a safe drain permits. A path change alone
   leaves old processes and their in-memory functions alive.
4. Inventory all affected old wrapper processes and descendants, including
   ones waiting in a lock retry. Wait for them to finish. After cron has been
   frozen, send `SIGSTOP` to the long-lived stats supervisor **itself** so its
   loop cannot start another batch during the idle check. Inspect its process
   tree and the canonical and database-heavy locks while it is stopped. If it
   has an active worker child or holds either work lock, send `SIGCONT` and let
   it finish, then repeat the stop-and-inspect check at a later handoff. A
   sleep child may finish while the parent is stopped. A child confirmed in
   zombie (`Z`) state has exited and holds no work lock; do not mistake it for
   live work. If a child state is uncertain, resume the parent to reap it and
   repeat the stop-and-inspect check. Once there is no live worker child, no
   work lock and no live descendant, send `SIGKILL` to the stopped supervisor
   and verify its exit and the release of its supervisor lock before
   activating a new supervisor. If the transition is abandoned while the
   supervisor is stopped, send `SIGCONT` before restoring the old cron.
   The supervisor has **no graceful drain command**; do not infer idleness
   from a momentary childless snapshot while its loop is running. Check the
   canonical lock, heavy lock and reservation sidecar after the drain. Do not
   activate new entries while any old protocol process is still alive. If
   quiescence cannot be proved, resume a stopped supervisor, then restore
   the backed-up crontab and postpone the cutover. The freeze may skip a
   scheduled settlement or publication; record any missed run and verify its
   next normal scheduled execution after activation.
5. In a **second atomic crontab replacement**, repoint P1 (`17 *`), P2 (`37 */2`) and odds
   P3 (`19 */6`) to the matcher package; fixture core (`17 */6`), hourly
   publisher (`11 *`) and settlement (`*/15`) to the core package; stats
   supervisor (`*/5`) to the leaderboard package; website odds delivery
   (`4,19,34,49 *`) to the delivery package; and squad reconciliation
   (`20,50 *`) to the runtime package. Preserve every existing environment
   prefix, argument, frequency and log path. Leave unrelated Models,
   Operations and other lock domains untouched.
6. Read back each exact cron entry, verify the new supervisor starts from the
   new release, and inspect the canonical lock and reservation owner. Observe
   a real fixture-core attempt while normal odds delivery and reconciliation
   are active; confirm one admissible source refresh, ordinary writer
   resumption, settlement continuity, an hourly guarded publisher renewal,
   a current release resolver, and authenticated customer fixtures. Watch at
   least the next two scheduled six-hour core runs. The first observation
   does not establish long-run capacity.

## Rollback and partial-cutover safety

Rollback also uses a two-stage barrier. First remove all affected candidate
cron entries atomically, leaving unrelated entries in place; read them back
and wait for every new wrapper and descendant to exit. Quiesce the new stats
supervisor with the same stop-and-inspect procedure before checking for active
children and work locks; release it to finish if it is busy, and resume it if
the rollback is abandoned. Do not delete any release directory while a
process uses it. An old normal writer ignores the
reservation, so do not leave a mixed protocol operating and describe it as a
successful rollback. If a reservation exists, identify its owner and expiry,
let an admitted core invocation drain, and clear only that owner after it
exits; otherwise wait for its bounded expiry. Only after proving the new
protocol has quiesced, restore the backed-up **entire** crontab, including
#80's `17 */6` core and hourly publisher and the old settlement path. Verify
the old supervisor is again the only holder of its own supervisor lock, then
normal writers, settlement and the current release resolver. Record any
scheduled run missed during the freeze. Rollback cannot make expired fixture
source evidence fresh or extend an expired release pin.

## Live execution and current verification boundary

All five archives passed their recorded SHA-256 checks on the VPS, and all
five installed directories passed full runtime-manifest verification. The
first two-stage cutover froze nine entries at 15:41:40 UTC, drained the old
stopped stats supervisor at 15:42:59 and activated nine new entries at
15:43:41. The new supervisor started at 15:45. The first new settlement at
15:45 failed: it found zero fixtures and `No seasons to export`. Its wrapper
and exporter matched the old settlement byte for byte, but the core candidate
had created a 147 KB local SQLite file while the old settlement read the
11.9 GB shared SQLite file. This was a deployment-link error, not evidence
that the reservation algorithm failed. We froze the candidate entries at
15:47:18, drained the stopped new supervisor and restored the full #80/old
crontab at 15:47:59, with no mixed-protocol process or reservation remaining.

We moved the candidate's empty local `data` directory into private staging,
linked core, matcher and squad runtime to their old shared data targets,
and verified that core, matcher, stats and squad candidates all resolve
`data/jxd.sqlite` to the same device/inode as old settlement; their manifests
still verify. Website odds delivery keeps its explicit database path. A
second complete nine-entry freeze and activation occurred at 15:49:46 and
15:50:07 respectively after proving no old protocol process or reservation
remained. **The #81 cron cutover is currently active, not yet fully verified.**
The 16:00 scheduled settlement used manifest `85450ebeada0`, reconciled 158
recent fixtures, exported 162 and finished with a durable `success` ledger
row at 16:01:23. The new stats supervisor started from manifest `563d92e85772`
at 15:55. The 16:11 hourly publisher made the expected guarded skip because
the 15:51 renewal was recent. The 16:15 settlement then exported 167
fixtures; 16:17 P1 matched 13 fixtures and 15,235 odds outcomes; 16:19 odds
delivery exported 127,217 rows; and the 16:20 squad run wrote 720
memberships. The stats supervisor remained active. Sustained source and
settlement continuity and the next 18:17 fixture-core run remain to be
observed.

One controlled #81 publisher invocation at 15:50:59 corrected a concurrent
fallback overwrite. It reused identical published projection rows (zero
projection writes), recorded the fresh primary source in release metadata
and renewed the current pin to 17:51:02. The separate GitHub fallback race
guard was merged to JXD main at `acbee957`; its next scheduled execution
still requires observation. If a candidate job fails, use the two-stage
rollback above before calling #81 fully verified. No paid capacity, disk,
spend-cap setting, service or job frequency changed.

## R20 publication guard follow-up — staged, not cut over

PR #83 corrects two publication races left by the merged GitHub workflow
guard. Its publisher acquires a session advisory lock across the intermediate
build-marker commit, resets the repeatable-read snapshot after acquiring the
lock, and compares the selected source evidence and proposed pin with the
committed current release. The hourly throttle applies only while the current
pin has at least 45 minutes remaining. Full publications persist source
evidence, as no-op renewals already do. A local PostgreSQL 17 two-connection
replay verified that the second publisher blocks on the lock and that the
post-lock commit is required to see the preceding publisher's committed row.

Review R21 identified one caller defect in the first staged package: a
correct benign supersession exit 2 would mark the admitted core job failed.
That first package (`38a77ccc7a39`) is **superseded and must not be activated**.
The corrected #81 backport is commit
`2e0d2c373fc3950553d464ad7599adc6e95a081b`. Its primary core wrapper
removes the prior publisher report, and accepts exit 2 only after a fresh
database read verifies the same live release, at least 45 minutes of pin
runway, source-bounded evidence and the exact benign skip reason. Unknown,
malformed and missing reports and real publisher failures remain failures.
The shared admission/exit handling in `common.sh` is unchanged. An extracted
admitted-command-chain test exercises these paths with the real verifier and a
stubbed database. The 79 fixture tests completed with 14 environment-gated
skips; shell syntax, Python compile and diff checks passed. The publisher
also rejects Supabase's transaction-pooler port 6543 for its session lock;
the effective VPS URL is a Supabase pooler on port 5432 without exposing
credentials in the record.

The corrected immutable package was derived from the active core archive by
replacing only `scripts/refresh_fixture_delivery.py` and
`scripts/vps/run_p3_fixture_core.sh`, then updating their manifest entries
and source identity. Its manifest is
`36221bc38deae6ef6920f415647fdaa300fe2e65` and archive SHA-256 is
`9e1a0aead856251aa6b0e3fcd818cd4e58a552c0e620f2126dfb9801428b2352`.
It is staged at
`/opt/odds-sync/JXD987-fixture-core-81-guard-v2-36221b-release` with the
same `.env`, `.venv` and `data` targets as active core. Archive SHA-256, all
43 manifest files, runtime ID, links, shell syntax and a no-write dry run
passed on the VPS. The staged verifier also accepted the live older-source
guard report after a fresh read of the current release. **No cron entry
points to this package yet.**

After loading each active package's `.env`, core/settlement, matcher, stats
and squad all resolve `JXD_DB_PATH` to the same real SQLite database, inode
`1072986`, 11,915,935,744 bytes and 20,643 fixture rows. Odds delivery's
explicit cron database path resolves to the same inode. This is stronger
evidence than checking candidate `data` links alone. At 16:28, two guarded
production reads through the staged publisher both exited 2 without a
publication: the selected primary source skipped a healthy release, and the
older fallback source was rejected as `newer_current_source`, preventing a
candidate 16:56 pin from replacing the current 17:51 pin. Those two reads
used the superseded first staged package; its publication decision is
unchanged in the corrected package.

For cutover, back up the current crontab, confirm no active core/publisher or
settlement process, then atomically replace only the three core-package
paths (fixture core, hourly publisher and settlement) with the staged path.
Keep all schedules, all other worker entries and all database/configuration
settings identical. Read back all entries and verify the next scheduled
settlement and publisher against manifest `36221bc38dea`. If either fails,
restore the backed-up three paths atomically to the still-installed
`JXD987-fixture-core-81-85450e-release`, then verify writer quiescence and
the current release before retrying. The broader two-stage rollback above
remains available if the #81 protocol itself fails.
