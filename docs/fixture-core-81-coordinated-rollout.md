# Fixture-core #81 coordinated admission rollout candidate — 8 November 2026

This is a prepared, **unapplied** five-checkout change. Jack approved and we
executed the narrower #80 cutover at 14:42 UTC. That approval does not cover
this broader reservation cutover. No additional infrastructure or spend is
proposed.

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

## Forward map for a separately approved rollout

1. Recheck all active releases and manifests, archive hashes, symlink targets,
   cron entries and the currently held locks. Confirm no candidate target
   path already exists. Do not start the rollout during a fixture-core source
   run, settlement batch or data-heavy maintenance hold.
2. Install the five immutable packages side by side, restore each release's
   existing `.env` and `.venv` symlink targets, and verify their entire
   manifests on the VPS. Keep all rollback directories untouched.
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
   sleep child may finish while the parent is stopped. Once there is no worker
   child, no work lock and no remaining descendant, send `SIGKILL` to the
   stopped supervisor and verify its exit and the release of its supervisor
   lock before activating a new supervisor.
   The supervisor has **no graceful drain command**; do not infer idleness
   from a momentary childless snapshot while its loop is running. Check the
   canonical lock, heavy lock and reservation sidecar after the drain. Do not
   activate new entries while any
   old protocol process is still alive. If quiescence cannot be proved, restore
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
children and work locks; release it to finish if it is busy. Do not delete any
release
directory while a process uses it. An old normal writer ignores the
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

The #81 cutover requires a separate owner approval and an operationally safe
supervisor handoff. Neither has occurred.
