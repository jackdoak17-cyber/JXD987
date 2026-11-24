# Final leaderboard cutover plan — accelerated pre-launch rollout

Status: **review required**

Plan date: **24 November 2026**

Operations repository baseline: **`2bdbe78e07882d4c6b9aa64006b85ccffd74f503`**

Website repository baseline observed during planning: **`094107310e3f5c78b08ae2092879190efd28b72e`**

Production database project: **`cxgnnelmgoagopngjosv`**

## 1. Decision requested

Approve an accelerated pre-launch cutover to the new published player leaderboard system.

The product has not launched and has no customers. We therefore do **not** need a prolonged customer migration, dual-read experiment, or seven-day wait before using the new system internally. The proposed sequence is:

1. satisfy the two existing shared-database admission gates;
2. run one exact-final-release production canary;
3. enable the bounded new worker and drain its backlog;
4. run the existing customer, source-oracle, and concurrency certifications against the new reader;
5. enable the new reader in production;
6. leave the legacy reader dormant as a rollback only during the seven-day pre-launch stability period;
7. remove the legacy reader after that period passes.

This plan intentionally eliminates unnecessary migration delay. It does **not** bypass database safety controls, because the leaderboard and Odds Search share the same Supabase project.

## 2. Executive summary

The new system is implemented and substantially tested. It has already completed many successful production shadow worker runs and produced published cohorts. The final repaired immutable Operations release is installed on the VPS, and both scheduled source reconciliation and the disabled leaderboard worker point at it.

The final release has not yet completed its own production canary. It is blocked by two truthful admission gates:

- the repaired source release needs 20 comparable successful performance samples before its importer baseline can be replaced;
- the rolling 24-hour worker temporary-data total must fall below the existing 512 MiB limit.

Those gates protect the shared database, not customer continuity. Once they pass, the cutover should proceed without an artificial soak between phases.

The seven-day gate begins **after** the new reader is active internally. It is a pre-launch stability gate, not a reason to keep the legacy reader serving production traffic.

## 3. Live snapshot used for this plan

Authoritative database read at `2026-11-24 10:59:49 UTC`:

| Item | Value |
|---|---:|
| Final source release evidence | `a41e89b4837a` |
| Comparable source samples | **5 / 20** |
| Current comparable target | 18 work units |
| Latest source run | success, 9 work units |
| Worker enabled | **false** |
| Backfill enabled | **false** |
| Capture enabled | **true** |
| Resource pause | **true** |
| Open worker runs | **0** |
| Rolling worker temp | **968,593,596 bytes** |
| Temp limit | **536,870,912 bytes** |
| Rolling worker WAL | 209,217,103 bytes |
| Rolling capture WAL | 227,794,944 bytes |
| WAL limit | 5,368,709,120 bytes |
| Capture queue | 251 fixtures |
| Pending generation jobs | 3,212 |
| Unpublished cohorts | 28 |
| Existing published cohorts | 81 |
| Historical failed generation rows | 26 |

The 26 failed generation rows are not active defects. Every affected fixture later completed a successful generation. Each failure is the intended stale source-version/hash rejection and must remain as audit history rather than being retried or deleted.

If no new worker temp is added, the current hourly ledger is projected to fall under the unchanged 512 MiB limit at approximately **23:00 UTC on 24 November**, when the prior-day 22:00 bucket ages out.

All live values must be reread immediately before each mutating phase. This snapshot is evidence for review, not an instruction to reuse stale numbers.

## 4. What is already complete

### 4.1 Database and policy

- The rolling 24-hour resource policy is installed.
- The hard limits remain 5 GiB worker WAL, 512 MiB worker temp, 3 GiB shadow storage, 1.0 index-to-heap ratio, and 20 GiB minimum available database disk.
- Sticky resource-pause and guarded-resume behavior is installed.
- `rebaseline_importer_only_v3(bigint,timestamptz)` is installed as `SECURITY INVOKER`, owned by `postgres`, with only explicit owner execution.
- `resume_resource_guard_v2(bigint,timestamptz)` is installed with the same ownership and execution boundary.
- No resource counter, limit, timeout, cost setting, quota, or capture history has been reset.

### 4.2 Worker and host cutover

- JXD pull request 113 repaired failure finalization and source heartbeat contention.
- JXD pull request 114 repaired the final two-entry host cutover validation.
- Final immutable VPS release: `/opt/odds-sync/JXD987-leaderboard-cutover-2bdbe78-release`.
- Final Operations commit: `2bdbe78e07882d4c6b9aa64006b85ccffd74f503`.
- Installed crontab SHA-256: `698c6b3e0de8c12800b83b99123007b2a4eb7e840910a96d1664a881b7baf020`.
- The runtime manifest verifies 55 required entries.
- Source reconciliation and leaderboard worker schedule entries both point at the final immutable release.
- The leaderboard worker is disabled in database state and no worker process is open.

### 4.3 Production test evidence

- Numerous production worker runs completed successfully on the preceding `f96caad...` worker release.
- Recent successful examples claimed and completed 13–143 items per bounded run with zero failed items and exact statement resource accounting.
- Existing successful publications prove the capture, generation, publication, and stored-reader architecture functions with real data.
- One worker run was interrupted during the wider Supabase database incident. It was recovered and conservatively accounted using the repaired failure path. That incident is the reason the exact final release still requires a fresh canary.

### 4.4 Website readiness

- The new reader is already implemented behind the server-only environment flag `PLAYER_LEADERBOARD_SHADOW_READS_ENABLED`.
- Default behavior remains the legacy reader while the flag is absent/false.
- Existing real-data certifications cover:
  - authenticated customer catalogue and result paths: `CERT_LEADERBOARDS_FULL=1`;
  - direct source-to-browser accuracy: `CERT_LEADERBOARDS_SOURCE_ORACLE=1`;
  - five simultaneous request bounded latency through one authenticated session: `CERT_LEADERBOARDS_CONCURRENCY=1`.
- The website treats an invalid or incomplete published shadow payload as unavailable rather than silently accepting malformed data.

## 5. Definition of done

The new system is implemented when all of the following are true:

1. Final source release has at least 20 genuine comparable successful samples.
2. Importer baseline is moved to that release without clearing the resource pause or enabling work.
3. Full resource guard passes with fresh external disk evidence and unchanged limits.
4. Exact final-release canary claims at least one item and completes all claimed items with zero failures.
5. Scheduled bounded worker is enabled; backfill remains disabled unless a separate gap audit proves it is required.
6. Capture queue and generation backlog drain without resource breaches or stuck leases.
7. All changed cohorts publish and the publication cursor is caught up.
8. Customer, source-oracle, and concurrency certifications pass against the new reader.
9. Production website uses the new reader by default.
10. Legacy reader is not serving normal production traffic.
11. Seven-day pre-launch stability gate has started with the new reader active.

The seven-day gate is required for launch readiness, but it does not block internal activation of the new system.

## 6. Non-negotiable constraints

- Do not generate manual source successes to reach the 20-sample gate.
- Do not reset resource counters or delete hourly resource history.
- Do not raise the 512 MiB temp limit, 5 GiB WAL limit, database capacity, or application timeouts as a shortcut.
- Do not hold the canonical writer lock while waiting for a future gate.
- Do not run certification, source reconciliation, and leaderboard heavy work concurrently.
- Do not enable backfill during initial activation.
- Do not enable the website shadow reader before a complete, current publication exists.
- Do not delete the legacy reader during the seven-day rollback window. It may remain dormant without serving requests.
- Do not classify a zero-work canary as a pass.
- Do not retry or delete the 26 historical stale-generation failures.
- Preserve normal Odds Search publisher priority throughout.

## 7. Prepared production controls

The first controller set reviewed with this plan is superseded and must not be executed. The corrected local set below addresses the independent review findings. It is not yet authorized or staged on the VPS; staging, remote byte-for-byte hash verification, and an independent rereview are required before execution.

| Controller | Purpose | SHA-256 |
|---|---|---|
| `leaderboard-2bd-rebaseline.sh` | Rebaseline only the importer after 20 genuine samples | `ce051d7b117c9146a7b0af1e27070863508960b05a61c927a4d4dd7f9cf83f6e` |
| `leaderboard-2bd-resume.sh` | Clear resource pause only if the full installed guard passes | `7928aed70d3af56ed4b712c0436796cb41964b6b5f053a6db3e9d96c7741bbc8` |
| `leaderboard-2bd-owned-250.sh` | One 250-bound, 30-second final-release canary with deterministic reconciliation | `d617034a77a17a05054f8380f87f8b60ba1e7c805a878a0a4beb5d072bf6a0e5` |
| `leaderboard-2bd-enable-scheduled.sh` | Enable normal bounded schedule using the latest exact canary and a fresh full guard | `4c76643949883cfd629e8f5f8c66857dfb0a2139a65639397b43ccdd847d5662` |
| `leaderboard-2bd-disable-scheduled.sh` | Fence new starts, disable, then recover at most one exact final-release run | `ded7a1afc8d121b3c392b418ca62f19dba52a99b0a1da6219f1ec0f4e8bdfaf7` |

The offline controller contract is `test-leaderboard-2bd-controller-contracts.py`, SHA-256 `1fc1fa5a44bbdf5015746a3afdeba5cf3576c5cdaabb7c4a4011f8ce52c5e5a3`. Nine tests pass. The tests execute the actual canary shell cleanup functions—with only the root-owner assertion adapted to the local test UID—with disable, recovery, and marker-removal failures injected; each failure must retain the fence. They also execute rollback marker creation, simulate a publisher deferral, resume with the exact retained token, remove the marker only after success, and refuse a foreign marker. The remaining contracts cover publisher reservation ordering, latest-run binding, fresh full-guard evaluation, and rollback fencing order. These are offline contracts, not hosted proof.

Each mutating controller checks both the fixture-core reservation and the normal publisher time window before holding the canonical writer lock. Forward mutations return a clean deferral when busy or reserved. The canary records that enable was attempted **before** the ambiguous commit boundary; every cleanup operation explicitly propagates failure, and state flags clear only after their operation succeeds. Cleanup commits worker=false first, then recovers at most one exact new final-release run/resource window, removes only its owned marker, and prints PASS only after reconciliation. Scheduled enable requires the supplied run to be the latest worker run, successful, exact-release, positive-work, statement-accounted, and resource-window closed; it refreshes external disk evidence and runs the complete installed guard in the enabling transaction. Rollback creates the worker quarantine first, prints its recovery token, and waits for the owned worker exclusion. A later ordinary deferral intentionally retains the fence. A retry must supply the exact token, verify the same root-owned marker, finish disable/recovery, and remove it only after success; a missing or foreign token is refused. Rollback never edits crontab.

## 8. Execution plan

### Phase 0 — review and freeze

Owner: reviewer + Operations lead.

1. Review this plan, the five corrected controllers, and their offline contract.
2. Confirm the deployed runtime bytes still match Operations commit `2bdbe78...`. A documentation-only change to GitHub `main` does not restart qualification when every deployed runtime byte and manifest hash remains identical.
3. Confirm the VPS runtime path, manifest hash, crontab hash, environment release SHA, and database DSN identity still match the reviewed values.
4. Confirm no transition/quarantine marker exists and no certification or source-heavy job owns an exclusion lock.
5. Confirm normal odds delivery has a recent committed success.
6. Rehash the corrected controllers locally, copy them to a new private root-owned staging location, and independently verify every remote byte/hash before admission.

Exit criterion: plan and controller review approved with no source drift.

Rollback: none required; this phase is read-only.

### Phase 1 — collect genuine source evidence

Owner: normal scheduler; operator observes only.

1. Allow ordinary source reconciliation to continue on release `a41e89b4837a`.
2. Count only successful runs whose work units are between 50% and 100% of the current target, with complete timing evidence.
3. Require `sample_count >= 20`.
4. Require the latest run to be a successful comparable run and no older than 15 minutes at mutation time.
5. Do not manually invoke source reconciliation to accelerate the count.

Exit criterion: the installed performance view reports at least 20 same-release comparable samples and a fresh comparable latest success.

Rollback: none required; no mutation occurs.

### Phase 2 — importer-only rebaseline

Owner: Operations lead under certification, worker, heavy, and canonical-writer exclusion.

1. Enter the reviewed publisher time window and require no active fixture-core reservation before taking the canonical writer lock. Otherwise defer cleanly.
2. Obtain fresh provider disk evidence with an actual observation timestamp no more than 210 seconds old.
3. Recheck worker=false, backfill=false, capture=true, resource_paused=true, and zero open worker runs.
4. Verify the installed rebaseline function definition, owner, invoker mode, and ACL.
5. Execute `leaderboard-2bd-rebaseline.sh` once.
6. Verify the importer release, sample count, target work units, total baseline, execution baseline, and processing baseline all match the current performance view.
7. Verify resource pause remains true and worker/backfill remain false.

Exit criterion: importer baseline points at `a41e89b4837a` with at least 20 samples; no processing is enabled.

Rollback: the database function is transactional. Any failed predicate leaves the previous baseline, pause, and switches unchanged. Do not retry until the failed predicate changes.

### Phase 3 — resource-window admission

Owner: Operations lead.

1. Enter the reviewed publisher time window and require no active fixture-core reservation before taking the canonical writer lock. Otherwise defer cleanly.
2. Wait until rolling worker temp is at or below 536,870,912 bytes.
3. Require combined rolling worker-and-capture WAL at or below 5,368,709,120 bytes, preserving the installed attribution rules.
4. Require shadow bytes, index ratio, actual available database disk, source regression, and evidence freshness to pass the installed guard.
5. Obtain a new provider disk observation within the 210-second admission margin.
6. Recheck worker=false, backfill=false, capture=true, zero open runs, and rebaselined source release.
7. Execute `leaderboard-2bd-resume.sh` once.
8. Verify `resource_paused=false`, pause reason null, `last_resource_evidence.ok=true`, no breaches, and worker/backfill still false.

Exit criterion: full installed guard is green with worker still disabled.

Rollback: `resume_resource_guard_v2` is transactional and reruns the guard after clearing the pause. Any breach aborts and restores the sticky pause automatically.

### Phase 4 — exact-final-release canary

Owner: Operations lead, with normal publisher priority.

1. Enter a safe phase of the normal 15-minute publisher cycle.
2. Hold certification, worker, heavy, reservation, and canonical-writer exclusions in the reviewed order.
3. Require fresh disk evidence and no quarantine/transition marker.
4. Execute `leaderboard-2bd-owned-250.sh` once.
5. The controller may temporarily enable the worker only for one direct run bounded by:
   - fixture limit 250;
   - capture limit 250;
   - generation-job limit 250;
   - 30-second internal work limit;
   - 45-second outer process limit.
6. Require exactly one new worker-run row.
7. Require claimed count greater than zero, succeeded=claimed, failed=0, final release SHA `2bdbe78...`, exact statement accounting, closed resource window, and zero open runs.
8. Once enable is attempted, require cleanup to reconcile state even if commit acknowledgement is lost: commit worker=false, recover at most one exact new run/resource window, verify zero open runs, remove only the owned quarantine, and only then print PASS.

Exit criterion: one positive exact-final-release canary passes and returns its exact run ID.

Rollback: any failure still runs the same disable/recovery reconciliation. Remove the owned marker if and only if the database is verified disabled with no open run. Retain it when reconciliation itself is incomplete; never print PASS before cleanup.

### Phase 5 — enable normal bounded processing

Owner: Operations lead.

1. Supply the exact successful canary run ID to `leaderboard-2bd-enable-scheduled.sh`.
2. Enter the reviewed publisher time window, require no active fixture-core reservation, and recheck the full source/runtime/crontab binding and fresh disk evidence.
3. Require worker=false, backfill=false, capture=true, resource_paused=false, and zero open runs.
4. Require the supplied ID to be the **latest** worker run, with positive work, success, exact release, exact accounting, and a closed resource window. A later failed or incomplete run invalidates the supplied ID.
5. Refresh external disk evidence inside the enabling transaction and require the complete installed guard to be green using that observation.
6. Enable only `worker_enabled=true`; leave backfill false.
7. Observe the next scheduled invocation from the protected final runtime.

Exit criterion: the normal scheduled worker starts and completes a positive run from the final release.

Rollback: run `leaderboard-2bd-disable-scheduled.sh`. It creates the owned worker quarantine before waiting for the worker lock and prints the exact recovery token to the private operator log. It commits worker=false before recovery, verifies/reconciles at most one exact final-release open run/resource window, leaves backfill false, and removes its marker only after success. A publisher/lock deferral retains the fence; rerun with `EXPECTED_RETAINED_FENCE_TOKEN` set to that exact token. A retry without the token or with a foreign marker must refuse to proceed. Do not edit crontab; its entries already point at the immutable final release. If the exact process does not yield within the bounded wait, retain the fence and escalate for separately authorized exact-process termination.

### Phase 6 — bounded backlog drain

Owner: Operations lead with read-only monitoring.

Do not raise limits to force a faster drain. Use the already installed one-minute schedule and 30-second work budget.

Monitor after each early run and then at five-minute intervals:

- run release SHA, claimed/succeeded/failed counts, duration, WAL, temp, and accounting mode;
- open run/resource-window count;
- resource-pause state and current evidence;
- capture queue depth and oldest dirty age;
- pending generation jobs;
- pending/unpublished publications;
- source publisher and source reconciliation health;
- database connection, statement cancellation, and checkpoint health.

Expected drain order is capture work first, then generation and publication. A temporarily flat generation count while capture is being consumed is not itself a failure.

Immediate stop conditions:

- any failed worker item;
- any run left open after its process exits;
- any resource breach or sticky pause;
- database authentication/checkout regression;
- normal odds delivery failure attributable to contention;
- source heartbeat failure;
- quarantine, lease, or fencing mismatch;
- publication cursor moving backward or published data becoming incomplete.

The 26 historical stale-generation failures are excluded from stop counting unless a **new** failed row appears.

Exit criterion: capture queue is empty or current with no aged dirty work, generation jobs are drained, and every changed cohort is published at its committed cursor.

Backfill remains disabled. Enable it only in a separately reviewed phase if a post-drain gap audit proves missing historical cohorts. Current database state reports no incomplete backfill checkpoint.

### Phase 7 — validate the new published reader

Owner: website + Operations engineer in an exclusive certification slot.

1. Create a protected website Preview from current website `main` with `PLAYER_LEADERBOARD_SHADOW_READS_ENABLED=1` server-side only.
2. Verify the Preview uses the shadow RPC and returns the expected model/publication evidence.
3. Run the existing authenticated customer acceptance suite with `CERT_LEADERBOARDS_FULL=1`.
4. Run the direct database/source-to-browser oracle with `CERT_LEADERBOARDS_SOURCE_ORACLE=1`.
5. Run the five-request latency suite with `CERT_LEADERBOARDS_CONCURRENCY=1`. It sends five simultaneous copies of one seeded request through one authenticated session; report only that proven scope, not multi-account capacity.
6. Require all tests to pass with nonempty results where the contract requires them.
7. Require no malformed/incomplete payload fallback, no unexpected legacy-reader use, no 5xx response, and all request limits under the existing 10-second hard test bound.
8. Compare representative player totals, per-90, averages, ratios, last-5, last-10, season, position, league, and historical-season queries to independent source evidence.
9. Attach an independent expected-ranking comparison for at least one representative cohort so omission of an expected top player cannot pass merely because the oracle recomputes only players returned by the API.
10. Certify one pinned Preview commit, preserve its deployment ID/publication evidence, and deploy that exact source commit to Production without rebuilding from a different tree.
11. Preserve private HTML/JSON evidence and exact source/deployment hashes.

Exit criterion: all three release-gate suites pass against the same complete publication and exact Preview source.

Rollback: disable the Preview flag. No production reader has changed yet.

### Phase 8 — production reader cutover

Owner: website release owner.

1. Confirm no source or publication changed between final certification and deployment, or rerun the affected comparison if it did.
2. Set `PLAYER_LEADERBOARD_SHADOW_READS_ENABLED=1` in Production.
3. Deploy the exact certified website commit.
4. Verify the Production alias points at that deployment.
5. Run a small authenticated Production smoke covering player total, per-90, last-N, season, multi-league, and one derived ratio.
6. Verify logs show the shadow path, not the legacy flexible batch reader.
7. Verify no customer-facing error and no malformed shadow payload.
8. Retain the exact Production deployment ID and the publication ID/cursor returned by the authenticated smoke.

Exit criterion: the new published reader is the Production default and smoke evidence passes.

Rollback: set the server-only flag false and roll back to the previously pinned flag-off deployment. Before stopping any legacy-only refresh, prove that this exact flag-off deployment still returns **current** data, not merely a successful stale response. The legacy reader remains dormant but usable during the seven-day gate.

### Phase 9 — retire the legacy path

Because there are no customers, no percentage rollout or long dual-read phase is required.

Immediately after Phase 8:

- stop treating the legacy reader as the normal path;
- remove any operational process that exists only to keep legacy leaderboard output current;
- retain the legacy code/RPC as a dormant rollback for the seven-day gate;
- do not send duplicate customer queries to both systems.

After the seven-day gate passes:

1. open a separate website PR removing the legacy branch from the player leaderboard request path;
2. remove its obsolete environment flag fallback and tests that exist only for legacy selection;
3. audit RPC usage before revoking or dropping any legacy database function;
4. make database retirement a separately reviewed migration with explicit dependency and rollback evidence;
5. update runbooks and monitoring to name only the new published system.

Dormant rollback code is not the same as operating the old system. It may remain for seven days without serving production traffic or consuming a separate worker budget.

### Phase 10 — seven-day pre-launch stability gate

Start this gate only after the authenticated Production smoke passes.

Schedule one low-volume authenticated probe in every UTC observation day, after a normal publisher cycle. Each probe must record the exact deployment/publication and enforce these initial named limits:

- for every eligible source change, elapsed time from that change to its committed publication no greater than 15 minutes; an unchanged cohort is not failed merely because its last publication timestamp is older;
- oldest dirty leaderboard queue age no greater than 15 minutes after the initial drain is complete;
- zero open worker runs outside an actively observed bounded invocation;
- installed resource guard `ok=true` with no breaches;
- latest normal odds-delivery report `ok=true`, `committed=true`, and no more than 20 minutes old;
- authenticated reader response successful, nonempty where required, and bound to the expected current publication.

A missing observation cannot certify that day. The daily probe is a checkpoint, not the only evidence: retain intervening worker runs, queue snapshots, publication receipts, source reconciliation, and normal odds-delivery records to judge the entire observation period. Tighten these limits if the measured steady state supports it; do not loosen them during the seven-day gate without a new review.

For seven consecutive days require:

- worker runs complete without failed items or unrecovered windows;
- resource guard remains green;
- normal Odds Search delivery and source reconciliation remain healthy;
- queue age stays bounded and publications stay current;
- no reader 5xx or incomplete-payload error;
- representative source-oracle checks continue to pass;
- p95 and maximum five-request latency bounds remain within the existing contract;
- no manual counter reset, limit increase, hidden retry, or data repair is needed.

Any material failure resets the gate only after the cause is fixed and a new final-release canary passes. The new system may remain active internally if the failure is understood and customer launch stays closed; otherwise use the dormant reader rollback.

Retire the legacy website branch only after seven consecutive observed passing days. Remove legacy database RPCs later, in the separately dependency-reviewed migration described in Phase 9.

## 9. Evidence required at each handoff

Every mutating phase must preserve:

- UTC start/end time;
- exact Git commit, immutable release path, runtime-manifest hash, crontab hash, and environment release SHA;
- lock admission and release evidence;
- before/after database state;
- relevant function definition hash, owner, invoker/definer mode, and ACL;
- fresh provider disk value and actual observation time;
- source performance view and sample count;
- worker-run ID and resource accounting;
- combined rolling worker-and-capture WAL against the unchanged 5 GiB limit;
- queue/generation/publication counts;
- private logs and SHA-256 manifest;
- explicit statement that no limit, timeout, counter, quota, or capacity setting changed.

Do not use file modification time as a substitute for provider observation time. Do not report a partial test run as a complete verdict.

## 10. Reviewer checklist

The reviewer should answer each item explicitly:

- [ ] Is the accelerated pre-launch decision acceptable: activate after canary, then run the seven-day gate while active internally?
- [ ] Are the 20-sample and rolling-resource gates preserved without a shortcut?
- [ ] Do the five corrected controllers bind the exact final release and fail closed on drift?
- [ ] Are lock ordering and normal Odds Search publisher priority preserved?
- [ ] Do rebaseline, resume, canary, scheduled enable, and rollback all defer for an active publisher reservation and unsafe publisher time window?
- [ ] Does rebaseline change only importer evidence and leave pause/worker/backfill unchanged?
- [ ] Does resume rerun the complete installed guard transactionally?
- [ ] Does the canary require positive work, exact resource accounting, lost-ack-safe cleanup, exact open-run recovery, and disable-before-PASS?
- [ ] Does scheduled enable require the supplied run to be the latest closed successful run and rerun the full guard with the fresh disk observation?
- [ ] Does rollback fence new starts before disabling, resume an exact retained fence after deferral, refuse foreign markers, and recover exact work without editing crontab?
- [ ] Is backfill correctly excluded from initial activation?
- [ ] Are the backlog stop conditions strict enough without inventing new thresholds?
- [ ] Do the three website certification suites cover customer behavior, source accuracy, and the accurately described five-request latency scope?
- [ ] Does independent expected-ranking evidence prevent an omitted top player from escaping the source oracle?
- [ ] Is the Production reader rollback a server-only flag/deployment change?
- [ ] Is the legacy reader dormant—not serving traffic—during the seven-day rollback window?
- [ ] Is the seven-day gate based on seven observed authenticated days with named publication-age, queue-age, and odds-delivery limits?
- [ ] Are database function removals deferred to a separate dependency-reviewed migration?

## 11. Estimated fastest timeline

These are dependencies, not promises:

- Source baseline: as soon as 20 natural comparable samples exist.
- Resource window: earliest projected clearance is approximately 23:00 UTC on 24 November if no new worker temp is added.
- Rebaseline + resume + canary: approximately 30 minutes once both gates are ready and the normal writer is idle.
- Initial scheduled activation: immediately after a positive canary.
- Backlog drain: determined by real bounded throughput; do not estimate from queue depth alone before the first final-release runs.
- Reader certification and Production switch: same day as backlog/publication completion if all three suites pass.
- Seven-day gate: starts immediately after the Production reader switch.
- Public launch: only after the seven-day gate passes.

## 12. Reviewer handoff state

At review time, no step in this document authorizes an immediate mutation by itself. The worker and backfill are still disabled, capture is enabled, the resource pause is active, and no worker run is open. The next permitted mutation is the importer-only rebaseline, but only after the live 20-sample predicate and every controller precondition pass.
