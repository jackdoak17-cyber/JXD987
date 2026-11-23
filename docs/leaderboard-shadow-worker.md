# Player leaderboard shadow worker

This worker is database-only. It never calls SportMonks or the Odds API.

The integration is inert unless all separately approved controls are enabled:

- `LEADERBOARD_SHADOW_CAPTURE_ENABLED=true` enables transactional capture in
  the authoritative fixture-detail publishers.
- `leaderboard_shadow_v1.pipeline_state.worker_enabled` permits normal worker
  execution.
- `leaderboard_shadow_v1.pipeline_state.backfill_enabled` permits historical
  bootstrap.
- The installed source-table triggers remain disabled until a separate
  migration explicitly enables them.

`run_leaderboard_shadow_worker.sh` uses a non-blocking host lock and a protected
environment file. It is intended for a one-minute schedule after production
authorization. A run processes at most 250 combined capture/publication fixture
operations or 30 seconds, whichever is reached first. The remaining time is
also applied as the PostgreSQL statement timeout and lock waits remain capped
at two seconds. Unfinished queue and generation work is retained for the next
scheduled run.

Leaderboard publication and historical statistics reconciliation share the
separate `/var/lock/oddssearch-db-heavy.lock` maintenance slot. The source
reconciler has priority: it waits up to 35 seconds for an already-bounded
leaderboard run, while a leaderboard run defers immediately when reconciliation
is active. This does not replace the canonical SQLite writer lock. It prevents
the two independently safe Supabase-heavy workloads from overlapping after
production evidence showed that overlap could regress source ingestion beyond
the 10% safety limit. Lock ownership is process-scoped and is released on exit,
timeout or signal; durable queues make every deferral resumable.

Every run enforces the approved resource guard before work and before
publication. PostgreSQL measures the shadow footprint and index ratio directly.
Each bounded run opens a durable resource-accounting window and checkpoints it
at every existing guard boundary. While `pg_stat_statements` remains
continuous, only shadow SQL WAL and temporary writes are charged. If its
bounded cache evicts an entry during the window, the checkpoint conservatively
charges all database WAL and temporary I/O generated during that same short
window. The cumulative accepted amount is stored independently of the cache,
so later evictions cannot erase usage. Capture-trigger events retain their
conservative transactional WAL allowance. A statistics reset, regressed
database temporary-I/O counter or hard limit still stops processing. The
next non-overlapping worker also closes any resource window left unfinished by
a killed process before it starts new work. The interrupted run is recorded as
failed, and its resource use is charged exactly when possible or conservatively
from its bounded database window, so a crash cannot create an accounting gap.
The
worker probes `has_parameter_privilege` once and sets
`temp_file_limit` from the approved maximum only when the connected role may do
so. Supabase's non-superuser `postgres` owner is supported without privilege
escalation: attributed temporary-I/O, statement/lock deadlines and bounded runs
remain mandatory, and publication is blocked after a breach. Worker output
records whether the optional session cap was enforced.

Platform disk headroom and source-ingestion performance must be refreshed in
`pipeline_state` within 15 minutes. Importer timing is split into end-to-end
latency, database-heavy-slot wait, provider time and non-provider processing.
End-to-end latency remains an acceptance alert and freshness signal; a
non-provider processing regression over the approved 10% limit is a hard
processing stop. This prevents deliberate importer-priority waiting or
provider latency from masquerading as slower database processing while still
failing closed on real processing regression, stale evidence or importer
failures.

Before each production run the worker reads the exact two-field JSON document
(`disk_available_bytes` and `observed_at`) at
`LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE` (default
`/var/lib/oddssearch/leaderboard-shadow-resource.json`). The file must be a
regular file and cannot be group/world writable. Its producer owns only the
authoritative Supabase platform-disk measurement. Importer p95 values are
derived inside PostgreSQL from at least 20 successful runs of the same release
and non-empty fixture workload. The baseline can be pinned only while worker
and backfill processing are disabled; normal evidence refreshes cannot replace
it. A new importer release or workload requires another explicit disabled-state
arm. The database rejects disk evidence older than five minutes and importer
evidence older than 15 minutes. Missing, stale, undersampled or breached
processing evidence pauses the pipeline and is exposed to Operations. Total
latency and full execution regressions remain explicit Operations warnings;
lock waiting and provider time stay visible and cannot be silently discarded.
After the historical reconciliation queue drains, its scheduled successful
zero-work run becomes a fresh idle heartbeat. That keeps hourly leaderboard
publication admissible without fabricating a performance comparison; a failed
or stale idle heartbeat still fails closed, and renewed non-empty work must
again match the pinned release and workload evidence.

The disabled-pipeline override is restricted to a socket-only database whose
name begins `oddssearch_m3`. It cannot be used against the production Supabase
connection.

Historical bootstrap uses `backfill-all`. Each call advances bounded batches
for supported, searchable, non-cup league-season cohorts. The fixed fixture-ID
watermark, count and hash distinguish bootstrap work from concurrently
committed live fixtures. A changed bounded source set fails closed.

Operational evidence is written to `worker_runs` and `pipeline_state`; the
Operations application reads only the restricted health function. Failed
claims are released, expired leases are reclaimable and publication does not
advance until every immutable generation job succeeds.

Initial publication catch-up uses the same one-minute tick, importer-priority
slot and 250-operation/30-second bound. Steady state keeps that small resumable
tick but adopts a one-hour publication freshness objective; the worker does no
work when the durable queue is empty. Operations alerts before queued changes
can silently exceed that objective, while the last complete atomic generation
remains customer-visible.

Production activation, trigger enablement, backfill and the 48-hour soak are
outside this candidate and require their own approved runbook.

## Draft single-use finite trial path

`run_leaderboard_finite_trial.sh` is a separate manual entrypoint. It verifies
the complete runtime manifest, protected environment file and exact Git
release before calling the whole-process supervisor. It has no cron entry and
does not turn on the ordinary worker or customer reader. The reviewed policy
ID, full release SHA and SHA-256 of the existing database URL must be pinned
in `LEADERBOARD_FINITE_TRIAL_POLICY_ID`,
`LEADERBOARD_FINITE_TRIAL_RELEASE_SHA` and
`LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256`, plus the reviewed SHA-256 of
the complete runtime manifest in `LEADERBOARD_FINITE_TRIAL_RUNTIME_MANIFEST_SHA256`, together with a reviewed
`LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE`. The URL itself is never logged.

The production-named `finite-once`/`finite-recover` commands require the
network `postgres` database, its pinned URL, a complete release SHA, a unique
supervisor connection token and both inherited worker/database-heavy lock
descriptors. Direct invocation fails before admission. The worker creates the
fixed transition and requests bound admission in one database transaction on
its own backend; refused admission rolls that transition back. The supervisor retains
those locks through the bounded process deadline, backend drain and recovery;
an uncertain result leaves its durable quarantine marker for an explicit
operator handoff. The marker retains every connection token across a supervisor
crash. An operator can run `run_leaderboard_finite_trial.sh reconcile` after
reviewing the marker: it holds both locks, drains only those connections and
the exact reserved owner, then runs bounded recovery. A separate
`.quarantine.heavy` marker blocks every shared database-heavy entrant until
exact backend absence is proved. It is cleared after authoritative drain, so
normal source reconciliation resumes even if a failed attempt has no
reservation. The trial-only `.quarantine` marker clears only after a settled
reservation. If the attempt created no reservation, that marker remains and
another trial stays blocked pending a separately reviewed disposition of
possible pre-admission WAL. It uses the private `leaderboard_trial_v1` SQL contract
from website draft PR #103. The ordinary wrapper also refuses the marker.

In the disposable PostgreSQL integration run, the same production-named code
path rejected a direct call and a wrong release, quarantined the unreserved
wrong-release attempt, captured a real fixture once in a separate clone,
settled its charge, refused a repeat and retained the exhausted old campaign
with capture on. Its `--production-trial-isolated-test` flag is restricted to a
socket-only `oddssearch_m3*` database and is never passed by the VPS wrapper.
This is draft code and local evidence. The fixed numeric policy, database
migrations, release archive, host configuration, action-time health and billing
check still require exact review before any production trial. The
[host cutover candidate](leaderboard-finite-trial-host-cutover.md) records why
both scheduled archive paths and the old long-lived statistics supervisor must
be retired together before the manual trial can run.
