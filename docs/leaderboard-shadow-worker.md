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
WAL and temporary writes are attributed from `pg_stat_statements` only for SQL
that targets the shadow schema; capture-trigger events receive a conservative
transactional WAL allowance. A reset, eviction or regression of a pinned
Milestone 3 query's evidence stops processing, while unrelated statement churn
does not. The worker probes `has_parameter_privilege` once and sets
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
