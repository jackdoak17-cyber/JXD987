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

Platform disk headroom and source-ingestion p95 are external operational
measurements and must be refreshed in `pipeline_state` within 15 minutes.
Before each production run the worker reads the exact four-field JSON
document at `LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE` (default
`/var/lib/oddssearch/leaderboard-shadow-resource.json`). The file must be a
regular file and cannot be group/world writable. Its producer owns the
authoritative Supabase platform-disk measurement and the independently measured
source-ingestion baseline/current p95. The database rejects evidence older than
five minutes; Operations fails evidence older than 15 minutes. Missing, stale
or breached evidence pauses the pipeline and is exposed to Operations; it
cannot be bypassed by a successful worker process.

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

Production activation, trigger enablement, backfill and the 48-hour soak are
outside this candidate and require their own approved runbook.
