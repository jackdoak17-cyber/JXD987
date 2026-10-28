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
authorization. The default batch processes at most 1,000 dirty fixtures, eight
cohorts and 1,000 generation jobs per run.

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
