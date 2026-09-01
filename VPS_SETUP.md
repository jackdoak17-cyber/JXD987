# VPS Setup - Odds Sync Migration (V6)

This guide migrates odds sync scheduling from GitHub Actions cron to VPS cron, with lock-safe wrappers and priority tiers.

## 1) Prerequisites
- Ubuntu VPS with Python 3.12+, `git`, `sqlite3`, `psql`, `curl`, `jq`
- Repo cloned to `/opt/odds-sync/JXD987`
- `.venv` created and `pip install -r requirements.txt` completed
- `.env` present with production secrets

Install packages:
```bash
sudo apt-get update
sudo apt-get install -y git python3 python3-venv python3-pip sqlite3 postgresql-client curl jq
```

## 2) Required environment variables
Existing required secrets:
- `ODDS_API_KEY`
- `SPORTMONKS_API_TOKEN`
- `SUPABASE_DB_URL` (or `SUPABASE_DB_URL_SESSION`)
- Optional REST check: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`

New operational vars:
- `ODDS_SYNC_LOCK_FILE=/var/lock/odds-sync.lock`
- `ODDS_SYNC_P3_MAX_DURATION_SECONDS=600`
- `ODDS_API_RATE_LIMIT_PER_HOUR=5000`
- `ODDS_SYNC_MAX_CONCURRENT=3`
- `HEALTHCHECK_PING_URL=` (optional global)
- `HEALTHCHECK_PING_URL_P1=` (optional)
- `HEALTHCHECK_PING_URL_P2=` (optional)
- `HEALTHCHECK_PING_URL_P3=` (optional)
- `HEALTHCHECK_PING_URL_MODELS=` (optional)

Models publish vars (betting picks):
- `MODELS_REPO_ROOT=/opt/odds-sync/Models` (default)
- `MODELS_ENV_PATH=/opt/odds-sync/JXD987/.env` (default)
- `MODELS_MAX_DURATION_SECONDS=900` (default)
- `MODELS_TRAIN_TIMEOUT_SECONDS=14400` (default)
- `MODELS_SNAPSHOT_TIMEOUT_SECONDS=1200` (default)
- `MODELS_TOP=50` (default)
- `MODELS_PUBLISH_R2=true|false` (default true; requires `CLOUDFLARE_R2_*` when enabled)

## 3) Wrapper scripts
Location:
- `scripts/vps/common.sh`
- `scripts/vps/runtime_files.txt`
- `scripts/vps/runtime_manifest.sha1`
- `scripts/vps/update_runtime_manifest.sh`
- `scripts/vps/deploy_runtime.sh`
- `scripts/vps/run_sync.sh`
- `scripts/vps/run_p1.sh`
- `scripts/vps/run_p2.sh`
- `scripts/vps/run_p3.sh`
- `scripts/vps/run_postmatch_settlement.sh`
- `scripts/vps/run_stats_reconciliation.sh`

Exit codes:
- `0`: success (healthcheck ping sent)
- `1`: failure (no ping)
- `2`: skipped due to lock contention (no ping)

Lock/timeout behavior:
- All writers that can touch the shared SQLite spool use the canonical
  `ODDS_SYNC_LOCK_FILE`, including odds ingestion, settlement, models, and
  stats reconciliation. Historical reconciliation runs one bounded batch at a
  time so live settlement can acquire the lock between batches.
- Normal writers honor a three-minute pre-tick/two-minute post-tick settlement reservation. P1/P2 use a finite, lock-handoff retry budget so a scheduled tick queues across a long settlement writer instead of losing the lane; the final timeout/skip is still recorded in the Operations heartbeat.
- Settlement has priority and waits up to `SETTLEMENT_LOCK_WAIT_SECONDS` (default 300s) for an already-running writer, so one transient lock owner cannot silently lose a stats tick
- `timeout` enforces kill-on-overrun
- **The full chain is wrapped as one subshell command**, so timeout covers every step (not only the first command)

Runtime drift protection:
- wrappers verify `scripts/vps/runtime_manifest.sha1` before doing any work
- if a critical runtime file on the VPS drifts from the committed manifest, the wrapper fails loudly instead of silently running stale logic
- refresh the manifest locally with `scripts/vps/update_runtime_manifest.sh`
- deploy the runtime file set with `scripts/vps/deploy_runtime.sh <host> <remote_repo_root>`; the target is intentionally required so a deployment cannot update an unscheduled checkout

The fixture-delivery cron in production must deploy to the same checkout used
by both the P3 and settlement entries. For the current production schedule that
target is `/opt/odds-sync/JXD987-fixture-metrics-closeout-release`:

```bash
scripts/vps/deploy_runtime.sh <host> /opt/odds-sync/JXD987-fixture-metrics-closeout-release
```

## 4) Phase 1 schedule (parity mode)
Use one cron entry equivalent to existing cadence:
```cron
*/20 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_sync.sh >> /var/log/odds-sync.log 2>&1
```

`run_sync.sh` chain:
1. preflight per league
2. sync per league (`sync_odds.py` + refresh-upcoming)
3. ingest per league (`export_odds_to_supabase_psql.py`, retention skipped)
4. retention once (`odds_retention_psql.py`)
5. best-effort recent fixture refresh + lightweight fixture-core Supabase export
6. best-effort confirmed-lineup refresh for imminent fixtures + lineup export

## 5) Phase 2 schedule (Path B)
Path B is enforced:
- P1/P2: fetch-only
- P2: fetch-only + best-effort confirmed-lineup refresh for imminent fixtures
- P3: SportMonks refresh + fetch + ingest + retention + best-effort recent fixture-core refresh/export
- Post-match settlement: every 15 minutes, refresh fixture cores and run the bounded full-detail delivery worker for recently started fixtures. The worker retries provider-pending fixtures and verifies source-to-Supabase parity before marking them delivered. P3 deliberately uses `--fixture-core-only` so it cannot delete or overwrite fixture-player/statistics detail while the detail pipeline is reconciling it.

P3 is an odds/core-data job, not the stats-detail cadence. If the Odds API is
in standby because its credential is unavailable, P3 may remain on the VPS's
low-frequency standby schedule; live stats detail is still driven by settlement
every 15 minutes and the historical reconciliation safety net every 5 minutes.

### Conservative production schedule for Supabase Micro

Use this schedule while the production Supabase project is on Micro compute or while `odds_outcomes` / `fixture_player_statistics` have high dead-row counts. It prioritizes site stability over near-real-time odds freshness.

```cron
*/10 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_p1.sh >> /var/log/odds-sync-p1.log 2>&1
*/15 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_p2.sh >> /var/log/odds-sync-p2.log 2>&1
7,37 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_p3.sh >> /var/log/odds-sync-p3.log 2>&1

# Post-match fixture settlement (the stats-critical live writer). The worker polls
# SportMonks detail until it is complete, then publishes only parity-verified rows.
*/15 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_postmatch_settlement.sh >> /var/log/odds-sync-settlement.log 2>&1

# Historical stats reconciliation safety net. The wrapper owns a separate
# supervisor lock, then takes the canonical data lock for one bounded batch at
# a time; it exits when drained and cron restarts it for newly queued fixtures.
*/5 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_stats_reconciliation.sh >> /var/log/stats-reconciliation-supervisor.log 2>&1

# Betting picks publish (Models -> Supabase, optional R2 fallback).
# Uses the canonical lock and honors the settlement reservation window.
22 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models.sh >> /var/log/models-publish.log 2>&1

# Odds snapshots for ML enrichment (R2).
15 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models_snapshots_open.sh >> /var/log/models-snapshot-open.log 2>&1
5,35 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models_snapshots_close.sh >> /var/log/models-snapshot-close.log 2>&1

# Weekly full retrain + publish (Tuesday 01:15 UTC).
15 1 * * 2 cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models_train_weekly.sh >> /var/log/models-train-weekly.log 2>&1
```

### Aggressive schedule for larger Supabase compute

Only use this after Supabase has enough IO/CPU headroom and the hot tables have been vacuumed/reindexed.

```cron
*/2 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_p1.sh >> /var/log/odds-sync-p1.log 2>&1
*/5 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_p2.sh >> /var/log/odds-sync-p2.log 2>&1
*/20 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_p3.sh >> /var/log/odds-sync-p3.log 2>&1

# Betting picks publish (Models -> Supabase, optional R2 fallback).
# Shares the same global lock as P3 and will skip while ingestion is running.
*/15 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models.sh >> /var/log/models-publish.log 2>&1

# Odds snapshots for ML enrichment (R2).
15 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models_snapshots_open.sh >> /var/log/models-snapshot-open.log 2>&1
*/10 * * * * cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models_snapshots_close.sh >> /var/log/models-snapshot-close.log 2>&1

# Weekly full retrain + publish (Tuesday 01:15 UTC).
15 1 * * 2 cd /opt/odds-sync/JXD987 && /opt/odds-sync/JXD987/scripts/vps/run_models_train_weekly.sh >> /var/log/models-train-weekly.log 2>&1
```

Important:
- No separate historical SportMonks cron entry (historical refresh remains inside `run_p3.sh`; the post-match worker polls only the bounded recent-fixture queue)
- Retention runs only in `run_p3.sh` (and `run_sync.sh` parity mode)

## 6) Healthcheck timeout formula
Because P1/P2 can skip while P3 holds the global lock, heartbeat timeout must include worst-case lock occupancy.

Use:
- P1 timeout: `ODDS_SYNC_P3_MAX_DURATION_SECONDS + 120`
- P2 timeout: `ODDS_SYNC_P3_MAX_DURATION_SECONDS + 120`
- P3 timeout: `ODDS_SYNC_P3_MAX_DURATION_SECONDS + 60`

Example (`ODDS_SYNC_P3_MAX_DURATION_SECONDS=600`):
- P1/P2 heartbeat timeout: 720s
- P3 heartbeat timeout: 660s

## 7) P1/P2 SLO note (expected behavior)
P1/P2 cron entries are bounded queue triggers. Their wrappers retry lock handoffs
for a finite budget (60 attempts at 15 seconds by default), while the canonical
lock still gives settlement priority and caps a normal writer before the next
settlement grace window. A persistent lock, provider outage, or repeated lease
handoff ends as an observable `skipped`/failure heartbeat; it is never converted
to success.

## 8) Cutover checklist (strict order)
1. Setup VPS dependencies and repo
   - Pass: wrappers exist and are executable
2. Load secrets into `.env`
   - Pass: preflight succeeds
3. Run manually once:
   ```bash
   cd /opt/odds-sync/JXD987
   scripts/vps/update_runtime_manifest.sh
   scripts/vps/run_sync.sh
   ```
   - Pass: exit `0`, retention report present, healthcheck pinged (if configured)
4. Deploy runtime file set explicitly after any VPS-affecting change:
   ```bash
   cd /opt/odds-sync/JXD987
   scripts/vps/deploy_runtime.sh <host>
   ```
   - Pass: remote `shasum -c scripts/vps/runtime_manifest.sha1` returns `OK` for every file
5. Spot-check Supabase writes
   - Pass: verify at least 5 rows across 2 leagues are updated
6. Disable GitHub Actions workflow **via GitHub UI only**
   - Actions tab -> `Sync Odds` workflow -> `Disable workflow`
   - Pass: workflow status shows disabled
7. Wait one full cycle
   - Pass: no scheduled GitHub run fires
8. Enable VPS crontab entries
   - Pass: `crontab -l` shows expected entries
9. Monitor 2 full P3 cycles (40 min)
   - Pass: freshness within expected windows, no repeated failures
10. Rollback if needed
   - Re-enable workflow in GitHub UI
   - Disable VPS cron (`crontab -e` remove entries)

## 9) Verification commands
```bash
# last logs
tail -n 200 /var/log/odds-sync.log

tail -n 200 /var/log/odds-sync-p1.log
tail -n 200 /var/log/odds-sync-p2.log
tail -n 200 /var/log/odds-sync-p3.log

# inspect reports
ls -lah /tmp/odds_*_report*.json
jq . /tmp/odds_sync_report_p3.json | head -n 40
jq . /tmp/odds_ingest_report_p3.json | head -n 40
jq . /tmp/odds_retention_report_p3.json | head -n 40
```

## 10) Notes on Phase 3B
If local intermediate DB remains SQLite, concurrent writes are not implemented.
Phase 3A (parallel fetch, sequential write) is final state for SQLite deployment.
