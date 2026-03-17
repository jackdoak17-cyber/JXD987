# Odds Sync Audit (Read-Only)

Date: 2026-03-17  
Repo: `JXD987`

## 1) GitHub Actions execution chain (current production)
Source: `.github/workflows/sync_odds.yml`

Scheduled trigger:
- `*/20 * * * *`

Main scheduled jobs:
1. `prices` job (matrix, `max-parallel: 1`, league IDs: `8,9,384,387,564,567,82,301,600,501,444,72`)
2. `retention` job (runs after `prices`)
3. `summary` job (runs after `prices` + `retention`)

Per-league `prices` sequence:
1. `preflight_supabase_psql.py`
2. `rest_preflight.py`
3. `sync_odds.py --leagues <league> --days-forward 14 --refresh-upcoming ...`
4. `export_odds_to_supabase_psql.py --leagues <league> --days-forward 14 --skip-retention --skip-retention-snapshots ...`

Retention sequence:
- `odds_retention_psql.py --days-back 1 --days-forward 14 --snapshot-days 30`

## 2) Environment variables used by odds pipeline

### Workflow-level (sync_odds.yml)
- `ODDS_API_KEY`
- `SPORTMONKS_API_TOKEN`
- `SUPABASE_DB_URL_SESSION`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `ODDS_MARKET_ALLOWLIST`
- `ODDS_BOOKMAKERS`
- `ODDS_API_RETRIES`
- `ODDS_API_SLEEP_BASE`
- `ODDS_API_SLEEP_MAX`
- `ODDS_LOCK_TIMEOUT`
- `ODDS_STATEMENT_TIMEOUT`
- `ODDS_IDLE_TX_TIMEOUT`
- `ODDS_ADVISORY_LOCK_KEY`

### Script-level (odds pipeline)
- `scripts/sync_odds.py`: `ODDS_MARKET_ALLOWLIST`, `SPORTMONKS_API_TOKEN`
- `jxd/odds_api_client.py`: `ODDS_API_KEY`, `ODDS_API_BASE`, `ODDS_API_RETRIES`, `ODDS_API_SLEEP_BASE`, `ODDS_API_SLEEP_MAX`
- `scripts/export_odds_to_supabase_psql.py`: `JXD_DB_PATH`, `SUPABASE_DB_URL`, `SUPABASE_DB_URL_SESSION`, `ODDS_* lock/timeout vars`, `ODDS_MARKET_ALLOWLIST`, `ODDS_USE_ADVISORY_LOCK`, `PSQL_RETRIES`, `PSQL_RETRY_SLEEP`, etc.
- `scripts/odds_retention_psql.py`: `SUPABASE_DB_URL` / `SUPABASE_DB_URL_SESSION`
- `scripts/preflight_supabase_psql.py`: `SUPABASE_DB_URL_SESSION` / `SUPABASE_DB_URL`

## 3) SportMonks refresh in odds pipeline
`scripts/sync_odds.py` currently runs SportMonks refresh logic when refresh flags are enabled:
- upcoming fixtures (`sync_upcoming_window`)
- squad refresh for teams in scope
- sidelined refresh for teams in scope

This is controlled by flags in `sync_odds.py`:
- `--refresh-upcoming`
- `--refresh-squads`
- `--refresh-squads-missing` / `--no-refresh-squads-missing`
- `--refresh-sidelined-window` / `--no-refresh-sidelined-window`

## 4) Fixture selection and kickoff field
Odds fetch scope is selected from SQLite `fixtures` table using `starting_at`:
- `scripts/sync_odds.py -> load_fixtures(...)`
- window predicate: `starting_at >= now` and `< now + days_forward`

Kickoff field used throughout: `fixtures.starting_at`.

## 5) Local intermediate database/file
The fetch script writes to local SQLite and ingest reads from it.

- Path source: `jxd/db.py`
- Env: `JXD_DB_PATH`
- Default path: `data/jxd.sqlite`
- Database type: SQLite

Flow:
1. `sync_odds.py` writes odds rows into SQLite `odds_outcomes`
2. `export_odds_to_supabase_psql.py` reads SQLite rows, stages to Postgres, upserts into Supabase

## 6) Fetch/write interleaving model (before migration)
In `scripts/sync_odds.py`, per-league API fetch and DB writes are interleaved in one loop:
- Fetch `/events`
- Match events to fixtures
- Fetch `/odds/multi` batches
- Parse + write (`delete_fixture_market_rows`, `upsert_outcomes`, `session.commit`) within same processing cycle

This is not a two-stage pipeline in current form.

## 7) Cleanup / delete / update SQL blocks in ingest
Source: `scripts/export_odds_to_supabase_psql.py`

Key cleanup blocks:
- `deleted_existing` by `fixture_id + bookmaker_id + market_key` derived from stage table
- normalization/merge delete+update for match/team shots keys using `fixture_window`
- cleanup deletes for invalid match/team/goals rows using `fixture_window`
- optional `missing_markets` deletion derived from stage table + allowlist
- retention cleanup (`retention_cleanup_query`) across out-of-range fixtures
- snapshots retention (`retention_snapshots_query`) across age/range

Scoping conclusion:
- Many cleanups are fixture-window scoped (not strictly per selected fixture IDs from stage).
- Retention is intentionally broad.
- Safe tiered ingest for P1/P2 is high risk without deeper refactor.

Decision for migration: **Path B**
- P1/P2: fetch-only
- P3: fetch + ingest + retention

## 8) Shared session / concurrency safety
- `scripts/sync_odds.py` uses one SQLAlchemy session (`session = get_session(engine)`) across leagues.
- `SyncService` in `jxd/sync.py` also uses one shared session passed in constructor.
- This confirms concurrent writes on shared session are unsafe without explicit redesign.

## 9) Existing lock/concurrency guards
- GitHub workflow has `concurrency: group: sync-odds`.
- Matrix has `max-parallel: 1` (serial league execution).
- Postgres advisory lock support exists in ingest script but is optional (`ODDS_USE_ADVISORY_LOCK`), not enforced globally.
- No global filesystem lock exists today across entire odds chain.

## 10) Runtime measurements (VPS benchmark)
Measured on VPS (12 leagues, 14-day window):
- Sync stage wrote: `40,879` outcomes
- Matched events: `114`
- Odds API calls total: `23` (`events: 11`, `odds/multi: 12`)
- Ingest runtime: `7.71s`
- Observed chain duration from logs:
  - Sync start to sync complete: ~`146s`
  - Ingest: ~`8s`
  - Total observed (sync + ingest): ~`154s`

Operational max-runtime recommendation:
- Set `ODDS_SYNC_P3_MAX_DURATION_SECONDS` with safety headroom above observed runtime.
- Suggested initial value: `600` seconds.

## 11) API budget status
Measured full-window run used `23` odds API calls.
Given current fixture volume, the proposed tier cadence remains within configured hourly budget with significant headroom.
