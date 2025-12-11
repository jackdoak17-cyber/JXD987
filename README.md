# JXD987 – SportMonks Football Data Store

Pipeline scaffolding to pull football data from SportMonks (teams, players, fixtures, H2H) and persist it to a database for modeling or app backends.

## Quick start
1. Python 3.11+ recommended.
2. Install deps: `python -m pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in:
   - `SPORTMONKS_API_TOKEN` (keep secret; do not commit)
   - `DATABASE_URL` (e.g., `postgresql+psycopg2://user:pass@host:5432/jxd` or `sqlite:///data/jxd.sqlite`)
   - Optionally tune `REQUESTS_PER_HOUR` (< 4,000 to stay under plan limits).
4. Run a sync (examples, safe to rerun):
   - `python -m jxd.cli sync-static` (countries/leagues/seasons/types/venues)
   - `python -m jxd.cli sync-teams --season-id 19734`
   - `python -m jxd.cli sync-players --season-id 19734`
   - `python -m jxd.cli sync-fixtures --season-id 19734` (lightweight fixtures + participants)
   - `python -m jxd.cli sync-fixture-details --season-id 19734 --limit 200` (fixtures + stats + lineups)
   - `python -m jxd.cli sync-fixtures-between 2025-01-01 2025-01-31 --with-details --league-ids 8,9 --limit 400`
   - `python -m jxd.cli sync-league-teams --league-ids 8,9,72` (fetch teams for configured leagues using current seasons)
   - `python -m jxd.cli sync-history --days-back 450 --days-forward 14 --with-details --league-ids 8,9,82` (rolling history with stats/lineups)
   - `python -m jxd.cli sync-bookmakers`
   - `python -m jxd.cli sync-odds --bookmaker-id 2 --league-ids 8,9,82` (Bet365 odds)
   - `python -m jxd.cli sync-player-odds --days-forward 7 --league-ids 8,9,82` (player prop odds)
   - `python -m jxd.cli sync-inplay-odds --bookmaker-id 2 --limit 200` (latest inplay odds snapshot)
   - `python -m jxd.cli sync-livescores` (live fixtures window)
   - `python -m jxd.cli sync-h2h --team-a 8 --team-b 14`
   - `python -m jxd.cli compute-forms --samples "5,10,15,20,25,50" --availability-sample 5`
   - `python -m jxd.cli normalize-odds`
5. Use Postgres for production; SQLite is fine for local prototyping. A compressed SQLite snapshot is kept in-repo at `data/jxd_dump.sql.xz` (created with `sqlite3 .dump | xz`). To restore locally:  
   ```bash
   xz -d -c data/jxd_dump.sql.xz | sqlite3 data/jxd.sqlite
   ```
   Full, uncompressed DBs are uploaded as GitHub Actions artifacts on every run.

## Notes
- Requests are rate-limited to stay under SportMonks’ 4,000/hour cap.
- Upserts keep records fresh; rerunning syncs is safe.
- Default DB is SQLite for quick runs; use Postgres for production or bigger volumes.
- Odds default to Bet365 (bookmaker_id=2). Override with `BOOKMAKER_ID` env or CLI flag.
- Includes use `;` per SportMonks spec. `filters=populate` is applied automatically when safe.

## Data model (tables)
- `countries`, `leagues`, `seasons`, `venues`, `types` (stat/event reference)
- `teams` (venue, country, logo, founded, national flag)
- `players` (name pieces, nationality, position, physicals, team, photo)
- `fixtures` + `fixture_participants` (teams per fixture with locations/results)
- `team_stats` (per fixture raw stats blob) and `player_stats` (lineups + detail rows)
- `bookmakers`, `markets`, `odds_outcomes` (Bet365 odds preserved per outcome)
- `odds_latest` (snapshot of latest odds per market/selection)
- `team_forms`, `player_forms` (aggregated form percentages/averages) and `player_availability`
- `head_to_head` (pair-wise cached responses + fixtures list)
All tables store the raw API payload in JSON columns to keep future fields available.

## Scheduling
- GitHub Actions: see `.github/workflows/sync.yml` (hourly odds + player props, quarter-hour inplay+livescores, twice-daily 7-day stats window, daily 450-day history with details and odds). Add `SPORTMONKS_API_TOKEN` as a repo secret. Artifacts contain the full `data/jxd.sqlite` and the compressed `data/jxd_dump.sql.xz` after each run. The daily history job now calls `scripts/full_refresh.sh` end-to-end.
- Local cron (alternative):  
  - Daily history (04:00): `python -m jxd.cli sync-history --days-back 450 --days-forward 14 --with-details --league-ids 8,9,82,384,387 --limit 1800 && python -m jxd.cli sync-odds --bookmaker-id 2 --league-ids 8,9,82,384,387 --limit 800 && python -m jxd.cli sync-player-odds --days-forward 7 --bookmaker-id 2 --league-ids 8,9,82,384,387 --limit 400 && python -m jxd.cli compute-forms --samples \"10,25,50\" --availability-sample 5 && python -m jxd.cli normalize-odds`  
  - Hourly odds: `python -m jxd.cli sync-odds --bookmaker-id 2 --league-ids 8,9,82 --limit 200 && python -m jxd.cli sync-player-odds --days-forward 7 --bookmaker-id 2 --league-ids 8,9,82 --limit 200`  
  - Midday stats window: `python -m jxd.cli sync-fixtures-between $(date +\%F) $(date -v+7d +\%F) --with-details --league-ids 8,9,82 --limit 400 && python -m jxd.cli compute-forms --samples "5,10,15,20,25,50" --availability-sample 5`
Adjust season IDs per league/year. `sync-h2h` can be run ad hoc for upcoming matches.

## Environment safety
- Keep your API token in `.env` or an injected environment variable. Never commit it.
- If using Postgres, ensure the user has minimal privileges on the target database.
- The client uses `Authorization: Bearer <token>` and sleeps between calls to respect rate limits.

## SportMonks best-practice mapping
- Uses `filters=populate` automatically for bulk endpoints without includes (per docs: raises page size to 1000).
- Keeps includes on heavy endpoints (`statistics`, `lineups`) only when needed.
- Odds fetcher supports per-bookmaker filtering (Bet365 default).
- League defaults mirror your old repo (`LEAGUE_IDS` in `.env`), but CLI flags can narrow scope to save requests.
