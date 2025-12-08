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
   - `python -m jxd.cli sync-static` (countries/leagues/seasons/venues)
   - `python -m jxd.cli sync-teams --season-id 19734`
   - `python -m jxd.cli sync-players --season-id 19734`
   - `python -m jxd.cli sync-fixtures --season-id 19734`
   - `python -m jxd.cli sync-h2h --team-a 8 --team-b 14`
5. Use Postgres for production; SQLite is fine for local prototyping.

## Notes
- Requests are rate-limited to stay under SportMonks’ 4,000/hour cap.
- Upserts keep records fresh; rerunning syncs is safe.
- Default DB is SQLite for quick runs; use Postgres for production or bigger volumes.

## Data model (tables)
- `countries`, `leagues`, `seasons`, `venues`
- `teams` (venue, country, logo, founded, national flag)
- `players` (name pieces, nationality, position, physicals, team, photo)
- `fixtures` (league/season, teams, status, scores, venue, weather, raw payload)
- `head_to_head` (pair-wise cached responses + fixtures list)
All tables store the raw API payload in `extra`/`summary`/`fixtures` JSON columns to keep future fields available.

## Scheduling
Run the sync commands via cron/systemd/GitHub Actions. Example cron (once daily at 04:00):
```
0 4 * * * cd /path/to/JXD987 && . .venv/bin/activate && python -m jxd.cli sync-static && python -m jxd.cli sync-teams --season-id 19734 && python -m jxd.cli sync-players --season-id 19734 && python -m jxd.cli sync-fixtures --season-id 19734
```
Adjust season IDs per league/year. `sync-h2h` can be run ad hoc for upcoming matches.

## Environment safety
- Keep your API token in `.env` or an injected environment variable. Never commit it.
- If using Postgres, ensure the user has minimal privileges on the target database.
- The client uses `Authorization: Bearer <token>` and sleeps between calls to respect rate limits.

