# Official standings runbook

## Source of truth

`public.official_team_standings` is the canonical standard league table. It is
published from SportMonks season standings and never reconstructed from fixture
scores for customer display. `public.official_standing_corrections` retains the
provider correction evidence, but the correction is not applied again because
the official standing row already supplies position and points.

Fixture-derived standings remain an independent audit only.

## Safe release order

1. Apply `20261124_official_sportmonks_standings.sql`.
2. Run the `Golden Checks` workflow with `official_standings_contract=true`.
   This performs no database writes and must prove Southampton season `27903`,
   team `65`, is position `6` with `27` points.
3. On the VPS release, run:

   ```bash
   python scripts/sync_official_standings.py \
     --leagues "$(source scripts/vps/common.sh; supported_league_csv)" \
     --report-out /tmp/official-standings-initial.json
   ```

   The initial run refreshes every current season and backfills seasons with
   fixtures that do not yet have an official snapshot.
4. Verify the canonical table and `team_standings_cache` have identical rank,
   points, played and goal totals for every published scope.
5. Deploy the website change and run its team Stats API/browser certification.
6. Run one complete P3 cycle. Its fixture delivery release must report
   `source=sportmonks_official` for standings.

## Stop conditions

Do not publish when any requested season has an empty table, an unknown stage,
duplicate teams or group positions, missing standing details, inconsistent
P/W/D/L or goal difference, a smaller team set than the last good snapshot, or
a failed exact Southampton expectation.

The synchroniser validates before opening its publication transaction. A
failure therefore preserves both the last canonical snapshot and the existing
compatibility cache. Fixture delivery also refuses to publish when an official
scope or team is missing.

## Rollback

Disable the new P3 invocation or roll back the pipeline release. Do not delete
the canonical rows: they remain the last-known-good provider snapshot. A failed
fixture delivery build cannot move the current publication pointer. The website
release can be rolled back independently while `team_standings_cache` continues
to expose the same official ranks.
