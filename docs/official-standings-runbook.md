# Official standings runbook

## Source of truth

`public.official_team_standings` is the canonical standard league table. It is
published from SportMonks season standings and never reconstructed from fixture
scores for customer display. `public.official_standing_corrections` retains the
provider correction evidence, but the correction is not applied again because
the official standing row already supplies position and points.

Fixture scores may be used by offline audits, but no fixture-derived table,
cache, release row or copied metric rank is a customer data source.

## Safe release order

1. Apply `20261124_official_sportmonks_standings.sql`.
2. Run the `Golden Checks` workflow with `official_standings_contract=true`.
   This performs no database writes and must prove Southampton season `27903`,
   team `65`, is position `6` with `27` points.
3. On the VPS release, run:

   ```bash
   python scripts/sync_official_standings.py \
     --leagues "$(source scripts/vps/common.sh; supported_league_csv)" \
     --all-fixture-seasons \
     --continue-on-error \
     --report-out /tmp/official-standings-initial.json
   ```

   Inspect the complete report before continuing. The reviewed provider
   inventory validates 71 of 72 fixture-backed seasons. League `989`, season
   `22847`, currently has no SportMonks standings and must be the only reported
   failure; it remains unavailable in the website rather than falling back to
   a reconstructed table. Any different or additional failure stops release.
   Routine P3 runs omit both flags and refresh only the latest started season
   for every supported league, so the known historical absence cannot block
   current-table freshness.
4. Deploy the fixture publisher that does not build
   `fixture_delivery_standings` or copy `league_rank` into fixture metrics.
   Activate the guarded GitHub publisher before the legacy VPS publisher tick,
   and verify one publication from that exact revision. The existing
   `--skip-if-publication-active` guard must make the older VPS runtime skip.
5. Deploy the website revision that reads every team position from
   `official_team_standings` and run its standings contracts, build and fixture
   smoke certification.
6. Apply the terminal `retire_legacy_standings` migration. It must replace the
   fixture metrics read functions first, then remove the copied metric column,
   release counter, refresh-run history and both duplicate standings tables.
7. Verify all supported current leagues and every returned fixture rank against
   `official_team_standings`. Confirm that no live function, view, cron command,
   website route or publisher references either retired table or the removed
   persisted rank.

## Stop conditions

Do not publish when any requested season has an empty table, an unknown stage,
duplicate teams or group positions, missing standing details, inconsistent
P/W/D/L or goal difference, a smaller team set than the last good snapshot, or
a failed exact Southampton expectation.

The synchroniser validates before opening its publication transaction. A
failure therefore preserves the last canonical snapshot. Customer readers fail
closed when an official scope or team is missing; they never reconstruct a
table or use an older duplicate.

## Rollback

Disable the new P3 invocation or roll back the publisher release. Do not delete
the canonical rows: they remain the last-known-good provider snapshot. A failed
fixture delivery build cannot move the current publication pointer. After the
terminal migration, do not roll the website or publisher back to a revision
that requires either retired standings table or the persisted `league_rank`
column. Recovery must preserve `official_team_standings` as the sole source.
