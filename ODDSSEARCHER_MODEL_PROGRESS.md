# OddsSearcher Model — Progress Log

Source spec: `/Users/jackdoak/Downloads/OddsSearcher_Model_Spec_v3.docx`

Rules:
- Do not print or commit secrets from `.env` / `.env.local`.
- Implement chapter-by-chapter and update this log as each chapter is shipped.

## Status

- [x] Chapter 1 — Overview (scope + markets)
- [x] Chapter 2 — Eligibility Gate
- [x] Chapter 3 — Layer 1: Baseline
- [x] Chapter 4 — Layer 2: Form & Consistency
- [x] Chapter 5 — Layer 3: Venue Adjustment
- [x] Chapter 6 — Layer 4: Opponent Adjustment (multipliers placeholder = 1.0)
- [x] Chapter 7 — Output & Value
- [x] Chapter 8 — Analysis Required (supporting analysis tables)
- [ ] Chapter 9 — Acceptance Criteria
- [x] Chapter 10 — Parameters & Review

## Log

### 2026-04-23
- Created progress log.
- Implemented initial OddsSearcher model API in `statswebsite-web`:
  - `GET /api/oddssearcher/model` returns probabilities, fair odds, best odds, and edge for player shots + SoT (1+/2+/3+).
  - Eligibility gate: recent starts + sidelined (injury/suspension).
  - Baseline Poisson from season per-90 scaled by expected minutes.
  - Form blending via last-10-starts hit-rates.
  - Venue blending via home/away splits (sample-size weighted).
  - Opponent tier computed dynamically; multiplier currently `1.0` pending Chapter 8 analysis.
- Added opponent-tier multipliers support:
  - Supabase table migration: `JXD987/supabase/migrations/20260423_oddssearcher_opponent_tier_multipliers.sql`.
  - Analysis + upsert route: `GET /api/oddssearcher/analysis/opponent-tiers?leagueId=...&seasonId=...` (writes unless `dryRun=1`).
  - Model now reads multipliers from `oddssearcher_opponent_tier_multipliers` and applies them per tier and position group.
- Captured all current parameter defaults in `statswebsite-web/src/lib/oddssearcher/modelConfig.ts`.
- Added acceptance support endpoints:
  - `GET /api/oddssearcher/standings-at` to manually validate dynamic league positions vs known standings.
  - `GET /api/oddssearcher/sanity` to scan upcoming fixtures for probability range + monotonicity violations.

## Remaining for Chapter 9

- Validate dynamic league position output against a known standings source for at least 1 league/season.
- Run venue analysis to confirm (or adjust) Chapter 5 weights.
- Run tier analysis and confirm multipliers look sensible (then re-run model sanity + spot-check outputs).

### 2026-04-24
- Layer 4 direction confirmed: opponent adjustment defaults to **shots conceded strength** (Option B).
- Added DB migration to support storing separate multipliers per opponent metric (`league_position`, `conceded_rank`, `possession_rank`) via `opponent_metric` column and updated PK.
