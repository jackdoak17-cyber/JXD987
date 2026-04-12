Model 1 reverse-engineering workspace

This directory is intentionally isolated from the existing pipeline and database
schema. It only:

- reads from the existing Supabase/Postgres database
- reads from the public Statshub value-bets API
- writes local artifacts under `artifacts/` and reports under `reports/`

It does not:

- modify existing tables
- add or change migrations
- write back into Supabase
- touch the current `jxd`, `scripts`, `Betting`, or `ValueBets` flows

Suggested entrypoint:

```bash
python research/model1_re/run_model1_research.py --date today
```
