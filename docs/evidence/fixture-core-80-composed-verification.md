# Exact #80 runtime composition evidence — 8 November 2026

The production source release was copied read-only before composition. The
installed package was extracted again into a fresh local directory for this
comparison. Excluding tests, bytecode and credential/environment links, both
trees have 43 files. Exactly five files differ: the publisher Python, the two
VPS wrappers, `common.sh`, and its regenerated runtime manifest. No file was
added or removed. The full 230-line unified diff is
`fixture-core-80-composed-runtime.diff`; its SHA-256 is
`e28cbc338ac338905ecfd87e7b4db129d038df50e0245f64d82b57e62866584a`.
The complete 40-entry manifest is
`fixture-core-80-runtime-manifest.sha1`, with SHA-1
`aae97d98942b5c17a1c1094f34e86bde82dada31`. The on-VPS source manifest
was `7607d31bedf89d276918e47a58c6c5817d24c31c`; the installed archive
SHA-256 was
`3f07af9651127761da93295740d0e0f58cde68dd12ff55d2830d816637250a89`.

The exact extracted archive and installed release both passed the full
runtime-manifest verifier. Focused publisher, source-provenance and handoff
checks passed as recorded in the release record. The archive contains no
`.env`, `.venv`, tests, bytecode or SQLite data; the installed release links
to the unchanged production environment and virtual environment.

The current repository's broader settlement tests were also run against both
the untouched older production runtime and the composed #80 runtime. Both
reported the **same** four failures and 11 passes/one skip. The four tests
expect later database-heavy and stats-reconciliation/deploy functions that
neither older release contains:

- `test_database_heavy_coordination_is_fail_closed_and_releases_on_exit`
- `test_runtime_deploy_publishes_manifest_last`
- `test_stats_reconciliation_cron_installer_is_shell_valid_and_idempotent`
- `test_stats_reconciliation_supervisor_is_bounded`

The current `test_odds_quote_verification_delivery.py` cannot collect in
either runtime because both lack the later
`scripts.odds_quote_verification` module. The current leaderboard-shadow
test likewise cannot collect because both lack the later shadow-worker
module. These are source-version comparisons, not passing tests or complete
certification. They do not replace live verification of the next genuine
fixture source, publication renewal, authenticated customer paths and normal
background work.
