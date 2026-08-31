# Changelog

Notable changes to this project, especially ones that require action when
upgrading an existing checkout. No formal release process yet - entries are
grouped under `Unreleased` until that changes.

## Unreleased

### Changed

- Live telemetry now writes to SQLite (`data.db`, `inverter_history` table)
  instead of per-run `data-*.csv` files. **If you have existing CSV files,
  run the one-off migration script** - see the README's "Upgrading" section.
- Added `hourly_summary`, a derived per-hour rollup (energy totals, sample
  count, per-phase grid voltage/frequency min-max, inverter/battery
  temperature min-max, and a `work_mode_label` breakdown), and a `/history`
  page for browsing both raw samples and hourly summaries in the browser.
  See `docs/superpowers/specs/2026-08-27-sqlite-storage-design.md` for the
  full storage design.
- `hourly_summary` is kept up to date automatically by the running app (on
  startup and on every hour rollover). A `--full-rescan` option on
  `_backfill_hourly_summary.py` covers the rare case of a gap that needs
  reprocessing after data was manually corrected or imported out of order.
