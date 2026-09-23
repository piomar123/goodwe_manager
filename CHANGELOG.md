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
- `hourly_summary` is kept up to date automatically by the running app (on
  startup and on every hour rollover). A `--full-rescan` option on
  `_backfill_hourly_summary.py` covers the rare case of a gap that needs
  reprocessing after data was manually corrected or imported out of order.
- `manager.log` now rotates at 10MB (5 backups, `manager.log.1`..`.5`), and
  `aiosqlite`'s per-operation DEBUG lines are no longer logged. An existing
  oversized `manager.log` is rotated away on the first write past 10MB
  after upgrading - archive or delete the resulting `manager.log.1`.
- Daily-reset energy counters (`e_day_exp`, `e_day_imp`, `e_load_day`,
  `e_bat_charge_day`, `e_bat_discharge_day`) are now MQTT/SSE-only, not
  persisted to `data.db` (see `sensors.py`'s `DB_SENSORS`). If you're
  re-running `_migrate_csv_to_sqlite.py` against old CSVs, their values for
  these columns are silently dropped on import - harmless, since nothing
  reads them from `data.db`.
