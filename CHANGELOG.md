# Changelog

Notable changes to this project, especially ones that require action when
upgrading an existing checkout. No formal release process yet - entries are
grouped under `Unreleased` until that changes.

## Unreleased

### Changed

- Optional Pylontech BMS poller: with `BMS_LOGGER_HOST` / `BMS_LOGGER_SERIAL`
  set in `.env`, cell temperatures, SOH, and every cell's voltage are read
  from the BMS through its SolarMan logger every 60 s, stored in `bms.db`,
  and published on MQTT `goodwe/bms`, along with per-module voltages and
  temperatures, every cell's temperature, current, cycle count,
  remaining capacity, BMS charge/discharge limits, and daily/lifetime energy
  counters. Run `pip install -r requirements.txt` after pulling (new
  dependency `pysolarmanv5`). Without those settings nothing changes. A
  `bms.db` from a pre-merge deployment of this branch is migrated on start
  (rows re-decoded; `cell_temp_*` now 0x1114/5, the 0.1 °C readings moved
  to `module_temp_*`).
- Battery control executor (off by default): MQTT `control/set`/`control/reserve/set`/`control/state`,
  dashboard override, `CONTROL_*` env keys (`CONTROL_MIN_SOC` new: the executor owns the on-grid
  minimum SoC). No behaviour change unless `CONTROL_MODE` is set.
  Today's write count is kept in `control_writes.json` (git-ignored) so the
  `CONTROL_MAX_WRITES_PER_DAY` warning keeps counting across restarts.
- The active control command and dashboard override are saved in
  `control_state.json` (git-ignored) and restored after a restart while
  still unexpired, so a deploy no longer drops Predbat's command to `auto`
  until its next call. Expired, stopped, or implausibly far-ahead entries are
  ignored; a command that arrives before the restore wins.
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
