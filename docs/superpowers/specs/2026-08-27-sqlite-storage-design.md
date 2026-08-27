# SQLite storage design

Status: design approved, not yet implemented.

## Motivation

Runtime telemetry is currently written to a new `data-YYYY-MM-DD_HH-MM-SS.csv`
file every process start. This makes historical analysis (`_calculate_income.py`)
painful — it has to discover, sort, and scan across many files, bisecting for
date/hour boundaries. There's also no persisted "current hour" baseline, so a
service restart mid-hour produces a wrong first `_hourly_*` reading. Separately,
RCE prices are re-fetched from PSE on every page view / script run with no
local cache.

This design replaces CSV files with SQLite, adds a materialized hourly summary
for fast historical queries, and adds a local RCE price cache with a scheduled
next-day prefetch.

## Two database files

- **`data.db`** — `inverter_history` (raw samples) + `hourly_summary`
  (derived). Kept together because they're written by the same code path and
  the backfill logic needs both.
- **`rce_prices.db`** — `rce_prices` + `rce_prices_fetched`. Split out because
  it has a completely different write pattern (occasional, from a background
  thread or Flask request) and lifecycle (tiny, never pruned) from the
  1-second telemetry writer. Keeping it in a separate file avoids lock
  contention between the two, and lets either be rebuilt independently.

Both files are gitignored, same as `data*.csv` and `playground/test.db`
already are.

## `inverter_history` (normalized columns, not JSON blob)

A JSON-blob-per-row schema was considered first (matches the existing
`playground/sql_tests.py` prototype) but measured out to ~4.8x the size of
the current CSVs, purely from repeating ~109 field names as text in every
row (~101 GB/year vs ~21 GB/year for CSV). Normalized columns come out close
to CSV size (~24 GB/year) since SQLite's per-row overhead roughly offsets not
repeating a header, and the schema stays queryable/typed. Given storage size
is an explicit concern here, normalized columns is the chosen approach.

```sql
CREATE TABLE inverter_history (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,  -- unix epoch, derived from the sensor 'timestamp' field at insert time
    -- one column per entry in SELECTED_SENSORS + CalculatedValuesEvaluator.headers(),
    -- typed REAL for numeric sensor values, TEXT for labels/mode strings
    ppv REAL, ppv1 REAL, ppv2 REAL, ...,
    pv1_mode_label TEXT, pv2_mode_label TEXT, ...,
    _hourly_meter_export REAL, _hourly_meter_import REAL, _hourly_load REAL
);
CREATE INDEX idx_inverter_history_timestamp ON inverter_history (timestamp);
PRAGMA journal_mode = WAL;         -- readers (Flask thread) don't block the writer (asyncio thread)
PRAGMA auto_vacuum = INCREMENTAL;  -- must be set before any table is created; enables reclaiming space after pruning
```

`main.py`'s `_get_inverter_data` opens one `aiosqlite` connection for the
process lifetime (replacing the current per-run `open()`/`csv.writer`), and
each poll becomes a single parameterized `INSERT` — no more
`asyncio.to_thread` wrapper, since `aiosqlite` is natively async.
`AsyncioThread.finish()` closes the connection.

Adding a new sensor to `SELECTED_SENSORS` in the future means an
`ALTER TABLE ... ADD COLUMN`, not a full rewrite — SQLite handles this
cheaply.

## Startup hour-start recovery

On startup, before starting the polling loop, seed
`CalculatedValuesEvaluator._hour_start_sensors` from the database instead of
leaving it `None` unconditionally:

```sql
SELECT * FROM inverter_history
WHERE timestamp >= :current_hour_start AND timestamp < :current_hour_end
ORDER BY timestamp ASC LIMIT 1
```

- If a row exists in the current hour bucket → restore the baseline from it
  (handles a short outage within the same hour correctly).
- If no row exists (empty DB, or the last sample predates the current hour —
  i.e. the outage crossed an hour boundary) → leave `_hour_start_sensors =
  None`. The existing logic in `calculate_values` already treats `None` as
  "no baseline yet" and sets it from the first incoming sample, which is the
  correct cold-start behavior for a new hour. No new state machine needed —
  just query the *first* row of the *current* hour, not the *latest* row
  overall.

## `hourly_summary`

```sql
CREATE TABLE hourly_summary (
    hour_start INTEGER PRIMARY KEY,  -- unix epoch, start of the hour
    meter_export_kwh REAL,
    meter_import_kwh REAL,
    load_kwh REAL,
    pv_kwh REAL,                -- from e_day diff
    battery_charge_kwh REAL,    -- from e_bat_charge_total diff
    battery_discharge_kwh REAL  -- from e_bat_discharge_total diff
);
```

Never pruned — this is the table that makes long-term historical analysis
possible even after raw samples age out (see Retention below).

Populated by a `backfill_hourly_summary()` function, not by live
finalization during polling (a crash right at the hour boundary would mean
it's never written). For every hour that (a) has raw samples and (b) has at
least one raw sample from the *next* hour (proving it's fully complete) and
(c) has no `hourly_summary` row yet, it computes each metric as
`MAX(cumulative_counter WHERE hour = h) - MAX(cumulative_counter WHERE hour =
h - 1)` — the same diff `_calculate_income.py`'s `calc_diff` computes today
between hour-boundary rows, just as a SQL aggregation. `INSERT OR REPLACE`
makes this idempotent, so it's safe to run:
- once in the migration script, to backfill full history from the CSVs, and
- once at every app startup, to catch up any hours completed while the
  service was down.

## Retention / pruning (raw samples only)

`inverter_history` at 1 sample/second, ~109 columns, is ~24 GB/year
uncompressed. A daily maintenance step deletes rows older than a configurable
window, default **180 days** (~12 GB steady-state), then runs
`PRAGMA incremental_vacuum` to actually reclaim the freed pages (requires
`auto_vacuum = INCREMENTAL`, set at table creation time). The
`hourly_summary` backfill must run before pruning deletes the raw rows it
would derive from — the daily maintenance step is: backfill, then prune.

`hourly_summary` and `rce_prices`/`rce_prices_fetched` are never pruned —
both are small enough (well under a MB and a few MB per year respectively)
that there's no growth concern.

## RCE price cache (`rce_prices.db`)

```sql
CREATE TABLE rce_prices (
    business_date TEXT NOT NULL,
    period TEXT NOT NULL,       -- '00:00' etc, matches convert_to_series_15min
    rce_pln REAL NOT NULL,
    PRIMARY KEY (business_date, period)
);
CREATE TABLE rce_prices_fetched (
    business_date TEXT PRIMARY KEY,
    period_count INTEGER NOT NULL,
    fetched_at INTEGER NOT NULL
);
```

`rce_prices_fetched` exists specifically to avoid a "row count == 96" cache
completeness check, which breaks on Poland's DST transition days (92 periods
in spring, 100 in fall). A cache hit is "a marker row exists for this
`business_date`," regardless of how many periods PSE actually returned for
it — trusting whatever was fetched once rather than asserting an assumed
count.

**Read path**: a new `get_rce_15min(date)` in `rce.py` becomes the single
entry point, replacing direct calls to `query_pse_rce_15min` everywhere
(`/prices`, `/prices/rce.json`, `/prices/rce.png`, `/forecast`,
`_calculate_income.py`):
1. If `rce_prices_fetched` has a row for `business_date`, read `rce_prices`
   for that date and return — no network call.
2. Otherwise, call the existing live `query_pse_rce_15min(date)`,
   `INSERT OR REPLACE` the results into `rce_prices`, insert the
   `rce_prices_fetched` marker, and return. This applies to *any* live
   fetch, whichever caller triggered it — there is exactly one path into the
   cache, so no route can bypass it.

Because every live fetch writes through, historical dates get cached lazily
on first request and stay cached forever (published prices never change).

**Automatic next-day prefetch**: a dedicated background thread, following
the same pattern as the existing `AsyncioThread`, no new scheduler
dependency:
- Wakes at 14:15 local time, calls `get_rce_15min(tomorrow)`.
- "Not published yet" (PSE returns an empty `value` list) is logged at
  `info`/`debug` and retried every 15 minutes — this is an expected daily
  occurrence, not an error.
- Unexpected failures (network errors, malformed responses) are logged at
  `warning`/`error` but use the same retry cadence rather than crashing the
  thread.
- Gives up for the day at a cutoff (20:00) with an `error` log, requiring
  human attention. This is safe to fail: the read path always falls back to
  a live fetch on a cache miss, so `/prices` for tomorrow keeps working
  (just with one live PSE call) even if the whole prefetch job is broken.

## Migration from CSV

A one-off `_migrate_csv_to_sqlite.py` (matching the existing `_`-prefix
convention for utility scripts):
1. Reads all `data-*.csv` sorted by filename, skips empty/malformed files
   (a few 0-byte files already exist in the repo).
2. Batch-inserts rows into `inverter_history` via `executemany`.
3. Runs `backfill_hourly_summary()` to derive the full `hourly_summary`
   history from the imported raw data.
4. Leaves the original CSV files on disk as a backup — no automatic
   deletion.

## `_calculate_income.py` rewrite

Replaces the current CSV directory scan + bisect + `find_hours` logic with:
```sql
SELECT * FROM hourly_summary WHERE hour_start BETWEEN ? AND ?
```
joined against `get_rce_15min` (hourly average, same as today's
`query_pse_rce`, just cache-backed). Meaningfully simpler than the current
file-boundary-crossing logic, and no longer touches raw samples at all.

## Testing

- `storage.py`: unit tests for insert/query round-trip against a temp DB
  file (or `:memory:`), including the hour-start-recovery query across a
  simulated outage that crosses an hour boundary.
- `backfill_hourly_summary()`: test against a small synthetic raw dataset
  with known expected per-hour diffs.
- Migration script: test against a small sample CSV, checking row count and
  value correctness after import.
- RCE cache: test the DST edge case explicitly (a date with 92 and a date
  with 100 periods should both cache correctly using the marker-table
  approach).
- Retention/pruning: test that pruning deletes only rows older than the
  window, never touches `hourly_summary`, and that `backfill_hourly_summary`
  is proven to have run for a given hour before that hour's raw rows are
  eligible for deletion.

## Out of scope for this design

- Normalizing/typing every individual sensor column precisely (some are
  diagnostic/rarely used) — the migration script and `main.py` insert code
  can carry over the same `str(...)` coercion used today and let SQLite's
  column affinity do the rest.
- Making the retention window and prefetch schedule configurable via `.env`
  — worth doing at implementation time, not a design-level decision.
