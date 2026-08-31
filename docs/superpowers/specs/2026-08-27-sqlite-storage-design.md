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

## Database files

- **`data.db`** — `hourly_summary` (derived) + `csv_migration_log`. Small
  and never partitioned (see "Long-term storage" below) — decades of hourly
  summaries fit comfortably in one file.
- **`data_<year>.db`** (`data_2024.db`, `data_2025.db`, ...) — one file per
  calendar year, each holding that year's `inverter_history` raw samples.
  See "Long-term storage: year-partitioned files, no pruning" below for why
  and how.
- **`rce_prices.db`** — `rce_prices` + `rce_prices_fetched`. Split out because
  it has a completely different write pattern (occasional, from a background
  thread or Flask request) and lifecycle (tiny, unbounded growth is fine —
  see below) from the 1-second telemetry writer. Keeping it in a separate
  file avoids lock contention between the two, and lets either be rebuilt
  independently.

All `data*.db` files are gitignored, same as `data*.csv` and
`playground/test.db` already are.

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
PRAGMA auto_vacuum = INCREMENTAL;  -- must be set before any table is created; reclaims space after the occasional
                                    -- delete (failed-migration cleanup, verification rollback) - not pruning-related,
                                    -- since raw data is kept forever (see "Long-term storage" below)
```

This schema is created identically in every `data_<year>.db` file (see
"Long-term storage" below) — one `inverter_history` table per year, not one
shared table across files.

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

Never partitioned or deleted — raw samples are kept forever too (see
"Long-term storage" below), but `hourly_summary` staying in one small,
un-split file is what keeps year-spanning historical analysis (e.g.
year-over-year income comparisons) a single-table query instead of a
`UNION ALL` across every year's `data_<year>.db`.

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

**`pv_kwh` is a special case.** Unlike the other counters, `e_day` is *not*
a lifetime cumulative total — it resets to `0` right after local midnight
(confirmed against real device data: `e_day` was `47.9` at `23:59:59` and
`0.0` moments after `00:00:00` the next day, while `e_total_exp` and the
other lifetime counters kept climbing straight through). Diffing
`MAX(e_day)` across the midnight-crossing hour the same way as the other
metrics would produce a large *negative* `pv_kwh` for that hour (this
hour's small since-midnight value minus the previous day's full total).
Fix: when `hour_start` and `hour_start - 1h` fall on different calendar
dates, `pv_kwh` for that hour is `MAX(e_day)` for the current hour alone —
`e_day` already only counts production since that midnight, so no
subtraction is needed. Every other hour, and every other metric, uses the
normal diff.

This "different calendar dates" check, like every other local-time
conversion in this design (`timestamp_epoch` itself, and hour bucketing),
depends on the host machine's system timezone actually being set to the
inverter's local timezone (Europe/Warsaw) — the code is internally
consistent regardless of which timezone the host uses, but only *correct*
relative to the inverter's own midnight if the host's timezone matches it.
This is a deployment requirement, not something verifiable from the code
alone.

## Long-term storage: year-partitioned files, no pruning

All historical `inverter_history` data is kept forever — there is no
deletion/retention policy. `inverter_history` at 1 sample/second,
~109 columns, is ~24 GB/year uncompressed, so an unpartitioned single file
would grow without bound over the years. Instead, `inverter_history` is
split into one SQLite file per calendar year: `data_2024.db`,
`data_2025.db`, `data_2026.db`, and so on, each containing that year's
slice of the table, with the identical schema in every file.

SQLite has no built-in automatic partitioning (no `PARTITION BY` equivalent)
— this is built on `ATTACH DATABASE`, the actual primitive SQLite offers for
spreading data across multiple files, plus application-level routing:

- **Write path**: the live writer (`AsyncioThread`'s single long-lived
  connection) attaches whichever calendar year is current —
  `ATTACH DATABASE 'data_<year>.db' AS y<year>` — creating the file (via the
  same DDL as above) the first time that year is written to, and inserts go
  to `y<year>.inverter_history`. At each year boundary the writer attaches
  the new year's file and switches the insert target; there's no need to
  ever `DETACH` an old year (SQLite's attach limit is 10 by default,
  configurable up to 125 — not a practical concern on a multi-decade
  timeline for one file per year).
- **Read path**: every caller that queries raw history
  (`history.py`, `_calculate_income.py`, `storage.backfill_hourly_summary`)
  attaches every `data_<year>.db` file that exists and queries against
  `CREATE VIEW inverter_history AS SELECT * FROM y2024.inverter_history
  UNION ALL SELECT * FROM y2025.inverter_history UNION ALL ...` (rebuilt
  whenever a new year file appears), so callers keep querying
  `inverter_history` as one logical table without knowing about the split.
  A query bounded to a date range (the common case for `/history`, income
  calculation, and hourly backfill) only touches the relevant underlying
  year file or two, thanks to normal SQLite query planning against the
  `UNION ALL` view.
- **Migration**: `_migrate_csv_to_sqlite.py` determines each row's target
  year from its own timestamp and attaches/creates the matching year file
  before inserting, rather than a single `--db-path` for all of history.

`hourly_summary` and `rce_prices`/`rce_prices_fetched` are never
partitioned — both stay comfortably small (well under a MB and a few MB per
year respectively, even across decades) since they're derived/cached data,
not raw 1-second telemetry.

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

1. Maintains a manifest table in `data.db`:
   ```sql
   CREATE TABLE csv_migration_log (
       filename TEXT PRIMARY KEY,
       row_count INTEGER,
       migrated_at INTEGER,
       status TEXT  -- 'done' or 'error'
   );
   ```
   Any file already marked `'done'` is skipped on a re-run. This makes the
   script safe to run again after fixing a bug, without duplicating rows or
   needing to re-import everything from scratch.
2. For each remaining `data-*.csv`, sorted by filename: empty/malformed
   files (a few 0-byte files already exist in the repo) are skipped with a
   warning, not treated as errors. Otherwise rows are parsed and
   batch-inserted into `inverter_history` via `executemany`.
3. **Verification, per file**: after inserting, the row count actually
   present in `inverter_history` for that file's timestamp range is compared
   against the row count read from the CSV, and a handful of columns on the
   first and last row are spot-checked against the DB. A mismatch marks the
   file `'error'` (not `'done'`) in the manifest and logs loudly, but does
   not abort the run — other files still get processed, and `'error'` files
   can be investigated and re-run individually afterward.
4. Supports `--dry-run` (same convention as `main.py`): parses and validates
   every file and reports what would be inserted and any errors found,
   without writing anything to the DB.
5. Once every file is `'done'`, runs `backfill_hourly_summary()` to derive
   the full `hourly_summary` history from the imported raw data.
6. **Never deletes the source CSV files.** They remain on disk indefinitely
   as the recovery path for a migration bug discovered later — since
   `inverter_history` itself now keeps everything forever too (no retention
   window), there's no natural point at which the CSVs stop being useful as
   a backup. Deleting old CSVs, if ever, is a manual decision — no script
   automates it.

## `_calculate_income.py` rewrite

Replaces the current CSV directory scan + bisect + `find_hours` logic with:
```sql
SELECT * FROM hourly_summary WHERE hour_start BETWEEN ? AND ?
```
joined against `get_rce_15min` (hourly average, same as today's
`query_pse_rce`, just cache-backed). Meaningfully simpler than the current
file-boundary-crossing logic, and no longer touches raw samples at all.

**Landing in two stages**, since the storage-core phase (raw samples +
`hourly_summary`) ships before the RCE cache phase:
1. **Storage-core phase**: `_calculate_income.py` is rewritten against
   `hourly_summary` as above, but still calls the existing live
   `query_pse_rce(date)` directly — `get_rce_15min` doesn't exist yet. This
   alone fixes the script's CSV dependency (it was silently misreporting
   or returning near-empty results for any date past the CSV corpus once
   `main.py` stopped writing new CSVs).
2. **RCE cache phase** (follow-up, once `get_rce_15min` exists): swap the
   `query_pse_rce(date)` call for the cache-backed `get_rce_15min(date)` -
   a one-line change at that point, not a redesign. Tracked here so it
   isn't forgotten when the RCE cache phase is implemented.

## Testing

- `storage.py`: unit tests for insert/query round-trip against a temp DB
  file (or `:memory:`), including the hour-start-recovery query across a
  simulated outage that crosses an hour boundary.
- `backfill_hourly_summary()`: test against a small synthetic raw dataset
  with known expected per-hour diffs.
- Migration script: unit tests with small synthetic CSV fixtures covering —
  a normal file (imported, verified, marked `'done'`); an empty/0-byte file
  (skipped, logged, not treated as an error); a malformed file (missing
  required columns, skipped and logged); a file already marked `'done'` in
  the manifest (skipped on re-run, no duplicate rows inserted); and a
  verification-mismatch case (marked `'error'`, logged, run continues with
  the next file). `--dry-run` tested to confirm zero rows are ever written.
- RCE cache: test the DST edge case explicitly (a date with 92 and a date
  with 100 periods should both cache correctly using the marker-table
  approach).
- Year partitioning: test that a write for a given timestamp lands in the
  correct `data_<year>.db` file (attaching/creating it as needed), and that
  the `UNION ALL` read view correctly returns rows spanning a query range
  that crosses a year boundary.

## Interactive history viewer

A read-only page for browsing recent `inverter_history` and `hourly_summary`
rows — a debug/monitoring view, not a dashboard (no charts here; charts are
covered separately by the RCE and forecast pages).

**One page, two tabbed sections** (`GET /history`, Bootstrap nav-tabs — the
same `bootstrap.bundle.min.js` already loaded on other pages provides the tab
behavior): "Raw samples" (`inverter_history`) and "Hourly summary"
(`hourly_summary`). Each tab has its own filter controls and table, following
the existing `prices.html`/`forecast.html` pattern of a small JS snippet
driving a `fetch()` call rather than a full-page reload per interaction.

**Data API** (JSON, mirrors the existing `/prices/rce.json` pattern):
- `GET /history/inverter.json?start=&end=&columns=&limit=&offset=`
- `GET /history/hourly.json?start=&end=&limit=&offset=`

Both return `{rows: [...], has_more: bool}`. Rather than `SELECT COUNT(*)`
for pagination (expensive on `inverter_history`, which grows unbounded
forever now that there's no retention window — tens of millions of rows
per year, across every `data_<year>.db`), the query fetches
`limit + 1` rows and `has_more` is simply "did we get more than `limit`" —
cheap regardless of table size, at the cost of not showing a total page
count (just Prev/Next).

**Column selector** (raw-samples tab only — `hourly_summary` only has 7
columns, all shown by default, no selector needed there): the server exposes
a fixed allow-list of "presentable" columns, not all ~109 raw sensor fields
(many are internal/diagnostic codes not meaningful to browse). Default
selection: `timestamp, ppv, ppv1, ppv2, pgrid, load_ptotal, battery_soc,
pbattery1, e_day, work_mode_label`. A checkbox dropdown lets the user add any
other allow-listed column.

**Column selection persistence** — two layers, resolved in this priority
order on page load:
1. An explicit `?columns=` in the URL, if present — keeps shared/bookmarked
   links reproducible regardless of the viewer's own saved preference.
2. Otherwise, a `localStorage` value (key `history.columns`) — every change
   to the checkboxes is saved here client-side, so a returning visitor with
   no query string sees their last-used columns instead of the default.
3. Otherwise, the default curated subset above.

This is deliberately client-side/per-browser only (no server-side user
preference storage) since it's a personal viewing convenience, not shared
application state.

**Date range filter**: two `<input type="date">` controls, same widget
already used for the single-date selectors on `forecast.html`/`prices.html`,
defaulting to "last 7 days" on first load. Also reflected in the URL query
string, same as `columns`, for shareable/bookmarkable/refresh-safe filtered
views.

**Pagination**: Prev/Next buttons, `limit` defaulting to 100, adjustable via
a small `<select>` (e.g. 50/100/250/500).

**Mobile usability**: each table wrapped in Bootstrap's `.table-responsive`
(available for free via the already-loaded `bootstrap.min.css` — horizontal
scroll on narrow viewports, no extra JS needed); filter controls stack into
full-width rows on small screens using the existing `.col-*` grid classes,
consistent with how `forecast.html`/`prices.html` already lay out their date
selector.

**Empty-state handling**: if a query returns zero rows (e.g. right after
startup, before the first hour has completed, or a date range with no data),
render a "no data for this range" message in place of the table rather than
an empty `<table>`.

### Testing

- Query-building logic (date range + column allow-list + `limit+1`
  pagination) unit-tested directly against a temp SQLite DB, independent of
  Flask.
- Manual check: column selection persists across a page reload with no
  query string, and an explicit `?columns=` in a shared link overrides the
  saved preference.

## Out of scope for this design

- Normalizing/typing every individual sensor column precisely (some are
  diagnostic/rarely used) — the migration script and `main.py` insert code
  can carry over the same `str(...)` coercion used today and let SQLite's
  column affinity do the rest.
- Making the prefetch schedule configurable via `.env` — worth doing at
  implementation time, not a design-level decision.
- Detaching old year files from the long-lived write connection, or any
  sharding finer than one file per year — not needed until the attach
  count (max 125) becomes a real constraint, which is decades away at one
  file per year.
