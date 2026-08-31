# Interactive History Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only `/history` page for browsing recent `inverter_history` and `hourly_summary` rows, with date-range filtering, a persisted column selector, and Prev/Next pagination.

**Architecture:** A new pure-Python module (`history.py`) owns all query-building logic (column allow-listing, date-range parsing, limit/offset pagination) and is unit-tested directly against a temp SQLite DB, independent of Flask. Three new thin Flask routes in `main.py` wire that module to JSON endpoints and a template. `templates/history.html` is a single self-contained page (no template inheritance, matching every other page in this app) with two Bootstrap tabs, each running its own small vanilla-JS `fetch()`-driven controller — no page reloads per filter change.

**Tech Stack:** Flask, sqlite3 (stdlib), Bootstrap 5 (already vendored via `static/bootstrap.min.css` + the CDN `bootstrap.bundle.min.js` used on every other page), vanilla JS (no framework), browser `localStorage`.

**Spec:** `docs/superpowers/specs/2026-08-27-sqlite-storage-design.md`, section "Interactive history viewer" (also read "`inverter_history` (normalized columns, not JSON blob)" and "`hourly_summary`" for the exact schemas this plan queries).

## Global Constraints

- The host machine's system timezone must be Europe/Warsaw — `datetime.fromtimestamp()` (no explicit tzinfo) is used throughout this codebase (see `storage.backfill_hourly_summary`) to convert epoch seconds back to the inverter's own local time, and this plan follows the same convention for displaying `hour_start`.
- No `SELECT COUNT(*)` for pagination — `inverter_history` can hold tens of millions of rows within the 180-day retention window (see spec's Retention section), so pagination must use the "fetch `limit + 1`, `has_more` = got more than `limit`" trick, never a row count.
- Column selection persistence is client-side only (`localStorage`), never server-side/per-user storage.
- Every page in this app is a single self-contained HTML file (no `{% extends %}` / Jinja macros) — `history.html` must follow the same shape as `prices.html`/`forecast.html`, not introduce template inheritance.
- `main.py` cannot be imported by a test process without `INVERTER_IP` set in the environment (it asserts this at import time) — this is why the existing test suite never imports `main`, and why this plan keeps `history.py` fully Flask-free and independently testable, verifying the new routes by manual `curl` instead of an automated Flask-test-client test.

---

### Task 1: `history.py` query-building module

**Files:**
- Create: `history.py`
- Test: `tests/test_history.py`

**Interfaces:**
- Consumes: `storage.init_db_sync(path, columns) -> sqlite3.Connection` and `storage.parse_timestamp_epoch(str) -> int` (both already exist in `storage.py`); `sensors.sensor_columns() -> list[tuple[str, str]]` (already exists, used only by the test to build a realistic `inverter_history` table).
- Produces (used by Task 2):
  - `RAW_COLUMNS: tuple[str, ...]` — the full allow-list of presentable `inverter_history` columns, in display order.
  - `DEFAULT_RAW_COLUMNS: tuple[str, ...]` — the curated default subset.
  - `HOURLY_COLUMNS: tuple[str, ...]` — the fixed 7 `hourly_summary` columns, in display order.
  - `resolve_raw_columns(requested: Optional[Iterable[str]]) -> list[str]`
  - `resolve_limit(value: Optional[str]) -> int`
  - `resolve_offset(value: Optional[str]) -> int`
  - `parse_date_or_default(value: Optional[str], default: date) -> date`
  - `default_date_range(today: date) -> tuple[date, date]`
  - `date_range_to_epoch(start_date: date, end_date: date) -> tuple[int, int]`
  - `fetch_inverter_rows(conn: sqlite3.Connection, columns: list[str], start_epoch: int, end_epoch: int, limit: int, offset: int) -> tuple[list[dict], bool]`
  - `fetch_hourly_rows(conn: sqlite3.Connection, start_epoch: int, end_epoch: int, limit: int, offset: int) -> tuple[list[dict], bool]`

- [ ] **Step 1: Write the failing tests for column/limit/offset resolution**

Create `tests/test_history.py` with this content:

```python
import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime

import storage
import history
from sensors import sensor_columns


class ResolveRawColumnsTest(unittest.TestCase):
    def test_none_returns_the_default_subset(self):
        self.assertEqual(history.resolve_raw_columns(None), list(history.DEFAULT_RAW_COLUMNS))

    def test_unknown_columns_are_dropped_silently(self):
        result = history.resolve_raw_columns(['ppv', 'not_a_real_column'])
        self.assertEqual(result, ['timestamp', 'ppv'])

    def test_all_unknown_falls_back_to_default(self):
        result = history.resolve_raw_columns(['not_a_real_column'])
        self.assertEqual(result, list(history.DEFAULT_RAW_COLUMNS))

    def test_empty_list_falls_back_to_default(self):
        result = history.resolve_raw_columns([])
        self.assertEqual(result, list(history.DEFAULT_RAW_COLUMNS))

    def test_timestamp_is_always_first_even_if_not_requested(self):
        result = history.resolve_raw_columns(['battery_soc', 'ppv'])
        self.assertEqual(result[0], 'timestamp')

    def test_result_follows_raw_columns_canonical_order_not_request_order(self):
        # ppv appears before battery_soc in RAW_COLUMNS
        result = history.resolve_raw_columns(['battery_soc', 'ppv'])
        self.assertEqual(result, ['timestamp', 'ppv', 'battery_soc'])

    def test_duplicate_requested_columns_are_deduplicated(self):
        result = history.resolve_raw_columns(['ppv', 'ppv', 'ppv'])
        self.assertEqual(result, ['timestamp', 'ppv'])


class ResolveLimitTest(unittest.TestCase):
    def test_valid_limit_is_kept(self):
        self.assertEqual(history.resolve_limit('250'), 250)

    def test_none_defaults_to_100(self):
        self.assertEqual(history.resolve_limit(None), 100)

    def test_not_in_allowed_set_defaults_to_100(self):
        self.assertEqual(history.resolve_limit('999'), 100)

    def test_non_numeric_defaults_to_100(self):
        self.assertEqual(history.resolve_limit('abc'), 100)


class ResolveOffsetTest(unittest.TestCase):
    def test_valid_offset_is_kept(self):
        self.assertEqual(history.resolve_offset('300'), 300)

    def test_none_defaults_to_zero(self):
        self.assertEqual(history.resolve_offset(None), 0)

    def test_negative_defaults_to_zero(self):
        self.assertEqual(history.resolve_offset('-5'), 0)

    def test_non_numeric_defaults_to_zero(self):
        self.assertEqual(history.resolve_offset('abc'), 0)


class DateRangeTest(unittest.TestCase):
    def test_parse_date_or_default_parses_iso_date(self):
        result = history.parse_date_or_default('2026-08-20', default=date(2000, 1, 1))
        self.assertEqual(result, date(2026, 8, 20))

    def test_parse_date_or_default_falls_back_on_none(self):
        result = history.parse_date_or_default(None, default=date(2000, 1, 1))
        self.assertEqual(result, date(2000, 1, 1))

    def test_parse_date_or_default_falls_back_on_garbage(self):
        result = history.parse_date_or_default('not-a-date', default=date(2000, 1, 1))
        self.assertEqual(result, date(2000, 1, 1))

    def test_default_date_range_is_last_7_days_inclusive(self):
        start, end = history.default_date_range(today=date(2026, 8, 29))
        self.assertEqual(start, date(2026, 8, 23))
        self.assertEqual(end, date(2026, 8, 29))

    def test_date_range_to_epoch_covers_full_days_local_time(self):
        start_epoch, end_epoch = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 28))
        self.assertEqual(datetime.fromtimestamp(start_epoch), datetime(2026, 8, 27, 0, 0, 0))
        # end is exclusive: start of the day AFTER end_date
        self.assertEqual(datetime.fromtimestamp(end_epoch), datetime(2026, 8, 29, 0, 0, 0))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_history.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'history'` (or `AttributeError` once the empty file exists) — `history.py` doesn't exist yet.

- [ ] **Step 3: Implement column/limit/offset/date resolution**

Create `history.py` with this content (rows-fetching functions are added in Step 5 — this step only covers the pieces the tests above exercise):

```python
"""Query-building logic for the read-only /history viewer. Kept Flask-free
so it's independently unit-testable — see the Global Constraints note in
docs/superpowers/plans/2026-08-29-history-viewer.md about why main.py can't
be imported by the test suite.
"""
import sqlite3
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

# Allow-list of "presentable" inverter_history columns, in display order.
# Deliberately a curated subset of the ~109 raw sensor fields — the rest
# are internal/diagnostic codes not meaningful to browse (see spec's
# "Interactive history viewer" section).
RAW_COLUMNS = (
    'timestamp', 'ppv', 'ppv1', 'ppv2', 'vpv1', 'vpv2', 'ipv1', 'ipv2',
    'pgrid', 'vgrid', 'igrid', 'fgrid', 'load_ptotal', 'house_consumption',
    'battery_soc', 'pbattery1', 'vbattery1', 'ibattery1', 'battery_temperature',
    'e_day', 'e_total_exp', 'e_total_imp', 'e_load_total',
    'meter_e_total_exp', 'meter_e_total_imp',
    'e_bat_charge_total', 'e_bat_discharge_total',
    'work_mode_label', 'operation_mode', 'grid_in_out_label',
    'temperature', 'temperature_air', 'rssi',
)

DEFAULT_RAW_COLUMNS = (
    'timestamp', 'ppv', 'ppv1', 'ppv2', 'pgrid', 'load_ptotal',
    'battery_soc', 'pbattery1', 'e_day', 'work_mode_label',
)

HOURLY_COLUMNS = (
    'hour_start', 'meter_export_kwh', 'meter_import_kwh', 'load_kwh',
    'pv_kwh', 'battery_charge_kwh', 'battery_discharge_kwh',
)

ALLOWED_LIMITS = (50, 100, 250, 500)
DEFAULT_LIMIT = 100


def resolve_raw_columns(requested: Optional[Iterable[str]]) -> list:
    """Filters `requested` down to RAW_COLUMNS, in RAW_COLUMNS' canonical
    order (not request order) so column position never depends on how a
    URL or localStorage value happens to be ordered. Unknown names are
    dropped silently. Falls back to DEFAULT_RAW_COLUMNS if nothing usable
    is left. 'timestamp' is always present, prepended if not requested,
    since a time-series table without it isn't meaningful.
    """
    if requested is None:
        return list(DEFAULT_RAW_COLUMNS)
    requested_set = set(requested)
    filtered = [c for c in RAW_COLUMNS if c in requested_set]
    if not filtered:
        return list(DEFAULT_RAW_COLUMNS)
    if 'timestamp' not in filtered:
        filtered = ['timestamp'] + filtered
    return filtered


def resolve_limit(value: Optional[str]) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    return parsed if parsed in ALLOWED_LIMITS else DEFAULT_LIMIT


def resolve_offset(value: Optional[str]) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0


def parse_date_or_default(value: Optional[str], default: date) -> date:
    if not value:
        return default
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        return default


def default_date_range(today: date) -> tuple:
    """Last 7 days, inclusive of today."""
    return today - timedelta(days=6), today


def date_range_to_epoch(start_date: date, end_date: date) -> tuple:
    """(start_epoch, end_epoch) where end_epoch is EXCLUSIVE - the start of
    the day after end_date - so callers can use `>= start AND < end`
    consistently with storage.py's hour-bucket convention. Both computed in
    naive local time via datetime(...).timestamp(), the same convention
    storage.py's parse_timestamp_epoch()/backfill_hourly_summary() use, so
    this requires the same host-timezone constraint (see Global
    Constraints).
    """
    start_epoch = int(datetime(start_date.year, start_date.month, start_date.day).timestamp())
    end_of_range = end_date + timedelta(days=1)
    end_epoch = int(datetime(end_of_range.year, end_of_range.month, end_of_range.day).timestamp())
    return start_epoch, end_epoch
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_history.py -v`
Expected: PASS (all `ResolveRawColumnsTest`, `ResolveLimitTest`, `ResolveOffsetTest`, `DateRangeTest` cases)

- [ ] **Step 5: Write the failing tests for row-fetching**

Append this to `tests/test_history.py`:

```python
def _sample_row(timestamp: str, **overrides) -> dict:
    row = {name: ('0' if sql_type == 'REAL' else '') for name, sql_type in sensor_columns()}
    row['timestamp'] = timestamp
    row.update(overrides)
    return row


class FetchInverterRowsTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())
        for i in range(5):
            storage.insert_sample_sync(self.conn, _sample_row(
                f'2026-08-27 10:0{i}:00', ppv=str(100 * i), battery_soc=str(50 + i)))

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_returns_only_the_requested_columns_in_order(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, has_more = history.fetch_inverter_rows(
            self.conn, ['timestamp', 'ppv'], start, end, limit=10, offset=0)
        self.assertEqual(list(rows[0].keys()), ['timestamp', 'ppv'])
        self.assertFalse(has_more)

    def test_orders_newest_first(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=10, offset=0)
        timestamps = [r['timestamp'] for r in rows]
        self.assertEqual(timestamps, sorted(timestamps, reverse=True))

    def test_has_more_true_when_more_rows_exist_than_limit(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, has_more = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=2, offset=0)
        self.assertEqual(len(rows), 2)
        self.assertTrue(has_more)

    def test_offset_skips_rows(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        first_page, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=2, offset=0)
        second_page, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=2, offset=2)
        self.assertNotEqual(first_page, second_page)

    def test_date_range_excludes_rows_outside_it(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 28), date(2026, 8, 28))
        rows, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=10, offset=0)
        self.assertEqual(rows, [])

    def test_rejects_a_column_not_in_the_allow_list(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        with self.assertRaises(ValueError):
            history.fetch_inverter_rows(
                self.conn, ['timestamp; DROP TABLE inverter_history'], start, end, limit=10, offset=0)


class FetchHourlyRowsTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def _insert_hourly(self, hour_start_str, **overrides):
        hour_start = storage.parse_timestamp_epoch(hour_start_str)
        row = {'hour_start': hour_start, 'meter_export_kwh': 1.0, 'meter_import_kwh': 0.0,
               'load_kwh': 0.5, 'pv_kwh': 1.5, 'battery_charge_kwh': 0.0, 'battery_discharge_kwh': 0.0}
        row.update(overrides)
        self.conn.execute(
            "INSERT INTO hourly_summary (hour_start, meter_export_kwh, meter_import_kwh, load_kwh, "
            "pv_kwh, battery_charge_kwh, battery_discharge_kwh) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row['hour_start'], row['meter_export_kwh'], row['meter_import_kwh'], row['load_kwh'],
             row['pv_kwh'], row['battery_charge_kwh'], row['battery_discharge_kwh']),
        )
        self.conn.commit()

    def test_formats_hour_start_as_a_local_timestamp_string(self):
        self._insert_hourly('2026-08-27 13:00:00')
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, _ = history.fetch_hourly_rows(self.conn, start, end, limit=10, offset=0)
        self.assertEqual(rows[0]['hour_start'], '2026-08-27 13:00')

    def test_returns_all_seven_columns(self):
        self._insert_hourly('2026-08-27 13:00:00')
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, _ = history.fetch_hourly_rows(self.conn, start, end, limit=10, offset=0)
        self.assertEqual(list(rows[0].keys()), list(history.HOURLY_COLUMNS))

    def test_has_more_true_when_more_rows_exist_than_limit(self):
        self._insert_hourly('2026-08-27 10:00:00')
        self._insert_hourly('2026-08-27 11:00:00')
        self._insert_hourly('2026-08-27 12:00:00')
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, has_more = history.fetch_hourly_rows(self.conn, start, end, limit=2, offset=0)
        self.assertEqual(len(rows), 2)
        self.assertTrue(has_more)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 6: Run the tests to verify they fail**

Run: `python -m pytest tests/test_history.py -v`
Expected: FAIL with `AttributeError: module 'history' has no attribute 'fetch_inverter_rows'`

- [ ] **Step 7: Implement the row-fetching functions**

Append this to `history.py` (keep the `import sqlite3` already at the top):

```python
def fetch_inverter_rows(conn: sqlite3.Connection, columns: list, start_epoch: int, end_epoch: int,
                        limit: int, offset: int) -> tuple:
    """Returns (rows, has_more). `columns` must already be the output of
    resolve_raw_columns() - re-validated here against RAW_COLUMNS as a
    defense-in-depth check before string-interpolating them into SQL,
    since column names can't be parameterized with `?` placeholders.
    """
    for column in columns:
        if column not in RAW_COLUMNS:
            raise ValueError(f"Column not in the allow-list: {column!r}")
    columns_sql = ', '.join(columns)
    cursor = conn.execute(
        f"SELECT {columns_sql} FROM inverter_history "
        "WHERE timestamp_epoch >= ? AND timestamp_epoch < ? "
        "ORDER BY timestamp_epoch DESC LIMIT ? OFFSET ?",
        (start_epoch, end_epoch, limit + 1, offset),
    )
    fetched = cursor.fetchall()
    has_more = len(fetched) > limit
    rows = [dict(zip(columns, row)) for row in fetched[:limit]]
    return rows, has_more


def fetch_hourly_rows(conn: sqlite3.Connection, start_epoch: int, end_epoch: int,
                      limit: int, offset: int) -> tuple:
    """Returns (rows, has_more). hour_start is formatted as a local
    'YYYY-MM-DD HH:00' string for display (see Global Constraints re: host
    timezone), replacing the raw epoch int.
    """
    columns_sql = ', '.join(HOURLY_COLUMNS)
    cursor = conn.execute(
        f"SELECT {columns_sql} FROM hourly_summary "
        "WHERE hour_start >= ? AND hour_start < ? "
        "ORDER BY hour_start DESC LIMIT ? OFFSET ?",
        (start_epoch, end_epoch, limit + 1, offset),
    )
    fetched = cursor.fetchall()
    has_more = len(fetched) > limit
    rows = []
    for row in fetched[:limit]:
        row_dict = dict(zip(HOURLY_COLUMNS, row))
        row_dict['hour_start'] = datetime.fromtimestamp(row_dict['hour_start']).strftime('%Y-%m-%d %H:00')
        rows.append(row_dict)
    return rows, has_more
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `python -m pytest tests/test_history.py -v`
Expected: PASS (all tests in the file)

- [ ] **Step 9: Commit**

```bash
git add history.py tests/test_history.py
git commit -m "feat: add history.py query-building module for the history viewer"
```

---

### Task 2: `/history` JSON API routes in `main.py`

**Files:**
- Modify: `main.py`
- Modify: `templates/index.html` (add a nav button)

**Interfaces:**
- Consumes: everything from Task 1's `history.py` (`RAW_COLUMNS`, `HOURLY_COLUMNS`, `resolve_raw_columns`, `resolve_limit`, `resolve_offset`, `parse_date_or_default`, `default_date_range`, `date_range_to_epoch`, `fetch_inverter_rows`, `fetch_hourly_rows`); `storage.DATA_DB_PATH` (already exists in `storage.py`).
- Produces (used by Task 3): three routes returning these exact JSON shapes:
  - `GET /history/inverter.json?start=&end=&columns=&limit=&offset=` → `{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "columns": [...], "limit": int, "offset": int, "rows": [...], "has_more": bool}`
  - `GET /history/hourly.json?start=&end=&limit=&offset=` → `{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "columns": [...], "limit": int, "offset": int, "rows": [...], "has_more": bool}`
  - `GET /history` → renders `templates/history.html` with `raw_columns` (= `history.RAW_COLUMNS`), `default_raw_columns` (= `history.DEFAULT_RAW_COLUMNS`), `default_start`, `default_end` (both `'YYYY-MM-DD'` strings from `history.default_date_range`) passed to the template.

No automated test for this task — per Global Constraints, `main.py` can't be imported by the test suite without `INVERTER_IP` set, matching how every other existing route in `main.py` (`/prices`, `/forecast`, etc.) has no test either. Verify manually with `curl` in Step 4 below.

- [ ] **Step 1: Add the `history` import**

In `main.py`, find this existing import line:

```python
import forecast
import storage
```

Change it to:

```python
import forecast
import history
import storage
```

- [ ] **Step 2: Add the three routes**

In `main.py`, find the existing `/forecast` route (it ends right before `@app.get('/listen')`):

```python
@app.get('/forecast')
def get_forecast():
    ...
        return flask.render_template('forecast.html', date=date_yyyymmdd, forecast=forecast_data)


@app.get('/listen')
```

Insert the three new routes between them, so the file reads:

```python
@app.get('/forecast')
def get_forecast():
    ...
        return flask.render_template('forecast.html', date=date_yyyymmdd, forecast=forecast_data)


@app.get('/history')
def get_history_page():
    default_start, default_end = history.default_date_range(datetime.now().date())
    return flask.render_template(
        'history.html',
        raw_columns=history.RAW_COLUMNS,
        default_raw_columns=history.DEFAULT_RAW_COLUMNS,
        default_start=default_start.strftime('%Y-%m-%d'),
        default_end=default_end.strftime('%Y-%m-%d'),
    )


def _parse_history_range_params():
    default_start, default_end = history.default_date_range(datetime.now().date())
    start_date = history.parse_date_or_default(request.args.get('start'), default_start)
    end_date = history.parse_date_or_default(request.args.get('end'), default_end)
    start_epoch, end_epoch = history.date_range_to_epoch(start_date, end_date)
    limit = history.resolve_limit(request.args.get('limit'))
    offset = history.resolve_offset(request.args.get('offset'))
    return start_date, end_date, start_epoch, end_epoch, limit, offset


@app.get('/history/inverter.json')
def get_history_inverter_json():
    start_date, end_date, start_epoch, end_epoch, limit, offset = _parse_history_range_params()
    columns_param = request.args.get('columns')
    requested_columns = columns_param.split(',') if columns_param else None
    columns = history.resolve_raw_columns(requested_columns)
    conn = sqlite3.connect(storage.DATA_DB_PATH)
    try:
        rows, has_more = history.fetch_inverter_rows(conn, columns, start_epoch, end_epoch, limit, offset)
    finally:
        conn.close()
    return flask.jsonify({
        'start': start_date.strftime('%Y-%m-%d'),
        'end': end_date.strftime('%Y-%m-%d'),
        'columns': columns,
        'limit': limit,
        'offset': offset,
        'rows': rows,
        'has_more': has_more,
    })


@app.get('/history/hourly.json')
def get_history_hourly_json():
    start_date, end_date, start_epoch, end_epoch, limit, offset = _parse_history_range_params()
    conn = sqlite3.connect(storage.DATA_DB_PATH)
    try:
        rows, has_more = history.fetch_hourly_rows(conn, start_epoch, end_epoch, limit, offset)
    finally:
        conn.close()
    return flask.jsonify({
        'start': start_date.strftime('%Y-%m-%d'),
        'end': end_date.strftime('%Y-%m-%d'),
        'columns': list(history.HOURLY_COLUMNS),
        'limit': limit,
        'offset': offset,
        'rows': rows,
        'has_more': has_more,
    })


@app.get('/listen')
```

- [ ] **Step 3: Add the `sqlite3` import**

`main.py` doesn't import the stdlib `sqlite3` module yet (it only uses `aiosqlite` for the async write path), and the routes added in Step 2 call `sqlite3.connect(...)` directly. Find the stdlib-imports block at the very top of `main.py`:

```python
import asyncio
import concurrent.futures
import io
import json
import logging
import os
import re
import sys
import threading
import time
```

Change it to:

```python
import asyncio
import concurrent.futures
import io
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
```

- [ ] **Step 4: Add the nav button and manually verify**

In `templates/index.html`, find:

```html
        <a href="/prices" class="btn btn-primary">📈 RCE</a>
        <a href="/forecast" class="btn btn-primary">⛅ Forecast</a>
```

Change it to:

```html
        <a href="/prices" class="btn btn-primary">📈 RCE</a>
        <a href="/forecast" class="btn btn-primary">⛅ Forecast</a>
        <a href="/history" class="btn btn-primary">🕘 History</a>
```

`templates/history.html` doesn't exist yet (that's Task 3), so `/history` itself will 500 until then — verify only the two JSON endpoints in this task. With the dev server running (`python main.py --dry-run`, from the venv, with a `.env` present):

```bash
curl -s 'http://127.0.0.1:5000/history/inverter.json?limit=2' | python -m json.tool
curl -s 'http://127.0.0.1:5000/history/hourly.json?limit=2' | python -m json.tool
curl -s 'http://127.0.0.1:5000/history/inverter.json?columns=ppv,not_a_real_column&limit=1' | python -m json.tool
```

Expected: all three return HTTP 200 with the JSON shape described in this task's Interfaces section; the third call's `"columns"` field is `["timestamp", "ppv"]` (the unknown column silently dropped, per Task 1's `resolve_raw_columns`).

- [ ] **Step 5: Commit**

```bash
git add main.py templates/index.html
git commit -m "feat: add /history/inverter.json and /history/hourly.json routes"
```

---

### Task 3: `templates/history.html` page

**Files:**
- Create: `templates/history.html`

**Interfaces:**
- Consumes: the template variables `raw_columns`, `default_raw_columns`, `default_start`, `default_end` from Task 2's `get_history_page()`; the JSON shape from Task 2's `/history/inverter.json` and `/history/hourly.json`.
- Produces: nothing consumed by a later task — this is the last task in the plan.

No automated test for this task (it's markup/JS, exercised through the manual browser check in Step 2). The query-building logic it depends on is already covered by Task 1's unit tests.

- [ ] **Step 1: Create the template**

Create `templates/history.html` with this content:

```html
<!doctype html>
<html lang="en">

<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="/static/bootstrap.min.css" rel="stylesheet">
  <title>History - Energy Manager</title>
</head>

<body>
  <div class="container">
    <h1>History</h1>

    <ul class="nav nav-tabs" id="history-tabs" role="tablist">
      <li class="nav-item" role="presentation">
        <button class="nav-link active" id="raw-tab-btn" data-bs-toggle="tab" data-bs-target="#raw-tab"
          type="button" role="tab">Raw samples</button>
      </li>
      <li class="nav-item" role="presentation">
        <button class="nav-link" id="hourly-tab-btn" data-bs-toggle="tab" data-bs-target="#hourly-tab"
          type="button" role="tab">Hourly summary</button>
      </li>
    </ul>

    <div class="tab-content">
      <div class="tab-pane fade show active" id="raw-tab" role="tabpanel">
        <div class="row mt-3 g-2 align-items-end">
          <div class="col-6 col-md-3">
            <label class="form-label">Start</label>
            <input id="raw-start" type="date" class="form-control form-control-sm" value="{{ default_start }}" />
          </div>
          <div class="col-6 col-md-3">
            <label class="form-label">End</label>
            <input id="raw-end" type="date" class="form-control form-control-sm" value="{{ default_end }}" />
          </div>
          <div class="col-6 col-md-2">
            <label class="form-label">Rows</label>
            <select id="raw-limit" class="form-select form-select-sm">
              <option value="50">50</option>
              <option value="100" selected>100</option>
              <option value="250">250</option>
              <option value="500">500</option>
            </select>
          </div>
          <div class="col-6 col-md-4">
            <details>
              <summary class="btn btn-sm btn-outline-secondary">Columns ▾</summary>
              <div id="raw-columns" class="border rounded p-2 mt-1" style="max-height:200px;overflow-y:auto">
                {% for column in raw_columns %}
                <div class="form-check">
                  <input class="form-check-input raw-column-checkbox" type="checkbox" value="{{ column }}"
                    id="col-{{ column }}" {% if column in default_raw_columns %}checked{% endif %} />
                  <label class="form-check-label" for="col-{{ column }}">{{ column }}</label>
                </div>
                {% endfor %}
              </div>
            </details>
          </div>
        </div>

        <div class="table-responsive mt-3">
          <table class="table table-sm table-striped">
            <thead id="raw-thead"></thead>
            <tbody id="raw-tbody"></tbody>
          </table>
        </div>
        <p id="raw-empty" class="text-muted d-none">No data for this range.</p>
        <div class="d-flex gap-2">
          <button id="raw-prev" class="btn btn-sm btn-outline-primary">&lt; Prev</button>
          <button id="raw-next" class="btn btn-sm btn-outline-primary">Next &gt;</button>
        </div>
      </div>

      <div class="tab-pane fade" id="hourly-tab" role="tabpanel">
        <div class="row mt-3 g-2 align-items-end">
          <div class="col-6 col-md-3">
            <label class="form-label">Start</label>
            <input id="hourly-start" type="date" class="form-control form-control-sm" value="{{ default_start }}" />
          </div>
          <div class="col-6 col-md-3">
            <label class="form-label">End</label>
            <input id="hourly-end" type="date" class="form-control form-control-sm" value="{{ default_end }}" />
          </div>
          <div class="col-6 col-md-2">
            <label class="form-label">Rows</label>
            <select id="hourly-limit" class="form-select form-select-sm">
              <option value="50">50</option>
              <option value="100" selected>100</option>
              <option value="250">250</option>
              <option value="500">500</option>
            </select>
          </div>
        </div>

        <div class="table-responsive mt-3">
          <table class="table table-sm table-striped">
            <thead id="hourly-thead"></thead>
            <tbody id="hourly-tbody"></tbody>
          </table>
        </div>
        <p id="hourly-empty" class="text-muted d-none">No data for this range.</p>
        <div class="d-flex gap-2">
          <button id="hourly-prev" class="btn btn-sm btn-outline-primary">&lt; Prev</button>
          <button id="hourly-next" class="btn btn-sm btn-outline-primary">Next &gt;</button>
        </div>
      </div>
    </div>

    <div class="row mt-3">
      <div class="col">
        <a href="/" class="btn btn-primary">&lt; Back</a>
      </div>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js"
    integrity="sha384-MrcW6ZMFYlzcLA8Nl+NtUVF0sA7MsXsP1UyJoMp4YLEuNSfAP+JcXn/tWtIaxVXM" crossorigin="anonymous"></script>
  <script>
    const COLUMNS_STORAGE_KEY = 'history.columns';

    function getSelectedColumns() {
      const urlColumns = new URLSearchParams(window.location.search).get('columns');
      if (urlColumns) return urlColumns.split(',');
      const saved = localStorage.getItem(COLUMNS_STORAGE_KEY);
      if (saved) return JSON.parse(saved);
      return Array.from(document.querySelectorAll('.raw-column-checkbox'))
        .filter(cb => cb.checked)
        .map(cb => cb.value);
    }

    function applySelectedColumnsToCheckboxes(columns) {
      const columnSet = new Set(columns);
      document.querySelectorAll('.raw-column-checkbox').forEach(cb => {
        cb.checked = columnSet.has(cb.value);
      });
    }

    function renderTable(theadId, tbodyId, emptyId, columns, rows) {
      const thead = document.getElementById(theadId);
      const tbody = document.getElementById(tbodyId);
      const empty = document.getElementById(emptyId);
      thead.innerHTML = '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
      if (rows.length === 0) {
        tbody.innerHTML = '';
        empty.classList.remove('d-none');
        return;
      }
      empty.classList.add('d-none');
      tbody.innerHTML = rows.map(row =>
        '<tr>' + columns.map(c => `<td>${row[c] === null || row[c] === undefined ? '' : row[c]}</td>`).join('') + '</tr>'
      ).join('');
    }

    function makeTabController(config) {
      let offset = 0;

      function buildParams() {
        const params = new URLSearchParams();
        params.set('start', document.getElementById(config.startInput).value);
        params.set('end', document.getElementById(config.endInput).value);
        params.set('limit', document.getElementById(config.limitInput).value);
        params.set('offset', String(offset));
        if (config.columnsEnabled) {
          params.set('columns', getSelectedColumns().join(','));
        }
        return params;
      }

      async function load() {
        const params = buildParams();
        const resp = await fetch(config.endpoint + '?' + params.toString());
        const data = await resp.json();
        renderTable(config.theadId, config.tbodyId, config.emptyId, data.columns, data.rows);
        document.getElementById(config.prevBtn).disabled = offset === 0;
        document.getElementById(config.nextBtn).disabled = !data.has_more;

        if (config.columnsEnabled) {
          applySelectedColumnsToCheckboxes(data.columns);
          localStorage.setItem(COLUMNS_STORAGE_KEY, JSON.stringify(data.columns));
        }

        const shareParams = new URLSearchParams(window.location.search);
        shareParams.set('start', data.start);
        shareParams.set('end', data.end);
        if (config.columnsEnabled) shareParams.set('columns', data.columns.join(','));
        window.history.replaceState(null, '', '?' + shareParams.toString());
      }

      document.getElementById(config.startInput).addEventListener('change', () => { offset = 0; load(); });
      document.getElementById(config.endInput).addEventListener('change', () => { offset = 0; load(); });
      document.getElementById(config.limitInput).addEventListener('change', () => { offset = 0; load(); });
      document.getElementById(config.prevBtn).addEventListener('click', () => {
        offset = Math.max(0, offset - parseInt(document.getElementById(config.limitInput).value, 10));
        load();
      });
      document.getElementById(config.nextBtn).addEventListener('click', () => {
        offset += parseInt(document.getElementById(config.limitInput).value, 10);
        load();
      });
      if (config.columnsEnabled) {
        document.querySelectorAll('.raw-column-checkbox').forEach(cb => {
          cb.addEventListener('change', () => { offset = 0; load(); });
        });
      }

      return { load };
    }

    applySelectedColumnsToCheckboxes(getSelectedColumns());

    const rawController = makeTabController({
      startInput: 'raw-start', endInput: 'raw-end', limitInput: 'raw-limit',
      prevBtn: 'raw-prev', nextBtn: 'raw-next',
      theadId: 'raw-thead', tbodyId: 'raw-tbody', emptyId: 'raw-empty',
      endpoint: '/history/inverter.json', columnsEnabled: true,
    });
    const hourlyController = makeTabController({
      startInput: 'hourly-start', endInput: 'hourly-end', limitInput: 'hourly-limit',
      prevBtn: 'hourly-prev', nextBtn: 'hourly-next',
      theadId: 'hourly-thead', tbodyId: 'hourly-tbody', emptyId: 'hourly-empty',
      endpoint: '/history/hourly.json', columnsEnabled: false,
    });

    rawController.load();
    document.getElementById('hourly-tab-btn').addEventListener('shown.bs.tab', () => hourlyController.load(), { once: true });
  </script>
</body>

</html>
```

- [ ] **Step 2: Manually verify in a browser**

Start the dev server from the venv (`.env` present, real or `--dry-run` inverter connection doesn't matter for this check):

```bash
python main.py --dry-run
```

Open `http://127.0.0.1:5000/history` and check:
1. The "Raw samples" tab loads a table on page load, with the default 10 columns from `DEFAULT_RAW_COLUMNS` checked in the "Columns ▾" panel.
2. Clicking the "Hourly summary" tab loads its own table (only on first click — confirms the `{ once: true }` lazy-load).
3. Unchecking a column in "Columns ▾" removes it from the table and the URL's `?columns=` updates; reloading the page with no query string still shows the same (now-saved) column set — confirms `localStorage` persistence.
4. Opening the page again with an explicit `?columns=timestamp,ppv` in the URL shows only those two columns regardless of the saved `localStorage` value — confirms the URL-param-wins priority.
5. Changing the Start/End date inputs reloads the table and updates the URL's `start`/`end` params.
6. Setting a date range with no data (e.g. a range before the DB's earliest row) shows the "No data for this range." message instead of an empty table.
7. Click "Next" / "Prev" — the button is disabled at the start of the range (`Prev`) and disabled once `has_more` is `false` (`Next`).
8. Resize the browser to a narrow (mobile) width — both tables scroll horizontally inside their `.table-responsive` wrapper instead of overflowing the page, and the filter controls stack into full-width rows.

- [ ] **Step 3: Commit**

```bash
git add templates/history.html
git commit -m "feat: add the interactive history viewer page"
```
