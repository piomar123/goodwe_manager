# SQLite Storage Core (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CSV telemetry writer with SQLite (`inverter_history` +
`hourly_summary` in `data.db`), add hour-start recovery on startup, and
provide a one-off, resumable migration script for the existing `data-*.csv`
files.

**Architecture:** A new `sensors.py` module holds the sensor/domain
definitions (moved out of `main.py` so they have no import-time
side-effects, e.g. no `.env` requirement, and can be imported by tests and
the migration script). A new `storage.py` module owns the SQLite schema and
all read/write access, with a sync (`sqlite3`) API for the migration script
and an async (`aiosqlite`) API for the live polling loop in `main.py`.
`_migrate_csv_to_sqlite.py` is a new one-off CLI script built on top of both.

**Tech Stack:** Python 3.9+ (match the project's existing venv), stdlib
`sqlite3` + `unittest`, `aiosqlite~=0.20.0` (already in `requirements.txt`).

**Spec:** `docs/superpowers/specs/2026-08-27-sqlite-storage-design.md`

## Global Constraints

- Python 3.9 compatible: no `match`/`case`, use `typing.Optional`/`Union`
  rather than `X | None` in type hints (the existing `dict | dict` merge in
  `main.py` is fine — that's PEP 584, not a type hint).
- `data.db` uses `PRAGMA journal_mode = WAL` and
  `PRAGMA auto_vacuum = INCREMENTAL` (the latter must be set before any
  table is created).
- **Deviation from the spec's literal SQL, same intent:** `timestamp_epoch`
  is a plain `INTEGER` column computed in Python at insert time via
  `datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').timestamp()`, not a SQL
  `GENERATED ALWAYS AS (unixepoch(...))` column — SQLite's `unixepoch()`
  assumes its input is UTC, but the inverter's `timestamp` field is local
  time, so the SQL version would silently shift every hour bucket by the
  local UTC offset. Computing it in Python, where `datetime.timestamp()`
  correctly uses the system's local timezone, avoids that entirely.
- **This requires the host machine's system timezone to be set to the
  inverter's local timezone (Europe/Warsaw).** Every naive-`datetime` local
  conversion in this plan (`parse_timestamp_epoch`, `current_hour_bounds`,
  and `backfill_hourly_summary`'s midnight-crossing check for `pv_kwh`) is
  symmetric and internally consistent regardless of *which* timezone the
  host is set to — but if it's set to anything other than Europe/Warsaw
  (e.g. a container defaulting to UTC), hour buckets and the midnight
  check will be correct relative to the *wrong* local day, silently
  disagreeing with when the inverter itself actually rolls `e_day` over.
  This is a deployment requirement, not something the code can verify by
  itself. Separately: Poland's DST transitions occur at 2/3 AM local time,
  not at midnight, so they don't collide with the midnight-crossing check —
  true for this specific deployment, not a general guarantee, so it's
  recorded here rather than assumed silently.
- Numeric sensor columns use `REAL` affinity, label/text columns (listed in
  `sensors.TEXT_SENSOR_COLUMNS`) use `TEXT` — no per-sensor fine-grained
  typing beyond that split (matches the spec's explicitly stated
  out-of-scope item).
- The migration script never deletes source CSV files, under any
  circumstances.
- `data.db`, `data.db-wal`, `data.db-shm` must be added to `.gitignore`
  (added in Task 5, when `main.py` first starts creating the real file).

---

### Task 1: Extract sensor domain definitions into `sensors.py`

**Files:**
- Create: `sensors.py`
- Create: `tests/test_sensors.py`
- Modify: `main.py:1-155` (remove `SELECTED_SENSORS` and
  `CalculatedValuesEvaluator`, import them from `sensors` instead)

**Interfaces:**
- Produces: `sensors.SELECTED_SENSORS: list[str]`,
  `sensors.CALCULATED_VALUE_HEADERS: list[str]`,
  `sensors.TEXT_SENSOR_COLUMNS: set[str]`,
  `sensors.TEXT_CALCULATED_COLUMNS: set[str]`,
  `sensors.sensor_columns() -> list[tuple[str, str]]`,
  `sensors.CalculatedValuesEvaluator` (same public API as today, plus a new
  `seed_hour_start(sensors_data: Optional[Mapping[str, Any]]) -> None`
  method).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sensors.py`:

```python
import unittest

from sensors import CalculatedValuesEvaluator, SELECTED_SENSORS, sensor_columns


class CalculatedValuesEvaluatorTest(unittest.TestCase):
    def test_first_sample_becomes_its_own_hour_start_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        sample = {
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        }

        calculated = evaluator.calculate_values(sample)

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_hourly_meter_export'], '0.00')
        self.assertEqual(calculated['_hourly_meter_import'], '0.00')
        self.assertEqual(calculated['_hourly_load'], '0.0')

    def test_running_totals_accumulate_within_the_same_hour(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 14:30:05',
            'meter_e_total_exp': '103.5',
            'meter_e_total_imp': '51.0',
            'e_load_total': '14.0',
        })

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_hourly_meter_export'], '3.50')
        self.assertEqual(calculated['_hourly_meter_import'], '1.00')
        self.assertEqual(calculated['_hourly_load'], '4.0')

    def test_new_hour_resets_the_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 15:00:02',
            'meter_e_total_exp': '110.0',
            'meter_e_total_imp': '55.0',
            'e_load_total': '20.0',
        })

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 15:00:02')
        self.assertEqual(calculated['_hourly_meter_export'], '0.00')

    def test_seed_hour_start_restores_a_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.seed_hour_start({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 14:15:00',
            'meter_e_total_exp': '101.0',
            'meter_e_total_imp': '50.5',
            'e_load_total': '12.0',
        })

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_hourly_meter_export'], '1.00')

    def test_seed_hour_start_with_none_leaves_evaluator_at_cold_start(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.seed_hour_start(None)

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 14:15:00',
            'meter_e_total_exp': '101.0',
            'meter_e_total_imp': '50.5',
            'e_load_total': '12.0',
        })

        # no baseline was restored, so this first sample becomes the baseline
        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:15:00')
        self.assertEqual(calculated['_hourly_meter_export'], '0.00')


class SensorColumnsTest(unittest.TestCase):
    def test_covers_every_selected_sensor_plus_calculated_headers(self):
        columns = sensor_columns()
        column_names = [name for name, _ in columns]

        self.assertEqual(len(columns), len(SELECTED_SENSORS) + 4)
        self.assertEqual(column_names[:len(SELECTED_SENSORS)], SELECTED_SENSORS)
        self.assertIn('_hourly_meter_export', column_names)

    def test_label_columns_are_text_everything_else_is_real(self):
        columns = dict(sensor_columns())

        self.assertEqual(columns['timestamp'], 'TEXT')
        self.assertEqual(columns['pv1_mode_label'], 'TEXT')
        self.assertEqual(columns['ppv'], 'REAL')
        self.assertEqual(columns['battery_soc'], 'REAL')


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /Users/piotr.marcinczyk/Dev/goodwe_manager && python3 -m unittest tests.test_sensors -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sensors'`

- [ ] **Step 3: Create `sensors.py`**

```python
"""
Sensor domain definitions shared by main.py, storage.py, and the CSV
migration script. Deliberately has no import-time side effects (no env var
requirements) so it can be imported safely from tests and utility scripts.
"""
from typing import Any, Mapping, Optional

SELECTED_SENSORS = [
    'timestamp',
    'ppv',
    'ppv1',
    'ppv2',
    'vpv1',
    'vpv2',
    'ipv1',
    'ipv2',
    'pv1_mode_label',
    'pv2_mode_label',
    'function_bit',
    'bus_voltage',
    'nbus_voltage',
    'operation_mode',
    'pgrid',
    'pgrid2',
    'pgrid3',
    'vgrid',
    'igrid',
    'fgrid',
    'vgrid2',
    'igrid2',
    'fgrid2',
    'vgrid3',
    'igrid3',
    'fgrid3',
    'meter_freq',
    'grid_mode',
    'grid_mode_label',
    'grid_in_out',
    'grid_in_out_label',
    'total_inverter_power',
    'active_power',
    'reactive_power',
    'apparent_power',
    'load_mode1',
    'load_mode2',
    'load_mode3',
    'load_p1',
    'load_p2',
    'load_p3',
    'load_ptotal',
    'house_consumption',
    'active_power1',
    'active_power2',
    'active_power3',
    'active_power_total',
    'reactive_power_total',
    'meter_active_power1',
    'meter_active_power2',
    'meter_active_power3',
    'meter_active_power_total',
    'meter_reactive_power1',
    'meter_reactive_power2',
    'meter_reactive_power3',
    'meter_reactive_power_total',
    'meter_apparent_power1',
    'meter_apparent_power2',
    'meter_apparent_power3',
    'meter_apparent_power_total',
    'meter_power_factor1',
    'meter_power_factor2',
    'meter_power_factor3',
    'meter_power_factor',
    'meter_type',
    'backup_p1',
    'backup_p2',
    'backup_p3',
    'backup_ptotal',
    'backup_v1',
    'backup_v2',
    'backup_v3',
    'backup_i1',
    'backup_i2',
    'backup_i3',
    'backup_f1',
    'backup_f2',
    'backup_f3',
    'ups_load',
    'temperature_air',
    'temperature',
    'vbattery1',
    'ibattery1',
    'pbattery1',
    'battery_mode_label',
    'battery_temperature',
    'battery_soc',
    'battery_charge_limit',
    'battery_discharge_limit',
    'battery_error',
    'battery_warning',
    'warning_code',
    'diagnose_result_label',
    'error_codes',
    'errors',
    'e_total_exp',
    'e_total_imp',
    'e_day',
    'e_load_total',
    'meter_e_total_exp',
    'meter_e_total_imp',
    'e_bat_charge_total',
    'e_bat_discharge_total',
    'work_mode_label',
    'rssi',
]

# Columns whose values are text labels/codes, not continuous numeric
# measurements. Everything else in SELECTED_SENSORS is stored as REAL.
TEXT_SENSOR_COLUMNS = {
    'timestamp',
    'pv1_mode_label',
    'pv2_mode_label',
    'grid_mode_label',
    'grid_in_out_label',
    'battery_mode_label',
    'diagnose_result_label',
    'work_mode_label',
    'error_codes',
    'errors',
}

CALCULATED_VALUE_HEADERS = [
    '_hour_start_timestamp',
    '_hourly_meter_export',
    '_hourly_meter_import',
    '_hourly_load',
]
TEXT_CALCULATED_COLUMNS = {'_hour_start_timestamp'}


def sensor_columns() -> list:
    """Ordered (column_name, sql_type) pairs for the inverter_history table,
    in the same order as SELECTED_SENSORS + CalculatedValuesEvaluator.headers().
    """
    columns = []
    for name in SELECTED_SENSORS:
        columns.append((name, 'TEXT' if name in TEXT_SENSOR_COLUMNS else 'REAL'))
    for name in CALCULATED_VALUE_HEADERS:
        columns.append((name, 'TEXT' if name in TEXT_CALCULATED_COLUMNS else 'REAL'))
    return columns


class CalculatedValuesEvaluator:
    def __init__(self):
        self._hour_start_sensors = None

    def calculate_values(self, sensors_data: Mapping[str, Any]) -> dict:
        if self._hour_start_sensors is None or sensors_data['timestamp'][:13] != self._hour_start_sensors['timestamp'][
                                                                                 :13]:
            self._hour_start_sensors = sensors_data
        calculated_values = {
            '_hour_start_timestamp': self._hour_start_sensors['timestamp'],
            '_hourly_meter_export': f"{float(sensors_data['meter_e_total_exp']) - float(self._hour_start_sensors['meter_e_total_exp']):.2f}",
            '_hourly_meter_import': f"{float(sensors_data['meter_e_total_imp']) - float(self._hour_start_sensors['meter_e_total_imp']):.2f}",
            '_hourly_load': f"{float(sensors_data['e_load_total']) - float(self._hour_start_sensors['e_load_total']):.1f}",
        }
        self._verify_header(calculated_values)
        return calculated_values

    def seed_hour_start(self, sensors_data: Optional[Mapping[str, Any]]) -> None:
        """Restores the hour-start baseline from a previously stored sample
        (see storage.get_current_hour_start_sample*), typically called once
        at startup. Passing None leaves the evaluator in its cold-start
        state, where the next incoming sample becomes the new baseline -
        the correct behavior when no sample exists for the current hour
        yet (empty DB, or the last sample predates the current hour).
        """
        self._hour_start_sensors = dict(sensors_data) if sensors_data is not None else None

    @staticmethod
    def headers():
        return CALCULATED_VALUE_HEADERS

    def _verify_header(self, calculated_values):
        for header, key in zip(self.headers(), calculated_values.keys()):
            if header != key:
                raise AssertionError(f"Implementation error: headers do not correspond to set keys: {key} != {header}, "
                                     f"{self.headers()} != {calculated_values.keys()}")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest tests.test_sensors -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Update `main.py` to import from `sensors`**

In `main.py`, replace the `SELECTED_SENSORS = [...]` list (currently lines
47-153) and the `CalculatedValuesEvaluator` class (currently lines 158-182)
with:

```python
from sensors import SELECTED_SENSORS, CalculatedValuesEvaluator
```

placed alongside the other local imports near the top of the file (after
`from rce import ...`). Remove the now-duplicate definitions from `main.py`
entirely - `AsyncioThread._calculated_values_evaluator = CalculatedValuesEvaluator()`
(around line 189) keeps working unchanged since it's now referencing the
imported class.

- [ ] **Step 6: Verify `main.py` still parses and the full test suite passes**

Run: `python3 -c "import ast; ast.parse(open('main.py').read())" && python3 -m unittest discover -s tests -v`
Expected: no syntax errors, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add sensors.py tests/test_sensors.py main.py
git commit -m "refactor: extract sensor domain definitions into sensors.py

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 2: `storage.py` core - schema, insert, hour-start query

**Files:**
- Create: `storage.py`
- Create: `tests/test_storage.py`

**Interfaces:**
- Consumes: `sensors.sensor_columns() -> list[tuple[str, str]]` (Task 1)
- Produces: `storage.DATA_DB_PATH: str`,
  `storage.build_ddl_statements(columns: list) -> list[str]`,
  `storage.init_db_sync(path: str, columns: list) -> sqlite3.Connection`,
  `storage.init_db_async(path: str, columns: list) -> aiosqlite.Connection`
  (async def), `storage.parse_timestamp_epoch(timestamp: str) -> int`,
  `storage.insert_sample_sync(conn: sqlite3.Connection, row: dict) -> None`,
  `storage.insert_sample_async(conn: aiosqlite.Connection, row: dict) -> None`
  (async def),
  `storage.get_current_hour_start_sample(conn: sqlite3.Connection, hour_start_epoch: int, hour_end_epoch: int) -> Optional[dict]`,
  `storage.get_current_hour_start_sample_async(conn: aiosqlite.Connection, hour_start_epoch: int, hour_end_epoch: int) -> Optional[dict]`
  (async def), `storage.current_hour_bounds(now: datetime) -> tuple[int, int]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_storage.py`:

```python
import asyncio
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime

import storage
from sensors import sensor_columns


def _sample_row(timestamp: str, **overrides) -> dict:
    row = {name: ('0' if sql_type == 'REAL' else '') for name, sql_type in sensor_columns()}
    row['timestamp'] = timestamp
    row.update(overrides)
    return row


class StorageSyncTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)  # sqlite3.connect creates it fresh
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_init_db_is_idempotent(self):
        # calling it again on the same file must not raise
        storage.init_db_sync(self.db_path, sensor_columns()).close()

    def test_insert_and_read_back_a_sample(self):
        row = _sample_row('2026-08-28 14:05:00', ppv='1234.5', battery_soc='87')

        storage.insert_sample_sync(self.conn, row)

        result = self.conn.execute("SELECT ppv, battery_soc, timestamp_epoch FROM inverter_history").fetchone()
        self.assertEqual(result[0], 1234.5)
        self.assertEqual(result[1], 87.0)

        # round-trip check that's independent of the test machine's timezone
        roundtrip = datetime.fromtimestamp(result[2]).strftime('%Y-%m-%d %H:%M:%S')
        self.assertEqual(roundtrip, '2026-08-28 14:05:00')

    def test_get_current_hour_start_sample_returns_none_when_empty(self):
        start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 0))

        result = storage.get_current_hour_start_sample(self.conn, start, end)

        self.assertIsNone(result)

    def test_get_current_hour_start_sample_returns_the_earliest_row_in_the_bucket(self):
        storage.insert_sample_sync(self.conn, _sample_row('2026-08-28 13:58:00'))  # previous hour
        storage.insert_sample_sync(self.conn, _sample_row('2026-08-28 14:00:03', ppv='10'))
        storage.insert_sample_sync(self.conn, _sample_row('2026-08-28 14:05:00', ppv='20'))  # later, same hour
        start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 0))

        result = storage.get_current_hour_start_sample(self.conn, start, end)

        self.assertEqual(result['timestamp'], '2026-08-28 14:00:03')
        self.assertEqual(result['ppv'], 10.0)


class ParseTimestampEpochTest(unittest.TestCase):
    def test_round_trips_through_local_time(self):
        epoch = storage.parse_timestamp_epoch('2026-08-28 14:05:00')

        roundtrip = datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')

        self.assertEqual(roundtrip, '2026-08-28 14:05:00')


class CurrentHourBoundsTest(unittest.TestCase):
    def test_returns_the_start_and_end_of_the_containing_hour(self):
        start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 12))

        self.assertEqual(end - start, 3600)
        self.assertEqual(
            datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S'),
            '2026-08-28 14:00:00',
        )


class StorageAsyncTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)

    def tearDown(self):
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_insert_and_read_back_a_sample_async(self):
        async def scenario():
            conn = await storage.init_db_async(self.db_path, sensor_columns())
            try:
                await storage.insert_sample_async(conn, _sample_row('2026-08-28 14:05:00', ppv='42'))
                cursor = await conn.execute("SELECT ppv FROM inverter_history")
                row = await cursor.fetchone()
                return row[0]
            finally:
                await conn.close()

        result = asyncio.new_event_loop().run_until_complete(scenario())
        self.assertEqual(result, 42.0)

    def test_get_current_hour_start_sample_async(self):
        async def scenario():
            conn = await storage.init_db_async(self.db_path, sensor_columns())
            try:
                await storage.insert_sample_async(conn, _sample_row('2026-08-28 14:00:03', ppv='10'))
                start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 0))
                return await storage.get_current_hour_start_sample_async(conn, start, end)
            finally:
                await conn.close()

        result = asyncio.new_event_loop().run_until_complete(scenario())
        self.assertEqual(result['timestamp'], '2026-08-28 14:00:03')


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest tests.test_storage -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'storage'`

- [ ] **Step 3: Create `storage.py`**

```python
"""
SQLite storage for inverter telemetry: raw per-sample history
(inverter_history) and derived per-hour totals (hourly_summary).
See docs/superpowers/specs/2026-08-27-sqlite-storage-design.md.
"""
import sqlite3
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Tuple

import aiosqlite

DATA_DB_PATH = 'data.db'


def parse_timestamp_epoch(timestamp: str) -> int:
    """Converts a sensor 'YYYY-MM-DD HH:MM:SS' local-time string into a unix
    epoch integer. Done in Python (not SQL) because the timestamp is local
    time, and SQLite's unixepoch() assumes UTC input.
    """
    return int(datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').timestamp())


def current_hour_bounds(now: datetime) -> Tuple[int, int]:
    """Returns (start_epoch, end_epoch) for the hour containing `now`."""
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    start_epoch = int(hour_start.timestamp())
    return start_epoch, start_epoch + 3600


def _column_ddl(columns: Iterable[Tuple[str, str]]) -> str:
    return ',\n            '.join(f'{name} {sql_type}' for name, sql_type in columns)


def build_ddl_statements(columns: list) -> list:
    return [
        "PRAGMA journal_mode = WAL",
        "PRAGMA auto_vacuum = INCREMENTAL",
        f"""
        CREATE TABLE IF NOT EXISTS inverter_history (
            id INTEGER PRIMARY KEY,
            timestamp_epoch INTEGER NOT NULL,
            {_column_ddl(columns)}
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_inverter_history_timestamp_epoch "
        "ON inverter_history (timestamp_epoch)",
        """
        CREATE TABLE IF NOT EXISTS hourly_summary (
            hour_start INTEGER PRIMARY KEY,
            meter_export_kwh REAL,
            meter_import_kwh REAL,
            load_kwh REAL,
            pv_kwh REAL,
            battery_charge_kwh REAL,
            battery_discharge_kwh REAL
        )
        """,
    ]


def init_db_sync(path: str, columns: list) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    for statement in build_ddl_statements(columns):
        conn.execute(statement)
    conn.commit()
    return conn


async def init_db_async(path: str, columns: list) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(path)
    for statement in build_ddl_statements(columns):
        await conn.execute(statement)
    await conn.commit()
    return conn


def _row_with_epoch(row: Mapping[str, Any]) -> dict:
    full_row = dict(row)
    full_row['timestamp_epoch'] = parse_timestamp_epoch(row['timestamp'])
    return full_row


def _insert_sql(column_names: list) -> str:
    placeholders = ', '.join('?' for _ in column_names)
    columns_sql = ', '.join(column_names)
    return f"INSERT INTO inverter_history ({columns_sql}) VALUES ({placeholders})"


def insert_sample_sync(conn: sqlite3.Connection, row: Mapping[str, Any]) -> None:
    full_row = _row_with_epoch(row)
    column_names = list(full_row.keys())
    conn.execute(_insert_sql(column_names), [full_row[c] for c in column_names])
    conn.commit()


async def insert_sample_async(conn: aiosqlite.Connection, row: Mapping[str, Any]) -> None:
    full_row = _row_with_epoch(row)
    column_names = list(full_row.keys())
    await conn.execute(_insert_sql(column_names), [full_row[c] for c in column_names])
    await conn.commit()


_HOUR_START_QUERY = (
    "SELECT * FROM inverter_history "
    "WHERE timestamp_epoch >= ? AND timestamp_epoch < ? "
    "ORDER BY timestamp_epoch ASC LIMIT 1"
)


def get_current_hour_start_sample(conn: sqlite3.Connection, hour_start_epoch: int,
                                  hour_end_epoch: int) -> Optional[dict]:
    cursor = conn.execute(_HOUR_START_QUERY, (hour_start_epoch, hour_end_epoch))
    row = cursor.fetchone()
    if row is None:
        return None
    column_names = [d[0] for d in cursor.description]
    return dict(zip(column_names, row))


async def get_current_hour_start_sample_async(conn: aiosqlite.Connection, hour_start_epoch: int,
                                               hour_end_epoch: int) -> Optional[dict]:
    async with conn.execute(_HOUR_START_QUERY, (hour_start_epoch, hour_end_epoch)) as cursor:
        row = await cursor.fetchone()
        if row is None:
            return None
        column_names = [d[0] for d in cursor.description]
        return dict(zip(column_names, row))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest tests.test_storage -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add storage.py tests/test_storage.py
git commit -m "feat: add storage.py - SQLite schema, insert, hour-start query

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 3: `hourly_summary` backfill

**Files:**
- Modify: `storage.py` (append to the file created in Task 2)
- Modify: `tests/test_storage.py` (append to the file created in Task 2)

**Interfaces:**
- Consumes: `storage.insert_sample_sync`, `storage.init_db_sync` (Task 2)
- Produces:
  `storage.find_hours_needing_backfill(conn: sqlite3.Connection) -> list[int]`,
  `storage.backfill_hourly_summary(conn: sqlite3.Connection) -> int`
  (returns count of hours backfilled).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_storage.py`:

```python
class BackfillHourlySummaryTest(unittest.TestCase):
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

    def _insert(self, timestamp, **overrides):
        storage.insert_sample_sync(self.conn, _sample_row(timestamp, **overrides))

    def test_backfills_an_hour_that_has_a_prior_and_a_following_hour(self):
        # hour 13:00 - baseline
        self._insert('2026-08-28 13:05:00', meter_e_total_exp='100.0', meter_e_total_imp='50.0',
                     e_load_total='10.0', e_day='5.0', e_bat_charge_total='1.0', e_bat_discharge_total='0.5')
        # hour 14:00 - the hour under test
        self._insert('2026-08-28 14:05:00', meter_e_total_exp='103.0', meter_e_total_imp='51.0',
                     e_load_total='14.0', e_day='9.0', e_bat_charge_total='2.0', e_bat_discharge_total='1.5')
        # hour 15:00 - proves 14:00 is complete
        self._insert('2026-08-28 15:05:00', meter_e_total_exp='110.0', meter_e_total_imp='55.0',
                     e_load_total='20.0', e_day='12.0', e_bat_charge_total='3.0', e_bat_discharge_total='2.0')

        backfilled = storage.backfill_hourly_summary(self.conn)

        self.assertEqual(backfilled, 2)  # hour 13:00 (NULL diffs) and hour 14:00 (real diffs)
        row = self.conn.execute(
            "SELECT meter_export_kwh, meter_import_kwh, load_kwh, pv_kwh, battery_charge_kwh, battery_discharge_kwh "
            "FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        self.assertEqual(row, (3.0, 1.0, 4.0, 4.0, 1.0, 1.0))

        # hour 15:00 has no following hour yet - not backfilled
        count_for_15 = self.conn.execute(
            "SELECT COUNT(*) FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 15:00:00'),),
        ).fetchone()[0]
        self.assertEqual(count_for_15, 0)

    def test_hour_with_no_prior_data_gets_null_metrics_but_is_marked_processed(self):
        self._insert('2026-08-28 14:05:00', meter_e_total_exp='103.0')
        self._insert('2026-08-28 15:05:00', meter_e_total_exp='110.0')  # proves 14:00 complete

        storage.backfill_hourly_summary(self.conn)

        row = self.conn.execute(
            "SELECT meter_export_kwh FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        self.assertIsNone(row[0])

    def test_is_idempotent(self):
        self._insert('2026-08-28 13:05:00', meter_e_total_exp='100.0')
        self._insert('2026-08-28 14:05:00', meter_e_total_exp='103.0')
        self._insert('2026-08-28 15:05:00', meter_e_total_exp='110.0')
        storage.backfill_hourly_summary(self.conn)

        second_run_count = storage.backfill_hourly_summary(self.conn)

        self.assertEqual(second_run_count, 0)

    def test_pv_kwh_does_not_go_negative_across_midnight(self):
        # e_day resets to 0 right after local midnight (confirmed against
        # real device data), unlike the other lifetime counters - so the
        # hour spanning midnight must not diff e_day against the previous
        # (different day's) value.
        self._insert('2026-08-28 23:05:00', e_day='47.9')          # last hour of the previous day
        self._insert('2026-08-29 00:05:00', e_day='0.3')           # the hour under test
        self._insert('2026-08-29 01:05:00', e_day='0.5')           # proves 00:00 is complete

        storage.backfill_hourly_summary(self.conn)

        row = self.conn.execute(
            "SELECT pv_kwh FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-29 00:00:00'),),
        ).fetchone()
        self.assertEqual(row[0], 0.3)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest tests.test_storage.BackfillHourlySummaryTest -v`
Expected: FAIL with `AttributeError: module 'storage' has no attribute 'backfill_hourly_summary'`

- [ ] **Step 3: Append the backfill implementation to `storage.py`**

```python
_HOURLY_METRIC_COLUMNS = [
    ('meter_e_total_exp', 'meter_export_kwh'),
    ('meter_e_total_imp', 'meter_import_kwh'),
    ('e_load_total', 'load_kwh'),
    ('e_day', 'pv_kwh'),
    ('e_bat_charge_total', 'battery_charge_kwh'),
    ('e_bat_discharge_total', 'battery_discharge_kwh'),
]


def find_hours_needing_backfill(conn: sqlite3.Connection) -> list:
    cursor = conn.execute("""
        WITH buckets AS (
            SELECT DISTINCT (timestamp_epoch / 3600) * 3600 AS bucket FROM inverter_history
        )
        SELECT bucket FROM buckets b
        WHERE bucket NOT IN (SELECT hour_start FROM hourly_summary)
          AND EXISTS (SELECT 1 FROM buckets nxt WHERE nxt.bucket = b.bucket + 3600)
        ORDER BY bucket
    """)
    return [row[0] for row in cursor.fetchall()]


def _max_counters(conn: sqlite3.Connection, start_epoch: int, end_epoch: int) -> Optional[dict]:
    source_columns = [source for source, _ in _HOURLY_METRIC_COLUMNS]
    select_sql = ', '.join(f'MAX({c})' for c in source_columns)
    row = conn.execute(
        f"SELECT {select_sql} FROM inverter_history WHERE timestamp_epoch >= ? AND timestamp_epoch < ?",
        (start_epoch, end_epoch),
    ).fetchone()
    if row is None or all(v is None for v in row):
        return None
    return dict(zip(source_columns, row))


def backfill_hourly_summary(conn: sqlite3.Connection) -> int:
    """Derives hourly_summary rows from inverter_history for every hour that
    has data in the following hour (proving it's complete) and doesn't have
    a hourly_summary row yet. If there's no data at all in the preceding
    hour, the row is still inserted (so it's not reprocessed every run) but
    with NULL metrics, since a true diff can't be computed without a prior
    baseline. Returns the number of hours backfilled.

    pv_kwh (sourced from e_day) is a special case: e_day resets to 0 right
    after local midnight, unlike the other lifetime-cumulative counters, so
    the hour whose start crosses a calendar-day boundary uses e_day's
    current-hour value alone rather than diffing against the previous
    (different day's) value.
    """
    backfilled = 0
    for hour_start in find_hours_needing_backfill(conn):
        current = _max_counters(conn, hour_start, hour_start + 3600)
        previous = _max_counters(conn, hour_start - 3600, hour_start)
        # fromtimestamp() (no tzinfo) uses the host's local timezone, the
        # exact symmetric inverse of parse_timestamp_epoch()'s naive-local
        # strptime(...).timestamp() - so .date() reflects the same local
        # calendar day the inverter itself resets e_day at. This requires
        # the host's system timezone to be set to the inverter's own
        # timezone (Europe/Warsaw); see the Global Constraints note.
        crosses_midnight = (
            datetime.fromtimestamp(hour_start).date() != datetime.fromtimestamp(hour_start - 3600).date()
        )
        metrics = {}
        for source, target in _HOURLY_METRIC_COLUMNS:
            if current is None:
                metrics[target] = None
            elif source == 'e_day' and crosses_midnight:
                metrics[target] = current[source]
            elif previous is None:
                metrics[target] = None
            else:
                metrics[target] = current[source] - previous[source]
        conn.execute(
            """
            INSERT OR REPLACE INTO hourly_summary
                (hour_start, meter_export_kwh, meter_import_kwh, load_kwh, pv_kwh,
                 battery_charge_kwh, battery_discharge_kwh)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (hour_start, metrics['meter_export_kwh'], metrics['meter_import_kwh'], metrics['load_kwh'],
             metrics['pv_kwh'], metrics['battery_charge_kwh'], metrics['battery_discharge_kwh']),
        )
        backfilled += 1
    conn.commit()
    return backfilled
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest tests.test_storage -v`
Expected: PASS (11 tests total in the file)

- [ ] **Step 5: Commit**

```bash
git add storage.py tests/test_storage.py
git commit -m "feat: add backfill_hourly_summary to storage.py

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 4: CSV migration script

**Files:**
- Create: `_migrate_csv_to_sqlite.py`
- Create: `tests/test_migrate_csv_to_sqlite.py`

**Interfaces:**
- Consumes: `storage.init_db_sync`, `storage.insert_sample_sync`,
  `storage.backfill_hourly_summary` (Tasks 2-3),
  `sensors.sensor_columns()` (Task 1)
- Produces: `_migrate_csv_to_sqlite.ensure_migration_log_table(conn) -> None`,
  `_migrate_csv_to_sqlite.already_migrated(conn, filename: str) -> bool`,
  `_migrate_csv_to_sqlite.read_csv_rows(csv_path: Path) -> list[dict]`,
  `_migrate_csv_to_sqlite.migrate_file(conn, csv_path: Path, dry_run: bool) -> str`
  (returns one of `'done'`, `'error'`, `'skipped'`, `'dry-run'`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_migrate_csv_to_sqlite.py`:

```python
import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

import storage
from sensors import sensor_columns

migrate = importlib.import_module('_migrate_csv_to_sqlite')

HEADER = 'timestamp,ppv,meter_e_total_exp,meter_e_total_imp,e_load_total\n'


class MigrateCsvToSqliteTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, 'data.db')
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())
        migrate.ensure_migration_log_table(self.conn)

    def tearDown(self):
        self.conn.close()

    def _write_csv(self, name: str, content: str) -> Path:
        path = Path(self.tmp_dir) / name
        path.write_text(content)
        return path

    def test_imports_a_normal_csv_and_marks_it_done(self):
        csv_path = self._write_csv('data-2026-08-28_10-00-00.csv',
                                   HEADER +
                                   '2026-08-28 10:00:00,100.0,1.0,0.5,0.1\n'
                                   '2026-08-28 10:00:01,101.0,1.1,0.5,0.2\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'done')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 2)
        log_status = self.conn.execute(
            "SELECT status, row_count FROM csv_migration_log WHERE filename = ?",
            (csv_path.name,),
        ).fetchone()
        self.assertEqual(log_status, ('done', 2))

    def test_skips_an_empty_file(self):
        csv_path = self._write_csv('data-2026-08-28_11-00-00.csv', '')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'skipped')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 0)

    def test_skips_a_file_missing_required_columns(self):
        csv_path = self._write_csv('data-2026-08-28_12-00-00.csv', 'timestamp,ppv\n2026-08-28 12:00:00,100.0\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'skipped')

    def test_skips_a_file_already_marked_done(self):
        csv_path = self._write_csv('data-2026-08-28_13-00-00.csv',
                                   HEADER + '2026-08-28 13:00:00,100.0,1.0,0.5,0.1\n')
        migrate.migrate_file(self.conn, csv_path, dry_run=False)

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'skipped')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 1)  # not duplicated

    def test_dry_run_writes_nothing(self):
        csv_path = self._write_csv('data-2026-08-28_14-00-00.csv',
                                   HEADER + '2026-08-28 14:00:00,100.0,1.0,0.5,0.1\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=True)

        self.assertEqual(status, 'dry-run')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 0)
        log_count = self.conn.execute("SELECT COUNT(*) FROM csv_migration_log").fetchone()[0]
        self.assertEqual(log_count, 0)

    def test_a_locked_database_is_reported_as_an_error_not_a_crash(self):
        csv_path = self._write_csv('data-2026-08-28_15-00-00.csv',
                                   HEADER + '2026-08-28 15:00:00,100.0,1.0,0.5,0.1\n')
        blocker = sqlite3.connect(self.db_path, timeout=0)
        blocker.execute("BEGIN EXCLUSIVE")

        try:
            # the lock is held for the entire call, so even the manifest
            # write inside migrate_file's own error handling will fail -
            # the only guaranteed-reliable signal under lock is the return
            # value itself, which must still be 'error', never an
            # unhandled exception
            status = migrate.migrate_file(self.conn, csv_path, dry_run=False)
            self.assertEqual(status, 'error')
        finally:
            blocker.rollback()
            blocker.close()

        # once the lock clears, the file was never marked 'done' (the
        # manifest write itself failed while locked), so a later re-run
        # retries it from scratch rather than silently skipping it
        self.assertFalse(migrate.already_migrated(self.conn, csv_path.name))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest tests.test_migrate_csv_to_sqlite -v`
Expected: FAIL with `ModuleNotFoundError: No module named '_migrate_csv_to_sqlite'`

- [ ] **Step 3: Create `_migrate_csv_to_sqlite.py`**

```python
"""
One-off migration of legacy data-*.csv files into data.db (inverter_history).
Safe to re-run: already-migrated files are skipped via the csv_migration_log
manifest table. Never deletes the source CSV files - keep them on disk at
least as long as inverter_history's own retention window (180 days) past a
successful, verified migration.

Usage: python _migrate_csv_to_sqlite.py [--dry-run] [--csv-dir DIR] [--db-path PATH]
"""
import argparse
import csv
import logging
import sqlite3
import time
from pathlib import Path

import storage
from sensors import sensor_columns

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {'timestamp', 'meter_e_total_exp', 'meter_e_total_imp', 'e_load_total'}


def ensure_migration_log_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS csv_migration_log (
            filename TEXT PRIMARY KEY,
            row_count INTEGER,
            migrated_at INTEGER,
            status TEXT
        )
    """)
    conn.commit()


def already_migrated(conn: sqlite3.Connection, filename: str) -> bool:
    row = conn.execute(
        "SELECT status FROM csv_migration_log WHERE filename = ?", (filename,)
    ).fetchone()
    return row is not None and row[0] == 'done'


def read_csv_rows(csv_path: Path) -> list:
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return []
        if not REQUIRED_COLUMNS.issubset(set(reader.fieldnames)):
            return []
        return list(reader)


def _record_migration(conn: sqlite3.Connection, filename: str, row_count: int, status: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO csv_migration_log (filename, row_count, migrated_at, status) "
        "VALUES (?, ?, ?, ?)",
        (filename, row_count, int(time.time()), status),
    )
    conn.commit()


def migrate_file(conn: sqlite3.Connection, csv_path: Path, dry_run: bool) -> str:
    filename = csv_path.name
    if already_migrated(conn, filename):
        logger.info(f"Skipping {filename}: already migrated")
        return 'skipped'

    rows = read_csv_rows(csv_path)
    if not rows:
        logger.warning(f"Skipping {filename}: empty or missing required columns")
        return 'skipped'

    if dry_run:
        logger.info(f"[dry-run] Would import {len(rows)} rows from {filename}")
        return 'dry-run'

    valid_columns = {name for name, _ in sensor_columns()}
    inserted = 0
    try:
        for row in rows:
            filtered_row = {k: v for k, v in row.items() if k in valid_columns}
            storage.insert_sample_sync(conn, filtered_row)
            inserted += 1
    except sqlite3.Error as e:
        logger.error(f"{filename}: insert failed after {inserted}/{len(rows)} rows: {e}")
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        try:
            _record_migration(conn, filename, len(rows), 'error')
        except sqlite3.Error:
            # the database is contested enough that even recording the
            # failure failed - migrate_file still reports 'error' to its
            # caller rather than crashing; the file stays unmarked in the
            # manifest, so a later re-run will retry it from scratch
            logger.error(f"{filename}: could not record migration failure in the manifest (database still locked)")
        return 'error'

    first_row_db = conn.execute(
        "SELECT ppv FROM inverter_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
        (rows[0]['timestamp'],),
    ).fetchone()
    verified = (
        inserted == len(rows)
        and first_row_db is not None
        and str(first_row_db[0]) == str(float(rows[0].get('ppv', 0)))
    )
    status = 'done' if verified else 'error'
    if not verified:
        logger.error(f"{filename}: verification failed (inserted={inserted}/{len(rows)}, spot-check mismatch)")
    else:
        logger.info(f"{filename}: imported and verified {inserted} rows")

    _record_migration(conn, filename, len(rows), status)
    return status


def main():
    parser = argparse.ArgumentParser(description="Migrate legacy data-*.csv files into data.db")
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--csv-dir', default='.')
    parser.add_argument('--db-path', default=storage.DATA_DB_PATH)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    conn = storage.init_db_sync(args.db_path, sensor_columns())
    ensure_migration_log_table(conn)

    csv_paths = sorted(Path(args.csv_dir).glob('data-*.csv'))
    logger.info(f"Found {len(csv_paths)} CSV files in {args.csv_dir}")

    results = {}
    for csv_path in csv_paths:
        status = migrate_file(conn, csv_path, args.dry_run)
        results[status] = results.get(status, 0) + 1

    logger.info(f"Migration summary: {results}")

    if not args.dry_run and results.get('error', 0) == 0:
        backfilled = storage.backfill_hourly_summary(conn)
        logger.info(f"Backfilled {backfilled} hourly_summary rows")
    elif results.get('error', 0):
        logger.warning("Skipping hourly_summary backfill because some files had errors - fix and re-run first")

    conn.close()


if __name__ == '__main__':
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest tests.test_migrate_csv_to_sqlite -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add _migrate_csv_to_sqlite.py tests/test_migrate_csv_to_sqlite.py
git commit -m "feat: add CSV-to-SQLite migration script

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 5: Wire `main.py`'s live polling loop to SQLite

**Files:**
- Modify: `main.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `storage.init_db_async`, `storage.insert_sample_async`,
  `storage.get_current_hour_start_sample_async`, `storage.current_hour_bounds`,
  `storage.DATA_DB_PATH` (Task 2), `sensors.sensor_columns()` (Task 1),
  `sensors.CalculatedValuesEvaluator.seed_hour_start` (Task 1)

This task changes the async inverter-polling loop, which requires a live
goodwe connection to exercise end-to-end - the existing codebase has no
tests for that integration either (`AsyncioThread`/`goodwe.connect` are
untested today). Automated coverage here is limited to what's already unit
tested in `storage.py`/`sensors.py`; this task is verified by manual
`--dry-run` execution instead (Step 5).

- [ ] **Step 1: Remove the CSV writer, add SQLite imports**

In `main.py`, remove the `import csv` line (no longer used anywhere in the
file). Add near the other local imports:

```python
import storage
```

(`sensors` is already imported per Task 1, Step 5.)

- [ ] **Step 2: Replace the CSV write path in `_get_inverter_data`**

Replace the body of `AsyncioThread._get_inverter_data` (the method that
currently opens `data-<timestamp>.csv` and writes rows via `csv.writer`)
with:

```python
async def _get_inverter_data(self):
    logger.info(f'Connecting to {self._inverter_address}')
    self._inverter = await goodwe.connect(self._inverter_address, family='ET', timeout=1, retries=60)
    logger.info(f'Connected to the inverter')
    self._db_conn = await storage.init_db_async(storage.DATA_DB_PATH, sensor_columns())
    try:
        await self._seed_hour_start_baseline()
        while True:
            inverter_runtime = await self._inverter.read_runtime_data()
            sensors_data = {sensor_id: str(inverter_runtime.get(sensor_id)) for sensor_id in SELECTED_SENSORS}
            sensors_data_with_calculated = sensors_data | self._calculated_values_evaluator.calculate_values(sensors_data)
            await storage.insert_sample_async(self._db_conn, sensors_data_with_calculated)
            announcer.announce(json.dumps(sensors_data_with_calculated))
            await asyncio.sleep(1)
            if self._should_stop.is_set():
                logger.info("Stopping the inverter communication routine")
                return
    finally:
        await self._db_conn.close()

async def _seed_hour_start_baseline(self):
    hour_start_epoch, hour_end_epoch = storage.current_hour_bounds(datetime.now())
    baseline = await storage.get_current_hour_start_sample_async(self._db_conn, hour_start_epoch, hour_end_epoch)
    self._calculated_values_evaluator.seed_hour_start(baseline)
```

Add `from sensors import sensor_columns` to the existing
`from sensors import SELECTED_SENSORS, CalculatedValuesEvaluator` import
line (Task 1, Step 5), making it:

```python
from sensors import SELECTED_SENSORS, CalculatedValuesEvaluator, sensor_columns
```

- [ ] **Step 3: Add the `_db_conn` attribute to `AsyncioThread`**

In the `AsyncioThread` class body, alongside the existing
`_asyncio_loop`/`_inverter` class attributes, add:

```python
_db_conn: Optional[aiosqlite.Connection] = None
```

Add `import aiosqlite` near the top of `main.py` with the other third-party
imports.

- [ ] **Step 4: Update `.gitignore`**

Add these lines near the existing `/data*.csv` entry:

```
/data.db
/data.db-wal
/data.db-shm
```

- [ ] **Step 5: Manual verification with `--dry-run`**

`--dry-run` mode skips the inverter connection entirely (see `main()`'s
existing `dry_run` handling), so this won't yet exercise the new DB writes -
that requires a real inverter connection. Confirm instead that the app still
starts cleanly with the code changes:

Run: `cd /Users/piotr.marcinczyk/Dev/goodwe_manager && timeout 5 python3 main.py --dry-run || true`
Expected: starts up, logs "Running in dry-run mode", no `ImportError` or
`NameError`/`AttributeError` on startup; exits cleanly after the timeout
(Ctrl-C/SIGTERM path, same as today).

- [ ] **Step 6: Run the full test suite**

Run: `python3 -m unittest discover -s tests -v`
Expected: all tests from Tasks 1-4 still pass.

- [ ] **Step 7: Commit**

```bash
git add main.py .gitignore
git commit -m "feat: write live telemetry to SQLite instead of CSV

Replaces the per-run data-*.csv writer with data.db (inverter_history),
and restores the current-hour baseline from the DB on startup instead of
always starting cold - fixes the 'current hour not persisted across
restarts' issue noted in the README.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** this plan implements the spec's `inverter_history`
  schema (normalized columns, per the spec's revised decision),
  `hourly_summary` + idempotent backfill, hour-start recovery (with the
  outage/DST-safe "first row of current hour, else None" logic), and the
  CSV migration script (manifest, verification, `--dry-run`, no deletion).
  Explicitly out of scope, per the phasing decision: RCE cache/prefetch
  thread, retention/pruning job, and the interactive history viewer -
  these are separate future plans against the same spec.
- **Correctness catches made while writing this plan (both reflected above
  and in the spec):** `timestamp_epoch` is computed in Python, not via a SQL
  `unixepoch()` generated column, because the inverter's timestamp is local
  time and `unixepoch()` assumes UTC. `pv_kwh` (from `e_day`) special-cases
  the midnight-crossing hour because `e_day` resets to `0` daily, unlike the
  other lifetime-cumulative counters — verified against real device data in
  `data-2024-08-12_09-08-38.csv`.
- **Type consistency:** `sensor_columns()` (Task 1) is consumed identically
  by `storage.build_ddl_statements`/`init_db_sync`/`init_db_async` (Task 2)
  and by `_migrate_csv_to_sqlite.migrate_file` (Task 4) - same
  `list[tuple[str, str]]` shape throughout. `CalculatedValuesEvaluator.seed_hour_start`
  (Task 1) matches the `Optional[dict]` return type of
  `storage.get_current_hour_start_sample_async` (Task 2) used in
  `main.py`'s `_seed_hour_start_baseline` (Task 5).
