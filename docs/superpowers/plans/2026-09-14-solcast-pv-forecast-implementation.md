# Solcast PV Forecast Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Solcast (c10/c50/c90) as a second PV production forecast source next to the existing
Meteosource scrape, with a shared fetch-history data model, a background prefetch thread, and an
updated `/forecast` chart/table.

**Architecture:** Two new leaf modules (`solcast.py` for the API client, `forecast_history.py` for
the SQLite snapshot history) sit alongside the existing `forecast.py`. A new background thread
(`forecast_prefetch.py`, modeled on `rce_prefetch.py`) fetches both sources on a shared 4-slot daily
schedule and writes every fetch into `forecast_history`. `main.py`'s `/forecast` routes are rewired
from the old 300s in-process cache to read from that history (with a live-fallback for Meteosource
only). `templates/forecast.html` gets a new chart layout, table columns, and a fetch-time dropdown;
its non-trivial per-request math is extracted into a new `static/js/forecast-calc.js`, tested the
same way `diagram-calc.js` already is.

**Tech Stack:** Python 3 (Flask, sqlite3, requests, BeautifulSoup - all already dependencies),
`unittest` (existing test convention, see `tests/test_rce_storage.py`/`tests/test_rce_prefetch.py`),
vanilla JS + Chart.js 4.4.4 (already loaded), Node's built-in `node:test` (see
`tests/js/diagram_calc.test.js`) for the new pure JS module.

**Spec:** `docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md`

## Global Constraints

- Solcast Hobbyist free tier: 10 API calls/day (UTC day), max 2 rooftop sites. This plan's prefetch
  schedule uses 4 slots x 2 calls = 8/day, never more.
- Prefetch wake times: `06:00`, `10:00`, `rce_prefetch.WAKE_TIME` (currently 14:15, imported not
  re-hardcoded), `18:00` - all local time.
- No automatic retry within a prefetch slot on failure - log a warning and let the next slot recover.
- Nothing is ever deleted from `forecast_snapshots` - past dates stay queryable forever.
- Never read, log, or print `SOLCAST_API_KEY`'s value anywhere (matches how `INVERTER_IP` etc.
  are already handled - env vars are asserted present, never echoed).
- Solcast's `pv_estimate10`/`pv_estimate`/`pv_estimate90` fields are believed to be **average kW
  over the period, not kWh** - unverified against a real Solcast response (no key exists yet). This
  plan makes that conversion an explicit, isolated constant (`solcast.PERIOD_KW_TO_KWH`) so it's a
  one-place fix if wrong, mirroring `forecast.py`'s existing `HOURLY_VALUE_TO_KWH` precedent for the
  same kind of unverified-unit situation.

---

## Task 1: `forecast_history.py` - fetch snapshot storage

**Files:**
- Create: `forecast_history.py`
- Test: `tests/test_forecast_history.py`

**Interfaces:**
- Produces: `init_db(path: Optional[str] = None) -> sqlite3.Connection`,
  `write_snapshot(conn, source: str, date: str, payload: dict, now: Optional[int] = None) -> int`,
  `get_snapshot(conn, source: str, date: str, fetched_at: int) -> Optional[dict]`,
  `get_fetch_times(conn, date: str) -> List[int]`,
  `get_latest_merged(conn, source: str, date: str) -> dict`,
  module constant `FORECAST_HISTORY_DB_PATH = 'forecast_history.db'`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_forecast_history.py`:

```python
import os
import tempfile
import unittest

import forecast_history


class ForecastHistoryTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)  # sqlite3.connect creates it fresh
        self.conn = forecast_history.init_db(self.db_path)

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_init_db_is_idempotent(self):
        forecast_history.init_db(self.db_path).close()

    def test_get_snapshot_missing_returns_none(self):
        self.assertIsNone(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 100))

    def test_write_then_get_snapshot_round_trips(self):
        payload = {'07:00': 0.5, '08:00': 0.9}
        fetched_at = forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', payload, now=1000)
        self.assertEqual(fetched_at, 1000)
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 1000), payload)

    def test_write_snapshot_with_different_payload_inserts_new_row(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.6}, now=2000)
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 1000), {'07:00': 0.5})
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 2000), {'07:00': 0.6})

    def test_write_snapshot_with_identical_payload_dedupes_by_bumping_valid_until(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        returned_fetched_at = forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=2000)
        # deduped into the original snapshot, not a new row at fetched_at=2000
        self.assertEqual(returned_fetched_at, 1000)
        self.assertIsNone(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 2000))
        row = self.conn.execute(
            "SELECT valid_until FROM forecast_snapshots WHERE source = 'meteosource' AND date = '2026-01-01' AND fetched_at = 1000"
        ).fetchone()
        self.assertEqual(row[0], 2000)

    def test_get_fetch_times_returns_distinct_times_across_both_sources_newest_first(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-01', {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}, now=2000)
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-01'), [2000, 1000])

    def test_get_fetch_times_only_for_requested_date(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-02', {'07:00': 0.5}, now=2000)
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-02'), [2000])

    def test_get_latest_merged_with_single_snapshot_returns_it(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5, '08:00': 0.9}, now=1000)
        self.assertEqual(
            forecast_history.get_latest_merged(self.conn, 'meteosource', '2026-01-01'),
            {'07:00': 0.5, '08:00': 0.9},
        )

    def test_get_latest_merged_prefers_newer_period_but_keeps_older_periods_the_newer_snapshot_lacks(self):
        # Simulates Solcast: an early snapshot covers periods a later,
        # forward-looking-only snapshot no longer includes.
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-01', {'07:00': 1.0, '08:00': 2.0}, now=1000)
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-01', {'08:00': 2.5, '09:00': 3.0}, now=2000)
        self.assertEqual(
            forecast_history.get_latest_merged(self.conn, 'solcast', '2026-01-01'),
            {'07:00': 1.0, '08:00': 2.5, '09:00': 3.0},
        )

    def test_get_latest_merged_with_no_snapshots_returns_empty_dict(self):
        self.assertEqual(forecast_history.get_latest_merged(self.conn, 'solcast', '2026-01-01'), {})


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `venv/bin/python3 -m pytest tests/test_forecast_history.py -v` (or
`venv/bin/python3 -m unittest tests.test_forecast_history -v`)
Expected: FAIL/ERROR - `ModuleNotFoundError: No module named 'forecast_history'`

- [ ] **Step 3: Write `forecast_history.py`**

```python
"""
forecast_history.py
SQLite-backed fetch history for PV production forecasts (forecast_history.db)
- one row per distinct forecast snapshot per (source, date), instead of a
single overwritten cache entry. See
docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md section 4
for the design rationale (why history, not a TTL cache; why the merge in
get_latest_merged is needed).
"""
import json
import sqlite3
import time
from typing import Dict, List, Optional

FORECAST_HISTORY_DB_PATH = 'forecast_history.db'


def init_db(path: Optional[str] = None) -> sqlite3.Connection:
    """Opens (creating if needed) the forecast history DB. `path` defaults
    to FORECAST_HISTORY_DB_PATH, looked up inside the function body (not as
    a bound default parameter) so tests can monkeypatch the module-level
    path and have default-path callers pick it up - same convention as
    rce_storage.init_db.
    """
    conn = sqlite3.connect(path or FORECAST_HISTORY_DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecast_snapshots (
            source TEXT NOT NULL,
            date TEXT NOT NULL,
            fetched_at INTEGER NOT NULL,
            valid_until INTEGER NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (source, date, fetched_at)
        )
    """)
    conn.commit()
    return conn


def _latest_snapshot_row(conn: sqlite3.Connection, source: str, date: str):
    return conn.execute(
        "SELECT fetched_at, payload FROM forecast_snapshots "
        "WHERE source = ? AND date = ? ORDER BY fetched_at DESC LIMIT 1",
        (source, date),
    ).fetchone()


def write_snapshot(conn: sqlite3.Connection, source: str, date: str, payload: Dict, now: Optional[int] = None) -> int:
    """Writes a forecast snapshot for (source, date). Dedupes against the
    immediately-prior snapshot for that (source, date): if `payload` is
    identical (compared as sorted-key JSON, so key order never matters) to
    the latest existing snapshot, this just bumps that snapshot's
    valid_until to `now` instead of inserting a duplicate row. Returns the
    fetched_at that now represents `payload` - either the new one, or the
    deduped-into existing one.
    """
    now = now if now is not None else int(time.time())
    payload_json = json.dumps(payload, sort_keys=True)
    existing = _latest_snapshot_row(conn, source, date)
    if existing is not None and existing[1] == payload_json:
        fetched_at = existing[0]
        conn.execute(
            "UPDATE forecast_snapshots SET valid_until = ? WHERE source = ? AND date = ? AND fetched_at = ?",
            (now, source, date, fetched_at),
        )
        conn.commit()
        return fetched_at
    conn.execute(
        "INSERT INTO forecast_snapshots (source, date, fetched_at, valid_until, payload) VALUES (?, ?, ?, ?, ?)",
        (source, date, now, now, payload_json),
    )
    conn.commit()
    return now


def get_snapshot(conn: sqlite3.Connection, source: str, date: str, fetched_at: int) -> Optional[Dict]:
    """Returns exactly what was fetched at `fetched_at` - gaps included.
    Backs the fetch-time dropdown's "view a specific past fetch" mode."""
    row = conn.execute(
        "SELECT payload FROM forecast_snapshots WHERE source = ? AND date = ? AND fetched_at = ?",
        (source, date, fetched_at),
    ).fetchone()
    return json.loads(row[0]) if row else None


def get_fetch_times(conn: sqlite3.Connection, date: str) -> List[int]:
    """Distinct fetched_at values across BOTH sources for `date`, newest
    first - backs the single shared fetch-time dropdown (both sources fetch
    on the same schedule, so in practice they share timestamps)."""
    rows = conn.execute(
        "SELECT DISTINCT fetched_at FROM forecast_snapshots WHERE date = ? ORDER BY fetched_at DESC",
        (date,),
    ).fetchall()
    return [r[0] for r in rows]


def get_latest_merged(conn: sqlite3.Connection, source: str, date: str) -> Dict:
    """The default ("latest") read: merges every snapshot for (source,
    date) period-by-period, oldest to newest, so a newer snapshot's periods
    overwrite an older snapshot's - but a period only the older snapshot
    ever covered survives. This is what makes "today" work for Solcast:
    each call is forward-looking from its own call time, so no single
    snapshot alone covers a full day once part of it is in the past
    relative to that snapshot's fetch time. Returns {} if there are no
    snapshots at all for (source, date).
    """
    rows = conn.execute(
        "SELECT payload FROM forecast_snapshots WHERE source = ? AND date = ? ORDER BY fetched_at ASC",
        (source, date),
    ).fetchall()
    merged: Dict = {}
    for (payload_json,) in rows:
        merged.update(json.loads(payload_json))
    return merged
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_forecast_history.py -v`
Expected: PASS (10 tests)

- [ ] **Step 5: Commit**

```bash
git add forecast_history.py tests/test_forecast_history.py
git commit -m "Add forecast_history.py: shared fetch-snapshot storage for PV forecasts

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 2: `forecast.py` - combined east+west helper

**Files:**
- Modify: `forecast.py`
- Test: `tests/test_forecast.py` (new)

**Interfaces:**
- Consumes: nothing new from Task 1.
- Produces: `ORIENTATIONS = (90, 270)` module constant,
  `fetch_pv_production_forecast_combined_hourly_kwh(date: str) -> Dict[str, float]` (keys `"HH:00"`,
  already summed east+west) - this is what both `forecast_prefetch.py` (Task 4) and `main.py`'s
  live-fallback path (Task 5) call; neither needs to know about individual orientations anymore.

- [ ] **Step 1: Write the failing test**

Create `tests/test_forecast.py`:

```python
import unittest
from unittest.mock import patch

import forecast


class FetchCombinedHourlyKwhTest(unittest.TestCase):
    @patch('forecast.fetch_pv_production_forecast_hourly_kwh')
    def test_sums_both_orientations_by_local_hour(self, mock_fetch):
        # epoch millis for 2026-01-01 07:00 and 08:00, read via
        # utcfromtimestamp per forecast.py's own documented convention
        def fake_fetch(date, orientation):
            base = {90: [(1767243600000, 0.5), (1767247200000, 0.9)],
                    270: [(1767243600000, 0.2), (1767247200000, 0.3)]}
            return base[orientation]
        mock_fetch.side_effect = fake_fetch

        result = forecast.fetch_pv_production_forecast_combined_hourly_kwh('2026-01-01')

        self.assertEqual(result, {'07:00': 0.7, '08:00': 1.2})
        self.assertEqual(mock_fetch.call_args_list, [
            unittest.mock.call('2026-01-01', 90),
            unittest.mock.call('2026-01-01', 270),
        ])

    @patch('forecast.fetch_pv_production_forecast_hourly_kwh')
    def test_missing_orientation_hour_treated_as_zero(self, mock_fetch):
        def fake_fetch(date, orientation):
            return [(1767243600000, 0.5)] if orientation == 90 else []
        mock_fetch.side_effect = fake_fetch

        result = forecast.fetch_pv_production_forecast_combined_hourly_kwh('2026-01-01')

        self.assertEqual(result, {'07:00': 0.5})


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `venv/bin/python3 -m pytest tests/test_forecast.py -v`
Expected: FAIL - `AttributeError: module 'forecast' has no attribute 'fetch_pv_production_forecast_combined_hourly_kwh'`

- [ ] **Step 3: Implement it in `forecast.py`**

Add near the top, after the `HOURLY_VALUE_TO_KWH` constant:

```python
# The app's east+west PV string assumption, centralized here (forecast.py
# is the module that actually talks to Meteosource per-orientation) rather
# than in main.py, so both the scheduled prefetch (forecast_prefetch.py)
# and main.py's on-demand fallback fetch share one implementation instead
# of duplicating the east+west sum.
ORIENTATIONS = (90, 270)
```

Add at the end, after `fetch_pv_production_forecast_hourly_kwh`:

```python
def fetch_pv_production_forecast_combined_hourly_kwh(date):
    """Same source as fetch_pv_production_forecast_hourly_kwh, summed
    across ORIENTATIONS (east+west) into one series. Returns {"HH:00": kwh}
    - a plain dict keyed by local hour label, ready to hand to
    forecast_history.write_snapshot. An hour missing from one orientation's
    series (shouldn't normally happen - see
    _fetch_pv_production_forecast_local_day_raw) is treated as 0 for that
    orientation, same as main.py's old per-request merge already did."""
    by_hour = {}
    for orientation in ORIENTATIONS:
        for timestamp_ms, kwh in fetch_pv_production_forecast_hourly_kwh(date, orientation):
            hour_label = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%H:%M')
            by_hour[hour_label] = round(by_hour.get(hour_label, 0) + kwh, 2)
    return by_hour
```

Update `main()`'s existing `orientations = (90, 270)` local variable to reference the new constant
instead (minor cleanup while touching this exact line):

```python
    orientations = ORIENTATIONS
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `venv/bin/python3 -m pytest tests/test_forecast.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add forecast.py tests/test_forecast.py
git commit -m "forecast.py: add fetch_pv_production_forecast_combined_hourly_kwh

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 3: `solcast.py` - Solcast API client

**Files:**
- Create: `solcast.py`
- Test: `tests/test_solcast.py`

**Interfaces:**
- Produces: `fetch_solcast_forecast_30min(resource_id: str) -> Dict[str, Dict[str, Dict[str, float]]]`
  (shape: `{"YYYY-MM-DD": {"HH:MM": {"c10": kwh, "c50": kwh, "c90": kwh}}}`),
  `sum_sites(*site_forecasts: Dict) -> Dict` (same nested shape, summed), module constant
  `PERIOD_KW_TO_KWH = 0.5`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_solcast.py`:

```python
import os
import unittest
from unittest.mock import patch, MagicMock

import solcast


def _fake_response(forecasts):
    resp = MagicMock()
    resp.json.return_value = {'forecasts': forecasts}
    resp.raise_for_status.return_value = None
    return resp


class FetchSolcastForecast30MinTest(unittest.TestCase):
    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_parses_periods_into_local_date_and_hhmm_buckets(self, mock_get):
        # period_end is the period's END, in UTC; period covers
        # [period_end - 30min, period_end). CEST (UTC+2) in September, so
        # 2026-09-14T12:00:00Z -> local 14:00, period start local 13:30.
        mock_get.return_value = _fake_response([
            {'period_end': '2026-09-14T12:00:00.0000000Z', 'pv_estimate10': 1.0, 'pv_estimate': 2.0, 'pv_estimate90': 3.0},
        ])

        result = solcast.fetch_solcast_forecast_30min('site-123')

        self.assertEqual(result, {'2026-09-14': {'13:30': {'c10': 0.5, 'c50': 1.0, 'c90': 1.5}}})

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_uses_api_key_from_env_and_pt30m_period(self, mock_get):
        mock_get.return_value = _fake_response([])
        solcast.fetch_solcast_forecast_30min('site-123')
        args, kwargs = mock_get.call_args
        self.assertIn('site-123', args[0])
        self.assertEqual(kwargs['params']['api_key'], 'test-key')
        self.assertEqual(kwargs['params']['period'], 'PT30M')

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_api_key_raises(self):
        with self.assertRaises(AssertionError):
            solcast.fetch_solcast_forecast_30min('site-123')

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_periods_spanning_a_local_midnight_land_in_the_right_date(self, mock_get):
        # 2026-09-14T22:00:00Z -> local 2026-09-15 00:00 (CEST, UTC+2);
        # period start local 2026-09-14 23:30.
        mock_get.return_value = _fake_response([
            {'period_end': '2026-09-14T22:00:00.0000000Z', 'pv_estimate10': 0, 'pv_estimate': 0, 'pv_estimate90': 0},
        ])
        result = solcast.fetch_solcast_forecast_30min('site-123')
        self.assertEqual(list(result.keys()), ['2026-09-14'])
        self.assertEqual(list(result['2026-09-14'].keys()), ['23:30'])


class SumSitesTest(unittest.TestCase):
    def test_sums_two_sites_matching_dates_and_periods(self):
        east = {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        west = {'2026-09-14': {'07:00': {'c10': 0.05, 'c50': 0.1, 'c90': 0.2}}}
        result = solcast.sum_sites(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': {'c10': 0.15, 'c50': 0.3, 'c90': 0.5}}})

    def test_period_present_in_only_one_site_is_treated_as_zero_for_the_other(self):
        east = {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        west = {'2026-09-14': {}}
        result = solcast.sum_sites(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}})

    def test_date_present_in_only_one_site_is_kept(self):
        east = {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        west = {'2026-09-15': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        result = solcast.sum_sites(east, west)
        self.assertEqual(set(result.keys()), {'2026-09-14', '2026-09-15'})


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `venv/bin/python3 -m pytest tests/test_solcast.py -v`
Expected: FAIL - `ModuleNotFoundError: No module named 'solcast'`

- [ ] **Step 3: Write `solcast.py`**

```python
"""
solcast.py
Fetches PV production forecasts from Solcast's free Hobbyist API
(https://docs.solcast.com.au/). Free tier: 10 calls/day, max 2 rooftop
sites, no historical data - see
docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md.
"""
import os
from datetime import datetime, timedelta
from typing import Dict

import requests

SOLCAST_API_BASE = 'https://api.solcast.com.au'

# Solcast's rooftop_sites/forecasts pv_estimate*/pv_estimate10/pv_estimate90
# fields are documented as average power (kW) over the period, not energy
# (kWh) - this converts a period's kW estimate to that period's kWh
# (kW * period-hours). NOT yet verified against a real account's actual
# response (this repo has no Solcast key checked in). If a real response
# turns out to already be in kWh, set this to 1.0 - same "one constant to
# fix" precedent as forecast.py's HOURLY_VALUE_TO_KWH for the analogous
# Meteosource uncertainty.
PERIOD_KW_TO_KWH = 0.5  # 30-minute period = 0.5 hours


def fetch_solcast_forecast_30min(resource_id: str) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Calls GET /rooftop_sites/{resource_id}/forecasts at Solcast's native
    30-minute period. Returns {"YYYY-MM-DD": {"HH:MM": {"c10": kwh, "c50":
    kwh, "c90": kwh}}} covering whatever calendar dates Solcast's rolling,
    forward-looking-from-call-time forecast returned - there is no
    date/count guarantee here, callers just use whatever comes back.
    HH:MM is each period's *start* time, in local time (period_end from the
    API, a real UTC ISO8601 timestamp - unlike Meteosource's quirky epoch
    field - minus 30 minutes, converted via datetime.astimezone()).
    """
    api_key = os.environ.get('SOLCAST_API_KEY')
    assert api_key, "SOLCAST_API_KEY environment variable not set"
    response = requests.get(
        f"{SOLCAST_API_BASE}/rooftop_sites/{resource_id}/forecasts",
        params={'format': 'json', 'period': 'PT30M', 'api_key': api_key},
    )
    response.raise_for_status()
    data = response.json()

    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    for period in data.get('forecasts', []):
        period_end_utc = datetime.fromisoformat(period['period_end'].replace('Z', '+00:00'))
        period_start_local = (period_end_utc - timedelta(minutes=30)).astimezone()
        date_str = period_start_local.strftime('%Y-%m-%d')
        hhmm = period_start_local.strftime('%H:%M')
        result.setdefault(date_str, {})[hhmm] = {
            'c10': round(period['pv_estimate10'] * PERIOD_KW_TO_KWH, 2),
            'c50': round(period['pv_estimate'] * PERIOD_KW_TO_KWH, 2),
            'c90': round(period['pv_estimate90'] * PERIOD_KW_TO_KWH, 2),
        }
    return result


def sum_sites(*site_forecasts: Dict[str, Dict[str, Dict[str, float]]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Sums 2+ fetch_solcast_forecast_30min() results (east+west) into one
    combined forecast, same nested shape. A date/period present in only
    some sites sums whichever sites have it, treating a missing site's
    period as 0kWh - mirrors forecast.py's own missing-orientation handling."""
    combined: Dict[str, Dict[str, Dict[str, float]]] = {}
    for site in site_forecasts:
        for date_str, periods in site.items():
            date_bucket = combined.setdefault(date_str, {})
            for hhmm, values in periods.items():
                period_bucket = date_bucket.setdefault(hhmm, {'c10': 0.0, 'c50': 0.0, 'c90': 0.0})
                for key in ('c10', 'c50', 'c90'):
                    period_bucket[key] = round(period_bucket[key] + values[key], 2)
    return combined
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_solcast.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add solcast.py tests/test_solcast.py
git commit -m "Add solcast.py: Solcast rooftop forecast API client

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 4: `forecast_prefetch.py` - shared background prefetch thread

**Files:**
- Create: `forecast_prefetch.py`
- Test: `tests/test_forecast_prefetch.py`

**Interfaces:**
- Consumes: `forecast.ORIENTATIONS`, `forecast.fetch_pv_production_forecast_combined_hourly_kwh`
  (Task 2); `forecast_history.init_db`, `forecast_history.write_snapshot` (Task 1);
  `solcast.fetch_solcast_forecast_30min`, `solcast.sum_sites` (Task 3); `rce_prefetch.WAKE_TIME`,
  `rce_prefetch.seconds_until` (existing).
- Produces: `WAKE_TIMES` (4-tuple of `datetime.time`), `next_wake_time(now, wake_times=WAKE_TIMES) ->
  datetime`, `fetch_and_store_meteosource(conn, date_yyyymmdd: str) -> None`,
  `fetch_and_store_solcast(conn) -> None`, class `ForecastPrefetchThread` with `.start()`/`.finish()`
  (same shape as `RcePrefetchThread`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_forecast_prefetch.py`:

```python
import os
import tempfile
import unittest
from datetime import datetime, time as dtime
from unittest.mock import patch

import forecast_history
import forecast_prefetch


class NextWakeTimeTest(unittest.TestCase):
    def test_picks_the_soonest_slot_later_today(self):
        now = datetime(2026, 1, 1, 7, 0)
        result = forecast_prefetch.next_wake_time(now, wake_times=(dtime(6, 0), dtime(10, 0), dtime(14, 15), dtime(18, 0)))
        self.assertEqual(result, datetime(2026, 1, 1, 10, 0))

    def test_rolls_to_tomorrows_first_slot_after_the_last_one_today(self):
        now = datetime(2026, 1, 1, 19, 0)
        result = forecast_prefetch.next_wake_time(now, wake_times=(dtime(6, 0), dtime(10, 0), dtime(14, 15), dtime(18, 0)))
        self.assertEqual(result, datetime(2026, 1, 2, 6, 0))

    def test_wake_times_includes_rce_prefetch_wake_time(self):
        import rce_prefetch
        self.assertIn(rce_prefetch.WAKE_TIME, forecast_prefetch.WAKE_TIMES)


class FetchAndStoreTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = forecast_history.init_db(self.db_path)

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    @patch('forecast_prefetch.forecast.fetch_pv_production_forecast_combined_hourly_kwh')
    def test_fetch_and_store_meteosource_writes_a_snapshot(self, mock_fetch):
        mock_fetch.return_value = {'07:00': 1.5}
        forecast_prefetch.fetch_and_store_meteosource(self.conn, '2026-01-01')
        mock_fetch.assert_called_once_with('2026-01-01')
        times = forecast_history.get_fetch_times(self.conn, '2026-01-01')
        self.assertEqual(len(times), 1)
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', times[0]), {'07:00': 1.5})

    @patch.dict(os.environ, {'SOLCAST_SITE_EAST_ID': 'east-1', 'SOLCAST_SITE_WEST_ID': 'west-1'})
    @patch('forecast_prefetch.solcast.fetch_solcast_forecast_30min')
    def test_fetch_and_store_solcast_sums_sites_and_writes_a_snapshot_per_date(self, mock_fetch):
        def fake_fetch(resource_id):
            if resource_id == 'east-1':
                return {'2026-01-01': {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}}
            return {'2026-01-01': {'07:00': {'c10': 0.5, 'c50': 1.0, 'c90': 1.5}}}
        mock_fetch.side_effect = fake_fetch

        forecast_prefetch.fetch_and_store_solcast(self.conn)

        times = forecast_history.get_fetch_times(self.conn, '2026-01-01')
        self.assertEqual(len(times), 1)
        self.assertEqual(
            forecast_history.get_snapshot(self.conn, 'solcast', '2026-01-01', times[0]),
            {'07:00': {'c10': 1.5, 'c50': 3.0, 'c90': 4.5}},
        )


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `venv/bin/python3 -m pytest tests/test_forecast_prefetch.py -v`
Expected: FAIL - `ModuleNotFoundError: No module named 'forecast_prefetch'`

- [ ] **Step 3: Write `forecast_prefetch.py`**

```python
"""
forecast_prefetch.py
Background thread that refreshes both PV forecast sources (Meteosource,
Solcast) on a shared schedule, writing every fetch into forecast_history's
snapshot table - see
docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md section 5.

Wakes at 4 fixed local times/day (daylight-biased, one aligned with
rce_prefetch.WAKE_TIME so a future EMS can correlate same-moment PV and
electricity-price forecasts - no EMS logic lives here, this only fetches
and stores). 2 Solcast calls/slot x 4 slots = 8/day, under Solcast's
10-calls/day free-tier cap with headroom to spare. On any fetch failure,
logs a warning and moves on rather than retrying within the same slot -
retrying would spend quota meant for the next slot, and the read path
(forecast_history.get_latest_merged) already tolerates a missing/stale
slot gracefully, same "safe to fail" framing as rce_prefetch.py.
"""
import logging
import os
import threading
from datetime import datetime, time as dtime, timedelta

import forecast
import forecast_history
import rce_prefetch
import solcast

logger = logging.getLogger(__name__)

WAKE_TIMES = (dtime(6, 0), dtime(10, 0), rce_prefetch.WAKE_TIME, dtime(18, 0))


def next_wake_time(now: datetime, wake_times=WAKE_TIMES) -> datetime:
    """The soonest of `wake_times` (today or tomorrow) strictly after `now`."""
    seconds_to_each = [rce_prefetch.seconds_until(now, t) for t in wake_times]
    return now + timedelta(seconds=min(seconds_to_each))


def fetch_and_store_meteosource(conn, date_yyyymmdd: str) -> None:
    payload = forecast.fetch_pv_production_forecast_combined_hourly_kwh(date_yyyymmdd)
    forecast_history.write_snapshot(conn, 'meteosource', date_yyyymmdd, payload)


def fetch_and_store_solcast(conn) -> None:
    east = solcast.fetch_solcast_forecast_30min(os.environ['SOLCAST_SITE_EAST_ID'])
    west = solcast.fetch_solcast_forecast_30min(os.environ['SOLCAST_SITE_WEST_ID'])
    combined_by_date = solcast.sum_sites(east, west)
    for date_str, payload in combined_by_date.items():
        forecast_history.write_snapshot(conn, 'solcast', date_str, payload)


class ForecastPrefetchThread(threading.Thread):
    def __init__(self, db_path=None):
        super().__init__(name='ForecastPrefetchThread', daemon=True)
        self._should_stop = threading.Event()
        self._db_path = db_path

    def run(self):
        conn = forecast_history.init_db(self._db_path)
        try:
            while not self._should_stop.is_set():
                now = datetime.now()
                wait_seconds = (next_wake_time(now) - now).total_seconds()
                if self._should_stop.wait(wait_seconds):
                    return
                today = datetime.now().strftime('%Y-%m-%d')
                try:
                    fetch_and_store_meteosource(conn, today)
                    logger.info(f"Prefetched Meteosource forecast for {today}")
                except Exception as e:
                    logger.warning(f"Meteosource prefetch failed: {e}")
                try:
                    fetch_and_store_solcast(conn)
                    logger.info("Prefetched Solcast forecast")
                except Exception as e:
                    logger.warning(f"Solcast prefetch failed: {e}")
        finally:
            conn.close()

    def finish(self):
        """Called from another thread to stop the prefetch thread, mirroring
        RcePrefetchThread.finish()'s shape."""
        logger.info("Finishing forecast prefetch thread...")
        self._should_stop.set()
        self.join(timeout=5)
        if self.is_alive():
            logger.warning("Forecast prefetch thread did not stop within timeout")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_forecast_prefetch.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add forecast_prefetch.py tests/test_forecast_prefetch.py
git commit -m "Add forecast_prefetch.py: shared 4-slot prefetch thread for both forecast sources

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 5: `main.py` wiring + `.env.example`

**Files:**
- Modify: `main.py` (forecast section: imports near line 28-35, `PV_ORIENTATIONS`/`ForecastData`
  around line 48-60, `_data_db_connection` area around line 64 for the new connection helper, the
  forecast cache/routes block at lines 519-639, the thread start/stop block around lines 306/755/769)
- Modify: `.env.example`
- Test: `tests/test_main_forecast_routes.py` (new)

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: Flask routes `GET /forecast` and `GET /forecast/hourly.json` returning the JSON shape
  documented in Step 3 below - this is what `templates/forecast.html`/`forecast-calc.js` (Tasks 6-7)
  consume.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_main_forecast_routes.py`. This uses Flask's test client against `main.app`,
monkeypatching `forecast_history` functions so no real DB/network is touched:

```python
import unittest
from unittest.mock import patch

import main


class ForecastHourlyJsonRouteTest(unittest.TestCase):
    def setUp(self):
        main.app.testing = True
        self.client = main.app.test_client()

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[2000, 1000])
    @patch('main.forecast_history.get_latest_merged')
    def test_returns_meteosource_solcast_and_fetch_times(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        def fake_merged(conn, source, date):
            if source == 'meteosource':
                return {'07:00': 1.5}
            return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
        mock_merged.side_effect = fake_merged

        resp = self.client.get('/forecast/hourly.json?date=2026-01-01')

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['meteosource']['hours'], [{'time': '07:00', 'kwh': 1.5}])
        self.assertTrue(data['solcast']['available'])
        self.assertEqual(data['solcast']['periods'], [{'time': '07:00', 'c10': 1.0, 'c50': 2.0, 'c90': 3.0}])
        self.assertEqual(data['fetch_times'], [2000, 1000])

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[])
    @patch('main.forecast_history.get_latest_merged', return_value={})
    def test_solcast_unavailable_when_no_snapshot_exists(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        resp = self.client.get('/forecast/hourly.json?date=2099-01-01')
        data = resp.get_json()
        self.assertFalse(data['solcast']['available'])
        self.assertEqual(data['solcast']['periods'], [])

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[1000])
    @patch('main.forecast_history.get_snapshot')
    def test_specific_fetched_at_uses_get_snapshot_not_merged(self, mock_snapshot, mock_fetch_times, mock_partial, mock_actual):
        # source-aware, not one shared return value: the route's Solcast
        # branch does `**v` per period, which would blow up on a plain
        # {"07:00": 1.5} float value if both sources returned the same
        # Meteosource-shaped payload.
        def fake_snapshot(conn, source, date, fetched_at):
            return {'07:00': 1.5} if source == 'meteosource' else {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
        mock_snapshot.side_effect = fake_snapshot

        resp = self.client.get('/forecast/hourly.json?date=2026-01-01&fetched_at=1000')
        data = resp.get_json()
        self.assertEqual(data['meteosource']['hours'], [{'time': '07:00', 'kwh': 1.5}])
        self.assertEqual(data['solcast']['periods'], [{'time': '07:00', 'c10': 1.0, 'c50': 2.0, 'c90': 3.0}])
        mock_snapshot.assert_any_call(unittest.mock.ANY, 'meteosource', '2026-01-01', 1000)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `venv/bin/python3 -m pytest tests/test_main_forecast_routes.py -v`
Expected: FAIL (route doesn't return this shape yet / `forecast_history` not imported in `main.py`)

- [ ] **Step 3: Rewire `main.py`**

Add imports near the existing `import forecast` / `from rce_prefetch import RcePrefetchThread`
(around line 28-34):

```python
import forecast_history
from forecast_prefetch import ForecastPrefetchThread
```

(`solcast` itself is not imported here - `main.py` only ever reads already-fetched data back out of
`forecast_history`; every direct Solcast API call lives in `forecast_prefetch.py`, which is the only
module that imports `solcast`.)

Replace `PV_ORIENTATIONS = (90, 270)` (line 48) - it's now `forecast.ORIENTATIONS`, and the
`ForecastData` namedtuple (line 60) is no longer needed in its old two-angle shape:

```python
# (PV_ORIENTATIONS removed - forecast.ORIENTATIONS from Task 2 is now the
# single source of truth for the east+west assumption.)
```

Remove the `ForecastData = namedtuple(...)` line entirely (its only use was the old
`/forecast` route, rewritten below).

Add a connection-helper for `forecast_history.db`, next to the existing `_data_db_connection()`
(around line 64), following that same `@contextlib.contextmanager` shape:

```python
@contextlib.contextmanager
def _forecast_history_connection():
    """Short-lived, synchronous connection to forecast_history.db, same
    connect/use/close shape as _data_db_connection - see that function's
    docstring."""
    conn = forecast_history.init_db()
    try:
        yield conn
    finally:
        conn.close()
```

Start/stop the new thread next to the existing `rce_prefetch_thread` (line 306, and the
start/finally block around lines 755/769):

```python
forecast_prefetch_thread = ForecastPrefetchThread()
```

```python
    asyncio_thread.start()
    rce_prefetch_thread.start()
    forecast_prefetch_thread.start()
```

```python
        asyncio_thread.finish()
        rce_prefetch_thread.finish()
        forecast_prefetch_thread.finish()
```

Replace the entire forecast cache/routes block (`_FORECAST_CACHE_TTL_SECONDS` /
`_forecast_cache` / `_forecast_cache_lock` / `_get_hourly_forecast_cached` /
`get_forecast` / `get_forecast_hourly_json`, lines 519-639 in the current file - keep
`_get_actual_hourly_pv_kwh` and `_get_actual_pv_kwh_so_far_this_hour`, which are unchanged) with:

```python
def _read_forecast_payload(conn, source, date_yyyymmdd, fetched_at):
    """Reads a forecast payload for `source`/`date_yyyymmdd`: the merged
    "latest" view (forecast_history.get_latest_merged) if `fetched_at` is
    None, else that exact snapshot (gaps included). Falls back to a live
    Meteosource fetch - the only source that can answer an arbitrary date
    on demand, see forecast_prefetch.py's docstring and the design spec
    section 7 - when there's nothing in history yet for that date, and
    persists the result so future reads for that date hit history too.
    """
    if fetched_at is not None:
        return forecast_history.get_snapshot(conn, source, date_yyyymmdd, fetched_at) or {}
    payload = forecast_history.get_latest_merged(conn, source, date_yyyymmdd)
    if not payload and source == 'meteosource':
        payload = forecast.fetch_pv_production_forecast_combined_hourly_kwh(date_yyyymmdd)
        forecast_history.write_snapshot(conn, source, date_yyyymmdd, payload)
    return payload


def _solcast_daily_totals(periods_dict):
    """periods_dict: {"HH:MM": {"c10":.., "c50":.., "c90":..}}. Returns
    (c10_total, c50_total, c90_total) day sums, rounded to 1 decimal to
    match the existing summary line's precision."""
    c10 = sum(v['c10'] for v in periods_dict.values())
    c50 = sum(v['c50'] for v in periods_dict.values())
    c90 = sum(v['c90'] for v in periods_dict.values())
    return round(c10, 1), round(c50, 1), round(c90, 1)


@app.get('/forecast')
def get_forecast():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    fetched_at = request.args.get('fetched_at', type=int)
    logger.debug(f"Fetching forecast for {date_yyyymmdd} (fetched_at={fetched_at})")

    with _forecast_history_connection() as conn:
        meteosource = _read_forecast_payload(conn, 'meteosource', date_yyyymmdd, fetched_at)
        solcast_periods = _read_forecast_payload(conn, 'solcast', date_yyyymmdd, fetched_at)

    meteosource_total = round(sum(meteosource.values()), 1)
    c10_total, c50_total, c90_total = _solcast_daily_totals(solcast_periods)
    summary = f"Meteosource: {meteosource_total} kWh"
    if solcast_periods:
        summary += f" · Solcast: {c50_total} ({c10_total}-{c90_total}) kWh"
    return flask.render_template('forecast.html', date=date_yyyymmdd, fetched_at=fetched_at, summary=summary)


@app.get('/forecast/hourly.json')
def get_forecast_hourly_json():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    fetched_at = request.args.get('fetched_at', type=int)
    logger.debug(f"Fetching hourly forecast JSON for {date_yyyymmdd} (fetched_at={fetched_at})")

    with _forecast_history_connection() as conn:
        meteosource = _read_forecast_payload(conn, 'meteosource', date_yyyymmdd, fetched_at)
        solcast_periods = _read_forecast_payload(conn, 'solcast', date_yyyymmdd, fetched_at)
        fetch_times = forecast_history.get_fetch_times(conn, date_yyyymmdd)

    actual_by_hour = _get_actual_hourly_pv_kwh(date_yyyymmdd)
    now = datetime.now()
    is_today = date_yyyymmdd == now.strftime('%Y-%m-%d')
    current_hour = now.strftime('%H:00') if is_today else None
    partial_kwh = _get_actual_pv_kwh_so_far_this_hour(now) if is_today else None

    return flask.jsonify({
        'meteosource': {
            'hours': [{'time': t, 'kwh': kwh} for t, kwh in sorted(meteosource.items())],
        },
        'solcast': {
            'available': bool(solcast_periods),
            'periods': [{'time': t, **v} for t, v in sorted(solcast_periods.items())],
        },
        'actual': {
            'hours': [{'time': t, 'kwh': kwh} for t, kwh in sorted(actual_by_hour.items())],
            'current_hour': current_hour,
            'current_hour_actual_partial_kwh': partial_kwh,
        },
        'fetch_times': fetch_times,
        'selected_fetched_at': fetched_at,
    })
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_main_forecast_routes.py -v`
Expected: PASS (3 tests)

Then run the **full** test suite to check nothing else broke (the old `ForecastData`/
`PV_ORIENTATIONS` removal could affect other tests referencing them):

Run: `venv/bin/python3 -m pytest tests/ -v --ignore=tests/js`
Expected: PASS, no failures. If `test_main_backfill.py` or any other test imports `PV_ORIENTATIONS`
or `ForecastData` from `main`, update that import to use `forecast.ORIENTATIONS` instead.

- [ ] **Step 5: Update `.env.example`**

```
SOLCAST_API_KEY=
SOLCAST_SITE_EAST_ID=
SOLCAST_SITE_WEST_ID=
```

(Appended after the existing `BACKUP_ACTIVE_THRESHOLD_W=35` line.)

- [ ] **Step 6: Commit**

```bash
git add main.py .env.example tests/test_main_forecast_routes.py
git commit -m "main.py: read /forecast from forecast_history instead of the old TTL cache

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 6: `static/js/forecast-calc.js` - pure chart/table math

**Files:**
- Create: `static/js/forecast-calc.js`
- Test: `tests/js/forecast_calc.test.js`

**Interfaces:**
- Produces (all pure functions, no DOM/Chart.js dependency):
  `solcastPeriodToX(hhmm: string) -> number`,
  `aggregateSolcastHourly(periods: {time, c10, c50, c90}[]) -> {time, c10, c50, c90}[]`.
  Both are what `templates/forecast.html` (Task 7) uses via a plain `<script src=...>` (the file must
  work as a browser global, same as `chart-theme.js`/`table.js` - see Step 3's export shape). The
  daily summary line stays server-rendered in `main.py` (Task 5) - matching how the old
  `forecast.html` already rendered its summary purely server-side - so there's no client-side
  summary-formatting function here to keep in sync with it.

- [ ] **Step 1: Write the failing tests**

Create `tests/js/forecast_calc.test.js`:

```javascript
const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  solcastPeriodToX, aggregateSolcastHourly,
} = require('../../static/js/forecast-calc.js');

test('solcastPeriodToX converts HH:MM to a fractional hour', () => {
  assert.equal(solcastPeriodToX('14:00'), 14);
  assert.equal(solcastPeriodToX('14:30'), 14.5);
});

test('aggregateSolcastHourly sums each hour\'s two 30-minute periods', () => {
  const periods = [
    { time: '07:00', c10: 1.0, c50: 2.0, c90: 3.0 },
    { time: '07:30', c10: 0.5, c50: 1.0, c90: 1.5 },
    { time: '08:00', c10: 2.0, c50: 3.0, c90: 4.0 },
  ];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result, [
    { time: '07:00', c10: 1.5, c50: 3.0, c90: 4.5 },
    { time: '08:00', c10: 2.0, c50: 3.0, c90: 4.0 },
  ]);
});

test('aggregateSolcastHourly treats a missing half-hour as zero', () => {
  const periods = [{ time: '07:30', c10: 0.5, c50: 1.0, c90: 1.5 }];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result, [{ time: '07:00', c10: 0.5, c50: 1.0, c90: 1.5 }]);
});

test('aggregateSolcastHourly returns hours in ascending order', () => {
  const periods = [
    { time: '09:00', c10: 1, c50: 1, c90: 1 },
    { time: '07:00', c10: 1, c50: 1, c90: 1 },
  ];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result.map(r => r.time), ['07:00', '09:00']);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `node --test tests/js/forecast_calc.test.js`
Expected: FAIL - `Cannot find module '../../static/js/forecast-calc.js'`

- [ ] **Step 3: Write `static/js/forecast-calc.js`**

```javascript
// Pure per-request math for the /forecast chart/table - no DOM, no
// Chart.js - so it can be unit tested the same way static/js/diagram-calc.js
// already is (see tests/js/forecast_calc.test.js). Loaded as a plain
// <script> in forecast.html (browser global ForecastCalc), and required
// directly in tests (Node's CommonJS module.exports below).

function solcastPeriodToX(hhmm) {
  const [h, m] = hhmm.split(':').map(Number);
  return h + m / 60;
}

// Sums each hour's two 30-minute Solcast periods into one hourly row - the
// table shows hourly figures even though the chart (forecast.html) plots
// Solcast at its native 30-minute resolution. A missing half (e.g. the
// very first/last period of a fetch window) is treated as 0, same
// convention forecast.py/solcast.py already use for a missing orientation.
function aggregateSolcastHourly(periods) {
  const byHour = {};
  for (const p of periods) {
    const hour = p.time.split(':')[0] + ':00';
    const bucket = byHour[hour] || { time: hour, c10: 0, c50: 0, c90: 0 };
    bucket.c10 = Math.round((bucket.c10 + p.c10) * 100) / 100;
    bucket.c50 = Math.round((bucket.c50 + p.c50) * 100) / 100;
    bucket.c90 = Math.round((bucket.c90 + p.c90) * 100) / 100;
    byHour[hour] = bucket;
  }
  return Object.keys(byHour).sort().map(h => byHour[h]);
}

const ForecastCalc = { solcastPeriodToX, aggregateSolcastHourly };
if (typeof module !== 'undefined' && module.exports) {
  module.exports = ForecastCalc;
} else {
  window.ForecastCalc = ForecastCalc;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test tests/js/forecast_calc.test.js`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add static/js/forecast-calc.js tests/js/forecast_calc.test.js
git commit -m "Add static/js/forecast-calc.js: pure chart/table math for /forecast

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 7: `templates/forecast.html` - chart, table, dropdown

**Files:**
- Modify: `templates/forecast.html`

**Interfaces:**
- Consumes: `/forecast/hourly.json`'s JSON shape (Task 5, Step 3), `ForecastCalc` (Task 6),
  `ChartTheme` (existing `chart-theme.js`), `renderTable` (existing `table.js`).
- Produces: the rendered page - no new interfaces for later tasks (this is the last task).

- [ ] **Step 1: Replace the summary block**

Replace:

```html
        <p>
          90°: {{ forecast.angle90_in_kWh }} kWh<br>
          270°: {{ forecast.angle270_in_kWh }} kWh<br>
          Total: {{ forecast.total_in_kWh }} kWh
        </p>
```

with:

```html
        <p>{{ summary }}</p>
```

- [ ] **Step 2: Add the fetch-time dropdown next to the date selector**

Replace:

```html
    <div class="row">
      <div class="col-3">
        <div class="input-group input-group-sm">
          <input id="date-selector" type="date" class="form-control" value="{{ date }}" />
        </div>
      </div>
    </div>
```

with:

```html
    <div class="row">
      <div class="col-3">
        <div class="input-group input-group-sm">
          <input id="date-selector" type="date" class="form-control" value="{{ date }}" />
        </div>
      </div>
      <div class="col-4">
        <div class="input-group input-group-sm">
          <select id="fetch-time-selector" class="form-select"></select>
        </div>
      </div>
    </div>
```

- [ ] **Step 3: Add the Solcast-unavailable note placeholder**

Add right after the chart's existing error `<div>`:

```html
        <div id="forecast-chart-error" class="alert alert-warning d-none" role="alert">
          Couldn't load the hourly forecast. Try reloading the page.
        </div>
        <div id="solcast-unavailable-note" class="alert alert-secondary d-none" role="alert">
          Solcast forecast unavailable for this date.
        </div>
```

- [ ] **Step 4: Load `forecast-calc.js`**

Add next to the existing `table.js` script tag:

```html
  <script src="/static/table.js"></script>
  <script src="/static/forecast-calc.js"></script>
```

- [ ] **Step 5: Rewrite the data-fetch + table-build section**

Replace the `fetch('/forecast/hourly.json?date=' + chartDate)` block through the `renderTable(...)`
call with:

```javascript
      const chartDate = '{{ date }}';
      const initialFetchedAt = {{ fetched_at | tojson }};
      let data;
      try {
        const url = new URL('/forecast/hourly.json', window.location.origin);
        url.searchParams.set('date', chartDate);
        if (initialFetchedAt) url.searchParams.set('fetched_at', initialFetchedAt);
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        data = await resp.json();
      } catch (err) {
        console.error('Failed to load hourly forecast:', err);
        document.getElementById('forecast-chart-error').classList.remove('d-none');
        return;
      }

      // Fetch-time dropdown: "Latest" (no fetched_at param) plus every
      // distinct past fetch for this date, newest first.
      const selector = document.getElementById('fetch-time-selector');
      const fmtFetchedAt = epoch => new Date(epoch * 1000).toLocaleString();
      selector.innerHTML = ['<option value="">Latest</option>']
        .concat(data.fetch_times.map(t => `<option value="${t}"${t === data.selected_fetched_at ? ' selected' : ''}>${fmtFetchedAt(t)}</option>`))
        .join('');
      selector.addEventListener('change', function () {
        const url = new URL(window.location.href);
        if (this.value) url.searchParams.set('fetched_at', this.value); else url.searchParams.delete('fetched_at');
        window.location.href = url.toString();
      });

      document.getElementById('solcast-unavailable-note').classList.toggle('d-none', data.solcast.available);

      const meteosourceByHour = Object.fromEntries(data.meteosource.hours.map(h => [h.time, h.kwh]));
      const hourLabels = data.meteosource.hours.map(h => h.time); // Meteosource is always hourly - the chart's 24-category backbone
      const hourCount = hourLabels.length;
      const actualByHour = Object.fromEntries(data.actual.hours.map(h => [h.time, h.kwh]));
      const actualValues = hourLabels.map(t => actualByHour[t] ?? null);
      const currentHourIndex = data.actual.current_hour ? hourLabels.indexOf(data.actual.current_hour) : -1;
      const partialKwh = data.actual.current_hour_actual_partial_kwh;
      const actualLineColor = '#22cc66';
      const trailingHourColor = '#ffffff';

      const chartActualValues = actualValues.slice();
      const hasRealPartial = partialKwh != null;
      if (currentHourIndex >= 0 && chartActualValues[currentHourIndex] == null) {
        if (hasRealPartial) {
          chartActualValues[currentHourIndex] = partialKwh;
        } else if (chartActualValues[currentHourIndex - 1] != null) {
          chartActualValues[currentHourIndex] = chartActualValues[currentHourIndex - 1];
        }
      }

      const solcastHourly = ForecastCalc.aggregateSolcastHourly(data.solcast.periods);
      const solcastByHour = Object.fromEntries(solcastHourly.map(h => [h.time, h]));
      const meteosourceTotal = Math.round(hourLabels.reduce((sum, t) => sum + (meteosourceByHour[t] || 0), 0) * 10) / 10;
      const solcastTotals = data.solcast.available ? {
        c10Total: Math.round(solcastHourly.reduce((s, h) => s + h.c10, 0) * 10) / 10,
        c50Total: Math.round(solcastHourly.reduce((s, h) => s + h.c50, 0) * 10) / 10,
        c90Total: Math.round(solcastHourly.reduce((s, h) => s + h.c90, 0) * 10) / 10,
      } : null;

      const columns = ['Hour', 'Meteosource', 'Solcast', 'Actual', 'Actual cumul.'];
      let cumActualKwh = 0;
      const rows = hourLabels.map((t, idx) => {
        const row = { Hour: t, Meteosource: (meteosourceByHour[t] || 0).toFixed(2) };
        const s = solcastByHour[t];
        row['Solcast'] = s ? `${s.c50.toFixed(2)} (${s.c10.toFixed(2)}–${s.c90.toFixed(2)})` : '—';
        if (actualByHour[t] != null) {
          cumActualKwh += actualByHour[t];
          row['Actual'] = actualByHour[t].toFixed(2);
          row['Actual cumul.'] = cumActualKwh.toFixed(2);
        } else if (idx === currentHourIndex && partialKwh != null) {
          row['Actual'] = `${partialKwh.toFixed(2)} (so far)`;
          row['Actual cumul.'] = `${(cumActualKwh + partialKwh).toFixed(2)} (so far)`;
        } else {
          row['Actual'] = '—';
          row['Actual cumul.'] = '—';
        }
        return row;
      });
      renderTable('forecast-table-head', 'forecast-table-body', 'forecast-table-empty', columns, rows);
```

- [ ] **Step 6: Rewrite the chart-building section**

Replace `forecastBarDatasets()`/`actualLineDataset()`/the `buildChart()` datasets array/the x-scale
with:

```javascript
      const c50Color = '#e0a458';

      function meteosourceBarDataset() {
        return {
          type: 'bar',
          label: 'Meteosource',
          data: hourLabels.map((t, idx) => ({ x: idx + 0.5, y: meteosourceByHour[t] || 0 })),
          backgroundColor: ChartTheme.seriesColor(0),
          barPercentage: 1.0,
          categoryPercentage: 1.0,
          order: 10, // bottom layer - see docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md §1
        };
      }

      function solcastDatasets() {
        if (!data.solcast.available) return [];
        // Native 30-minute points, unlike Meteosource's hourly bars - see
        // spec §1 "Mixed resolution".
        const points = key => data.solcast.periods.map(p => ({ x: ForecastCalc.solcastPeriodToX(p.time), y: p[key] }));
        return [
          { type: 'line', label: 'Solcast c10', data: points('c10'), borderWidth: 0, pointRadius: 0, fill: false, order: 3 },
          { type: 'line', label: 'Solcast c10–c90 range', data: points('c90'), borderWidth: 0, pointRadius: 0, fill: '-1', backgroundColor: ChartTheme.withAlpha(c50Color, 0.18), order: 3 },
          { type: 'line', label: 'Solcast c50', data: points('c50'), borderColor: c50Color, backgroundColor: c50Color, borderWidth: 2, pointRadius: 0, fill: false, order: 2 },
        ];
      }

      function actualLineDataset() {
        return {
          type: 'line',
          label: 'Actual',
          data: chartActualValues.map((v, idx) => ({ x: idx, y: v })),
          borderColor: actualLineColor,
          spanGaps: false,
          stepped: 'before',
          pointRadius: 0,
          order: -1, // top layer
          borderWidth: 1.5,
          fill: 'origin',
          backgroundColor: ChartTheme.withAlpha(actualLineColor, 0.4),
        };
      }
```

In `buildChart()`, replace `const datasets = [...forecastBarDatasets(), actualLineDataset()];` with:

```javascript
        const datasets = [meteosourceBarDataset(), ...solcastDatasets(), actualLineDataset()];
```

In the tooltip's `filter` callback (inside `buildChart()`'s options), add the Solcast band's
internal helper datasets to the existing current-hour-placeholder filter so they never show their
own tooltip row (only the visible "Solcast c50" line should):

```javascript
                filter: item => !(item.dataset.label === 'Actual' && item.dataIndex === currentHourIndex && partialKwh == null)
                  && item.dataset.label !== 'Solcast c10' && item.dataset.label !== 'Solcast c10–c90 range',
```

And in the legend's `labels.filter` (add if not already present, next to `legend: ChartTheme.legend`
in the chart options):

```javascript
            plugins: {
              legend: { ...ChartTheme.legend, labels: { ...ChartTheme.legend.labels, filter: item => item.text !== 'Solcast c10' && item.text !== 'Solcast c10–c90 range' } },
```

- [ ] **Step 7: Manually verify against the real app**

Run the app in dry-run mode (no real inverter needed):

```bash
venv/bin/python3 main.py --dry-run
```

Then, in another terminal:

```bash
curl -s 'http://localhost:5000/forecast/hourly.json?date=2026-09-14' | python3 -m json.tool | head -40
```

Expected: valid JSON matching the shape from Task 5 Step 3 - `meteosource.hours`,
`solcast.available` (`false`, since no `SOLCAST_API_KEY`/site IDs are configured yet in this
environment - the "unavailable" note should be what actually renders), `actual.hours`, `fetch_times`.

Then open `http://<LAN IP>:5000/forecast` in a browser (see the `local-dryrun-url-format` memory
convention - use the LAN IP, not localhost) and confirm:
- The page loads with no console errors.
- The Meteosource bar renders (Solcast will show the "unavailable" note until real API
  credentials exist - that's expected in this environment).
- The date selector and (empty-but-present) fetch-time dropdown both render without errors.
- The table shows the new `Hour | Meteosource | Solcast | Actual | Actual cumul.` columns, with
  `Solcast` reading `—` for every row (no Solcast data yet).

- [ ] **Step 8: Commit**

```bash
git add templates/forecast.html
git commit -m "forecast.html: Solcast c10/c50/c90 band, fetch-time dropdown, new table columns

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Post-implementation (not part of this plan's tasks - manual, once you have real Solcast credentials)

1. Create 2 rooftop sites in Solcast's dashboard (east/west, matching real `PV_LAT`/`PV_LON`/
   `PV_TILT`, azimuth 90°/270°, each at half `PV_POWER`) - see spec section 8.
2. Fill in the real `.env` (`SOLCAST_API_KEY`, `SOLCAST_SITE_EAST_ID`, `SOLCAST_SITE_WEST_ID`)
   yourself, locally and on `raspberry4.local` - not via this plan/assistant.
3. Verify `solcast.PERIOD_KW_TO_KWH`'s kW-vs-kWh assumption against one real response (compare a
   known-sunny hour's `c50` total against Meteosource's/real production for the same hour) and fix
   the constant if it's off by exactly 2x in either direction.
