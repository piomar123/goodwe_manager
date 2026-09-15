# Solcast Historical Estimate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Solcast's `estimated_actuals` endpoint (its satellite-derived historical estimate of
what a site actually produced) as a third `/forecast` chart/table series, "Solcast Estimated
Actual", shown only for fully-elapsed past dates - alongside a reschedule of the existing PV
forecast prefetch wake-times to make room for it within Solcast's 10-calls/day free-tier budget.

**Architecture:** Five small, mostly-independent changes: `solcast.py` gains a new API-client
function (mirrors the existing forecast-fetching one almost exactly, just a different endpoint/
response shape); `forecast_prefetch.py`'s wake schedule is replaced (3 forecast slots instead of 4,
plus one new dedicated daily actuals slot) and gains a new fetch-and-store function;
`forecast_history.py`'s storage needs no schema change at all (already generic by `source` string) -
just a stale-docstring fix; `main.py`'s `/forecast/hourly.json` route reads the new source, gated to
past dates only; `static/js/forecast-calc.js` gains a small aggregation helper; `templates/
forecast.html` renders the new series on the chart (dashed line) and table (new column), both
gated on availability.

**Tech Stack:** Python 3 (Flask, `requests`, `unittest`), vanilla JS + Chart.js 4.4.4, Node's
built-in `node:test`.

**Spec:** `docs/superpowers/specs/2026-09-15-solcast-historical-estimate-design.md`

## Global Constraints

- Endpoint: `GET /rooftop_sites/{resource_id}/estimated_actuals`, response key `estimated_actuals`
  (not `forecasts`), each period has only `pv_estimate` (no `pv_estimate10`/`pv_estimate90`).
- `hours` query param controls how far back one call reaches, **capped at 168** (exactly 7 days) -
  confirmed live; a value over 168 gets a `400`. Default to `hours=168` (the max).
- New storage `source` value: `'solcast_actuals'`, stored in the existing `forecast_snapshots`
  table - **no schema change**, `write_snapshot`/`get_snapshot`/`get_latest_merged`/`get_fetch_times`
  are already generic by `source` string.
- New fetch schedule (replaces the current 4-slot one entirely):
  `FORECAST_WAKE_TIMES = (06:00, 11:00, 21:00)` for Meteosource + Solcast forecast (was
  `06:00, 10:00, 14:15, 18:00`), plus a new, separate `ACTUALS_WAKE_TIME = 23:00` for Solcast
  `estimated_actuals`. Total stays at **8 Solcast calls/day** (3×2 forecast + 1×2 actuals), same
  2/day headroom as before.
- Read path: the `solcast_actuals` source is only ever read/shown for a **fully elapsed past date**
  (`date_yyyymmdd < today`) - never for today or a future date, regardless of whether data exists.
- Series name (exact string, used verbatim in the chart legend/label and the table column header):
  **"Solcast Estimated Actual"**.
- `solcast_actuals` payload shape is **flat** `{"HH:MM": kwh}` (matching Meteosource's convention),
  not Solcast forecast's nested `{"HH:MM": {"c10":..,"c50":..,"c90":..}}` - a historical estimate
  isn't a probabilistic range.
- No new `.env`/config entries - reuses `SOLCAST_API_KEY`, `SOLCAST_SITE_EAST_ID`,
  `SOLCAST_SITE_WEST_ID`.

---

## Task 1: `solcast.py` - `estimated_actuals` client function

**Files:**
- Modify: `solcast.py`
- Test: `tests/test_solcast.py`

**Interfaces:**
- Consumes: nothing new (same `SOLCAST_API_BASE`, `PERIOD_KW_TO_KWH` module constants already in
  the file).
- Produces: `fetch_solcast_estimated_actuals_30min(resource_id: str, hours: int = 168) ->
  Dict[str, Dict[str, float]]` (flat `{"YYYY-MM-DD": {"HH:MM": kwh}}`), `sum_sites_flat(*
  site_payloads: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]`. Task 2 calls both by
  these exact names/signatures.

- [ ] **Step 1: Write the failing tests**

Add to the end of `tests/test_solcast.py` (before the `if __name__ == '__main__':` line), and add
this helper near the top of the file, right after the existing `_fake_response` helper:

```python
def _fake_actuals_response(estimated_actuals):
    resp = MagicMock()
    resp.json.return_value = {'estimated_actuals': estimated_actuals}
    resp.raise_for_status.return_value = None
    return resp


class FetchSolcastEstimatedActuals30MinTest(unittest.TestCase):
    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_parses_periods_into_local_date_and_hhmm_buckets(self, mock_get):
        # Same period_end/local-time conversion as the forecast endpoint's
        # own test - CEST (UTC+2) in September.
        mock_get.return_value = _fake_actuals_response([
            {'pv_estimate': 2.0, 'period_end': '2026-09-14T12:00:00.0000000Z', 'period': 'PT30M'},
        ])

        result = solcast.fetch_solcast_estimated_actuals_30min('site-123')

        self.assertEqual(result, {'2026-09-14': {'13:30': 1.0}})

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_uses_api_key_hours_and_pt30m_period(self, mock_get):
        mock_get.return_value = _fake_actuals_response([])
        solcast.fetch_solcast_estimated_actuals_30min('site-123', hours=48)
        args, kwargs = mock_get.call_args
        self.assertIn('site-123', args[0])
        self.assertIn('estimated_actuals', args[0])
        self.assertEqual(kwargs['params']['api_key'], 'test-key')
        self.assertEqual(kwargs['params']['period'], 'PT30M')
        self.assertEqual(kwargs['params']['hours'], 48)

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_defaults_to_168_hours(self, mock_get):
        mock_get.return_value = _fake_actuals_response([])
        solcast.fetch_solcast_estimated_actuals_30min('site-123')
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['hours'], 168)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_api_key_raises(self):
        with self.assertRaises(AssertionError):
            solcast.fetch_solcast_estimated_actuals_30min('site-123')


class SumSitesFlatTest(unittest.TestCase):
    def test_sums_two_sites_matching_dates_and_periods(self):
        east = {'2026-09-14': {'07:00': 0.2}}
        west = {'2026-09-14': {'07:00': 0.1}}
        result = solcast.sum_sites_flat(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': 0.3}})

    def test_period_present_in_only_one_site_is_treated_as_zero_for_the_other(self):
        east = {'2026-09-14': {'07:00': 0.2}}
        west = {'2026-09-14': {}}
        result = solcast.sum_sites_flat(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': 0.2}})

    def test_date_present_in_only_one_site_is_kept(self):
        east = {'2026-09-14': {'07:00': 0.2}}
        west = {'2026-09-15': {'07:00': 0.2}}
        result = solcast.sum_sites_flat(east, west)
        self.assertEqual(set(result.keys()), {'2026-09-14', '2026-09-15'})
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `venv/bin/python3 -m pytest tests/test_solcast.py -v`
Expected: FAIL - `AttributeError: module 'solcast' has no attribute 'fetch_solcast_estimated_actuals_30min'`

- [ ] **Step 3: Implement the client function**

In `solcast.py`, add this after `fetch_solcast_forecast_30min` and before `sum_sites`:

```python
def fetch_solcast_estimated_actuals_30min(resource_id: str, hours: int = 168) -> Dict[str, Dict[str, float]]:
    """Calls GET /rooftop_sites/{resource_id}/estimated_actuals at Solcast's
    native 30-minute period, covering the trailing `hours` hours (max 168 -
    7 days, Solcast's own cap; a >168 request gets a 400). Returns
    {"YYYY-MM-DD": {"HH:MM": kwh}} - flat per-period kWh, unlike
    fetch_solcast_forecast_30min's {c10,c50,c90} nesting, since a historical
    estimate isn't a probabilistic range. Same period_end-fractional-
    seconds-strip and period-start-local conversion as
    fetch_solcast_forecast_30min - see
    docs/superpowers/specs/2026-09-15-solcast-historical-estimate-design.md.
    """
    api_key = os.environ.get('SOLCAST_API_KEY')
    assert api_key, "SOLCAST_API_KEY environment variable not set"
    response = requests.get(
        f"{SOLCAST_API_BASE}/rooftop_sites/{resource_id}/estimated_actuals",
        params={'format': 'json', 'period': 'PT30M', 'hours': hours, 'api_key': api_key},
    )
    response.raise_for_status()
    data = response.json()

    result: Dict[str, Dict[str, float]] = {}
    for period in data.get('estimated_actuals', []):
        period_end_str = period['period_end'].split('.')[0] + '+00:00'
        period_end_utc = datetime.fromisoformat(period_end_str)
        period_start_local = (period_end_utc - timedelta(minutes=30)).astimezone()
        date_str = period_start_local.strftime('%Y-%m-%d')
        hhmm = period_start_local.strftime('%H:%M')
        result.setdefault(date_str, {})[hhmm] = round(period['pv_estimate'] * PERIOD_KW_TO_KWH, 2)
    return result
```

Add this after `sum_sites` (at the end of the file):

```python
def sum_sites_flat(*site_payloads: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    """Sums 2+ fetch_solcast_estimated_actuals_30min() results (east+west)
    into one combined payload, same nested {date: {time: kwh}} shape. A
    date/period present in only some sites sums whichever sites have it,
    treating a missing site's period as 0kWh - same missing-orientation
    convention as sum_sites. Kept separate from sum_sites rather than
    generalizing one function across both shapes - sum_sites is typed
    specifically around {c10,c50,c90} and the summing loop itself is three
    lines, so a shape-detection branch would add more complexity than it
    removes."""
    combined: Dict[str, Dict[str, float]] = {}
    for site in site_payloads:
        for date_str, periods in site.items():
            date_bucket = combined.setdefault(date_str, {})
            for hhmm, kwh in periods.items():
                date_bucket[hhmm] = round(date_bucket.get(hhmm, 0.0) + kwh, 2)
    return combined
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_solcast.py -v`
Expected: PASS (14 tests - 7 existing: 4 `FetchSolcastForecast30MinTest` + 3 `SumSitesTest`; 7 new:
4 `FetchSolcastEstimatedActuals30MinTest` + 3 `SumSitesFlatTest`)

- [ ] **Step 5: Commit**

```bash
git add solcast.py tests/test_solcast.py
git commit -m "solcast.py: add estimated_actuals client (fetch_solcast_estimated_actuals_30min, sum_sites_flat)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 2: `forecast_prefetch.py` - new wake schedule + actuals fetch-and-store

**Files:**
- Modify: `forecast_prefetch.py` (full-file rewrite - the change touches the module docstring, the
  wake-time constants, and `run()`'s loop structure)
- Modify: `forecast_history.py:92-93` (one docstring line - see Step 4)
- Modify: `tests/test_forecast_prefetch.py` (full-file rewrite)

**Interfaces:**
- Consumes: `solcast.fetch_solcast_estimated_actuals_30min` and `solcast.sum_sites_flat` (Task 1,
  exact names/signatures above).
- Produces: `forecast_prefetch.FORECAST_WAKE_TIMES: Tuple[time, ...]` (3 entries),
  `forecast_prefetch.ACTUALS_WAKE_TIME: time`, `forecast_prefetch.fetch_and_store_solcast_actuals(conn)
  -> None`. No later task depends on these beyond this task's own `ForecastPrefetchThread.run()`.

- [ ] **Step 1: Write the failing tests**

Replace the full contents of `tests/test_forecast_prefetch.py`:

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


class WakeScheduleTest(unittest.TestCase):
    def test_forecast_wake_times_has_three_slots(self):
        self.assertEqual(len(forecast_prefetch.FORECAST_WAKE_TIMES), 3)

    def test_actuals_wake_time_is_not_one_of_the_forecast_slots(self):
        self.assertNotIn(forecast_prefetch.ACTUALS_WAKE_TIME, forecast_prefetch.FORECAST_WAKE_TIMES)

    def test_actuals_wake_time_is_after_every_forecast_slot(self):
        # Pins the specific schedule chosen (23:00, after 06:00/11:00/21:00)
        # - not a hard design requirement (exact actuals timing isn't
        # critical, see forecast_prefetch.py's module docstring), just a
        # regression guard on the concrete choice.
        self.assertTrue(all(forecast_prefetch.ACTUALS_WAKE_TIME > t for t in forecast_prefetch.FORECAST_WAKE_TIMES))


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

    @patch.dict(os.environ, {'SOLCAST_SITE_EAST_ID': 'east-1', 'SOLCAST_SITE_WEST_ID': 'west-1'})
    @patch('forecast_prefetch.solcast.fetch_solcast_estimated_actuals_30min')
    def test_fetch_and_store_solcast_actuals_sums_sites_and_writes_a_snapshot_per_date(self, mock_fetch):
        def fake_fetch(resource_id):
            if resource_id == 'east-1':
                return {'2026-01-01': {'07:00': 0.5}}
            return {'2026-01-01': {'07:00': 0.25}}
        mock_fetch.side_effect = fake_fetch

        forecast_prefetch.fetch_and_store_solcast_actuals(self.conn)

        times = forecast_history.get_fetch_times(self.conn, '2026-01-01')
        self.assertEqual(len(times), 1)
        self.assertEqual(
            forecast_history.get_snapshot(self.conn, 'solcast_actuals', '2026-01-01', times[0]),
            {'07:00': 0.75},
        )


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `venv/bin/python3 -m pytest tests/test_forecast_prefetch.py -v`
Expected: FAIL - `AttributeError: module 'forecast_prefetch' has no attribute 'FORECAST_WAKE_TIMES'`
(and similar for `ACTUALS_WAKE_TIME`/`fetch_and_store_solcast_actuals`)

- [ ] **Step 3: Replace the full contents of `forecast_prefetch.py`**

```python
"""
forecast_prefetch.py
Background thread that refreshes all three PV forecast/actuals sources
(Meteosource forecast, Solcast forecast, Solcast estimated_actuals) on two
independent schedules, writing every fetch into forecast_history's snapshot
table - see
docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md section 5
and docs/superpowers/specs/2026-09-15-solcast-historical-estimate-design.md
section 3 (this schedule).

Forecast wake-times (Meteosource + Solcast forecast, both orientations):
06:00 (dawn, satellite imagery becomes usable), 11:00 (fresh, with margin
before the tariff's midday cheap-charging window in both its winter
13:00-15:00 and summer 15:00-17:00 forms), 21:00 (freshest possible
forecast right before the 22:00-06:00 overnight cheap-charging window, for
overnight grid-charge sizing - the gap the old 4-slot schedule left
entirely, its latest slot being 18:00). 2 Solcast calls/slot x 3 slots =
6/day.

Actuals wake-time (Solcast estimated_actuals, both orientations): 23:00,
once/day - safely after all three forecast slots and past sunset even at
midsummer. Exact timing isn't critical: forecast_history.get_latest_merged's
existing period-level merge-across-snapshots means even a day fetched
before its very last production hour gets backfilled by the next day's
7-day-rolling re-fetch of the same date (Solcast's `hours` cap on that
endpoint is 168 = exactly 7 days). 2 Solcast calls/day.

Total: 8 Solcast calls/day, leaving 2/day of headroom under Solcast's
10/day free-tier cap - same headroom the original 4-forecast-slot schedule
had, just reallocated to fund this daily actuals fetch.

On any fetch failure, logs a warning and moves on rather than retrying
within the same slot - retrying would spend quota meant for the next slot,
and the read path (forecast_history.get_latest_merged) already tolerates a
missing/stale slot gracefully, same "safe to fail" framing as
rce_prefetch.py.
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

FORECAST_WAKE_TIMES = (dtime(6, 0), dtime(11, 0), dtime(21, 0))
ACTUALS_WAKE_TIME = dtime(23, 0)


def next_wake_time(now: datetime, wake_times=FORECAST_WAKE_TIMES) -> datetime:
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


def fetch_and_store_solcast_actuals(conn) -> None:
    east = solcast.fetch_solcast_estimated_actuals_30min(os.environ['SOLCAST_SITE_EAST_ID'])
    west = solcast.fetch_solcast_estimated_actuals_30min(os.environ['SOLCAST_SITE_WEST_ID'])
    combined_by_date = solcast.sum_sites_flat(east, west)
    for date_str, payload in combined_by_date.items():
        forecast_history.write_snapshot(conn, 'solcast_actuals', date_str, payload)


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
                next_forecast = next_wake_time(now, FORECAST_WAKE_TIMES)
                next_actuals = next_wake_time(now, (ACTUALS_WAKE_TIME,))
                next_wake = min(next_forecast, next_actuals)
                wait_seconds = (next_wake - now).total_seconds()
                if self._should_stop.wait(wait_seconds):
                    return
                today = datetime.now().strftime('%Y-%m-%d')
                if next_wake == next_forecast:
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
                if next_wake == next_actuals:
                    try:
                        fetch_and_store_solcast_actuals(conn)
                        logger.info("Prefetched Solcast estimated actuals")
                    except Exception as e:
                        logger.warning(f"Solcast estimated-actuals prefetch failed: {e}")
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

Note on the dual-schedule loop: `next_wake` is the earlier of `next_forecast`/`next_actuals`, computed
from the same `now`. After waking, `next_wake == next_forecast` runs the forecast fetches,
`next_wake == next_actuals` runs the actuals fetch - both run in the rare case they land on the exact
same moment (both conditions true simultaneously), since these are separate `if`s, not `if`/`elif`.

- [ ] **Step 4: Fix a stale docstring in `forecast_history.py`**

`get_fetch_times`'s docstring currently says "across BOTH sources" and claims they share timestamps
in practice - both statements are now more wrong than before (a third source exists, and the two
original sources were already independently timestamped, ~10 minutes apart in practice, despite the
claim). Fix it to describe reality:

Replace:

```python
def get_fetch_times(conn: sqlite3.Connection, date: str) -> List[int]:
    """Distinct fetched_at values across BOTH sources for `date`, newest
    first - backs the single shared fetch-time dropdown (both sources fetch
    on the same schedule, so in practice they share timestamps)."""
```

with:

```python
def get_fetch_times(conn: sqlite3.Connection, date: str) -> List[int]:
    """Distinct fetched_at values across every source for `date`, newest
    first - backs the single shared fetch-time dropdown. Sources are
    fetched independently (see forecast_prefetch.py's wake schedule) and do
    not actually share timestamps in practice, despite feeding one shared
    dropdown - selecting a timestamp only one source has data for is a
    known, pre-existing UX gap (not addressed here)."""
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_forecast_prefetch.py tests/test_forecast_history.py -v`
Expected: PASS (8 tests in `test_forecast_prefetch.py`: 2 `NextWakeTimeTest` + 3 `WakeScheduleTest` +
3 `FetchAndStoreTest`. All of `test_forecast_history.py`'s existing tests unaffected by the
docstring-only change.)

Then run the full test suite to check nothing else broke:

Run: `venv/bin/python3 -m pytest tests/ -v --ignore=tests/js`
Expected: PASS, no failures.

- [ ] **Step 6: Commit**

```bash
git add forecast_prefetch.py forecast_history.py tests/test_forecast_prefetch.py
git commit -m "forecast_prefetch.py: new wake schedule (3 forecast slots + daily actuals fetch)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 3: `main.py` - read path for `/forecast/hourly.json`

**Files:**
- Modify: `main.py:671-705` (the `get_forecast_hourly_json()` route)
- Modify: `tests/test_main_forecast_routes.py` (extend `ForecastHourlyJsonRouteTest`, and fix two
  existing tests' mock fixtures - see Step 1)

**Interfaces:**
- Consumes: `_read_forecast_payload(conn, source, date_yyyymmdd, fetched_at)` (existing, unchanged -
  works for the new `'solcast_actuals'` source without any change to that function itself, since
  it's already generic by source name).
- Produces: the `/forecast/hourly.json` response gains a `'solcast_actuals'` key
  (`{'available': bool, 'periods': [{'time': str, 'kwh': float}, ...]}`). Task 5 (the template)
  consumes this shape directly.

- [ ] **Step 1: Write the failing tests**

In `tests/test_main_forecast_routes.py`, first fix two existing tests in `ForecastHourlyJsonRouteTest`
whose mock fixtures currently return the wrong shape for any source other than `'meteosource'` (this
matters now that a third source, `'solcast_actuals'`, goes through the same `get_latest_merged`/
`get_snapshot` mocks) - these fixes don't change what the tests assert, they just make the fixture
data honest about which source it represents:

Replace:

```python
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
```

with:

```python
    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[2000, 1000])
    @patch('main.forecast_history.get_latest_merged')
    def test_returns_meteosource_solcast_and_fetch_times(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        def fake_merged(conn, source, date):
            if source == 'meteosource':
                return {'07:00': 1.5}
            elif source == 'solcast':
                return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
            return {}
        mock_merged.side_effect = fake_merged
```

Replace:

```python
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
```

with:

```python
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
            if source == 'meteosource':
                return {'07:00': 1.5}
            elif source == 'solcast':
                return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
            return {}
        mock_snapshot.side_effect = fake_snapshot
```

Then add these two new tests to the end of `ForecastHourlyJsonRouteTest` (before the closing of the
class, i.e. before `class` or `if __name__` - check the file's actual end and add there):

```python
    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[])
    @patch('main.forecast_history.get_latest_merged')
    def test_returns_solcast_actuals_for_a_past_date(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        def fake_merged(conn, source, date):
            if source == 'solcast_actuals':
                return {'07:00': 0.75}
            return {}
        mock_merged.side_effect = fake_merged

        resp = self.client.get('/forecast/hourly.json?date=2020-01-01')  # safely in the past

        data = resp.get_json()
        self.assertTrue(data['solcast_actuals']['available'])
        self.assertEqual(data['solcast_actuals']['periods'], [{'time': '07:00', 'kwh': 0.75}])

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[])
    @patch('main.forecast_history.get_latest_merged')
    def test_omits_solcast_actuals_for_todays_date(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        # Even if a snapshot exists (e.g. a stray/manual fetch), today's
        # date must not surface it - see this plan's Global Constraints and
        # spec §4's past-dates-only gating.
        def fake_merged(conn, source, date):
            if source == 'solcast_actuals':
                return {'07:00': 0.75}
            return {}
        mock_merged.side_effect = fake_merged

        today = datetime.now().strftime('%Y-%m-%d')
        resp = self.client.get(f'/forecast/hourly.json?date={today}')

        data = resp.get_json()
        self.assertFalse(data['solcast_actuals']['available'])
        self.assertEqual(data['solcast_actuals']['periods'], [])
        for call in mock_merged.call_args_list:
            self.assertNotEqual(call.args[1], 'solcast_actuals')
```

(`datetime` is already imported at the top of this test file - `from datetime import datetime`.)

- [ ] **Step 2: Run the tests to verify the new ones fail**

Run: `venv/bin/python3 -m pytest tests/test_main_forecast_routes.py -v`
Expected: the two new tests FAIL with `KeyError: 'solcast_actuals'`; the two fixed existing tests
and all others still PASS (the fixture fix doesn't change their assertions).

- [ ] **Step 3: Update `get_forecast_hourly_json()`**

In `main.py`, replace the full route function:

```python
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

with:

```python
@app.get('/forecast/hourly.json')
def get_forecast_hourly_json():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    fetched_at = request.args.get('fetched_at', type=int)
    logger.debug(f"Fetching hourly forecast JSON for {date_yyyymmdd} (fetched_at={fetched_at})")

    now = datetime.now()
    is_past_date = date_yyyymmdd < now.strftime('%Y-%m-%d')

    with _forecast_history_connection() as conn:
        meteosource = _read_forecast_payload(conn, 'meteosource', date_yyyymmdd, fetched_at)
        solcast_periods = _read_forecast_payload(conn, 'solcast', date_yyyymmdd, fetched_at)
        # Solcast's estimated_actuals is only ever meaningful for a fully
        # elapsed past day (see
        # docs/superpowers/specs/2026-09-15-solcast-historical-estimate-design.md
        # §4) - skip the read entirely for today/future dates rather than
        # showing an estimate of an estimate next to the real Actual line.
        solcast_actuals_periods = (
            _read_forecast_payload(conn, 'solcast_actuals', date_yyyymmdd, fetched_at)
            if is_past_date else {}
        )
        fetch_times = forecast_history.get_fetch_times(conn, date_yyyymmdd)

    actual_by_hour = _get_actual_hourly_pv_kwh(date_yyyymmdd)
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
        'solcast_actuals': {
            'available': bool(solcast_actuals_periods),
            'periods': [{'time': t, 'kwh': kwh} for t, kwh in sorted(solcast_actuals_periods.items())],
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

(Note: `now = datetime.now()` moved earlier so both `is_past_date` and `is_today` reuse the same
`now` instead of calling `datetime.now()` twice.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_main_forecast_routes.py -v`
Expected: PASS (7 tests: 2 `ForecastSummaryAccuracyDeltaGatingTest` + 5 `ForecastHourlyJsonRouteTest`
- 3 existing + 2 new)

Then run the full test suite:

Run: `venv/bin/python3 -m pytest tests/ -v --ignore=tests/js`
Expected: PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add main.py tests/test_main_forecast_routes.py
git commit -m "main.py: read solcast_actuals for /forecast/hourly.json, past dates only

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 4: `static/js/forecast-calc.js` - `aggregateSolcastActualsHourly`

**Files:**
- Modify: `static/js/forecast-calc.js`
- Modify: `tests/js/forecast_calc.test.js`

**Interfaces:**
- Consumes: nothing new.
- Produces: `ForecastCalc.aggregateSolcastActualsHourly(periods: {time: string, kwh: number}[]) ->
  {time: string, kwh: number}[]`. Task 5 calls this by this exact name on `data.solcast_actuals.periods`.

- [ ] **Step 1: Write the failing tests**

Replace the full contents of `tests/js/forecast_calc.test.js`:

```javascript
const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  aggregateSolcastHourly,
  aggregateSolcastActualsHourly,
} = require('../../static/js/forecast-calc.js');

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

test('aggregateSolcastActualsHourly sums each hour\'s two 30-minute periods', () => {
  const periods = [
    { time: '07:00', kwh: 1.0 },
    { time: '07:30', kwh: 0.5 },
    { time: '08:00', kwh: 2.0 },
  ];
  const result = aggregateSolcastActualsHourly(periods);
  assert.deepEqual(result, [
    { time: '07:00', kwh: 1.5 },
    { time: '08:00', kwh: 2.0 },
  ]);
});

test('aggregateSolcastActualsHourly treats a missing half-hour as zero', () => {
  const periods = [{ time: '07:30', kwh: 0.5 }];
  const result = aggregateSolcastActualsHourly(periods);
  assert.deepEqual(result, [{ time: '07:00', kwh: 0.5 }]);
});

test('aggregateSolcastActualsHourly returns hours in ascending order', () => {
  const periods = [
    { time: '09:00', kwh: 1 },
    { time: '07:00', kwh: 1 },
  ];
  const result = aggregateSolcastActualsHourly(periods);
  assert.deepEqual(result.map(r => r.time), ['07:00', '09:00']);
});
```

- [ ] **Step 2: Run the tests to verify the new ones fail**

Run: `node --test tests/js/forecast_calc.test.js`
Expected: the 3 existing tests PASS; the 3 new `aggregateSolcastActualsHourly` tests FAIL with
`TypeError: aggregateSolcastActualsHourly is not a function`.

- [ ] **Step 3: Implement `aggregateSolcastActualsHourly`**

Replace the full contents of `static/js/forecast-calc.js`:

```javascript
// Pure per-request math for the /forecast chart/table - no DOM, no
// Chart.js - so it can be unit tested the same way static/js/diagram-calc.js
// already is (see tests/js/forecast_calc.test.js). Loaded as a plain
// <script> in forecast.html (browser global ForecastCalc), and required
// directly in tests (Node's CommonJS module.exports below).

// Sums each hour's two 30-minute Solcast periods into one hourly row - both
// the table and the chart (forecast.html) render Solcast at this hourly
// resolution, even though the underlying data is fetched/stored at
// Solcast's native 30-minute period (see
// docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md §1's
// amendment for why the chart no longer plots the native resolution
// directly). A missing half (e.g. the very first/last period of a fetch
// window) is treated as 0, same convention forecast.py/solcast.py already
// use for a missing orientation.
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

// Same hour-bucketing convention as aggregateSolcastHourly, but for Solcast
// estimated_actuals' flat {time, kwh} period shape (a historical estimate
// is a single value, not a {c10,c50,c90} range) - kept as a separate
// function rather than generalizing one aggregator across both shapes, for
// the same reason solcast.py's sum_sites/sum_sites_flat stay separate. See
// docs/superpowers/specs/2026-09-15-solcast-historical-estimate-design.md §5.
function aggregateSolcastActualsHourly(periods) {
  const byHour = {};
  for (const p of periods) {
    const hour = p.time.split(':')[0] + ':00';
    const bucket = byHour[hour] || { time: hour, kwh: 0 };
    bucket.kwh = Math.round((bucket.kwh + p.kwh) * 100) / 100;
    byHour[hour] = bucket;
  }
  return Object.keys(byHour).sort().map(h => byHour[h]);
}

const ForecastCalc = { aggregateSolcastHourly, aggregateSolcastActualsHourly };
if (typeof module !== 'undefined' && module.exports) {
  module.exports = ForecastCalc;
} else {
  window.ForecastCalc = ForecastCalc;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test tests/js/forecast_calc.test.js`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add static/js/forecast-calc.js tests/js/forecast_calc.test.js
git commit -m "forecast-calc.js: add aggregateSolcastActualsHourly

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 5: `templates/forecast.html` - render the new series

**Files:**
- Modify: `templates/forecast.html`

**Interfaces:**
- Consumes: `ForecastCalc.aggregateSolcastActualsHourly` (Task 4, exact name above),
  `data.solcast_actuals.available`/`data.solcast_actuals.periods` (Task 3's JSON shape).
- Produces: the rendered page - no later task depends on this (last task).

- [ ] **Step 1: Aggregate the new source alongside the existing Solcast aggregation**

Replace:

```javascript
      const solcastHourly = ForecastCalc.aggregateSolcastHourly(data.solcast.periods);
      const solcastByHour = Object.fromEntries(solcastHourly.map(h => [h.time, h]));
```

with:

```javascript
      const solcastHourly = ForecastCalc.aggregateSolcastHourly(data.solcast.periods);
      const solcastByHour = Object.fromEntries(solcastHourly.map(h => [h.time, h]));
      const solcastActualsHourly = ForecastCalc.aggregateSolcastActualsHourly(data.solcast_actuals.periods);
      const solcastActualsByHour = Object.fromEntries(solcastActualsHourly.map(h => [h.time, h]));
```

- [ ] **Step 2: Add the table column**

Replace:

```javascript
      const columns = ['Hour', 'Meteosource', 'Solcast', 'Actual', 'Actual cumul.'];
      let cumActualKwh = 0;
      const rows = hourLabels.map((t, idx) => {
        const row = { Hour: t, Meteosource: (meteosourceByHour[t] || 0).toFixed(2) };
        const s = solcastByHour[t];
        row['Solcast'] = s ? `${s.c50.toFixed(2)} (${s.c10.toFixed(2)}–${s.c90.toFixed(2)})` : '—';
        if (actualByHour[t] != null) {
```

with:

```javascript
      const columns = ['Hour', 'Meteosource', 'Solcast'];
      if (data.solcast_actuals.available) columns.push('Solcast Estimated Actual');
      columns.push('Actual', 'Actual cumul.');
      let cumActualKwh = 0;
      const rows = hourLabels.map((t, idx) => {
        const row = { Hour: t, Meteosource: (meteosourceByHour[t] || 0).toFixed(2) };
        const s = solcastByHour[t];
        row['Solcast'] = s ? `${s.c50.toFixed(2)} (${s.c10.toFixed(2)}–${s.c90.toFixed(2)})` : '—';
        if (data.solcast_actuals.available) {
          const sa = solcastActualsByHour[t];
          row['Solcast Estimated Actual'] = sa ? sa.kwh.toFixed(2) : '—';
        }
        if (actualByHour[t] != null) {
```

(The rest of that function - the `actualByHour`/`partialKwh` branches and the closing `return row; });`
- is unchanged; this replace only touches the `columns` declaration and adds the new `if` block right
after the `row['Solcast']` line, before the existing `if (actualByHour[t] != null) {`.)

- [ ] **Step 3: Add the chart dataset function**

Replace:

```javascript
      const c50Color = '#e0a458';

      function meteosourceBarDataset() {
```

with:

```javascript
      const c50Color = '#e0a458';
      const solcastActualsColor = ChartTheme.seriesColor(3); // teal - distinct from Meteosource's blue-purple, Solcast's amber, and Actual's green

      function solcastActualsDatasets() {
        if (!data.solcast_actuals.available) return [];
        // Hourly-aggregated, same x-positions (idx + 0.5) as Meteosource/
        // Solcast forecast - see
        // docs/superpowers/specs/2026-09-15-solcast-historical-estimate-design.md
        // §5. Dashed border to visually read as "estimate", not a
        // measurement - deliberately its own color/label (not reusing the
        // real "Actual" line's green) so the two are never confused. A
        // missing hour is pinned to 0, same convention solcastDatasets()
        // uses for the same reason (a line dataset needs a numeric y at
        // every x).
        const points = hourLabels.map((t, idx) => ({ x: idx + 0.5, y: (solcastActualsByHour[t] || {}).kwh ?? 0 }));
        return [
          {
            type: 'line',
            label: 'Solcast Estimated Actual',
            data: points,
            borderColor: solcastActualsColor,
            borderDash: [6, 4],
            borderWidth: 2,
            pointRadius: 0,
            fill: false,
            order: 1, // above Solcast forecast (order 2/3), below real Actual (order -1)
          },
        ];
      }

      function meteosourceBarDataset() {
```

- [ ] **Step 4: Include the new dataset in the chart**

Replace:

```javascript
        const datasets = [meteosourceBarDataset(), ...solcastDatasets(), actualLineDataset()];
```

with:

```javascript
        const datasets = [meteosourceBarDataset(), ...solcastDatasets(), ...solcastActualsDatasets(), actualLineDataset()];
```

- [ ] **Step 5: Manually verify against the real app**

Run the app in dry-run mode:

```bash
venv/bin/python3 main.py --dry-run
```

**Do not read `.env` or trigger a live Solcast API call for this verification** - use curl-based
structural checks instead, consistent with how this branch's earlier amendment task was verified:

```bash
curl -s "http://localhost:5001/forecast?date=2020-01-01" | grep -o 'aggregateSolcastActualsHourly' | head -1
curl -s "http://localhost:5001/forecast?date=2020-01-01" | grep -o "Solcast Estimated Actual" | head -1
curl -s "http://localhost:5001/forecast/hourly.json?date=2020-01-01" | python3 -m json.tool | grep -A2 '"solcast_actuals"'
```

Confirm:
- The page's script references `aggregateSolcastActualsHourly` and the literal string
  `"Solcast Estimated Actual"`.
- `/forecast/hourly.json?date=2020-01-01` (a past date) includes a `"solcast_actuals"` key with
  `"available"` and `"periods"` fields (will show `"available": false, "periods": []` in dry-run
  without real data - that's expected and correct; the key's presence is what this checks).
- `/forecast/hourly.json?date=<today's date>` does **not** show `"solcast_actuals"` as available
  (structurally it'll still have the key with `available: false` per the shape above - confirm it's
  `false`, not omitted, matching Task 3's always-present-but-gated shape).

Stop the server afterward using its exact PID (`lsof -i :5001 -sTCP:LISTEN -t`, then `kill <pid>` -
never a broad `pkill -f`).

- [ ] **Step 6: Commit**

```bash
git add templates/forecast.html
git commit -m "forecast.html: render Solcast Estimated Actual (chart line + table column)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```
