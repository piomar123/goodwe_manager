# RCE Price Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Cap every subagent's model at Sonnet (do not use Opus).

**Goal:** Add a local SQLite cache (`rce_prices.db`) for RCE 15-minute electricity prices, with `get_rce_15min(date)` in `rce.py` as the single read/write-through entry point, plus a background thread that prefetches tomorrow's prices at 14:15 local time.

**Architecture:** A new `rce_storage.py` module owns the `rce_prices.db` schema and low-level read/write functions (mirrors `storage.py`'s role for `data.db`, but kept separate since it's a different file with a different write pattern/lifecycle). `rce.py` gains `get_rce_15min(date)`, which checks `rce_prices_fetched` for a completeness marker, and on a miss calls the existing live `query_pse_rce_15min()` then writes through. Every existing direct call to `query_pse_rce_15min` (in `main.py`'s two `/prices` routes and `rce.py`'s own `query_pse_rce` hourly-averaging wrapper and its CLI `main()`) is replaced with `get_rce_15min`, so there is exactly one path into the cache. A new `rce_prefetch.py` module provides a `threading.Thread` subclass, modeled on `main.py`'s existing `AsyncioThread`, plus pure/injectable helper functions (`seconds_until`, `past_cutoff`, `run_prefetch_cycle`) so the wake/retry/cutoff timing logic is unit-testable without real sleeping or real clocks.

**Tech Stack:** Python 3, stdlib `sqlite3` (sync, like `main.py`'s existing `/history/*.json` routes — no `aiosqlite` needed here, this cache is written from both sync Flask request threads and a plain background thread, not the asyncio loop), stdlib `threading`, stdlib `unittest`.

**Spec:** `docs/superpowers/specs/2026-08-27-sqlite-storage-design.md`, section "RCE price cache (`rce_prices.db`)" — read it alongside this plan.

## Global Constraints

- Two tables in `rce_prices.db`: `rce_prices (business_date, period, rce_pln)` PK `(business_date, period)`, and `rce_prices_fetched (business_date PK, period_count, fetched_at)`.
- A cache hit is defined as "a marker row exists in `rce_prices_fetched` for this `business_date`" — **never** a row-count check against `rce_prices` (Poland's DST transitions mean a valid day has 92 or 100 periods, not always 96).
- `get_rce_15min(date)` in `rce.py` is the single read/write-through entry point. Every direct call to `query_pse_rce_15min` elsewhere in the codebase must be replaced with it, so no route can bypass the cache.
- Any live fetch (`query_pse_rce_15min` call), from whichever caller triggered it, writes through via `INSERT OR REPLACE` into `rce_prices` plus inserting the `rce_prices_fetched` marker.
- Prefetch thread: wakes at **14:15** local time, calls `get_rce_15min(tomorrow)`. "Not published yet" retried every **15 minutes** (expected daily occurrence — log at info/debug, not error). Gives up at **20:00** cutoff with an `error` log. No new scheduler dependency — a plain `threading.Thread`, matching the existing `AsyncioThread` pattern in `main.py`.
- The read path's live-fetch fallback on a cache miss means `/prices` keeps working even if the prefetch thread is broken — do not make any route depend on the prefetch thread having already run.
- `rce_prices.db` (and its `-wal`/`-shm` siblings) must be gitignored, matching `data.db`'s existing entries.

---

## File Structure

- **Create `rce_storage.py`** — schema + low-level read/write functions for `rce_prices.db`: `init_db`, `is_cached`, `get_cached_prices`, `store_prices`.
- **Create `tests/test_rce_storage.py`** — unit tests for the above against a temp DB file.
- **Modify `rce.py`** — add `get_rce_15min(date)`; rewire `query_pse_rce()` and the CLI `main()` to call it instead of `query_pse_rce_15min` directly.
- **Create `tests/test_rce.py`** — unit tests for `get_rce_15min`'s cache-hit/cache-miss/write-through behavior, with `query_pse_rce_15min` mocked out (no real network calls).
- **Modify `main.py`** — swap the two direct `query_pse_rce_15min` calls (`/prices/rce.json`, `/prices/rce.png`) for `get_rce_15min`; wire up `RcePrefetchThread` start/stop alongside the existing `asyncio_thread`.
- **Modify `.gitignore`** — add `/rce_prices.db`, `/rce_prices.db-wal`, `/rce_prices.db-shm`.
- **Create `rce_prefetch.py`** — `seconds_until`, `past_cutoff`, `run_prefetch_cycle` pure/injectable helpers, plus `RcePrefetchThread`.
- **Create `tests/test_rce_prefetch.py`** — unit tests for the helpers and the retry/cutoff/stop behavior of `run_prefetch_cycle`, using fake clocks/sleep functions (no real waiting).

---

### Task 1: `rce_storage.py` — schema and read/write functions

**Files:**
- Create: `rce_storage.py`
- Test: `tests/test_rce_storage.py`

**Interfaces:**
- Produces: `RCE_DB_PATH: str` (module-level default path, `'rce_prices.db'`); `init_db(path: str = None) -> sqlite3.Connection` (resolves `path or RCE_DB_PATH` **inside the function body**, not as a bound default parameter, so tests can monkeypatch `rce_storage.RCE_DB_PATH` and have callers that use the default pick it up); `is_cached(conn: sqlite3.Connection, business_date: str) -> bool`; `get_cached_prices(conn: sqlite3.Connection, business_date: str) -> list[tuple[str, float]]`; `store_prices(conn: sqlite3.Connection, business_date: str, series: list[tuple[str, float]]) -> None`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_rce_storage.py`:

```python
import os
import tempfile
import unittest

import rce_storage


class RceStorageTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)  # sqlite3.connect creates it fresh
        self.conn = rce_storage.init_db(self.db_path)

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_init_db_is_idempotent(self):
        # calling it again on the same file must not raise
        rce_storage.init_db(self.db_path).close()

    def test_is_cached_false_when_no_marker(self):
        self.assertFalse(rce_storage.is_cached(self.conn, '2026-01-01'))

    def test_store_prices_then_is_cached_true(self):
        series = [('00:00', 100.0), ('00:15', 110.0), ('24:00', 110.0)]
        rce_storage.store_prices(self.conn, '2026-01-01', series)
        self.assertTrue(rce_storage.is_cached(self.conn, '2026-01-01'))

    def test_store_prices_then_get_cached_prices_round_trips(self):
        series = [('00:00', 100.0), ('00:15', 110.5), ('00:30', 90.25), ('24:00', 90.25)]
        rce_storage.store_prices(self.conn, '2026-01-01', series)
        result = rce_storage.get_cached_prices(self.conn, '2026-01-01')
        self.assertEqual(result, series)

    def test_get_cached_prices_only_returns_the_requested_date(self):
        rce_storage.store_prices(self.conn, '2026-01-01', [('00:00', 1.0), ('24:00', 1.0)])
        rce_storage.store_prices(self.conn, '2026-01-02', [('00:00', 2.0), ('24:00', 2.0)])
        result = rce_storage.get_cached_prices(self.conn, '2026-01-02')
        self.assertEqual(result, [('00:00', 2.0), ('24:00', 2.0)])

    def test_store_prices_is_idempotent_via_insert_or_replace(self):
        rce_storage.store_prices(self.conn, '2026-01-01', [('00:00', 1.0)])
        rce_storage.store_prices(self.conn, '2026-01-01', [('00:00', 2.0)])
        result = rce_storage.get_cached_prices(self.conn, '2026-01-01')
        self.assertEqual(result, [('00:00', 2.0)])

    def test_dst_spring_92_periods_caches_correctly(self):
        # spring-forward day: 92 quarter-hour periods instead of the usual 96
        series = [(f'{h:02}:{m:02}', float(h * 4 + m // 15)) for h in range(23) for m in (0, 15, 30, 45)]
        series.append(('24:00', series[-1][1]))
        self.assertEqual(len(series), 93)
        rce_storage.store_prices(self.conn, '2026-03-29', series)
        self.assertTrue(rce_storage.is_cached(self.conn, '2026-03-29'))
        self.assertEqual(rce_storage.get_cached_prices(self.conn, '2026-03-29'), series)

    def test_dst_fall_100_periods_caches_correctly(self):
        # fall-back day: 100 quarter-hour periods instead of the usual 96
        # (a normal day's 96, plus one repeated hour's worth of 4 quarters).
        # The repeated hour uses synthetic non-colliding labels ('23:46'..
        # '23:49', sorting between '23:45' and '24:00') since real PSE
        # period-label text for the repeated hour isn't specified by this
        # design - this test only exercises the storage layer's ability to
        # hold a 100-period business_date under the (business_date, period)
        # primary key and get_cached_prices' ORDER BY period returning rows
        # in the same order they were stored, not real PSE collision
        # behavior for that hour.
        series = [(f'{h:02}:{m:02}', float(h * 4 + m // 15)) for h in range(24) for m in (0, 15, 30, 45)]
        series += [(f'23:{46 + i}', 99.0) for i in range(4)]
        series.append(('24:00', series[-1][1]))
        self.assertEqual(len(series), 101)
        rce_storage.store_prices(self.conn, '2026-10-25', series)
        self.assertTrue(rce_storage.is_cached(self.conn, '2026-10-25'))
        self.assertEqual(rce_storage.get_cached_prices(self.conn, '2026-10-25'), series)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_rce_storage.py -v`
Expected: FAIL (or collection error) — `rce_storage` module doesn't exist yet.

- [ ] **Step 3: Write the implementation**

Create `rce_storage.py`:

```python
"""
SQLite cache for RCE 15-minute electricity prices (rce_prices.db).
See docs/superpowers/specs/2026-08-27-sqlite-storage-design.md,
"RCE price cache" section.
"""
import sqlite3
import time
from typing import List, Optional, Tuple

RCE_DB_PATH = 'rce_prices.db'


def init_db(path: Optional[str] = None) -> sqlite3.Connection:
    """Opens (creating if needed) the RCE price cache DB and ensures both
    tables exist. `path` defaults to the module-level RCE_DB_PATH, looked
    up here inside the function body (not as a bound default parameter
    value) so tests can monkeypatch rce_storage.RCE_DB_PATH and have
    default-path callers pick up the patched value.
    """
    conn = sqlite3.connect(path or RCE_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rce_prices (
            business_date TEXT NOT NULL,
            period TEXT NOT NULL,
            rce_pln REAL NOT NULL,
            PRIMARY KEY (business_date, period)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rce_prices_fetched (
            business_date TEXT PRIMARY KEY,
            period_count INTEGER NOT NULL,
            fetched_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    return conn


def is_cached(conn: sqlite3.Connection, business_date: str) -> bool:
    """A cache hit is "a marker row exists in rce_prices_fetched for this
    business_date" - deliberately NOT a row-count check against
    rce_prices, since Poland's DST transitions mean a valid day has 92 or
    100 periods instead of always 96.
    """
    row = conn.execute(
        "SELECT 1 FROM rce_prices_fetched WHERE business_date = ?",
        (business_date,),
    ).fetchone()
    return row is not None


def get_cached_prices(conn: sqlite3.Connection, business_date: str) -> List[Tuple[str, float]]:
    rows = conn.execute(
        "SELECT period, rce_pln FROM rce_prices WHERE business_date = ? ORDER BY period",
        (business_date,),
    ).fetchall()
    return [(period, rce_pln) for period, rce_pln in rows]


def store_prices(conn: sqlite3.Connection, business_date: str, series: List[Tuple[str, float]]) -> None:
    """Write-through cache store: INSERT OR REPLACE every (period, rce_pln)
    row, plus the rce_prices_fetched completeness marker, committed
    together. period_count just records how many periods were fetched
    (92/96/100 depending on DST) - it's never asserted against later, only
    used for observability.
    """
    conn.executemany(
        "INSERT OR REPLACE INTO rce_prices (business_date, period, rce_pln) VALUES (?, ?, ?)",
        [(business_date, period, rce_pln) for period, rce_pln in series],
    )
    conn.execute(
        "INSERT OR REPLACE INTO rce_prices_fetched (business_date, period_count, fetched_at) VALUES (?, ?, ?)",
        (business_date, len(series), int(time.time())),
    )
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_rce_storage.py -v`
Expected: PASS (all 8 tests)

- [ ] **Step 5: Add `.gitignore` entries**

Append to `.gitignore` (near the existing `/data.db` entries):

```
/rce_prices.db
/rce_prices.db-wal
/rce_prices.db-shm
```

- [ ] **Step 6: Commit**

```bash
git add rce_storage.py tests/test_rce_storage.py .gitignore
git commit -m "Add rce_storage.py: rce_prices.db schema and read/write functions"
```

---

### Task 2: `get_rce_15min(date)` — cache-backed entry point in `rce.py`

**Files:**
- Modify: `rce.py`
- Test: `tests/test_rce.py`

**Interfaces:**
- Consumes: `rce_storage.init_db`, `rce_storage.is_cached`, `rce_storage.get_cached_prices`, `rce_storage.store_prices`, `rce_storage.RCE_DB_PATH` (from Task 1).
- Produces: `get_rce_15min(query_date: datetime.date) -> list[tuple[str, float]]` in `rce.py` — the single read/write-through entry point later tasks (Task 3, Task 4) call instead of `query_pse_rce_15min`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_rce.py`:

```python
import os
import tempfile
import unittest
from datetime import date
from unittest.mock import patch

import rce
import rce_storage


class GetRce15MinTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        patcher = patch.object(rce_storage, 'RCE_DB_PATH', self.db_path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_cache_miss_calls_live_fetch_and_writes_through(self):
        series = [('00:00', 100.0), ('00:15', 110.0), ('24:00', 110.0)]
        with patch.object(rce, 'query_pse_rce_15min', return_value=series) as mock_fetch:
            result = rce.get_rce_15min(date(2026, 1, 1))
        self.assertEqual(result, series)
        mock_fetch.assert_called_once_with(date(2026, 1, 1))
        conn = rce_storage.init_db()
        try:
            self.assertTrue(rce_storage.is_cached(conn, '2026-01-01'))
            self.assertEqual(rce_storage.get_cached_prices(conn, '2026-01-01'), series)
        finally:
            conn.close()

    def test_cache_hit_does_not_call_live_fetch(self):
        conn = rce_storage.init_db()
        series = [('00:00', 50.0), ('24:00', 50.0)]
        rce_storage.store_prices(conn, '2026-01-02', series)
        conn.close()

        with patch.object(rce, 'query_pse_rce_15min') as mock_fetch:
            result = rce.get_rce_15min(date(2026, 1, 2))
        mock_fetch.assert_not_called()
        self.assertEqual(result, series)

    def test_query_pse_rce_uses_the_cache(self):
        # query_pse_rce (hourly average) must go through get_rce_15min,
        # not call query_pse_rce_15min directly - so a second call for the
        # same date is served from cache with no network call.
        series = [(f'{h:02}:00', float(h)) for h in range(24)]
        series.append(('24:00', 23.0))
        with patch.object(rce, 'query_pse_rce_15min', return_value=series) as mock_fetch:
            rce.query_pse_rce(date(2026, 1, 3))
            rce.query_pse_rce(date(2026, 1, 3))
        mock_fetch.assert_called_once_with(date(2026, 1, 3))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_rce.py -v`
Expected: FAIL — `rce.get_rce_15min` doesn't exist yet, and `query_pse_rce` still calls `query_pse_rce_15min` directly (so the third test fails on the call-count assertion).

- [ ] **Step 3: Write the implementation**

In `rce.py`, add the import near the top (with the other local-module-free imports — this project has no other local imports in `rce.py` today, so add it right after the stdlib/third-party import block):

```python
import rce_storage
```

Add `get_rce_15min` directly after `query_pse_rce_15min`'s definition:

```python
def get_rce_15min(query_date: datetime.date) -> list[tuple[str, float]]:
    """Single read/write-through entry point for 15-minute RCE prices -
    every other direct call to query_pse_rce_15min in this codebase has
    been replaced with this function, so there's exactly one path into the
    cache.

    Cache hit (a marker row exists in rce_prices_fetched for this date):
    reads rce_prices.db, no network call. Cache miss: calls the live
    query_pse_rce_15min(), then INSERT OR REPLACEs the result into
    rce_prices.db (both the price rows and the completeness marker)
    before returning - so any live fetch, from whichever caller triggered
    it, gets cached.
    """
    business_date = query_date.strftime('%Y-%m-%d')
    conn = rce_storage.init_db()
    try:
        if rce_storage.is_cached(conn, business_date):
            return rce_storage.get_cached_prices(conn, business_date)
        series = query_pse_rce_15min(query_date)
        rce_storage.store_prices(conn, business_date, series)
        return series
    finally:
        conn.close()
```

Change `query_pse_rce` to call `get_rce_15min` instead of `query_pse_rce_15min`:

```python
def query_pse_rce(query_date: datetime.date) -> list[tuple[str, float]]:
    """
    Returns hourly RCE prices. To get 15-minute intervals use get_rce_15min().
    """
    rce_15min = get_rce_15min(query_date)
    # noinspection PyTypeChecker
    s = np.array_split(rce_15min, range(4, len(rce_15min), 4))
    return [(chunk[0][0].item(), np.mean([float(price) for _, price in chunk]).item()) for chunk in s]
```

In the CLI `main()`, change:

```python
    rce = query_pse_rce_15min(date)
```

to:

```python
    rce = get_rce_15min(date)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_rce.py -v`
Expected: PASS (all 3 tests)

- [ ] **Step 5: Run the full test suite to check for regressions**

Run: `python -m pytest tests/ -v`
Expected: PASS (no existing test imports/calls `query_pse_rce_15min` directly from `rce.py`'s CLI path, so this should be a clean pass — if `tests/test_calculate_income.py` mocks `rce.query_pse_rce`, verify it still passes since that function's internals changed but its signature/behavior didn't)

- [ ] **Step 6: Commit**

```bash
git add rce.py tests/test_rce.py
git commit -m "Add get_rce_15min cache-backed entry point; route query_pse_rce and CLI through it"
```

---

### Task 3: Wire `main.py`'s `/prices` routes to `get_rce_15min`

**Files:**
- Modify: `main.py`

**Interfaces:**
- Consumes: `rce.get_rce_15min` (from Task 2).

- [ ] **Step 1: Update the import**

In `main.py`, change:

```python
from rce import parse_date, plot_rce, setup_plot_style, query_pse_rce_15min
```

to:

```python
from rce import parse_date, plot_rce, setup_plot_style, get_rce_15min
```

- [ ] **Step 2: Update `get_prices_json`**

Change the body of `get_prices_json()` (currently `rce = query_pse_rce_15min(date)`) to:

```python
    rce = get_rce_15min(date)
```

- [ ] **Step 3: Update `get_prices_image`**

Change the body of `get_prices_image()` (currently `rce = query_pse_rce_15min(date)`) to:

```python
    rce = get_rce_15min(date)
```

- [ ] **Step 4: Manually verify no other reference to `query_pse_rce_15min` remains in `main.py`**

Run: `grep -n query_pse_rce_15min main.py`
Expected: no output.

- [ ] **Step 5: Run the full test suite**

Run: `python -m pytest tests/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add main.py
git commit -m "Route /prices/rce.json and /prices/rce.png through get_rce_15min"
```

---

### Task 4: `rce_prefetch.py` — background next-day prefetch thread

**Files:**
- Create: `rce_prefetch.py`
- Test: `tests/test_rce_prefetch.py`
- Modify: `main.py`

**Interfaces:**
- Consumes: `rce.get_rce_15min(date) -> list[tuple[str, float]]` (from Task 2) — raises `RuntimeError` when PSE hasn't published yet (see `query_pse_rce_15min`'s existing `raise RuntimeError(f"No data found for {date_yyyymmdd}")`).
- Produces: `rce_prefetch.WAKE_TIME`, `rce_prefetch.CUTOFF_TIME`, `rce_prefetch.RETRY_INTERVAL_SECONDS` (module constants); `seconds_until(now: datetime, target_time: time) -> float`; `past_cutoff(now: datetime, cutoff_time: time) -> bool`; `run_prefetch_cycle(fetch_fn, target_date, sleep_fn, now_fn=datetime.now, cutoff_time=CUTOFF_TIME, retry_interval_seconds=RETRY_INTERVAL_SECONDS, should_stop=lambda: False) -> bool`; `RcePrefetchThread` (a `threading.Thread` subclass with a no-arg constructor and a `finish()` method, mirroring `AsyncioThread.finish()`'s shape).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_rce_prefetch.py`:

```python
import unittest
from datetime import date, datetime, time as dtime, timedelta

import rce_prefetch


class SecondsUntilTest(unittest.TestCase):
    def test_target_later_today(self):
        now = datetime(2026, 1, 1, 10, 0, 0)
        result = rce_prefetch.seconds_until(now, dtime(14, 15))
        self.assertEqual(result, 4 * 3600 + 15 * 60)

    def test_target_already_passed_today_rolls_to_tomorrow(self):
        now = datetime(2026, 1, 1, 15, 0, 0)
        result = rce_prefetch.seconds_until(now, dtime(14, 15))
        expected = (datetime(2026, 1, 2, 14, 15, 0) - now).total_seconds()
        self.assertEqual(result, expected)

    def test_target_equal_to_now_rolls_to_tomorrow(self):
        now = datetime(2026, 1, 1, 14, 15, 0)
        result = rce_prefetch.seconds_until(now, dtime(14, 15))
        self.assertEqual(result, 24 * 3600)


class PastCutoffTest(unittest.TestCase):
    def test_before_cutoff(self):
        self.assertFalse(rce_prefetch.past_cutoff(datetime(2026, 1, 1, 19, 59), dtime(20, 0)))

    def test_at_cutoff(self):
        self.assertTrue(rce_prefetch.past_cutoff(datetime(2026, 1, 1, 20, 0), dtime(20, 0)))

    def test_after_cutoff(self):
        self.assertTrue(rce_prefetch.past_cutoff(datetime(2026, 1, 1, 20, 1), dtime(20, 0)))


class RunPrefetchCycleTest(unittest.TestCase):
    def test_succeeds_on_first_try(self):
        calls = []
        sleeps = []
        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=calls.append,
            target_date=date(2026, 1, 2),
            sleep_fn=sleeps.append,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
        )
        self.assertTrue(result)
        self.assertEqual(calls, [date(2026, 1, 2)])
        self.assertEqual(sleeps, [])

    def test_retries_on_not_yet_published_then_succeeds(self):
        attempts = {'n': 0}

        def fetch_fn(d):
            attempts['n'] += 1
            if attempts['n'] < 3:
                raise RuntimeError("No data found for 2026-01-02")

        sleeps = []
        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=fetch_fn,
            target_date=date(2026, 1, 2),
            sleep_fn=sleeps.append,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
        )
        self.assertTrue(result)
        self.assertEqual(attempts['n'], 3)
        self.assertEqual(sleeps, [rce_prefetch.RETRY_INTERVAL_SECONDS, rce_prefetch.RETRY_INTERVAL_SECONDS])

    def test_retries_on_unexpected_error_then_succeeds(self):
        attempts = {'n': 0}

        def fetch_fn(d):
            attempts['n'] += 1
            if attempts['n'] < 2:
                raise ConnectionError("network down")

        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=fetch_fn,
            target_date=date(2026, 1, 2),
            sleep_fn=lambda s: None,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
        )
        self.assertTrue(result)
        self.assertEqual(attempts['n'], 2)

    def test_gives_up_at_cutoff(self):
        clock = {'now': datetime(2026, 1, 1, 19, 59, 50)}

        def fetch_fn(d):
            raise RuntimeError("No data found")

        def sleep_fn(seconds):
            clock['now'] += timedelta(seconds=seconds)

        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=fetch_fn,
            target_date=date(2026, 1, 2),
            sleep_fn=sleep_fn,
            now_fn=lambda: clock['now'],
        )
        self.assertFalse(result)

    def test_should_stop_halts_the_loop_without_calling_fetch_fn(self):
        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=lambda d: (_ for _ in ()).throw(AssertionError("fetch_fn should not be called")),
            target_date=date(2026, 1, 2),
            sleep_fn=lambda s: None,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
            should_stop=lambda: True,
        )
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_rce_prefetch.py -v`
Expected: FAIL — `rce_prefetch` module doesn't exist yet.

- [ ] **Step 3: Write the implementation**

Create `rce_prefetch.py`:

```python
"""
Background next-day RCE price prefetch thread. Wakes at 14:15 local time
and calls get_rce_15min(tomorrow) so tomorrow's prices are already cached
before anyone requests /prices for it. "Not published yet" (PSE returns an
empty value list, surfaced by query_pse_rce_15min as a RuntimeError) is
logged at info and retried every 15 minutes - an expected daily occurrence,
not an error. Unexpected failures are logged at warning and retried on the
same cadence rather than crashing the thread. Gives up for the day at a
20:00 cutoff with an error log. Modeled on main.py's AsyncioThread pattern:
a plain daemon thread, no new scheduler dependency.

This is safe to fail: the read path (rce.get_rce_15min) always falls back
to a live fetch on a cache miss, so /prices for tomorrow keeps working
(just with one live PSE call) even if this whole thread is broken.

See docs/superpowers/specs/2026-08-27-sqlite-storage-design.md,
"RCE price cache" section.
"""
import logging
import threading
from datetime import datetime, time as dtime, timedelta

import rce

logger = logging.getLogger(__name__)

WAKE_TIME = dtime(14, 15)
CUTOFF_TIME = dtime(20, 0)
RETRY_INTERVAL_SECONDS = 15 * 60


def seconds_until(now: datetime, target_time: dtime) -> float:
    """Seconds from `now` until the next occurrence of `target_time` -
    today if it hasn't happened yet, tomorrow if it already has (or is
    happening right now).
    """
    target = datetime.combine(now.date(), target_time)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def past_cutoff(now: datetime, cutoff_time: dtime) -> bool:
    return now.time() >= cutoff_time


def run_prefetch_cycle(fetch_fn, target_date, sleep_fn, now_fn=datetime.now,
                       cutoff_time=CUTOFF_TIME, retry_interval_seconds=RETRY_INTERVAL_SECONDS,
                       should_stop=lambda: False) -> bool:
    """Repeatedly calls fetch_fn(target_date) until it succeeds, `cutoff_time`
    local time is reached for the day (gives up, logs error, returns
    False), or should_stop() becomes True (returns False without logging an
    error - this is a normal shutdown, not a failure to publish). A
    RuntimeError from fetch_fn (query_pse_rce_15min's "not published yet"
    signal) is logged at info and retried; any other exception is logged at
    warning and also retried, on the same cadence. Returns True on success.
    """
    while not should_stop():
        if past_cutoff(now_fn(), cutoff_time):
            logger.error(f"Giving up prefetching RCE prices for {target_date} - not published by cutoff")
            return False
        try:
            fetch_fn(target_date)
            logger.info(f"Prefetched RCE prices for {target_date}")
            return True
        except RuntimeError as e:
            logger.info(f"RCE prices for {target_date} not published yet: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error prefetching RCE prices for {target_date}: {e}")
        sleep_fn(retry_interval_seconds)
    return False


class RcePrefetchThread(threading.Thread):
    def __init__(self):
        super().__init__(name='RcePrefetchThread', daemon=True)
        self._should_stop = threading.Event()

    def run(self):
        while not self._should_stop.is_set():
            wait_seconds = seconds_until(datetime.now(), WAKE_TIME)
            if self._should_stop.wait(wait_seconds):
                return
            tomorrow = (datetime.now() + timedelta(days=1)).date()
            run_prefetch_cycle(
                fetch_fn=rce.get_rce_15min,
                target_date=tomorrow,
                sleep_fn=lambda seconds: self._should_stop.wait(seconds),
                should_stop=self._should_stop.is_set,
            )

    def finish(self):
        """Called from another thread to stop the prefetch thread, mirroring
        AsyncioThread.finish()'s shape."""
        logger.info("Finishing RCE prefetch thread...")
        self._should_stop.set()
        self.join(timeout=5)
        if self.is_alive():
            logger.warning("RCE prefetch thread did not stop within timeout")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_rce_prefetch.py -v`
Expected: PASS (all 9 tests)

- [ ] **Step 5: Wire the thread into `main.py`**

Add the import near the other local imports in `main.py`:

```python
from rce_prefetch import RcePrefetchThread
```

Add a module-level instance next to `asyncio_thread`:

```python
app = flask.Flask(__name__, static_url_path='/static')
asyncio_thread = AsyncioThread(inverter_address=INVERTER_IP, daemon=False)
rce_prefetch_thread = RcePrefetchThread()
```

In `main()`, start it alongside `asyncio_thread.start()`:

```python
    asyncio_thread.start()
    rce_prefetch_thread.start()
    # atexit.register(stop_threads)
    try:
        app.run('0.0.0.0', port=APP_PORT, debug=True, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down")
    finally:
        logger.info("Finishing the application...")
        asyncio_thread.finish()
        rce_prefetch_thread.finish()
        logger.info("Finished all threads")
```

- [ ] **Step 6: Run the full test suite**

Run: `python -m pytest tests/ -v`
Expected: PASS

- [ ] **Step 7: Manually verify the app still starts**

Run: `python main.py --dry-run` (Ctrl+C after a few seconds once you see the Flask startup log lines)
Expected: no traceback on startup; `rce_prefetch_thread` starts without error (it will just be sleeping until 14:15, nothing else observable at this point).

- [ ] **Step 8: Commit**

```bash
git add rce_prefetch.py tests/test_rce_prefetch.py main.py
git commit -m "Add background 14:15 next-day RCE prefetch thread"
```

---

## Self-Review Notes

- **Spec coverage:** schema (Task 1) ✓; completeness-marker cache-hit semantics, not row-count (Task 1, `is_cached`) ✓; `get_rce_15min` as single entry point (Task 2) ✓; every direct `query_pse_rce_15min` call site replaced — `main.py`'s two `/prices` routes (Task 3), `rce.py`'s `query_pse_rce` and CLI `main()` (Task 2) ✓; DST 92/100-period edge case (Task 1 tests) ✓; write-through applies to any live fetch regardless of caller (Task 2 test 3, `query_pse_rce` routing through `get_rce_15min`) ✓; prefetch thread wake/retry/cutoff timing (Task 4) ✓; no new scheduler dependency (plain `threading.Thread`) ✓; read-path fallback keeping `/prices` working even if prefetch is broken (inherent in Task 2's design — no route depends on the prefetch thread) ✓.
- **Spec text vs. actual code:** the spec's `/forecast` mention was verified against `forecast.py`/`main.py` and found stale — `/forecast` never calls any RCE function, so no task touches it. `_calculate_income.py`'s spec mention was verified too: it calls `query_pse_rce` (hourly), not `query_pse_rce_15min` directly, so it needs no code change — it becomes cache-backed automatically once `query_pse_rce` is rewired in Task 2 (confirmed by Task 2's `test_query_pse_rce_uses_the_cache`).
- **Placeholder scan:** no TBD/TODO/"add error handling"-style steps; every step has literal code.
- **Type consistency:** `get_rce_15min(query_date: datetime.date) -> list[tuple[str, float]]` matches `query_pse_rce_15min`'s existing return type exactly, so callers (`main.py`, `query_pse_rce`) don't need any other changes. `rce_storage`'s functions take a plain `sqlite3.Connection` throughout, consistent with `storage.py`'s existing sync-path pattern in this codebase.
