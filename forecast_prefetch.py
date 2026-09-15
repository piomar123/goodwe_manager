"""
forecast_prefetch.py
Background thread that refreshes all three PV forecast/actuals sources
(Meteosource forecast, Solcast forecast, Solcast estimated_actuals) on two
independent schedules, writing every fetch into forecast_history's snapshot
table - see https://github.com/piomar123/goodwe_manager/pull/26 for the
design rationale (both the original schedule and this one's rework) - the
design spec docs were removed from the tree, but are still visible in that
PR's history.

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
