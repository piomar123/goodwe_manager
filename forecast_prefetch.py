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
