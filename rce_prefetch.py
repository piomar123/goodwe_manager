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
