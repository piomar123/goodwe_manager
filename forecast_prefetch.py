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

Startup catch-up (run_catch_up): unlike rce_prefetch.py's read path (which
falls back to a live fetch on a cache miss), forecast_history.get_latest_merged
has no such fallback - a cold start (deploy, crash, dev restart) would
otherwise leave /forecast empty until the next scheduled wake time above.
ForecastPrefetchThread.run calls run_catch_up once before entering the
loop: per source, it fetches immediately only if nothing has been written
since the most recent wake time that should already have fired, so a
quick restart costs nothing extra and only a genuine cold start spends
quota. Solcast actuals catch-up never writes today - see
fetch_and_store_solcast_actuals's max_date - to avoid ever permanently
stranding a day with only a partial snapshot.

Shared fetched_at per wake-up: Meteosource and Solcast forecast are two
separate API round-trips (fetch_and_store_solcast alone makes two, for
east+west), so fetching them back-to-back at the same wake-up used to
still give them different fetched_at values a few seconds apart (each
write_snapshot call defaulting its own to time.time() at write time) -
hit in production as two near-identical forecast_history dropdown entries
for one wake-up, each with only one source's data, the other showing an
empty chart/table. Both the loop below and run_catch_up now compute one
epoch (`fetch_epoch`) per wake-up/catch-up and pass it to every
fetch_and_store_* call for that cycle, so sources fetched together land
under one shared fetched_at.
"""
import logging
import os
import threading
from datetime import datetime, time as dtime, timedelta
from typing import Optional

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


def last_wake_time(now: datetime, wake_times=FORECAST_WAKE_TIMES) -> datetime:
    """The most recent occurrence of any of `wake_times` at-or-before `now`
    - today's if one has already happened (or is happening right now),
    else the last one from yesterday. Mirrors next_wake_time's "soonest
    strictly after" but looking backward - used by the startup catch-up to
    find the threshold a source's last snapshot should be newer than."""
    candidates = []
    for wake_time in wake_times:
        candidate = datetime.combine(now.date(), wake_time)
        if candidate > now:
            candidate -= timedelta(days=1)
        candidates.append(candidate)
    return max(candidates)


def fetch_and_store_meteosource(conn, date_yyyymmdd: str, now: Optional[int] = None) -> None:
    """`now`, if given, is passed straight through as write_snapshot's
    fetched_at - see fetch_and_store_solcast's docstring for why callers
    that fetch multiple sources for the same wake-up share one `now`
    rather than letting each write_snapshot call default its own."""
    payload = forecast.fetch_pv_production_forecast_combined_hourly_kwh(date_yyyymmdd)
    forecast_history.write_snapshot(conn, 'meteosource', date_yyyymmdd, payload, now=now)


def fetch_and_store_solcast(conn, now: Optional[int] = None) -> None:
    """`now`, if given, is used as every written snapshot's fetched_at
    (both the per-date writes below, and - when a caller passes the same
    `now` to fetch_and_store_meteosource for the same wake-up - Meteosource's
    too). Without this, each write_snapshot call defaults its own fetched_at
    to the wall-clock moment it runs; Meteosource and Solcast are two
    separate API round-trips (this one itself makes two, east+west) a few
    seconds apart, which used to split one wake-up into two forecast_history
    dropdown entries where only one source has data at either - a real bug
    hit in production, not a hypothetical."""
    east = solcast.fetch_solcast_forecast_30min(os.environ['SOLCAST_SITE_EAST_ID'])
    west = solcast.fetch_solcast_forecast_30min(os.environ['SOLCAST_SITE_WEST_ID'])
    combined_by_date = solcast.sum_sites(east, west)
    for date_str, payload in combined_by_date.items():
        forecast_history.write_snapshot(conn, 'solcast', date_str, payload, now=now)


def fetch_and_store_solcast_actuals(conn, max_date: Optional[str] = None, now: Optional[int] = None) -> None:
    """`max_date` (YYYY-MM-DD), if given, skips writing any date >=
    max_date - used by run_catch_up (with max_date=today) so a restart
    mid-day never writes a partial "today" snapshot: if that were the only
    actuals fetch of the day and the service went down again before the
    normal 23:00 slot, the day would be stuck permanently incomplete once
    it's displayed as "yesterday" (get_latest_merged only has that partial
    snapshot to merge from). The regular scheduled call (inside
    ForecastPrefetchThread.run's loop) always omits max_date - by 23:00
    "today" is complete-enough (see module docstring), so today's actuals
    stay the normal slot's job, never catch-up's. `now`, if given, is used
    as every written date's fetched_at - see fetch_and_store_solcast's
    docstring for why."""
    east = solcast.fetch_solcast_estimated_actuals_30min(os.environ['SOLCAST_SITE_EAST_ID'])
    west = solcast.fetch_solcast_estimated_actuals_30min(os.environ['SOLCAST_SITE_WEST_ID'])
    combined_by_date = solcast.sum_sites_flat(east, west)
    for date_str, payload in combined_by_date.items():
        if max_date is not None and date_str >= max_date:
            continue
        forecast_history.write_snapshot(conn, 'solcast_actuals', date_str, payload, now=now)


def run_catch_up(conn, now: datetime) -> None:
    """Runs once at thread startup, before the normal wake-time loop below:
    fetches whatever's stale so a restart (deploy, crash, dev testing)
    doesn't leave /forecast empty until the next scheduled wake time.
    Per-source, "stale" means no snapshot has been written since the most
    recent wake time that should already have fired (last_wake_time) - a
    quick restart minutes after a real fetch finds everything fresh and
    fetches nothing extra; a genuine cold start (long downtime, fresh
    install) fetches whatever's missing once. Solcast actuals catch-up
    always passes max_date=today (see fetch_and_store_solcast_actuals) -
    today's actuals stay the normal 23:00 slot's job. Failures are logged
    and don't block the other catch-up fetches, same "safe to fail"
    framing as the scheduled fetches in the loop below.

    Meteosource is the one exception to "per-source staleness": it fetches
    whenever it's individually stale OR Solcast is about to fetch (even if
    Meteosource alone looks fresh). Meteosource has no meaningful rate
    limit, unlike Solcast's scarce daily quota, so there's no reason to
    ever skip it when we're already paying for a Solcast call in the same
    cycle - doing so only left them on different fetched_at values for no
    savings (hit in production: a Solcast-only catch-up snapshot with no
    Meteosource counterpart at that timestamp, see forecast.html's
    Meteosource-driven chart/table backbone for why that's worse than it
    sounds)."""
    today = now.strftime('%Y-%m-%d')
    fetch_epoch = int(now.timestamp())

    forecast_threshold = last_wake_time(now, FORECAST_WAKE_TIMES).timestamp()
    meteosource_stale = not forecast_history.has_fetched_since(conn, 'meteosource', forecast_threshold)
    solcast_stale = not forecast_history.has_fetched_since(conn, 'solcast', forecast_threshold)
    if meteosource_stale or solcast_stale:
        try:
            fetch_and_store_meteosource(conn, today, now=fetch_epoch)
            logger.info("Catch-up: fetched Meteosource forecast")
        except Exception as e:
            logger.warning(f"Catch-up Meteosource fetch failed: {e}")
    if solcast_stale:
        try:
            fetch_and_store_solcast(conn, now=fetch_epoch)
            logger.info("Catch-up: fetched Solcast forecast")
        except Exception as e:
            logger.warning(f"Catch-up Solcast forecast fetch failed: {e}")

    actuals_threshold = last_wake_time(now, (ACTUALS_WAKE_TIME,)).timestamp()
    if not forecast_history.has_fetched_since(conn, 'solcast_actuals', actuals_threshold):
        try:
            fetch_and_store_solcast_actuals(conn, max_date=today, now=fetch_epoch)
            logger.info("Catch-up: fetched Solcast estimated actuals (through yesterday)")
        except Exception as e:
            logger.warning(f"Catch-up Solcast estimated-actuals fetch failed: {e}")


class ForecastPrefetchThread(threading.Thread):
    def __init__(self, db_path=None):
        super().__init__(name='ForecastPrefetchThread', daemon=True)
        self._should_stop = threading.Event()
        self._db_path = db_path

    def run(self):
        conn = forecast_history.init_db(self._db_path)
        try:
            run_catch_up(conn, datetime.now())
            while not self._should_stop.is_set():
                now = datetime.now()
                next_forecast = next_wake_time(now, FORECAST_WAKE_TIMES)
                next_actuals = next_wake_time(now, (ACTUALS_WAKE_TIME,))
                next_wake = min(next_forecast, next_actuals)
                wait_seconds = (next_wake - now).total_seconds()
                if self._should_stop.wait(wait_seconds):
                    return
                # One shared moment for whichever branch(es) fire below - a
                # single instant, not re-read per branch, so a coincidental
                # tie (next_wake == both) still gives forecast and actuals
                # the same fetched_at rather than two calls to datetime.now().
                wake_time = datetime.now()
                if next_wake == next_forecast:
                    today = wake_time.strftime('%Y-%m-%d')
                    fetch_epoch = int(wake_time.timestamp())
                    try:
                        fetch_and_store_meteosource(conn, today, now=fetch_epoch)
                        logger.info(f"Prefetched Meteosource forecast for {today}")
                    except Exception as e:
                        logger.warning(f"Meteosource prefetch failed: {e}")
                    try:
                        fetch_and_store_solcast(conn, now=fetch_epoch)
                        logger.info("Prefetched Solcast forecast")
                    except Exception as e:
                        logger.warning(f"Solcast prefetch failed: {e}")
                if next_wake == next_actuals:
                    try:
                        fetch_and_store_solcast_actuals(conn, now=int(wake_time.timestamp()))
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
