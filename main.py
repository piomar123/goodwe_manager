import asyncio
import concurrent.futures
import contextlib
import io
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Mapping, Tuple

import aiosqlite
import dotenv
import flask
import goodwe
import matplotlib
from flask import request
from goodwe.sensor import EcoModeV2

import eco_encoder
import forecast
import forecast_history
import history
import storage
from announcer import MessageAnnouncer
from error_logging import install_uncaught_exception_logging
from forecast_prefetch import ForecastPrefetchThread
from rce import parse_date, plot_rce, setup_plot_style, get_rce_15min
from rce_prefetch import RcePrefetchThread
from sensors import SELECTED_SENSORS, CalculatedValuesEvaluator, sensor_columns

dotenv.load_dotenv()
INVERTER_IP = os.environ.get('INVERTER_IP')
assert INVERTER_IP, "INVERTER_IP environment variable is not set, copy .env.example to .env and set it"
APP_PORT = int(os.environ.get('APP_PORT', 5000))
# Backup output above this is treated as real usage rather than CT-crosstalk
# noise (see docs/superpowers/notes/2026-09-08-backup-threshold-investigation.md -
# a flat constant for now, pending the adaptive-threshold formula explored
# there).
BACKUP_ACTIVE_THRESHOLD_W = float(os.environ.get('BACKUP_ACTIVE_THRESHOLD_W', 35))

# https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
# https://gist.github.com/werediver/4358735?permalink_comment_id=3421708

logger = logging.getLogger(__name__)
announcer = MessageAnnouncer()
dry_run = False

EVERY_DAY = 0b1111111
EVERY_DAY_STR = 'all'


@contextlib.contextmanager
def _data_db_connection():
    """A short-lived, synchronous connection to data.db, always closed on
    the way out - every route/helper here that isn't using the shared
    aiosqlite connection needs exactly this (connect, use, close), which
    was previously duplicated as its own try/finally at each call site.
    Note this only closes the connection, unlike sqlite3.Connection's own
    `with conn:` context manager, which manages the transaction (commit/
    rollback) but doesn't close anything.
    """
    conn = sqlite3.connect(storage.DATA_DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


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


class AsyncioThread(threading.Thread):
    _asyncio_loop: Optional[asyncio.AbstractEventLoop] = None
    _inverter: Optional[goodwe.Inverter] = None
    _db_conn: Optional[aiosqlite.Connection] = None
    _should_stop = threading.Event()
    _calculated_values_evaluator = CalculatedValuesEvaluator()
    # Safety valve for the hour-rollover backfill retry loop (see
    # _get_inverter_data): normally a pending hour verifies within a retry
    # or two, but if it never can (e.g. a whole hour got silently skipped -
    # its own next-hour proof-bucket then never exists either), retrying
    # every ~1s forever would be an unbounded cost for a gap that was never
    # going to be recoverable anyway. ~5 minutes at the loop's ~1s cadence
    # is generous - past that, give up and move on instead of spinning.
    _PENDING_BACKFILL_RETRY_LIMIT = 300

    def __init__(self,
                 group=None,
                 target=None,
                 name=None,
                 args=(),
                 kwargs=None,
                 *,
                 daemon=None,
                 inverter_address: str) -> None:
        super().__init__(group, target, name if name is not None else 'AsyncioThread', args, kwargs, daemon=daemon)
        self._inverter_address = inverter_address

    def run(self):
        loop = asyncio.new_event_loop()
        self._asyncio_loop = loop
        asyncio.set_event_loop(loop)
        try:
            if not dry_run:
                loop.create_task(self._get_inverter_data_with_retry())
            loop.run_forever()
        finally:
            self._drain_and_close_loop(loop)
            logger.info("Finished the asyncio loop")

    @property
    def loop(self):
        return self._asyncio_loop

    @property
    def inverter(self):
        return self._inverter

    def run_coroutine_threadsafe(self, coro) -> concurrent.futures.Future:
        """Run a coroutine from another thread in the asyncio loop and return a Future"""
        loop = self._asyncio_loop
        if loop is None:
            raise RuntimeError('The asyncio loop is not running')
        return asyncio.run_coroutine_threadsafe(coro, loop)

    def finish(self):
        """Called from another thread to finish and stop the asyncio loop"""
        logger.info("Finishing asyncio loop...")
        self._should_stop.set()
        loop = self._asyncio_loop
        if loop is None:
            return
        try:
            stop_future = asyncio.run_coroutine_threadsafe(self._stop_event_loop(), loop)
            stop_future.result(timeout=5)
        except concurrent.futures.TimeoutError:
            logger.warning("Timed out while requesting asyncio loop stop")
        except RuntimeError:
            # Loop is already closed or closing.
            return
        logger.info("Waiting for the asyncio loop finish result...")
        self.join(timeout=30)
        if self.is_alive():
            logger.warning("Asyncio thread did not stop within timeout")

    @staticmethod
    async def _stop_event_loop():
        asyncio.get_running_loop().stop()

    @staticmethod
    def _drain_and_close_loop(loop: asyncio.AbstractEventLoop):
        logger.info("Waiting for the asyncio tasks to finish...")
        pending = asyncio.all_tasks(loop)
        if pending:
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        logger.info("Stopping and closing the asyncio loop")
        loop.close()

    async def _get_inverter_data_with_retry(self):
        while True:
            try:
                if self._should_stop.is_set():
                    logger.info("Stopping the inverter communication routine")
                    return
                await self._get_inverter_data()
                logger.info("Finished the inverter communication routine")
                return
            except Exception as e:
                self._inverter = None
                logger.error(f'Error in the inverter communication routine: {e}')
                await asyncio.sleep(5)

    async def _get_inverter_data(self):
        logger.info(f'Connecting to {self._inverter_address}')
        self._inverter = await goodwe.connect(self._inverter_address, family='ET', timeout=1, retries=60)
        logger.info(f'Connected to the inverter')
        self._db_conn = await storage.init_db_async(storage.DATA_DB_PATH, sensor_columns())
        try:
            await self._seed_hour_start_baseline()
            await self._backfill_hourly_summary()
            current_hour_start, _ = storage.current_hour_bounds(datetime.now())
            pending_backfill_retries = 0
            while True:
                read_start = time.monotonic()
                inverter_runtime = await self._inverter.read_runtime_data()
                read_done = time.monotonic()
                server_received_at = time.time()
                sensors_data = {sid: (None if (v := inverter_runtime.get(sid)) is None else str(v)) for sid in SELECTED_SENSORS}
                sensors_data_with_calculated = sensors_data | self._calculated_values_evaluator.calculate_values(sensors_data)
                await storage.insert_sample_async(self._db_conn, sensors_data_with_calculated)
                # Freshness/lag fields for the UI only - deliberately not
                # persisted (would need a schema/migration change), just
                # attached to the SSE payload. `_read_duration_seconds` isolates
                # how long the inverter itself took to answer (the wifi link
                # to it is the likely culprit for slow updates), separately
                # from `_server_received_at`, which the client uses to measure
                # its own delivery/render lag on top of that.
                announce_payload = sensors_data_with_calculated | {
                    '_read_duration_seconds': round(read_done - read_start, 3),
                    '_server_received_at': server_received_at,
                }
                announcer.announce(json.dumps(announce_payload))
                new_hour_start, _ = storage.current_hour_bounds(datetime.now())
                if new_hour_start != current_hour_start:
                    current_hour_start, pending_backfill_retries = await self._advance_hour_or_retry_backfill(
                        current_hour_start, new_hour_start, pending_backfill_retries)
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

    @staticmethod
    async def _backfill_hourly_summary(verify_hour_start: Optional[int] = None) -> bool:
        """Derives any newly-completed hourly_summary rows from
        inverter_history. Runs on a plain sqlite3 connection (not the shared
        aiosqlite one) via a worker thread, since storage.backfill_hourly_summary
        is synchronous and issues several queries per hour - a short-lived
        connection here avoids sharing sqlite3's not-thread-safe-by-default
        connection object with the asyncio loop's own aiosqlite connection.

        If verify_hour_start is given, returns whether that specific hour has
        a hourly_summary row now (whether this call just inserted it or it
        was already there beforehand) - used by the polling loop to know
        whether an hour-rollover's backfill attempt actually succeeded, since
        it can legitimately find nothing to do yet (see the loop's comment).
        Without verify_hour_start, always returns True.
        """
        def _run():
            with _data_db_connection() as conn:
                backfilled = storage.backfill_hourly_summary(conn)
                if verify_hour_start is None:
                    return backfilled, True
                row = conn.execute("SELECT 1 FROM hourly_summary WHERE hour_start = ?", (verify_hour_start,)).fetchone()
                return backfilled, row is not None

        backfilled, verified = await asyncio.to_thread(_run)
        if backfilled:
            logger.info(f"Backfilled {backfilled} hourly_summary row(s)")
        return verified

    async def _advance_hour_or_retry_backfill(self, current_hour_start: int, new_hour_start: int, pending_retries: int) -> tuple:
        """Called once per polling-loop iteration where the wall clock has
        rolled past current_hour_start. Returns the (current_hour_start,
        pending_retries) the loop should carry into its next iteration.

        current_hour_start's hour can be backfilled once inverter_history
        has a sample in the new hour, proving the old one is complete. On
        the very first iteration after the boundary that proof doesn't
        always exist yet: the sample inserted a few lines up in the caller
        can still belong to the *old* hour (insert happens a moment before
        the hour check), so backfill_hourly_summary correctly finds nothing
        to do. In that case current_hour_start is deliberately *not*
        adopted as new_hour_start, so the caller keeps calling this again
        on every following iteration (a couple of seconds) instead of only
        retrying at the *next* hour's rollover, up to an hour later, which
        is what an unconditional update used to do.

        pending_retries caps that retrying: if current_hour_start's hour can
        never be proven complete (e.g. it was itself silently skipped
        entirely - a single read_runtime_data() stall spanning more than an
        hour - so it can never gain its own proof-bucket), retrying forever
        would be an unbounded cost for a gap that was never recoverable
        anyway. Past _PENDING_BACKFILL_RETRY_LIMIT retries, give up and
        move on instead of spinning.
        """
        if await self._backfill_hourly_summary(verify_hour_start=current_hour_start):
            return new_hour_start, 0
        pending_retries += 1
        if pending_retries >= self._PENDING_BACKFILL_RETRY_LIMIT:
            logger.warning(
                f"Giving up waiting for hour {current_hour_start} to become "
                f"backfillable after {pending_retries} retries - moving on"
            )
            return new_hour_start, 0
        return current_hour_start, pending_retries

    def ensure_inverter_ready(self):
        if self._asyncio_loop is None:
            raise RuntimeError('The asyncio loop is not running')
        while self._inverter is None:
            logger.warning("The inverter is not connected yet")
            time.sleep(1)


app = flask.Flask(__name__, static_url_path='/static')
asyncio_thread = AsyncioThread(inverter_address=INVERTER_IP, daemon=False)
rce_prefetch_thread = RcePrefetchThread()
forecast_prefetch_thread = ForecastPrefetchThread()


@app.route('/')
def serve_index():
    return flask.render_template('index.html', backup_active_threshold_w=BACKUP_ACTIVE_THRESHOLD_W)


class EcoMode(Enum):
    CHARGE = 'charge'
    DISCHARGE = 'discharge'


class EcoSlot:
    TIME_PATTERN = re.compile(r'(\d|[01]\d|2[0-3]):([0-5]\d)')

    def __init__(self, index: int, on_off: bool, start_time: str, end_time: str, days: int, mode: EcoMode, power: int):
        if not self.TIME_PATTERN.fullmatch(start_time):
            raise ValueError(f'Invalid start time: {start_time}')
        if not self.TIME_PATTERN.fullmatch(end_time):
            raise ValueError(f'Invalid end time: {end_time}')
        if not days & 0b1111111:
            raise ValueError(f'Invalid days: {days:08b}')
        self.index = index
        self.on_off = on_off
        self.start_time = start_time
        self.end_time = end_time
        self.days = days
        self.mode = mode
        self.power = power

    @staticmethod
    def from_goodwe_eco(index: int, gw_eco: EcoModeV2) -> 'EcoSlot':
        return EcoSlot(index,
                       gw_eco.on_off < 0,
                       f'{gw_eco.start_h:02}:{gw_eco.start_m:02}',
                       f'{gw_eco.end_h:02}:{gw_eco.end_m:02}',
                       gw_eco.day_bits,
                       EcoMode.DISCHARGE if gw_eco.power >= 0 else EcoMode.CHARGE,
                       abs(gw_eco.get_power()))

    def to_goodwe_eco(self) -> EcoModeV2:
        gw_eco = EcoModeV2(f'eco_mode_{self.index}', -1, 'eco_mode')
        gw_eco.start_h, gw_eco.start_m = map(int, self.start_time.split(':'))
        gw_eco.end_h, gw_eco.end_m = map(int, self.end_time.split(':'))
        gw_eco.on_off = -1 if self.on_off else 0
        gw_eco.day_bits = self.days
        gw_eco.soc = 100
        gw_eco.power = self.power if self.mode == EcoMode.DISCHARGE else -self.power
        gw_eco.month_bits = 0
        return gw_eco

    def is_charge(self) -> bool:
        return self.mode == EcoMode.CHARGE

    def is_discharge(self) -> bool:
        return self.mode == EcoMode.DISCHARGE

    def get_days(self) -> str:
        if self.days == EVERY_DAY:
            return EVERY_DAY_STR
        return f'0b{self.days:07b}'

    def __str__(self):
        return f"{self.index}: {'ON' if self.on_off else 'OFF'} {self.start_time}-{self.end_time} {self.mode} {self.power}% on {self.days}"

    @staticmethod
    def to_days_int(s: str) -> int:
        s = s.strip().lower()
        if s == EVERY_DAY_STR:
            return EVERY_DAY
        elif s.startswith('0b'):
            return int(s, 2)
        elif s.startswith('0x'):
            return int(s, 16)
        else:
            return int(s)


@app.get('/eco')
def get_eco():
    logger.debug('Serving the eco page')
    asyncio_thread.ensure_inverter_ready()
    eco_mode_future: concurrent.futures.Future = asyncio_thread.run_coroutine_threadsafe(read_eco_settings())
    logger.debug('Waiting for the response from the inverter')
    eco_configs: list[EcoModeV2] = eco_mode_future.result(timeout=60)
    return flask.render_template('eco.html',
                                 eco_slots=[EcoSlot.from_goodwe_eco(i, cfg) for i, cfg in
                                            enumerate(eco_configs, start=1)])



@app.post('/eco/<int:index>')
def update_eco(index: int):
    logger.debug('Updating eco settings')
    asyncio_thread.ensure_inverter_ready()
    slot = EcoSlot(index,
                   'on_off' in request.form,
                   request.form['start_time'],
                   request.form['end_time'],
                   EcoSlot.to_days_int(request.form['days']),
                   EcoMode(request.form['mode']),
                   int(request.form['power']))
    logger.info(f"Updating eco mode {index}: {slot}")
    write_future = asyncio_thread.run_coroutine_threadsafe(write_eco_setting(slot))
    write_future.result(timeout=60)
    return flask.redirect('/eco')


async def read_eco_settings():
    logger.info('Reading the eco settings')
    return await asyncio.gather(*[asyncio_thread.inverter.read_setting(f'eco_mode_{i}') for i in range(1, 5)])


async def write_eco_setting(setting: EcoSlot):
    gw_eco: EcoModeV2 = setting.to_goodwe_eco()
    logger.info(f"Writing eco mode {gw_eco.id_}: {gw_eco}")
    encoded_bytes = eco_encoder.encode_schedule(gw_eco)
    await asyncio_thread.inverter.write_setting(gw_eco.id_, encoded_bytes)


_SETTING_KEY_TO_TYPE = {
    'unbalanced_output': bool,
    'shadow_scan': bool,
    'grid_export': bool,
    'grid_export_limit': int,
    'battery_discharge_depth': int,
    'battery_discharge_depth_offline': int,
    'dod_holding': bool,
    'fast_charging': bool,
    'fast_charging_power': int,
    'fast_charging_soc': int,
}


async def read_inverter_settings() -> dict:
    logger.info('Reading inverter config')
    setting_keys = _SETTING_KEY_TO_TYPE.keys()
    setting_values = await asyncio.gather(*[asyncio_thread.inverter.read_setting(c) for c in setting_keys])
    return {k: v for k, v in zip(setting_keys, setting_values)}


async def write_inverter_setting(setting: str, value: Any):
    logger.info(f"Writing setting {setting} with '{value}'")
    await asyncio_thread.inverter.write_setting(setting, value)


@app.get('/config')
def get_config():
    logger.debug('Serving the config page')
    asyncio_thread.ensure_inverter_ready()
    settings_future: concurrent.futures.Future[dict] = asyncio_thread.run_coroutine_threadsafe(read_inverter_settings())
    logger.debug('Waiting for the response from the inverter')
    settings: dict[str, Any] = settings_future.result(timeout=60)
    logger.info(settings)
    return flask.render_template('config.html', settings=settings)


@app.post('/config/<setting>')
def update_config(setting: str):
    logger.debug(f'Updating setting: {setting}')
    asyncio_thread.ensure_inverter_ready()
    value_type = _SETTING_KEY_TO_TYPE.get(setting)
    if value_type is None:
        raise ValueError(f"Unsupported setting: {setting}")
    if value_type == bool:
        value = 1 if 'on_off' in request.form else 0
    elif value_type == int:
        value = int(request.form['value'])
    else:
        raise ValueError(f"Unknown setting type: {value_type}")
    write_future = asyncio_thread.run_coroutine_threadsafe(write_inverter_setting(setting, value))
    write_future.result(timeout=60)
    return flask.redirect('/config')


@app.get('/prices')
def get_prices():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    logger.debug(f'Serving the RCE prices page for date: {date_yyyymmdd}')
    return flask.render_template('prices.html', date=date_yyyymmdd)


@app.get('/prices/rce.json')
def get_prices_json():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    logger.debug(f'Serving RCE prices JSON for date: {date_yyyymmdd}')
    rce = get_rce_15min(date)
    return flask.jsonify({
        'date': date_yyyymmdd,
        'series': [{'time': time, 'price': price} for time, price in rce],
    })


@app.get('/prices/rce.png')
def get_prices_image():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    logger.debug('Generating RCE prices images for date: {date_yyyymmdd}')
    rce = get_rce_15min(date)
    logger.debug(rce)
    fig = plot_rce(rce, date_yyyymmdd)
    output_io = io.BytesIO()
    fig.savefig(output_io, format='png')
    fig.clear()
    return flask.Response(output_io.getvalue(), mimetype='image/png')


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


def _accuracy_delta_pct(forecast_total, actual_total):
    """Δ = round((forecast_total - actual_total) / actual_total * 100),
    signed - positive means the forecast overestimated, negative means it
    underestimated. None if actual_total is falsy (0 or None) - a real
    zero-production day (e.g. total snow cover) can't be divided into, and
    get_forecast() already only passes a real actual_total when one is
    computable (see its own gating logic)."""
    if not actual_total:
        return None
    return round((forecast_total - actual_total) / actual_total * 100)


def _build_forecast_summary(meteosource_total, solcast_totals, actual_total):
    r"""meteosource_total: day-total Meteosource kWh. solcast_totals:
    (c10_total, c50_total, c90_total) tuple, or None if Solcast has no data
    for this date. actual_total: day-total real Actual kWh if the accuracy
    delta is computable for this date (a fully elapsed past day with all 24
    hours recorded - see get_forecast's gating), else None to omit deltas
    entirely. Returns the summary string - one line if Solcast has no data,
    two `\n`-joined lines otherwise; forecast.html renders it with CSS
    `white-space: pre-line` so the `\n` becomes a real line break without
    needing `| safe` + `<br>`.
    """
    meteosource_line = f"Meteosource: {meteosource_total} kWh"
    meteosource_delta = _accuracy_delta_pct(meteosource_total, actual_total)
    if meteosource_delta is not None:
        meteosource_line += f" (Δ {meteosource_delta:+d}% vs actual)"
    lines = [meteosource_line]

    if solcast_totals is not None:
        c10_total, c50_total, c90_total = solcast_totals
        solcast_line = f"Solcast: {c50_total} ({c10_total}-{c90_total}) kWh"
        solcast_delta = _accuracy_delta_pct(c50_total, actual_total)
        if solcast_delta is not None:
            solcast_line += f" (Δ {solcast_delta:+d}% vs actual)"
        lines.append(solcast_line)

    return "\n".join(lines)


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
    solcast_totals = _solcast_daily_totals(solcast_periods) if solcast_periods else None

    # Accuracy delta only for a fully elapsed past date with complete
    # telemetry - see this plan's Global Constraints and spec §3's
    # amendment for why (a partial/incomplete actual total would make the
    # delta misleading, not informative). Also only for the merged "Latest"
    # view (fetched_at is None): a specific historical snapshot may only
    # cover part of the day (Solcast/Meteosource fetches are forward-looking),
    # so its forecast total is itself partial and comparing it to the full-day
    # actual total would be just as misleading.
    is_past_date = date_yyyymmdd < datetime.now().strftime('%Y-%m-%d')
    actual_by_hour = _get_actual_hourly_pv_kwh(date_yyyymmdd) if is_past_date else {}
    actual_total = (
        round(sum(actual_by_hour.values()), 1)
        if fetched_at is None and is_past_date and len(actual_by_hour) == 24
        else None
    )

    summary = _build_forecast_summary(meteosource_total, solcast_totals, actual_total)
    return flask.render_template('forecast.html', date=date_yyyymmdd, fetched_at=fetched_at, summary=summary)


def _get_actual_hourly_pv_kwh(date_yyyymmdd):
    """Actual measured PV production per hour for `date_yyyymmdd`, read from
    the inverter's own hourly_summary table (pv_kwh column) - the only
    figure reliable enough to compare against the forecast, since telemetry
    doesn't track production per string/orientation. Returns {'HH:00': kwh}
    for hours that have a recorded sample; hours with no data yet (e.g.
    later today, or a future date) are simply absent from the dict.
    Queried fresh on every call (unlike the scraped forecast) since it's a
    cheap local SQLite read and the data changes hour to hour.
    """
    day = datetime.strptime(date_yyyymmdd, '%Y-%m-%d').date()
    start_epoch, end_epoch = history.date_range_to_epoch(day, day)
    try:
        with _data_db_connection() as conn:
            rows, _ = history.fetch_hourly_rows(conn, start_epoch, end_epoch, limit=24, offset=0)
    except sqlite3.Error as e:
        # Optional/supplementary data - e.g. a fresh checkout (--dry-run,
        # never connected to the inverter) has no hourly_summary table yet.
        # Fail soft: the forecast chart itself doesn't depend on this.
        logger.warning(f"Couldn't read actual hourly PV production for {date_yyyymmdd}: {e}")
        return {}
    return {row['hour_start'][-5:]: row['pv_kwh'] for row in rows if row['pv_kwh'] is not None}


def _get_actual_pv_kwh_so_far_this_hour(now):
    """Live, partial-hour counterpart to _get_actual_hourly_pv_kwh: how much
    the currently in-progress hour has produced so far (not yet a full
    hour's worth, unlike hourly_summary's completed-hour rows). None if
    unavailable (fresh DB, no samples yet this hour, etc.) - fails soft for
    the same reasons _get_actual_hourly_pv_kwh does.
    """
    hour_start_epoch, _ = storage.current_hour_bounds(now)
    try:
        with _data_db_connection() as conn:
            value = storage.get_pv_kwh_so_far(conn, hour_start_epoch, int(now.timestamp()))
    except sqlite3.Error as e:
        logger.warning(f"Couldn't read this hour's partial PV production: {e}")
        return None
    return round(value, 2) if value is not None else None


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
        # https://github.com/piomar123/goodwe_manager/pull/26 for the
        # design rationale - the design spec doc itself was removed from
        # the tree, but is still visible in that PR's history) - skip the
        # read entirely for today/future dates rather than showing an
        # estimate of an estimate next to the real Actual line.
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


@app.get('/history')
def get_history_page():
    default_start, default_end = history.default_date_range(datetime.now().date())
    start_date = history.parse_date_or_default(request.args.get('start'), default_start)
    end_date = history.parse_date_or_default(request.args.get('end'), default_end)
    start_time = history.parse_time_or_default(request.args.get('start_time'), None)
    end_time = history.parse_time_or_default(request.args.get('end_time'), None)
    return flask.render_template(
        'history.html',
        raw_columns=history.RAW_COLUMNS,
        default_raw_columns=history.DEFAULT_RAW_COLUMNS,
        default_start=start_date.strftime('%Y-%m-%d'),
        default_end=end_date.strftime('%Y-%m-%d'),
        default_start_time=start_time.strftime('%H:%M') if start_time else '',
        default_end_time=end_time.strftime('%H:%M') if end_time else '',
    )


def _parse_history_range_params():
    default_start, default_end = history.default_date_range(datetime.now().date())
    start_date = history.parse_date_or_default(request.args.get('start'), default_start)
    end_date = history.parse_date_or_default(request.args.get('end'), default_end)
    # start_time/end_time are optional (only the raw-samples tab's UI sends
    # them - hourly_summary is already bucketed at hour granularity, so
    # narrowing it further by time-of-day wouldn't mean anything extra) -
    # a missing or unparseable value falls back to None, which preserves
    # date_range_to_epoch()'s original whole-day behavior
    start_time = history.parse_time_or_default(request.args.get('start_time'), None)
    end_time = history.parse_time_or_default(request.args.get('end_time'), None)
    start_epoch, end_epoch = history.date_range_to_epoch(start_date, end_date, start_time, end_time)
    limit = history.resolve_limit(request.args.get('limit'))
    offset = history.resolve_offset(request.args.get('offset'))
    return start_date, end_date, start_time, end_time, start_epoch, end_epoch, limit, offset


@app.get('/history/inverter.json')
def get_history_inverter_json():
    start_date, end_date, start_time, end_time, start_epoch, end_epoch, limit, offset = _parse_history_range_params()
    columns_param = request.args.get('columns')
    requested_columns = columns_param.split(',') if columns_param else None
    columns = history.resolve_raw_columns(requested_columns)
    with _data_db_connection() as conn:
        rows, has_more = history.fetch_inverter_rows(conn, columns, start_epoch, end_epoch, limit, offset)
    return flask.jsonify({
        'start': start_date.strftime('%Y-%m-%d'),
        'end': end_date.strftime('%Y-%m-%d'),
        'start_time': start_time.strftime('%H:%M') if start_time else None,
        'end_time': end_time.strftime('%H:%M') if end_time else None,
        'columns': columns,
        'limit': limit,
        'offset': offset,
        'rows': rows,
        'has_more': has_more,
    })


@app.get('/history/hourly.json')
def get_history_hourly_json():
    start_date, end_date, _start_time, _end_time, start_epoch, end_epoch, limit, offset = _parse_history_range_params()
    with _data_db_connection() as conn:
        rows, has_more = history.fetch_hourly_rows(conn, start_epoch, end_epoch, limit, offset)
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
def listen():
    def stream_messages(remote_addr):
        messages = announcer.listen()  # returns a queue.Queue
        try:
            while True:
                msg = messages.get()  # blocks until a new message arrives
                if msg is None:
                    break
                yield str(msg)
        finally:
            logger.info(f'Listener disconnected {remote_addr}')
            announcer.unsubscribe(messages)

    return flask.Response(stream_messages(flask.request.remote_addr), mimetype='text/event-stream')


def main():
    global dry_run
    setup_plot_style()
    matplotlib.use('agg')
    file_handler = logging.FileHandler('manager.log')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s',
                        handlers=[file_handler, console_handler])
    logging.getLogger('goodwe.protocol').setLevel(logging.INFO)
    install_uncaught_exception_logging(logger)
    if len(sys.argv) > 1 and sys.argv[1] == '--dry-run':
        logger.warning("Running in dry-run mode without inverter connection")
        dry_run = True

    asyncio_thread.start()
    rce_prefetch_thread.start()
    forecast_prefetch_thread.start()
    # atexit.register(stop_threads)
    try:
        # threaded=True is Flask's own default (Flask.run() sets it via
        # options.setdefault before handing off to Werkzeug, whose raw
        # run_simple() defaults to False) - passed explicitly here so a
        # long-lived /listen SSE connection can never be mistaken for the
        # reason other requests stall behind it.
        app.run('0.0.0.0', port=APP_PORT, debug=True, use_reloader=False, threaded=True)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down")
    finally:
        logger.info("Finishing the application...")
        asyncio_thread.finish()
        rce_prefetch_thread.finish()
        forecast_prefetch_thread.finish()
        logger.info("Finished all threads")


if __name__ == '__main__':
    main()
