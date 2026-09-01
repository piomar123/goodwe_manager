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
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Mapping

import aiosqlite
import dotenv
import flask
import goodwe
import matplotlib
from flask import request
from goodwe.sensor import EcoModeV2

import eco_encoder
import forecast
import history
import storage
from announcer import MessageAnnouncer
from error_logging import install_uncaught_exception_logging
from rce import parse_date, plot_rce, setup_plot_style, get_rce_15min
from rce_prefetch import RcePrefetchThread
from sensors import SELECTED_SENSORS, CalculatedValuesEvaluator, sensor_columns

dotenv.load_dotenv()
INVERTER_IP = os.environ.get('INVERTER_IP')
assert INVERTER_IP, "INVERTER_IP environment variable is not set, copy .env.example to .env and set it"
APP_PORT = int(os.environ.get('APP_PORT', 5000))

# FIXME poor-man's config - convert to .env and de-hard-code
PV_ORIENTATIONS = (90, 270)  # this is used for the forecast only, if the count of orientations is changed, modify also ForecastData tuple and forecast.html template

# https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
# https://gist.github.com/werediver/4358735?permalink_comment_id=3421708

logger = logging.getLogger(__name__)
announcer = MessageAnnouncer()
dry_run = False

EVERY_DAY = 0b1111111
EVERY_DAY_STR = 'all'

ForecastData = namedtuple('ForecastData', ('angle90_in_kWh', 'angle270_in_kWh', 'total_in_kWh'))


class AsyncioThread(threading.Thread):
    _asyncio_loop: Optional[asyncio.AbstractEventLoop] = None
    _inverter: Optional[goodwe.Inverter] = None
    _db_conn: Optional[aiosqlite.Connection] = None
    _should_stop = threading.Event()
    _calculated_values_evaluator = CalculatedValuesEvaluator()

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
            while True:
                inverter_runtime = await self._inverter.read_runtime_data()
                sensors_data = {sid: (None if (v := inverter_runtime.get(sid)) is None else str(v)) for sid in SELECTED_SENSORS}
                sensors_data_with_calculated = sensors_data | self._calculated_values_evaluator.calculate_values(sensors_data)
                await storage.insert_sample_async(self._db_conn, sensors_data_with_calculated)
                announcer.announce(json.dumps(sensors_data_with_calculated))
                new_hour_start, _ = storage.current_hour_bounds(datetime.now())
                if new_hour_start != current_hour_start:
                    # the wall clock just rolled into a new hour - the hour
                    # that just ended now has a sample in the following
                    # (current) hour, so it can be backfilled immediately,
                    # rather than waiting on a fixed polling interval
                    current_hour_start = new_hour_start
                    await self._backfill_hourly_summary()
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
    async def _backfill_hourly_summary():
        """Derives any newly-completed hourly_summary rows from
        inverter_history. Runs on a plain sqlite3 connection (not the shared
        aiosqlite one) via a worker thread, since storage.backfill_hourly_summary
        is synchronous and issues several queries per hour - a short-lived
        connection here avoids sharing sqlite3's not-thread-safe-by-default
        connection object with the asyncio loop's own aiosqlite connection.
        """
        def _run():
            conn = sqlite3.connect(storage.DATA_DB_PATH)
            try:
                return storage.backfill_hourly_summary(conn)
            finally:
                conn.close()

        backfilled = await asyncio.to_thread(_run)
        if backfilled:
            logger.info(f"Backfilled {backfilled} hourly_summary row(s)")

    def ensure_inverter_ready(self):
        if self._asyncio_loop is None:
            raise RuntimeError('The asyncio loop is not running')
        while self._inverter is None:
            logger.warning("The inverter is not connected yet")
            time.sleep(1)


app = flask.Flask(__name__, static_url_path='/static')
asyncio_thread = AsyncioThread(inverter_address=INVERTER_IP, daemon=False)
rce_prefetch_thread = RcePrefetchThread()


@app.route('/')
def serve_index():
    return flask.render_template('index.html')


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


@app.get('/forecast')
def get_forecast():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    logger.debug(f"Fetching forecast for {date_yyyymmdd}")
    orientations = PV_ORIENTATIONS
    with ThreadPoolExecutor(max_workers=2) as executor:
        forecast_futures = [executor.submit(forecast.fetch_pv_production_forecast_kwh, date_yyyymmdd, orientation) for orientation in orientations]
        forecasts = [future.result() for future in forecast_futures]
        logger.debug(f"Forecasts: {forecasts} kWh")
        total_kwh = forecasts[0] + forecasts[1]
        forecast_data = ForecastData(angle90_in_kWh=f"{forecasts[0]:.1f}", angle270_in_kWh=f"{forecasts[1]:.1f}", total_in_kWh=f"{total_kwh:.1f}")
        return flask.render_template('forecast.html', date=date_yyyymmdd, forecast=forecast_data)


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
    conn = sqlite3.connect(storage.DATA_DB_PATH)
    try:
        rows, has_more = history.fetch_inverter_rows(conn, columns, start_epoch, end_epoch, limit, offset)
    finally:
        conn.close()
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


if __name__ == '__main__':
    main()
