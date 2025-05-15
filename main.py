import asyncio
import concurrent.futures
import csv
import io
import json
import logging
import os
import re
import sys
import threading
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Mapping

import dotenv
import flask
import goodwe
import matplotlib
from flask import request
from goodwe.sensor import EcoModeV2

import eco_encoder
import forecast
from announcer import MessageAnnouncer
from rce import parse_date, query_pse_rce, plot_rce, setup_plot_style

dotenv.load_dotenv()
INVERTER_IP = os.environ.get('INVERTER_IP')
assert INVERTER_IP, "INVERTER_IP environment variable is not set, copy .env.example to .env and set it"

# FIXME poor-man's config - convert to .env and de-hard-code
PV_ORIENTATIONS = (90, 270)  # this is used for the forecast only, if the count of orientations is changed, modify also ForecastData tuple and forecast.html template

# https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
# https://gist.github.com/werediver/4358735?permalink_comment_id=3421708

logger = logging.getLogger(__name__)
announcer = MessageAnnouncer()
dry_run = False

EVERY_DAY = 0b1111111
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
    'e_day',  # today PV production
    'e_load_total',
    'meter_e_total_exp',
    'meter_e_total_imp',
    'e_bat_charge_total',
    'e_bat_discharge_total',
    'work_mode_label',
    'rssi',
]

ForecastData = namedtuple('ForecastData', ('angle90_in_kWh', 'angle270_in_kWh', 'total_in_kWh'))


class CalculatedValuesEvaluator:
    _hour_start_sensors = None

    def calculate_values(self, sensors_data: Mapping[str, Any]) -> dict[str, Any]:
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

    @staticmethod
    def headers():
        return ['_hour_start_timestamp', '_hourly_meter_export', '_hourly_meter_import', '_hourly_load']

    def _verify_header(self, calculated_values):
        for header, key in zip(self.headers(), calculated_values.keys()):
            if header != key:
                raise AssertionError(f"Implementation error: headers do not correspond to set keys: {key} != {header}, "
                                     f"{self.headers()} != {calculated_values.keys()}")


class AsyncioThread(threading.Thread):
    _asyncio_loop: Optional[asyncio.AbstractEventLoop] = None
    _inverter: Optional[goodwe.Inverter] = None
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
        self._asyncio_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._asyncio_loop)
        if not dry_run:
            self._asyncio_loop.create_task(self._get_inverter_data_with_retry())
        self._asyncio_loop.run_forever()
        logger.info("Finished the asyncio loop")

    @property
    def loop(self):
        return self._asyncio_loop

    @property
    def inverter(self):
        return self._inverter

    def run_coroutine_threadsafe(self, coro) -> concurrent.futures.Future:
        """Run a coroutine from another thread in the asyncio loop and return a Future"""
        return asyncio.run_coroutine_threadsafe(coro, self._asyncio_loop)

    def finish(self):
        """Called from another thread to finish and stop the asyncio loop"""
        logger.info("Finishing asyncio loop...")
        self._should_stop.set()
        if self._asyncio_loop is None:
            return
        finished = threading.Event()
        asyncio.run_coroutine_threadsafe(self.wait_for_asyncio_finish(self._asyncio_loop, finished), self._asyncio_loop)
        logger.info("Waiting for the asyncio loop finish result...")
        finished.wait(timeout=30)
        while self._asyncio_loop.is_running():
            logger.info("Asyncio loop still running...")
            time.sleep(1)
        self._asyncio_loop.close()

    @staticmethod
    async def wait_for_asyncio_finish(loop: asyncio.AbstractEventLoop, finished: threading.Event):
        logger.info("Waiting for the asyncio tasks to finish...")
        tasks_to_wait = asyncio.all_tasks(loop) - {asyncio.current_task(loop)}
        if tasks_to_wait:
            done, pending = await asyncio.wait(tasks_to_wait, timeout=15)
            logger.debug(f"Done: {done}, Pending: {pending}")
            if pending:
                logger.warning(f"Still running tasks after timeout: {pending}")
                for task in pending:
                    task.cancel()
        logger.info("Stopping the asyncio loop")
        loop.stop()
        finished.set()  # we need an external Event because loop is already stopped and won't return the future result

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
        log_file_with_current_timestamp = 'data-' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.csv'
        with open(log_file_with_current_timestamp, mode='w', newline='') as data_file:
            csv_writer = csv.writer(data_file, dialect='excel')
            headers = SELECTED_SENSORS + self._calculated_values_evaluator.headers()
            await asyncio.to_thread(csv_writer.writerow, headers)
            while True:
                inverter_runtime = await self._inverter.read_runtime_data()
                sensors_data = {sensor_id: str(inverter_runtime.get(sensor_id)) for sensor_id in SELECTED_SENSORS}
                sensors_data_with_calculated = sensors_data | self._calculated_values_evaluator.calculate_values(sensors_data)
                await asyncio.to_thread(csv_writer.writerow, sensors_data_with_calculated.values())  # assuming the set keeps the insertion order
                announcer.announce(json.dumps(sensors_data_with_calculated))
                await asyncio.sleep(1)
                if self._should_stop.is_set():
                    logger.info("Stopping the inverter communication routine")
                    return

    def ensure_inverter_ready(self):
        if self._asyncio_loop is None:
            raise RuntimeError('The asyncio loop is not running')
        while self._inverter is None:
            logger.warning("The inverter is not connected yet")
            time.sleep(1)


app = flask.Flask(__name__, static_url_path='/static')
asyncio_thread = AsyncioThread(inverter_address=INVERTER_IP, daemon=False)


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

    def __str__(self):
        return f"{self.index}: {'ON' if self.on_off else 'OFF'} {self.start_time}-{self.end_time} {self.mode} {self.power}% on {self.days}"


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
                   EVERY_DAY,
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


@app.get('/prices/rce.png')
def get_prices_image():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    logger.debug('Generating RCE prices images for date: {date_yyyymmdd}')
    rce = query_pse_rce(date)
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
    # TODO: https://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python
    setup_plot_style()
    matplotlib.use('agg')
    file_handler = logging.FileHandler('manager.log')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s',
                        handlers=[file_handler, console_handler])
    logging.getLogger('goodwe.protocol').setLevel(logging.INFO)
    if len(sys.argv) > 1 and sys.argv[1] == '--dry-run':
        logger.warning("Running in dry-run mode without inverter connection")
        dry_run = True

    asyncio_thread.start()
    # atexit.register(stop_threads)
    app.run('0.0.0.0', debug=True, use_reloader=False)

    logger.info("Finishing the application...")
    asyncio_thread.finish()
    logger.info("Waiting for the background thread to finish...")
    asyncio_thread.join()
    logger.info("Finished all threads")


if __name__ == '__main__':
    main()
