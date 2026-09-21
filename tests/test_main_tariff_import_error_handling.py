"""
tests/test_main_tariff_import_error_handling.py
Covers I2 from the final whole-branch review: a bad TARIFF_IMPORT_CONFIG
(missing file, or YAML missing a required key like default_price) must
never propagate out of AsyncioThread._get_inverter_data - that method is
wrapped by _get_inverter_data_with_retry's broad retry-on-any-exception
loop, which exists for INVERTER connection failures, not tariff config
problems. Before the fix, both the initial-publish block and the
midnight-rollover block called tariff_engine.load_config/bands_for_day
with no try/except, so a FileNotFoundError or KeyError from a bad config
would bounce the entire inverter connection every 5 seconds forever.

Also covers the pre-merge review finding that the export-price and PV
forecast publish calls right next to the tariff one had the exact same
bug: no try/except, so e.g. a locked rce_prices.db (RcePrefetchThread
writing concurrently) or a malformed forecast_history.db snapshot would
escalate into an inverter reconnect for a completely unrelated reason.
"""
import asyncio
import concurrent.futures
import os
import tempfile
import time
import unittest
from datetime import datetime
from unittest import mock

os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import storage
from sensors import sensor_columns, SELECTED_SENSORS

import main
import mqtt_bridge


def _closed_coro_future(coro) -> concurrent.futures.Future:
    """Stand-in for AsyncioThread.run_coroutine_threadsafe in tests that
    don't have a real event loop running: closes the coroutine (avoiding
    a "coroutine was never awaited" warning) and returns an
    already-completed Future, since main.py's _fire_and_forget() calls
    .add_done_callback() on whatever this returns."""
    coro.close()
    future: concurrent.futures.Future = concurrent.futures.Future()
    future.set_result(None)
    return future


class _FakeMqttClient:
    """Records nothing, just satisfies the async-with/publish protocol
    MqttBridge expects - avoids any real network I/O while still letting
    mqtt.enabled be True, which is required for main.py to reach the
    tariff-publish call sites under test (they're both inside `if
    mqtt.enabled:` blocks)."""
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        pass

    async def publish(self, topic, payload, retain=False):
        pass


class PublishImportPricesRaisesOnBadConfigTest(unittest.TestCase):
    """Confirms the underlying failure modes this finding is about are
    real - i.e. that _publish_import_prices (which wraps
    tariff_engine.load_config/bands_for_day) actually raises on a bad
    TARIFF_IMPORT_CONFIG, rather than assuming it from reading the code."""

    def setUp(self):
        self._orig_config = main.TARIFF_IMPORT_CONFIG

    def tearDown(self):
        main.TARIFF_IMPORT_CONFIG = self._orig_config

    def test_raises_file_not_found_for_a_missing_config_file(self):
        main.TARIFF_IMPORT_CONFIG = '/nonexistent/path/tariff_import.yaml'
        with self.assertRaises(FileNotFoundError):
            main._publish_import_prices()

    def test_raises_key_error_for_yaml_missing_default_price(self):
        fd, path = tempfile.mkstemp(suffix='.yaml')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("components:\n  total:\n    prices:\n      cheap: 0.01\n")
            main.TARIFF_IMPORT_CONFIG = path
            with self.assertRaises(Exception):
                main._publish_import_prices()
        finally:
            os.remove(path)


def _fake_inverter():
    inverter = mock.Mock()

    async def read_runtime_data():
        # A dict with real (zero) values for every sensor the polling
        # loop reads - CalculatedValuesEvaluator.calculate_values() does
        # float() conversions on several of these and would raise
        # TypeError on None, which would be an unrelated test failure,
        # not the tariff-config bug this test targets.
        data = {sid: 0 for sid in SELECTED_SENSORS if sid != 'timestamp'}
        data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return data

    inverter.read_runtime_data = read_runtime_data
    return inverter


class _SideChannelFailureDoesNotBounceInverterConnectionTestBase(unittest.TestCase):
    """Shared scaffolding for proving a side-channel publish failure
    (tariff config, export prices, PV forecast) never propagates out of
    AsyncioThread._get_inverter_data - that method is wrapped by
    _get_inverter_data_with_retry's broad retry-on-any-exception loop,
    which exists for INVERTER connection failures only. Subclasses patch
    whichever side channel they're testing to raise, then run
    _get_inverter_data for real via test_does_not_raise."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        storage.init_db_sync(self.db_path, sensor_columns()).close()
        self._orig_db_path = storage.DATA_DB_PATH
        storage.DATA_DB_PATH = self.db_path

        self._orig_config = main.TARIFF_IMPORT_CONFIG
        main.TARIFF_IMPORT_CONFIG = '/nonexistent/path/tariff_import.yaml'

        # Enable the mqtt-gated blocks (both tariff-publish call sites
        # live inside `if mqtt.enabled:`) without any real broker.
        self._orig_mqtt = main.mqtt
        main.mqtt = mqtt_bridge.MqttBridge(host='localhost', client_factory=lambda **kwargs: _FakeMqttClient())

        # main.py's module-level `asyncio_thread` singleton isn't actually
        # running its own event loop in this test (we drive
        # _get_inverter_data directly via a throwaway AsyncioThread
        # instance instead), so its real run_coroutine_threadsafe would
        # raise "the asyncio loop is not running" - unrelated to the
        # tariff-config bug under test. _publish_pv_forecast (which is
        # unconditional, unlike the two tariff-gated calls) uses it too,
        # so stub it out to a no-op here.
        self._orig_run_coro = main.asyncio_thread.run_coroutine_threadsafe
        main.asyncio_thread.run_coroutine_threadsafe = _closed_coro_future

        thread = main.AsyncioThread.__new__(main.AsyncioThread)
        thread._inverter_address = '127.0.0.1'
        thread._should_stop = mock.Mock()
        # Let the polling loop run exactly one iteration, then stop.
        thread._should_stop.is_set = mock.Mock(side_effect=[False, True])
        self.thread = thread

    def tearDown(self):
        storage.DATA_DB_PATH = self._orig_db_path
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)
        main.TARIFF_IMPORT_CONFIG = self._orig_config
        main.mqtt = self._orig_mqtt
        main.asyncio_thread.run_coroutine_threadsafe = self._orig_run_coro

    def _assert_get_inverter_data_does_not_raise(self, failure_description):
        async def fake_connect(*args, **kwargs):
            return _fake_inverter()

        with mock.patch.object(main.goodwe, 'connect', fake_connect):
            try:
                asyncio.run(self.thread._get_inverter_data())
            except Exception as e:
                self.fail(f"_get_inverter_data raised {e!r} due to {failure_description} "
                          f"- this should have been caught and logged, not propagated")


class BadTariffConfigDoesNotBounceInverterConnectionTest(_SideChannelFailureDoesNotBounceInverterConnectionTestBase):
    """setUp already points TARIFF_IMPORT_CONFIG at a nonexistent file -
    proves the try/except around both tariff-publish call sites (initial
    publish block and midnight-rollover block) keeps that error from
    propagating."""

    def test_get_inverter_data_does_not_raise_on_bad_tariff_config(self):
        self._assert_get_inverter_data_does_not_raise("a bad TARIFF_IMPORT_CONFIG")


class ExportPriceFailureDoesNotBounceInverterConnectionTest(_SideChannelFailureDoesNotBounceInverterConnectionTestBase):
    """Forces export_price.build_export_price_payload to raise (standing
    in for e.g. a locked rce_prices.db, contended with RcePrefetchThread's
    own writes) - proves the try/except added around the export-price
    publish call sites keeps that error from propagating too."""

    def setUp(self):
        super().setUp()
        self._export_price_patcher = mock.patch.object(
            main.export_price, 'build_export_price_payload',
            side_effect=RuntimeError("simulated rce_prices.db lock contention"))
        self._export_price_patcher.start()

    def tearDown(self):
        self._export_price_patcher.stop()
        super().tearDown()

    def test_get_inverter_data_does_not_raise_on_export_price_failure(self):
        self._assert_get_inverter_data_does_not_raise("an export-price publish failure")


class PvForecastFailureDoesNotBounceInverterConnectionTest(_SideChannelFailureDoesNotBounceInverterConnectionTestBase):
    """Forces _publish_pv_forecast to raise (standing in for e.g. a
    missing/malformed forecast_history.db snapshot) - proves the
    try/except added around its call site keeps that error from
    propagating too."""

    def setUp(self):
        super().setUp()
        self._forecast_patcher = mock.patch.object(
            main, '_publish_pv_forecast',
            side_effect=RuntimeError("simulated forecast_history.db read failure"))
        self._forecast_patcher.start()

    def tearDown(self):
        self._forecast_patcher.stop()
        super().tearDown()

    def test_get_inverter_data_does_not_raise_on_pv_forecast_failure(self):
        self._assert_get_inverter_data_does_not_raise("a PV forecast publish failure")


class TelemetryPublishFailureDoesNotBounceInverterConnectionTest(_SideChannelFailureDoesNotBounceInverterConnectionTestBase):
    """Forces mqtt.publish_telemetry to raise (standing in for the
    asyncio.wait_for timeout it's now wrapped in - see main.py's telemetry
    publish block) - proves the try/except around it keeps that error
    from propagating too, same as the other side-channel publishes."""

    def setUp(self):
        super().setUp()
        self._telemetry_patcher = mock.patch.object(
            main.mqtt, 'publish_telemetry',
            side_effect=asyncio.TimeoutError("simulated telemetry publish timeout"))
        self._telemetry_patcher.start()

    def tearDown(self):
        self._telemetry_patcher.stop()
        super().tearDown()

    def test_get_inverter_data_does_not_raise_on_telemetry_timeout(self):
        self._assert_get_inverter_data_does_not_raise("a telemetry publish timeout")


class PricePublishRetrySchedulingTest(_SideChannelFailureDoesNotBounceInverterConnectionTestBase):
    """Covers the pre-merge review finding that a failed price publish
    previously had no retry before the next natural trigger (startup or
    midnight rollover) - up to 24h of Predbat silently planning against a
    stale/empty retained payload. _publish_export_prices_safely/
    _publish_import_prices_safely now schedule a sooner retry
    (_export_price_retry_due_at/_import_price_retry_due_at) on failure
    and clear it on success; the main loop checks these every iteration."""

    def test_export_price_failure_schedules_a_retry(self):
        with mock.patch.object(main.export_price, 'build_export_price_payload',
                                side_effect=RuntimeError("simulated rce_prices.db lock contention")):
            asyncio.run(self.thread._publish_export_prices_safely())

        self.assertIsNotNone(self.thread._export_price_retry_due_at)
        self.assertGreater(self.thread._export_price_retry_due_at, time.monotonic())

    def test_export_price_success_clears_a_pending_retry(self):
        self.thread._export_price_retry_due_at = time.monotonic() + 100

        asyncio.run(self.thread._publish_export_prices_safely())

        self.assertIsNone(self.thread._export_price_retry_due_at)

    def test_import_price_failure_schedules_a_retry(self):
        # setUp already points TARIFF_IMPORT_CONFIG at a nonexistent file.
        self.thread._publish_import_prices_safely()

        self.assertIsNotNone(self.thread._import_price_retry_due_at)
        self.assertGreater(self.thread._import_price_retry_due_at, time.monotonic())

    def test_import_price_success_clears_a_pending_retry(self):
        fd, path = tempfile.mkstemp(suffix='.yaml')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write("components:\n  total:\n    prices:\n      flat: 0.5\n    "
                        "bands:\n      default:\n        - price: flat\n    default_price: flat\n")
            main.TARIFF_IMPORT_CONFIG = path
            self.thread._import_price_retry_due_at = time.monotonic() + 100

            self.thread._publish_import_prices_safely()

            self.assertIsNone(self.thread._import_price_retry_due_at)
        finally:
            os.remove(path)


if __name__ == '__main__':
    unittest.main()
