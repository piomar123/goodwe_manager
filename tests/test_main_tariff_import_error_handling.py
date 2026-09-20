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
"""
import asyncio
import os
import tempfile
import unittest
from datetime import datetime
from unittest import mock

os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import storage
from sensors import sensor_columns, SELECTED_SENSORS

import main
import mqtt_bridge


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


class BadTariffConfigDoesNotBounceInverterConnectionTest(unittest.TestCase):
    """Runs the real AsyncioThread._get_inverter_data with a bad
    TARIFF_IMPORT_CONFIG and a mocked inverter/goodwe.connect, and asserts
    it does NOT raise - proving the try/except added around both call
    sites (initial publish block and midnight-rollover block) actually
    keeps a tariff config error from propagating into
    _get_inverter_data_with_retry's inverter-failure retry loop."""

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
        main.asyncio_thread.run_coroutine_threadsafe = lambda coro: coro.close()

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

    def test_get_inverter_data_does_not_raise_on_bad_tariff_config(self):
        async def fake_connect(*args, **kwargs):
            return _fake_inverter()

        with mock.patch.object(main.goodwe, 'connect', fake_connect):
            try:
                asyncio.run(self.thread._get_inverter_data())
            except Exception as e:
                self.fail(f"_get_inverter_data raised {e!r} due to a bad TARIFF_IMPORT_CONFIG "
                          f"- this should have been caught and logged, not propagated")


if __name__ == '__main__':
    unittest.main()
