"""
tests/test_main_dry_run_guards.py
Covers the pre-merge review finding that --dry-run doesn't actually
prevent a real MQTT connection/publish: RcePrefetchThread and
ForecastPrefetchThread start unconditionally (regardless of dry_run),
and their on_success/on_solcast_updated callbacks
(_publish_export_prices/_publish_pv_forecast) previously had no dry_run
check of their own - asyncio_thread's loop never runs mqtt.connect() in
dry-run (see main.run()'s `if not dry_run`), so those callbacks would
still hit mqtt._publish's opportunistic-reconnect path and try to
connect to a real broker, defeating the point of dry-run.
"""
import concurrent.futures
import os
import unittest
from unittest import mock

os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import main


def _closed_coro_future(coro) -> concurrent.futures.Future:
    """See tests/test_main_tariff_import_error_handling.py's identical
    helper - main.py's _fire_and_forget() calls .add_done_callback() on
    whatever run_coroutine_threadsafe returns, so a bare None (or a
    lambda that just closes the coroutine) isn't a valid stand-in here."""
    coro.close()
    future: concurrent.futures.Future = concurrent.futures.Future()
    future.set_result(None)
    return future


class DryRunGuardsPreventRealMqttActivityTest(unittest.TestCase):
    def setUp(self):
        self._orig_dry_run = main.dry_run
        main.dry_run = True

    def tearDown(self):
        main.dry_run = self._orig_dry_run

    def test_publish_export_prices_is_a_noop_in_dry_run(self):
        with mock.patch.object(main.asyncio_thread, 'run_coroutine_threadsafe') as run_coro:
            main._publish_export_prices()
        run_coro.assert_not_called()

    def test_publish_pv_forecast_is_a_noop_in_dry_run(self):
        with mock.patch.object(main.asyncio_thread, 'run_coroutine_threadsafe') as run_coro:
            main._publish_pv_forecast()
        run_coro.assert_not_called()

    def test_publish_export_prices_still_works_outside_dry_run(self):
        main.dry_run = False
        with mock.patch.object(main.asyncio_thread, 'run_coroutine_threadsafe',
                                side_effect=_closed_coro_future) as run_coro:
            main._publish_export_prices()
        run_coro.assert_called_once()


class FireAndForgetLogsUnhandledExceptionsTest(unittest.TestCase):
    """_fire_and_forget's whole purpose is to surface an exception raised
    inside a fire-and-forget coroutine (previously silently swallowed,
    since nothing ever inspected run_coroutine_threadsafe's Future) -
    proves the done-callback actually logs it."""

    def test_exception_from_the_coroutine_is_logged(self):
        future: concurrent.futures.Future = concurrent.futures.Future()
        future.set_exception(RuntimeError("simulated bug inside a fire-and-forget publish"))

        with mock.patch.object(main.logger, 'error') as log_error:
            main._log_fire_and_forget_exception(future)

        log_error.assert_called_once()
        self.assertIn("simulated bug", log_error.call_args[0][0])

    def test_a_successful_future_logs_nothing(self):
        future: concurrent.futures.Future = concurrent.futures.Future()
        future.set_result(None)

        with mock.patch.object(main.logger, 'error') as log_error:
            main._log_fire_and_forget_exception(future)

        log_error.assert_not_called()


if __name__ == '__main__':
    unittest.main()
