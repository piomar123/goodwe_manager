"""
tests/test_main_control_routes.py
Flask /control/* routes - the runtime itself is covered by
tests/test_control_runtime.py; here only the HTTP layer and its guards.
"""
import concurrent.futures
import os
import unittest
from unittest import mock

os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import control
import main


def _run_now(coro) -> concurrent.futures.Future:
    """Stand-in for asyncio_thread.run_coroutine_threadsafe: run the coroutine
    to completion synchronously."""
    import asyncio
    future: concurrent.futures.Future = concurrent.futures.Future()
    future.set_result(asyncio.run(coro))
    return future


class ControlRoutesDisabledTest(unittest.TestCase):
    def test_routes_404_when_control_off(self):
        with mock.patch.object(main, 'control_runtime_instance', None):
            client = main.app.test_client()
            self.assertEqual(client.get('/control/state').status_code, 404)
            self.assertEqual(client.post('/control/override', data={}).status_code, 404)
            self.assertEqual(client.post('/control/override/clear').status_code, 404)


class ControlRoutesEnabledTest(unittest.TestCase):
    def setUp(self):
        self.runtime = mock.Mock()
        self.runtime.last_state = {'mode': 'auto'}
        patches = [
            mock.patch.object(main, 'control_runtime_instance', self.runtime),
            mock.patch.object(main.asyncio_thread, 'run_coroutine_threadsafe', side_effect=_run_now),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.client = main.app.test_client()

    def test_state(self):
        response = self.client.get('/control/state')
        self.assertEqual(response.get_json(), {'mode': 'auto'})

    def test_override_valid(self):
        response = self.client.post('/control/override', data={'mode': 'export', 'power_w': '1500',
                                                               'target_soc': '40', 'duration_min': '30'})
        self.assertEqual(response.status_code, 302)
        cmd = self.runtime.set_override.call_args.args[0]
        self.assertEqual((cmd.mode, cmd.power_w, cmd.target_soc, cmd.source),
                         (control.Mode.EXPORT, 1500, 40, 'dashboard'))

    def test_override_invalid_is_400(self):
        response = self.client.post('/control/override', data={'mode': 'charge', 'power_w': '',
                                                               'target_soc': '', 'duration_min': '30'})
        self.assertEqual(response.status_code, 400)
        self.runtime.set_override.assert_not_called()

    def test_clear(self):
        response = self.client.post('/control/override/clear')
        self.assertEqual(response.status_code, 302)
        self.runtime.clear_override.assert_called_once()


class ControlConfigTest(unittest.TestCase):
    def test_default_env_leaves_control_off(self):
        self.assertIsNone(control.config_from_env({}))


if __name__ == '__main__':
    unittest.main()
