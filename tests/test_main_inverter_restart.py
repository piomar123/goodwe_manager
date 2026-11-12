import asyncio
import concurrent.futures
import unittest
from unittest.mock import AsyncMock, patch

import main


def _run_now(coro) -> concurrent.futures.Future:
    future = concurrent.futures.Future()
    future.set_result(asyncio.run(coro))
    return future


class InverterRestartRouteTest(unittest.TestCase):
    def setUp(self):
        main.app.testing = True
        self.client = main.app.test_client()

    def test_post_restarts_the_inverter_and_returns_to_config(self):
        inverter = AsyncMock()
        with patch.object(main.asyncio_thread, 'ensure_inverter_ready'), \
                patch.object(main.asyncio_thread, 'run_coroutine_threadsafe', side_effect=_run_now), \
                patch.object(type(main.asyncio_thread), 'inverter', new=inverter):
            response = self.client.post('/inverter/restart')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'], '/config')
        inverter.restart.assert_awaited_once_with()

    def test_get_is_not_allowed(self):
        self.assertEqual(self.client.get('/inverter/restart').status_code, 405)


if __name__ == '__main__':
    unittest.main()
