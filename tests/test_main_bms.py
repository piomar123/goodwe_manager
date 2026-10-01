"""
tests/test_main_bms.py
main.py wiring of the optional BMS poller: only started when configured and
not in --dry-run, never allowed to take goodwe_manager down with it.
"""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import bms_poller
from announcer import MessageAnnouncer
import main
from tests.test_bms_poller import CELLS, SUMMARY, WHEN

CONFIG = bms_poller.BmsConfig('h', 7)


class FakeLoop:
    def __init__(self):
        self.coros = []

    def create_task(self, coro):
        self.coros.append(coro.__qualname__)
        coro.close()


class CreateLoopTasksTest(unittest.TestCase):
    def setUp(self):
        self._orig = (main.dry_run, main.BMS_CONFIG)

    def tearDown(self):
        main.dry_run, main.BMS_CONFIG = self._orig

    def tasks(self):
        loop = FakeLoop()
        main.asyncio_thread._create_loop_tasks(loop)
        return loop.coros

    def test_bms_task_started_when_configured(self):
        main.dry_run, main.BMS_CONFIG = False, CONFIG
        self.assertIn('AsyncioThread._run_bms_poller', self.tasks())

    def test_no_bms_task_when_disabled(self):
        main.dry_run, main.BMS_CONFIG = False, None
        self.assertNotIn('AsyncioThread._run_bms_poller', self.tasks())

    def test_no_bms_task_in_dry_run(self):
        main.dry_run, main.BMS_CONFIG = True, CONFIG
        self.assertEqual(self.tasks(), [])


class BmsSampleHandlerTest(unittest.TestCase):
    def test_db_failure_still_publishes(self):
        sample = bms_poller.decode(SUMMARY, CELLS, WHEN)
        publish = mock.AsyncMock()
        with mock.patch('bms_storage.insert_sample', mock.AsyncMock(side_effect=RuntimeError('locked'))), \
                mock.patch.object(main.mqtt, 'publish_bms', publish), \
                self.assertLogs('main', 'WARNING'):
            asyncio.run(main._bms_sample_handler(conn=object())(sample))
        publish.assert_awaited_once()
        self.assertNotIn('raw_1100', publish.await_args.args[0])


class BmsDashboardEventTest(unittest.TestCase):
    def test_each_sample_is_announced_as_a_sticky_bms_event(self):
        sample = bms_poller.decode(SUMMARY, CELLS, WHEN)
        fresh = MessageAnnouncer()
        with mock.patch('bms_storage.insert_sample', mock.AsyncMock()), \
                mock.patch.object(main.mqtt, 'publish_bms', mock.AsyncMock()), \
                mock.patch.object(main, 'announcer', fresh):
            asyncio.run(main._bms_sample_handler(conn=object())(sample))
            listener = fresh.listen()  # a browser connecting after the sample

        msg = listener.get_nowait()
        self.assertEqual(msg.event, 'bms')
        data = json.loads(msg.data)
        self.assertEqual(len(data['cell_mv']), 60)
        self.assertEqual(data['soh'], 97)
        self.assertNotIn('raw_1100', data)


class RunBmsPollerTest(unittest.TestCase):
    def test_unopenable_db_logs_error_and_returns(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch('bms_storage.BMS_DB_PATH', os.path.join(tmp, 'missing-dir', 'bms.db')), \
                self.assertLogs('main', 'ERROR') as logs:
            asyncio.run(main.asyncio_thread._run_bms_poller(CONFIG))
        self.assertIn('BMS poller disabled', logs.output[0])


class ImportTimeTest(unittest.TestCase):
    def test_bms_config_is_not_parsed_at_import(self):
        # Logging isn't configured at import time - anything logged then is
        # lost (or bare on stderr), so parsing waits for main().
        env = dict(os.environ, INVERTER_IP='127.0.0.1', BMS_LOGGER_HOST='h', BMS_LOGGER_SERIAL='abc')
        result = subprocess.run([sys.executable, '-c', 'import main; print(main.BMS_CONFIG)'],
                                capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().splitlines()[-1], 'None')
        self.assertNotIn('BMS poller', result.stderr)


if __name__ == '__main__':
    unittest.main()
