import logging
import logging.handlers
import os
import tempfile
import unittest

# main.py asserts INVERTER_IP is set at import time - see test_main_backfill.py
os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import main


class ConfigureLoggingTest(unittest.TestCase):
    def setUp(self):
        root = logging.getLogger()
        self._saved_root = (root.level, list(root.handlers))
        self._saved_levels = {name: logging.getLogger(name).level for name in ('aiosqlite', 'goodwe.protocol')}
        self._tmpdir = tempfile.TemporaryDirectory()
        self.log_path = os.path.join(self._tmpdir.name, 'manager.log')

    def tearDown(self):
        root = logging.getLogger()
        for handler in root.handlers:
            if handler not in self._saved_root[1]:
                handler.close()
        root.handlers = self._saved_root[1]
        root.setLevel(self._saved_root[0])
        for name, level in self._saved_levels.items():
            logging.getLogger(name).setLevel(level)
        self._tmpdir.cleanup()

    def _file_handlers(self):
        return [h for h in logging.getLogger().handlers if isinstance(h, logging.FileHandler)]

    def test_file_handler_rotates_by_size(self):
        main.configure_logging(self.log_path)

        handlers = self._file_handlers()
        self.assertEqual(len(handlers), 1)
        handler = handlers[0]
        self.assertIsInstance(handler, logging.handlers.RotatingFileHandler)
        self.assertEqual(handler.maxBytes, main.LOG_MAX_BYTES)
        self.assertEqual(handler.backupCount, main.LOG_BACKUP_COUNT)
        self.assertGreater(handler.maxBytes, 0)
        self.assertGreater(handler.backupCount, 0)

    def test_aiosqlite_debug_chatter_not_written(self):
        # aiosqlite logs two DEBUG lines per DB operation ("executing ..." /
        # "operation ... completed") - that alone grew manager.log by
        # ~350MB/day once telemetry moved to SQLite.
        main.configure_logging(self.log_path)

        logging.getLogger('aiosqlite').debug('executing functools.partial(...)')
        logging.getLogger('aiosqlite').warning('aiosqlite warning still logged')
        logging.getLogger('main').debug('app debug still logged')
        for handler in self._file_handlers():
            handler.flush()

        with open(self.log_path) as f:
            contents = f.read()
        self.assertNotIn('executing functools.partial', contents)
        self.assertIn('aiosqlite warning still logged', contents)
        self.assertIn('app debug still logged', contents)


if __name__ == '__main__':
    unittest.main()
