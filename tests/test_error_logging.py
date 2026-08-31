import logging
import sys
import threading
import unittest

from error_logging import install_uncaught_exception_logging


class InstallUncaughtExceptionLoggingTest(unittest.TestCase):
    def setUp(self):
        self._orig_excepthook = sys.excepthook
        self._orig_threading_excepthook = threading.excepthook
        self.logger = logging.getLogger('test-error-logging')

    def tearDown(self):
        sys.excepthook = self._orig_excepthook
        threading.excepthook = self._orig_threading_excepthook

    def test_logs_uncaught_exception_from_main_thread(self):
        install_uncaught_exception_logging(self.logger)

        try:
            raise ValueError("boom")
        except ValueError:
            exc_info = sys.exc_info()

        with self.assertLogs(self.logger, level='CRITICAL') as cm:
            sys.excepthook(*exc_info)

        self.assertIn("boom", cm.output[0])

    def test_logs_uncaught_exception_from_background_thread(self):
        install_uncaught_exception_logging(self.logger)

        try:
            raise RuntimeError("thread boom")
        except RuntimeError:
            exc_info = sys.exc_info()

        args = threading.ExceptHookArgs((*exc_info, threading.current_thread()))
        with self.assertLogs(self.logger, level='CRITICAL') as cm:
            threading.excepthook(args)

        self.assertIn("thread boom", cm.output[0])

    def test_keyboard_interrupt_is_not_logged_as_an_error(self):
        install_uncaught_exception_logging(self.logger)

        try:
            raise KeyboardInterrupt()
        except KeyboardInterrupt:
            exc_info = sys.exc_info()

        with self.assertRaises(AssertionError):
            # assertLogs raises AssertionError itself if nothing was logged
            with self.assertLogs(self.logger, level='CRITICAL'):
                sys.excepthook(*exc_info)


if __name__ == '__main__':
    unittest.main()
