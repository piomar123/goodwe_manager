"""
Global uncaught-exception logging, for both the main thread and background
threads (e.g. AsyncioThread) - without this, an exception outside the
existing try/except blocks is only printed to stderr and easy to miss.
"""
import logging
import sys
import threading


def install_uncaught_exception_logging(logger: logging.Logger) -> None:
    def log_unhandled(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    def log_unhandled_in_thread(args: threading.ExceptHookArgs):
        thread_name = args.thread.name if args.thread else '?'
        logger.critical(f"Uncaught exception in thread '{thread_name}'",
                        exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

    sys.excepthook = log_unhandled
    threading.excepthook = log_unhandled_in_thread
