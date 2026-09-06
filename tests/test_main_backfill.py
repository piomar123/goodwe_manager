import asyncio
import os
import tempfile
import unittest

# main.py asserts INVERTER_IP is set at import time (it's the real app's
# required config) - set a dummy value before importing so this test file
# can import it standalone, same as running with a real .env would.
os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import storage
from sensors import sensor_columns

import main


def _sample_row(timestamp: str, **overrides) -> dict:
    row = {name: ('0' if sql_type == 'REAL' else '') for name, sql_type in sensor_columns()}
    row['timestamp'] = timestamp
    row.update(overrides)
    return row


class BackfillHourlySummaryVerifyHourStartTest(unittest.TestCase):
    """Covers AsyncioThread._backfill_hourly_summary's verify_hour_start
    param, added to fix a race in the polling loop's hour-rollover trigger:
    see main.py's comment above the call site for the full story - in
    short, the loop must be able to tell whether an hour actually got
    backfilled (so it can keep retrying next tick) rather than assuming a
    single attempt always succeeds (which used to leave a real gap for up
    to an hour whenever the race was lost)."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)  # sqlite3.connect creates it fresh
        storage.init_db_sync(self.db_path, sensor_columns()).close()
        self._orig_db_path = storage.DATA_DB_PATH
        storage.DATA_DB_PATH = self.db_path

    def tearDown(self):
        storage.DATA_DB_PATH = self._orig_db_path
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def _insert(self, timestamp, **overrides):
        conn = storage.init_db_sync(self.db_path, sensor_columns())
        try:
            storage.insert_sample_sync(conn, _sample_row(timestamp, **overrides))
        finally:
            conn.close()

    def test_returns_false_when_the_hour_cant_be_backfilled_yet(self):
        # Only the hour itself has data - no sample in the *following* hour
        # yet, so backfill_hourly_summary has no way to prove it's complete.
        # This is exactly the race: the wall clock ticked over to a new hour,
        # but the new hour's own first sample hasn't landed yet.
        self._insert('2026-08-28 14:05:00')
        hour_start = storage.parse_timestamp_epoch('2026-08-28 14:00:00')

        verified = asyncio.run(main.AsyncioThread._backfill_hourly_summary(verify_hour_start=hour_start))

        self.assertFalse(verified)

    def test_returns_true_once_the_following_hour_has_a_sample(self):
        self._insert('2026-08-28 14:05:00')
        self._insert('2026-08-28 15:05:00')  # proves 14:00 is complete
        hour_start = storage.parse_timestamp_epoch('2026-08-28 14:00:00')

        verified = asyncio.run(main.AsyncioThread._backfill_hourly_summary(verify_hour_start=hour_start))

        self.assertTrue(verified)

    def test_returns_true_for_an_hour_backfilled_by_an_earlier_call(self):
        # Simulates the loop's retry: a first call finds nothing (the race),
        # then - once the next hour's sample has arrived - a second call
        # both performs and confirms the backfill, same as the loop would do
        # on its very next 1-second tick.
        self._insert('2026-08-28 14:05:00')
        hour_start = storage.parse_timestamp_epoch('2026-08-28 14:00:00')
        self.assertFalse(asyncio.run(main.AsyncioThread._backfill_hourly_summary(verify_hour_start=hour_start)))

        self._insert('2026-08-28 15:05:00')  # the following hour's first sample finally lands

        self.assertTrue(asyncio.run(main.AsyncioThread._backfill_hourly_summary(verify_hour_start=hour_start)))

    def test_without_verify_hour_start_still_runs_the_backfill_and_returns_true(self):
        self._insert('2026-08-28 14:05:00')
        self._insert('2026-08-28 15:05:00')

        result = asyncio.run(main.AsyncioThread._backfill_hourly_summary())

        self.assertTrue(result)
        conn = storage.init_db_sync(self.db_path, sensor_columns())
        try:
            row = conn.execute(
                "SELECT 1 FROM hourly_summary WHERE hour_start = ?",
                (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
            ).fetchone()
            self.assertIsNotNone(row)
        finally:
            conn.close()


class AdvanceHourOrRetryBackfillTest(unittest.TestCase):
    """Covers the retry-cap safety valve in _advance_hour_or_retry_backfill:
    if an hour can never be verified (e.g. it was itself silently skipped
    entirely, so it can never gain the proof-bucket it needs),
    retrying _backfill_hourly_summary forever every ~1s would be an
    unbounded cost for a gap that was never recoverable anyway."""

    def setUp(self):
        self._orig_limit = main.AsyncioThread._PENDING_BACKFILL_RETRY_LIMIT
        main.AsyncioThread._PENDING_BACKFILL_RETRY_LIMIT = 3  # keep the test fast

    def tearDown(self):
        main.AsyncioThread._PENDING_BACKFILL_RETRY_LIMIT = self._orig_limit

    def _thread_with_backfill_result(self, results):
        """A bare (un-started) AsyncioThread instance with
        _backfill_hourly_summary stubbed to return each of `results` in
        turn, one per call - avoids needing a real sqlite file or a real
        hour-rollover race to drive this decision logic."""
        thread = main.AsyncioThread.__new__(main.AsyncioThread)
        results_iter = iter(results)

        async def fake_backfill_hourly_summary(verify_hour_start=None):
            return next(results_iter)

        thread._backfill_hourly_summary = fake_backfill_hourly_summary
        return thread

    def test_adopts_new_hour_start_once_verified(self):
        thread = self._thread_with_backfill_result([True])

        current, retries = asyncio.run(thread._advance_hour_or_retry_backfill(100, 200, 0))

        self.assertEqual(current, 200)
        self.assertEqual(retries, 0)

    def test_keeps_retrying_current_hour_start_while_unverified(self):
        thread = self._thread_with_backfill_result([False])

        current, retries = asyncio.run(thread._advance_hour_or_retry_backfill(100, 200, 0))

        self.assertEqual(current, 100)  # not adopted yet - caller will call again next tick
        self.assertEqual(retries, 1)

    def test_gives_up_and_advances_once_the_retry_limit_is_reached(self):
        thread = self._thread_with_backfill_result([False, False, False])
        current, retries = 100, 0

        for _ in range(main.AsyncioThread._PENDING_BACKFILL_RETRY_LIMIT):
            current, retries = asyncio.run(thread._advance_hour_or_retry_backfill(current, 200, retries))

        self.assertEqual(current, 200)  # gave up waiting and moved on
        self.assertEqual(retries, 0)  # reset, ready to track the next hour


if __name__ == '__main__':
    unittest.main()
