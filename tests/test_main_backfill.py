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


if __name__ == '__main__':
    unittest.main()
