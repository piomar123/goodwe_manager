import asyncio
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime

import storage
from sensors import sensor_columns


def _sample_row(timestamp: str, **overrides) -> dict:
    row = {name: ('0' if sql_type == 'REAL' else '') for name, sql_type in sensor_columns()}
    row['timestamp'] = timestamp
    row.update(overrides)
    return row


class StorageSyncTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)  # sqlite3.connect creates it fresh
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_init_db_is_idempotent(self):
        # calling it again on the same file must not raise
        storage.init_db_sync(self.db_path, sensor_columns()).close()

    def test_insert_and_read_back_a_sample(self):
        row = _sample_row('2026-08-28 14:05:00', ppv='1234.5', battery_soc='87')

        storage.insert_sample_sync(self.conn, row)

        result = self.conn.execute("SELECT ppv, battery_soc, timestamp_epoch FROM inverter_history").fetchone()
        self.assertEqual(result[0], 1234.5)
        self.assertEqual(result[1], 87.0)

        # round-trip check that's independent of the test machine's timezone
        roundtrip = datetime.fromtimestamp(result[2]).strftime('%Y-%m-%d %H:%M:%S')
        self.assertEqual(roundtrip, '2026-08-28 14:05:00')

    def test_get_current_hour_start_sample_returns_none_when_empty(self):
        start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 0))

        result = storage.get_current_hour_start_sample(self.conn, start, end)

        self.assertIsNone(result)

    def test_get_current_hour_start_sample_returns_the_earliest_row_in_the_bucket(self):
        storage.insert_sample_sync(self.conn, _sample_row('2026-08-28 13:58:00'))  # previous hour
        storage.insert_sample_sync(self.conn, _sample_row('2026-08-28 14:00:03', ppv='10'))
        storage.insert_sample_sync(self.conn, _sample_row('2026-08-28 14:05:00', ppv='20'))  # later, same hour
        start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 0))

        result = storage.get_current_hour_start_sample(self.conn, start, end)

        self.assertEqual(result['timestamp'], '2026-08-28 14:00:03')
        self.assertEqual(result['ppv'], 10.0)


class ParseTimestampEpochTest(unittest.TestCase):
    def test_round_trips_through_local_time(self):
        epoch = storage.parse_timestamp_epoch('2026-08-28 14:05:00')

        roundtrip = datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')

        self.assertEqual(roundtrip, '2026-08-28 14:05:00')


class CurrentHourBoundsTest(unittest.TestCase):
    def test_returns_the_start_and_end_of_the_containing_hour(self):
        start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 12))

        self.assertEqual(end - start, 3600)
        self.assertEqual(
            datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S'),
            '2026-08-28 14:00:00',
        )


class StorageAsyncTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)

    def tearDown(self):
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_insert_and_read_back_a_sample_async(self):
        async def scenario():
            conn = await storage.init_db_async(self.db_path, sensor_columns())
            try:
                await storage.insert_sample_async(conn, _sample_row('2026-08-28 14:05:00', ppv='42'))
                cursor = await conn.execute("SELECT ppv FROM inverter_history")
                row = await cursor.fetchone()
                return row[0]
            finally:
                await conn.close()

        result = asyncio.new_event_loop().run_until_complete(scenario())
        self.assertEqual(result, 42.0)

    def test_get_current_hour_start_sample_async(self):
        async def scenario():
            conn = await storage.init_db_async(self.db_path, sensor_columns())
            try:
                await storage.insert_sample_async(conn, _sample_row('2026-08-28 14:00:03', ppv='10'))
                start, end = storage.current_hour_bounds(datetime(2026, 8, 28, 14, 37, 0))
                return await storage.get_current_hour_start_sample_async(conn, start, end)
            finally:
                await conn.close()

        result = asyncio.new_event_loop().run_until_complete(scenario())
        self.assertEqual(result['timestamp'], '2026-08-28 14:00:03')


if __name__ == '__main__':
    unittest.main()
