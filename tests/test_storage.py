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


class BackfillHourlySummaryTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def _insert(self, timestamp, **overrides):
        storage.insert_sample_sync(self.conn, _sample_row(timestamp, **overrides))

    def test_backfills_an_hour_that_has_a_prior_and_a_following_hour(self):
        # hour 13:00 - baseline
        self._insert('2026-08-28 13:05:00', meter_e_total_exp='100.0', meter_e_total_imp='50.0',
                     e_load_total='10.0', e_day='5.0', e_bat_charge_total='1.0', e_bat_discharge_total='0.5')
        # hour 14:00 - the hour under test
        self._insert('2026-08-28 14:05:00', meter_e_total_exp='103.0', meter_e_total_imp='51.0',
                     e_load_total='14.0', e_day='9.0', e_bat_charge_total='2.0', e_bat_discharge_total='1.5')
        # hour 15:00 - proves 14:00 is complete
        self._insert('2026-08-28 15:05:00', meter_e_total_exp='110.0', meter_e_total_imp='55.0',
                     e_load_total='20.0', e_day='12.0', e_bat_charge_total='3.0', e_bat_discharge_total='2.0')

        backfilled = storage.backfill_hourly_summary(self.conn)

        self.assertEqual(backfilled, 2)  # hour 13:00 (NULL diffs) and hour 14:00 (real diffs)
        row = self.conn.execute(
            "SELECT meter_export_kwh, meter_import_kwh, load_kwh, pv_kwh, battery_charge_kwh, battery_discharge_kwh "
            "FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        self.assertEqual(row, (3.0, 1.0, 4.0, 4.0, 1.0, 1.0))

        # hour 15:00 has no following hour yet - not backfilled
        count_for_15 = self.conn.execute(
            "SELECT COUNT(*) FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 15:00:00'),),
        ).fetchone()[0]
        self.assertEqual(count_for_15, 0)

    def test_hour_with_no_prior_data_gets_null_metrics_but_is_marked_processed(self):
        self._insert('2026-08-28 14:05:00', meter_e_total_exp='103.0')
        self._insert('2026-08-28 15:05:00', meter_e_total_exp='110.0')  # proves 14:00 complete

        storage.backfill_hourly_summary(self.conn)

        row = self.conn.execute(
            "SELECT meter_export_kwh FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        self.assertIsNone(row[0])

    def test_is_idempotent(self):
        self._insert('2026-08-28 13:05:00', meter_e_total_exp='100.0')
        self._insert('2026-08-28 14:05:00', meter_e_total_exp='103.0')
        self._insert('2026-08-28 15:05:00', meter_e_total_exp='110.0')
        storage.backfill_hourly_summary(self.conn)

        second_run_count = storage.backfill_hourly_summary(self.conn)

        self.assertEqual(second_run_count, 0)

    def test_pv_kwh_does_not_go_negative_across_midnight(self):
        # e_day resets to 0 right after local midnight (confirmed against
        # real device data), unlike the other lifetime counters - so the
        # hour spanning midnight must not diff e_day against the previous
        # (different day's) value.
        self._insert('2026-08-28 23:05:00', e_day='47.9')          # last hour of the previous day
        self._insert('2026-08-29 00:05:00', e_day='0.3')           # the hour under test
        self._insert('2026-08-29 01:05:00', e_day='0.5')           # proves 00:00 is complete

        storage.backfill_hourly_summary(self.conn)

        row = self.conn.execute(
            "SELECT pv_kwh FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-29 00:00:00'),),
        ).fetchone()
        self.assertEqual(row[0], 0.3)

    def test_a_null_metric_column_in_one_hour_does_not_crash_or_poison_other_metrics(self):
        # hour 13:00 - baseline, all 6 metric-source columns set
        self._insert('2026-08-28 13:05:00', meter_e_total_exp='100.0', meter_e_total_imp='50.0',
                     e_load_total='10.0', e_day='5.0', e_bat_charge_total='1.0', e_bat_discharge_total='0.5')
        # hour 14:00 - the hour under test: e_bat_charge_total is NULL for this
        # sample (e.g. the inverter didn't return that sensor this poll), but
        # every other metric source column is set normally.
        self._insert('2026-08-28 14:05:00', meter_e_total_exp='103.0', meter_e_total_imp='51.0',
                     e_load_total='14.0', e_day='9.0', e_bat_charge_total=None, e_bat_discharge_total='1.5')
        # hour 15:00 - proves 14:00 is complete
        self._insert('2026-08-28 15:05:00', meter_e_total_exp='110.0', meter_e_total_imp='55.0',
                     e_load_total='20.0', e_day='12.0', e_bat_charge_total='3.0', e_bat_discharge_total='2.0')

        # must not raise TypeError
        backfilled = storage.backfill_hourly_summary(self.conn)

        self.assertEqual(backfilled, 2)
        row = self.conn.execute(
            "SELECT meter_export_kwh, meter_import_kwh, load_kwh, pv_kwh, battery_charge_kwh, battery_discharge_kwh "
            "FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        # only battery_charge_kwh (sourced from the NULL column) is None;
        # every other metric still computes normally for that hour
        self.assertEqual(row, (3.0, 1.0, 4.0, 4.0, None, 1.0))


if __name__ == '__main__':
    unittest.main()
