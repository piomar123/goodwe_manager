import asyncio
import json
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

    def test_init_db_sync_adds_missing_columns_to_an_existing_table(self):
        self.conn.close()
        fd, small_db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(small_db_path)
        try:
            small_columns = sensor_columns()[:-1]
            new_column_name, new_column_type = sensor_columns()[-1]
            conn = storage.init_db_sync(small_db_path, small_columns)
            conn.execute(
                "INSERT INTO inverter_history (timestamp_epoch, timestamp) VALUES (?, ?)",
                (12345, '2026-08-28 14:00:00'),
            )
            conn.commit()
            conn.close()

            conn = storage.init_db_sync(small_db_path, sensor_columns())

            column_names = {row[1] for row in conn.execute("PRAGMA table_info(inverter_history)")}
            self.assertIn(new_column_name, column_names)
            row = conn.execute("SELECT timestamp_epoch, timestamp FROM inverter_history").fetchone()
            self.assertEqual(row, (12345, '2026-08-28 14:00:00'))
            conn.close()
        finally:
            for suffix in ('', '-wal', '-shm'):
                path = small_db_path + suffix
                if os.path.exists(path):
                    os.remove(path)

    def test_init_db_sync_adds_new_hourly_summary_columns_to_an_existing_table(self):
        fd, small_db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(small_db_path)
        try:
            conn = sqlite3.connect(small_db_path)
            conn.execute("""
                CREATE TABLE hourly_summary (
                    hour_start INTEGER PRIMARY KEY,
                    meter_export_kwh REAL,
                    meter_import_kwh REAL,
                    load_kwh REAL,
                    pv_kwh REAL,
                    battery_charge_kwh REAL,
                    battery_discharge_kwh REAL
                )
            """)
            conn.execute(
                "INSERT INTO hourly_summary (hour_start, meter_export_kwh) VALUES (?, ?)",
                (12345, 1.5),
            )
            conn.commit()
            conn.close()

            conn = storage.init_db_sync(small_db_path, sensor_columns())

            column_names = {row[1] for row in conn.execute("PRAGMA table_info(hourly_summary)")}
            for expected in ('sample_count',
                              'vgrid_min', 'vgrid_max', 'vgrid2_min', 'vgrid2_max', 'vgrid3_min', 'vgrid3_max',
                              'fgrid_min', 'fgrid_max', 'fgrid2_min', 'fgrid2_max', 'fgrid3_min', 'fgrid3_max',
                              'inverter_temp_min', 'inverter_temp_max', 'battery_temp_min', 'battery_temp_max',
                              'work_mode_breakdown'):
                self.assertIn(expected, column_names)
            row = conn.execute("SELECT hour_start, meter_export_kwh FROM hourly_summary").fetchone()
            self.assertEqual(row, (12345, 1.5))
            conn.close()
        finally:
            for suffix in ('', '-wal', '-shm'):
                path = small_db_path + suffix
                if os.path.exists(path):
                    os.remove(path)

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

    def test_init_db_async_adds_missing_columns_to_an_existing_table(self):
        async def scenario():
            small_columns = sensor_columns()[:-1]
            new_column_name, _ = sensor_columns()[-1]
            conn = await storage.init_db_async(self.db_path, small_columns)
            await conn.execute(
                "INSERT INTO inverter_history (timestamp_epoch, timestamp) VALUES (?, ?)",
                (12345, '2026-08-28 14:00:00'),
            )
            await conn.commit()
            await conn.close()

            conn = await storage.init_db_async(self.db_path, sensor_columns())
            cursor = await conn.execute("PRAGMA table_info(inverter_history)")
            column_names = {row[1] for row in await cursor.fetchall()}
            cursor2 = await conn.execute("SELECT timestamp_epoch, timestamp FROM inverter_history")
            row = await cursor2.fetchone()
            await conn.close()
            return column_names, row

        column_names, row = asyncio.new_event_loop().run_until_complete(scenario())
        new_column_name, _ = sensor_columns()[-1]
        self.assertIn(new_column_name, column_names)
        self.assertEqual(row, (12345, '2026-08-28 14:00:00'))

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

    def test_computes_sample_count_and_quality_stats_for_the_hour(self):
        # hour 14:00 - two samples with distinct grid/temperature readings
        self._insert('2026-08-28 14:05:00', vgrid='229.5', vgrid2='230.1', vgrid3='228.9',
                     fgrid='49.98', fgrid2='50.01', fgrid3='49.95',
                     temperature='42.0', battery_temperature='28.0')
        self._insert('2026-08-28 14:35:00', vgrid='231.0', vgrid2='229.0', vgrid3='230.5',
                     fgrid='50.05', fgrid2='49.90', fgrid3='50.02',
                     temperature='45.5', battery_temperature='29.5')
        # hour 15:00 - proves 14:00 is complete
        self._insert('2026-08-28 15:05:00')

        storage.backfill_hourly_summary(self.conn)

        row = self.conn.execute(
            "SELECT sample_count, "
            "vgrid_min, vgrid_max, vgrid2_min, vgrid2_max, vgrid3_min, vgrid3_max, "
            "fgrid_min, fgrid_max, fgrid2_min, fgrid2_max, fgrid3_min, fgrid3_max, "
            "inverter_temp_min, inverter_temp_max, battery_temp_min, battery_temp_max "
            "FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        self.assertEqual(row, (
            2,
            229.5, 231.0, 229.0, 230.1, 228.9, 230.5,
            49.98, 50.05, 49.90, 50.01, 49.95, 50.02,
            42.0, 45.5, 28.0, 29.5,
        ))

    def test_quality_stats_are_null_per_phase_when_that_phase_is_null(self):
        # single-phase-style sample: only phase 1 populated, phases 2/3 NULL
        self._insert('2026-08-28 14:05:00', vgrid='230.0', vgrid2=None, vgrid3=None,
                     fgrid='50.0', fgrid2=None, fgrid3=None)
        self._insert('2026-08-28 15:05:00')  # proves 14:00 is complete

        storage.backfill_hourly_summary(self.conn)

        row = self.conn.execute(
            "SELECT vgrid_min, vgrid_max, vgrid2_min, vgrid2_max, vgrid3_min, vgrid3_max, "
            "fgrid_min, fgrid_max, fgrid2_min, fgrid2_max, fgrid3_min, fgrid3_max "
            "FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        self.assertEqual(row, (230.0, 230.0, None, None, None, None, 50.0, 50.0, None, None, None, None))

    def test_records_work_mode_label_breakdown_as_json_sample_counts(self):
        self._insert('2026-08-28 14:05:00', work_mode_label='Normal (On-Grid)')
        self._insert('2026-08-28 14:15:00', work_mode_label='Normal (On-Grid)')
        self._insert('2026-08-28 14:25:00', work_mode_label='Fault')
        self._insert('2026-08-28 15:05:00')  # proves 14:00 is complete

        storage.backfill_hourly_summary(self.conn)

        row = self.conn.execute(
            "SELECT work_mode_breakdown FROM hourly_summary WHERE hour_start = ?",
            (storage.parse_timestamp_epoch('2026-08-28 14:00:00'),),
        ).fetchone()
        self.assertEqual(json.loads(row[0]), {'Normal (On-Grid)': 2, 'Fault': 1})


class FindHoursNeedingBackfillTest(unittest.TestCase):
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

    def test_finds_a_multi_hour_gap_left_behind_by_a_long_service_outage(self):
        # hour 10:00 was already backfilled a while ago (sets the watermark)
        self._insert('2026-08-28 10:05:00')
        self._insert('2026-08-28 11:05:00')  # proves 10:00 is complete
        storage.backfill_hourly_summary(self.conn)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM hourly_summary").fetchone()[0], 1,
        )

        # the service was down; raw samples resume covering an untouched
        # multi-hour span, none of which has a hourly_summary row yet
        self._insert('2026-08-28 15:05:00')
        self._insert('2026-08-28 16:05:00')
        self._insert('2026-08-28 17:05:00')  # proves 16:00 is complete

        needing_backfill = storage.find_hours_needing_backfill(self.conn)

        self.assertEqual(needing_backfill, [
            storage.parse_timestamp_epoch('2026-08-28 15:00:00'),
            storage.parse_timestamp_epoch('2026-08-28 16:00:00'),
        ])

    def test_does_not_return_hours_already_backfilled(self):
        self._insert('2026-08-28 10:05:00')
        self._insert('2026-08-28 11:05:00')
        storage.backfill_hourly_summary(self.conn)

        self.assertEqual(storage.find_hours_needing_backfill(self.conn), [])


if __name__ == '__main__':
    unittest.main()
