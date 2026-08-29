import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime

import storage
import history
from sensors import sensor_columns


class ResolveRawColumnsTest(unittest.TestCase):
    def test_none_returns_the_default_subset(self):
        self.assertEqual(history.resolve_raw_columns(None), list(history.DEFAULT_RAW_COLUMNS))

    def test_unknown_columns_are_dropped_silently(self):
        result = history.resolve_raw_columns(['ppv', 'not_a_real_column'])
        self.assertEqual(result, ['timestamp', 'ppv'])

    def test_all_unknown_falls_back_to_default(self):
        result = history.resolve_raw_columns(['not_a_real_column'])
        self.assertEqual(result, list(history.DEFAULT_RAW_COLUMNS))

    def test_empty_list_falls_back_to_default(self):
        result = history.resolve_raw_columns([])
        self.assertEqual(result, list(history.DEFAULT_RAW_COLUMNS))

    def test_timestamp_is_always_first_even_if_not_requested(self):
        result = history.resolve_raw_columns(['battery_soc', 'ppv'])
        self.assertEqual(result[0], 'timestamp')

    def test_result_follows_raw_columns_canonical_order_not_request_order(self):
        # ppv appears before battery_soc in RAW_COLUMNS
        result = history.resolve_raw_columns(['battery_soc', 'ppv'])
        self.assertEqual(result, ['timestamp', 'ppv', 'battery_soc'])

    def test_duplicate_requested_columns_are_deduplicated(self):
        result = history.resolve_raw_columns(['ppv', 'ppv', 'ppv'])
        self.assertEqual(result, ['timestamp', 'ppv'])


class ResolveLimitTest(unittest.TestCase):
    def test_valid_limit_is_kept(self):
        self.assertEqual(history.resolve_limit('250'), 250)

    def test_none_defaults_to_100(self):
        self.assertEqual(history.resolve_limit(None), 100)

    def test_not_in_allowed_set_defaults_to_100(self):
        self.assertEqual(history.resolve_limit('999'), 100)

    def test_non_numeric_defaults_to_100(self):
        self.assertEqual(history.resolve_limit('abc'), 100)


class ResolveOffsetTest(unittest.TestCase):
    def test_valid_offset_is_kept(self):
        self.assertEqual(history.resolve_offset('300'), 300)

    def test_none_defaults_to_zero(self):
        self.assertEqual(history.resolve_offset(None), 0)

    def test_negative_defaults_to_zero(self):
        self.assertEqual(history.resolve_offset('-5'), 0)

    def test_non_numeric_defaults_to_zero(self):
        self.assertEqual(history.resolve_offset('abc'), 0)


class DateRangeTest(unittest.TestCase):
    def test_parse_date_or_default_parses_iso_date(self):
        result = history.parse_date_or_default('2026-08-20', default=date(2000, 1, 1))
        self.assertEqual(result, date(2026, 8, 20))

    def test_parse_date_or_default_falls_back_on_none(self):
        result = history.parse_date_or_default(None, default=date(2000, 1, 1))
        self.assertEqual(result, date(2000, 1, 1))

    def test_parse_date_or_default_falls_back_on_garbage(self):
        result = history.parse_date_or_default('not-a-date', default=date(2000, 1, 1))
        self.assertEqual(result, date(2000, 1, 1))

    def test_default_date_range_is_last_7_days_inclusive(self):
        start, end = history.default_date_range(today=date(2026, 8, 29))
        self.assertEqual(start, date(2026, 8, 23))
        self.assertEqual(end, date(2026, 8, 29))

    def test_date_range_to_epoch_covers_full_days_local_time(self):
        start_epoch, end_epoch = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 28))
        self.assertEqual(datetime.fromtimestamp(start_epoch), datetime(2026, 8, 27, 0, 0, 0))
        # end is exclusive: start of the day AFTER end_date
        self.assertEqual(datetime.fromtimestamp(end_epoch), datetime(2026, 8, 29, 0, 0, 0))


def _sample_row(timestamp: str, **overrides) -> dict:
    row = {name: ('0' if sql_type == 'REAL' else '') for name, sql_type in sensor_columns()}
    row['timestamp'] = timestamp
    row.update(overrides)
    return row


class FetchInverterRowsTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())
        for i in range(5):
            storage.insert_sample_sync(self.conn, _sample_row(
                f'2026-08-27 10:0{i}:00', ppv=str(100 * i), battery_soc=str(50 + i)))

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_returns_only_the_requested_columns_in_order(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, has_more = history.fetch_inverter_rows(
            self.conn, ['timestamp', 'ppv'], start, end, limit=10, offset=0)
        self.assertEqual(list(rows[0].keys()), ['timestamp', 'ppv'])
        self.assertFalse(has_more)

    def test_orders_newest_first(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=10, offset=0)
        timestamps = [r['timestamp'] for r in rows]
        self.assertEqual(timestamps, sorted(timestamps, reverse=True))

    def test_has_more_true_when_more_rows_exist_than_limit(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, has_more = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=2, offset=0)
        self.assertEqual(len(rows), 2)
        self.assertTrue(has_more)

    def test_offset_skips_rows(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        first_page, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=2, offset=0)
        second_page, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=2, offset=2)
        self.assertNotEqual(first_page, second_page)

    def test_date_range_excludes_rows_outside_it(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 28), date(2026, 8, 28))
        rows, _ = history.fetch_inverter_rows(
            self.conn, ['timestamp'], start, end, limit=10, offset=0)
        self.assertEqual(rows, [])

    def test_rejects_a_column_not_in_the_allow_list(self):
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        with self.assertRaises(ValueError):
            history.fetch_inverter_rows(
                self.conn, ['timestamp; DROP TABLE inverter_history'], start, end, limit=10, offset=0)


class FetchHourlyRowsTest(unittest.TestCase):
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

    def _insert_hourly(self, hour_start_str, **overrides):
        hour_start = storage.parse_timestamp_epoch(hour_start_str)
        row = {'hour_start': hour_start, 'meter_export_kwh': 1.0, 'meter_import_kwh': 0.0,
               'load_kwh': 0.5, 'pv_kwh': 1.5, 'battery_charge_kwh': 0.0, 'battery_discharge_kwh': 0.0}
        row.update(overrides)
        self.conn.execute(
            "INSERT INTO hourly_summary (hour_start, meter_export_kwh, meter_import_kwh, load_kwh, "
            "pv_kwh, battery_charge_kwh, battery_discharge_kwh) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row['hour_start'], row['meter_export_kwh'], row['meter_import_kwh'], row['load_kwh'],
             row['pv_kwh'], row['battery_charge_kwh'], row['battery_discharge_kwh']),
        )
        self.conn.commit()

    def test_formats_hour_start_as_a_local_timestamp_string(self):
        self._insert_hourly('2026-08-27 13:00:00')
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, _ = history.fetch_hourly_rows(self.conn, start, end, limit=10, offset=0)
        self.assertEqual(rows[0]['hour_start'], '2026-08-27 13:00')

    def test_returns_all_seven_columns(self):
        self._insert_hourly('2026-08-27 13:00:00')
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, _ = history.fetch_hourly_rows(self.conn, start, end, limit=10, offset=0)
        self.assertEqual(list(rows[0].keys()), list(history.HOURLY_COLUMNS))

    def test_has_more_true_when_more_rows_exist_than_limit(self):
        self._insert_hourly('2026-08-27 10:00:00')
        self._insert_hourly('2026-08-27 11:00:00')
        self._insert_hourly('2026-08-27 12:00:00')
        start, end = history.date_range_to_epoch(date(2026, 8, 27), date(2026, 8, 27))
        rows, has_more = history.fetch_hourly_rows(self.conn, start, end, limit=2, offset=0)
        self.assertEqual(len(rows), 2)
        self.assertTrue(has_more)


if __name__ == '__main__':
    unittest.main()
