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


if __name__ == '__main__':
    unittest.main()
