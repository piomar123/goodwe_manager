import os
import tempfile
import unittest
from datetime import datetime

import storage
from sensors import sensor_columns
import _calculate_income as income


class FetchHourlySummaryTest(unittest.TestCase):
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

    def _insert_hourly_summary(self, hour_start_str, export_kwh, import_kwh, load_kwh):
        hour_start = storage.parse_timestamp_epoch(hour_start_str)
        self.conn.execute(
            "INSERT INTO hourly_summary (hour_start, meter_export_kwh, meter_import_kwh, load_kwh) "
            "VALUES (?, ?, ?, ?)",
            (hour_start, export_kwh, import_kwh, load_kwh),
        )
        self.conn.commit()

    def test_returns_rows_keyed_by_hour_of_day_for_the_requested_date(self):
        self._insert_hourly_summary('2026-08-28 00:00:00', 1.0, 0.5, 2.0)
        self._insert_hourly_summary('2026-08-28 13:00:00', 3.0, 1.0, 4.0)
        self._insert_hourly_summary('2026-08-29 00:00:00', 9.0, 9.0, 9.0)  # different day, excluded

        rows = income.fetch_hourly_summary(self.conn, datetime(2026, 8, 28))

        self.assertEqual(set(rows.keys()), {0, 13})
        self.assertEqual(rows[13], (3.0, 1.0, 4.0))

    def test_returns_empty_dict_when_no_data_for_the_date(self):
        rows = income.fetch_hourly_summary(self.conn, datetime(2026, 8, 28))

        self.assertEqual(rows, {})


class ComputeHourIncomeTest(unittest.TestCase):
    def test_positive_balance_is_valued_at_the_rce_price(self):
        result = income.compute_hour_income(hourly_export=5.0, hourly_import=1.0, load_kwh=3.0,
                                            rce_price_pln_per_mwh=400.0)

        # balance = 4.0 kWh exported net, priced at 0.4 zl/kWh = 1.6 zl
        self.assertAlmostEqual(result['balance_kwh'], 4.0)
        self.assertAlmostEqual(result['meter_pln'], 1.6)
        # no_buy_pln values the load at the flat import price, independent of the meter balance
        self.assertAlmostEqual(result['no_buy_pln'], 3.0 * income.IMPORT_PRICE_KWH)
        self.assertAlmostEqual(result['gain_pln'], result['meter_pln'] + result['no_buy_pln'])

    def test_negative_balance_is_valued_at_the_flat_import_price_not_the_rce_price(self):
        result = income.compute_hour_income(hourly_export=1.0, hourly_import=5.0, load_kwh=3.0,
                                            rce_price_pln_per_mwh=400.0)

        self.assertAlmostEqual(result['balance_kwh'], -4.0)
        self.assertAlmostEqual(result['meter_pln'], -4.0 * income.IMPORT_PRICE_KWH)


if __name__ == '__main__':
    unittest.main()
