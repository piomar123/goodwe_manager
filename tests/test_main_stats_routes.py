import os
import tempfile
import unittest
from datetime import date, datetime
from unittest.mock import patch

import settlement
import storage
from sensors import sensor_columns

import main

TZ = settlement.LOCAL_TZ


class StatsRoutesTest(unittest.TestCase):
    def setUp(self):
        main.app.testing = True
        self.client = main.app.test_client()
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        conn = storage.init_db_sync(self.db_path, sensor_columns())
        settlement.init_table(conn)
        history = settlement.TariffHistory([settlement.TariffPeriod(date(2024, 1, 1), flat_price=1.10)])
        for day, imp, exp in ((date(2025, 12, 31), 2.0, 0.0), (date(2026, 1, 1), 1.0, 3.0)):
            conn.execute(
                "INSERT INTO hourly_summary (hour_start, meter_import_kwh, meter_export_kwh, load_kwh, pv_kwh) "
                "VALUES (?, ?, ?, 2.0, 4.0)",
                (int(datetime(day.year, day.month, day.day, 12, tzinfo=TZ).timestamp()), imp, exp))
            conn.commit()
            settlement.update_day(conn, day, history, lambda d: [('12:00', 400.0)])
        conn.close()
        self._orig = storage.DATA_DB_PATH
        storage.DATA_DB_PATH = self.db_path

    def tearDown(self):
        storage.DATA_DB_PATH = self._orig
        for suffix in ('', '-wal', '-shm'):
            if os.path.exists(self.db_path + suffix):
                os.remove(self.db_path + suffix)

    def test_stats_page_lists_months_year_totals_and_selected_month(self):
        with patch.object(main, 'INVESTMENT_NET_PLN', 29300.0):
            response = self.client.get('/stats?month=2025-12')
        self.assertEqual(response.status_code, 200)
        page = response.get_data(as_text=True)
        self.assertIn('2025-12', page)
        self.assertIn('2026-01', page)
        self.assertIn('Daily 2025-12', page)
        self.assertIn('29300 zł', page)
        self.assertIn("initStickyTableHeader(document.getElementById('monthly-table'))", page)
        self.assertIn("initStickyTableHeader(document.getElementById('daily-table'))", page)

    def test_stats_page_works_with_no_data(self):
        conn = storage.init_db_sync(self.db_path, sensor_columns())
        conn.execute("DELETE FROM daily_settlement")
        conn.commit()
        conn.close()
        self.assertEqual(self.client.get('/stats').status_code, 200)

    def test_csv_has_header_and_a_row_per_day(self):
        lines = self.client.get('/stats/daily.csv').get_data(as_text=True).strip().split('\n')
        self.assertTrue(lines[0].startswith('day,hours,'))
        self.assertEqual(len(lines), 3)


class UpdateSettlementTest(unittest.TestCase):
    def test_never_raises(self):
        with patch.object(settlement, 'tariff_history_from_env', side_effect=ValueError('bad yaml')):
            main._update_settlement(None)  # logs a warning only
