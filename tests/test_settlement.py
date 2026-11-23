import os
import tempfile
import textwrap
import unittest
from datetime import date, datetime, timedelta

import settlement
import storage
from sensors import sensor_columns

TZ = settlement.LOCAL_TZ


def _epoch(y, m, d, h, fold=0):
    return int(datetime(y, m, d, h, tzinfo=TZ, fold=fold).timestamp())


def _flat(price):
    return lambda dt: price


class HourlyExportPricesTest(unittest.TestCase):
    def test_averages_quarters_applies_bonus_and_zeroes_negative_hours(self):
        rce = [('10:00', 400.0), ('10:15', 500.0), ('10:30', 600.0), ('10:45', 500.0),
               ('11:00', -100.0), ('11:15', 50.0), ('11:30', -100.0), ('11:45', -50.0),
               ('24:00', 999.0)]
        prices = settlement.hourly_export_prices(rce, date(2025, 2, 1))
        self.assertAlmostEqual(prices['10'], 0.5 * 1.23)
        self.assertEqual(prices['11'], 0.0)
        self.assertNotIn('24', prices)

    def test_no_bonus_before_february_2025(self):
        prices = settlement.hourly_export_prices([('10:00', 500.0)], date(2025, 1, 31))
        self.assertAlmostEqual(prices['10'], 0.5)

    def test_fall_back_repeated_hour_is_its_own_key(self):
        prices = settlement.hourly_export_prices([('02:45', 100.0), ('02a:00', 300.0)], date(2026, 10, 25))
        self.assertIn('02', prices)
        self.assertIn('02a', prices)
        self.assertEqual(settlement.hour_key_of(datetime.fromtimestamp(_epoch(2026, 10, 25, 2, fold=1), TZ)), '02a')


class ComputeDayTest(unittest.TestCase):
    def test_nets_within_the_hour_and_prices_each_side(self):
        hours = [
            # hour, import, export, load, pv, b_chg, b_dis
            (_epoch(2026, 6, 1, 12), 0.5, 2.5, 1.0, 4.0, 0.0, 0.0),   # net export 2.0
            (_epoch(2026, 6, 1, 20), 1.5, 0.5, 2.0, 0.0, 0.0, 1.0),   # net import 1.0
        ]
        totals = settlement.compute_day(hours, _flat(1.0), {'12': 0.6, '20': 0.9}, pv_extra_load_factor=0.10)
        self.assertEqual(totals['hours'], 2)
        self.assertAlmostEqual(totals['import_kwh'], 2.0)
        self.assertAlmostEqual(totals['export_kwh'], 3.0)
        self.assertAlmostEqual(totals['net_export_kwh'], 2.0)
        self.assertAlmostEqual(totals['net_import_kwh'], 1.0)
        self.assertAlmostEqual(totals['export_credit_pln'], 1.2)
        self.assertAlmostEqual(totals['net_import_pln'], 1.0)
        # PV used in the house at 12:00 = 4.0 - 2.5 = 1.5, 10 % of it is PV-induced load
        self.assertAlmostEqual(totals['pv_self_kwh'], 1.5)
        self.assertAlmostEqual(totals['baseline_kwh'], 1.0 - 0.15 + 2.0)
        self.assertAlmostEqual(totals['baseline_pln'], 2.85)
        self.assertAlmostEqual(totals['savings_pln'], 2.85 - 1.0 + 1.2)

    def test_cheap_grid_energy_used_at_expensive_hour_counts_as_saving(self):
        night, evening = _epoch(2026, 1, 5, 2), _epoch(2026, 1, 5, 18)
        price = lambda dt: 0.7 if dt.hour == 2 else 1.3
        hours = [(night, 5.0, 0.0, 1.0, 0.0, 4.0, 0.0),
                 (evening, 0.0, 0.0, 4.0, 0.0, 0.0, 4.0)]
        totals = settlement.compute_day(hours, price, {})
        self.assertAlmostEqual(totals['net_import_pln'], 3.5)
        self.assertAlmostEqual(totals['baseline_pln'], 0.7 + 5.2)
        self.assertAlmostEqual(totals['savings_pln'], 5.9 - 3.5)

    def test_skips_incomplete_hours_and_flags_missing_export_price(self):
        hours = [(_epoch(2026, 6, 1, 0), None, None, None, None, None, None),
                 (_epoch(2026, 6, 1, 13), 0.0, 1.0, 0.5, 1.5, None, None)]
        totals = settlement.compute_day(hours, _flat(1.0), {})
        self.assertEqual(totals['hours'], 1)
        self.assertEqual(totals['export_price_missing_hours'], 1)
        self.assertEqual(totals['export_credit_pln'], 0.0)

    def test_hours_before_the_first_tariff_period_are_left_out(self):
        hours = [(_epoch(2026, 6, 1, 0), 1.0, 0.0, 1.0, 0.0, 0.0, 0.0)]
        totals = settlement.compute_day(hours, lambda dt: None, {})
        self.assertEqual(totals['hours'], 0)


class TariffHistoryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _write(self, name, text):
        path = os.path.join(self.dir, name)
        with open(path, 'w') as f:
            f.write(textwrap.dedent(text))
        return path

    def test_flat_then_tariff_engine_period(self):
        self._write('g12.yaml', """
            components:
              total:
                prices: {cheap: 0.5, expensive: 1.5}
                bands:
                  default:
                    - days: "Work,Sa,Su,Holiday"
                      start: "22:00"
                      end: "06:00"
                      price: cheap
                default_price: expensive
            """)
        path = self._write('history.yaml', """
            periods:
              - from: 2025-09-18
                tariff: g12.yaml
              - from: 2024-01-01
                price: 1.10
            """)
        history = settlement.load_tariff_history(path)
        self.assertIsNone(history.price_at(datetime(2023, 12, 31, 12, tzinfo=TZ)))
        self.assertEqual(history.price_at(datetime(2025, 9, 17, 12, tzinfo=TZ)), 1.10)
        self.assertEqual(history.price_at(datetime(2025, 9, 18, 12, tzinfo=TZ)), 1.5)
        self.assertEqual(history.price_at(datetime(2025, 9, 18, 23, tzinfo=TZ)), 0.5)

    def test_rejects_period_with_both_price_and_tariff(self):
        path = self._write('bad.yaml', """
            periods:
              - from: 2024-01-01
                price: 1.0
                tariff: x.yaml
            """)
        with self.assertRaises(ValueError):
            settlement.load_tariff_history(path)


class StorageTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())
        settlement.init_table(self.conn)

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            if os.path.exists(self.db_path + suffix):
                os.remove(self.db_path + suffix)

    def _hour(self, epoch, imp, exp, load, pv):
        self.conn.execute(
            "INSERT INTO hourly_summary (hour_start, meter_import_kwh, meter_export_kwh, load_kwh, pv_kwh, "
            "battery_charge_kwh, battery_discharge_kwh) VALUES (?, ?, ?, ?, ?, 0, 0)",
            (epoch, imp, exp, load, pv))
        self.conn.commit()

    def test_update_day_stores_and_monthly_sums(self):
        self._hour(_epoch(2026, 6, 1, 12), 0.0, 2.0, 1.0, 3.0)
        self._hour(_epoch(2026, 6, 2, 12), 1.0, 0.0, 1.0, 0.0)
        self._hour(_epoch(2026, 7, 1, 12), 1.0, 0.0, 1.0, 0.0)
        history = settlement.TariffHistory([settlement.TariffPeriod(date(2024, 1, 1), flat_price=1.0)])
        rce = lambda day: [(f'{h:02d}:00', 500.0) for h in range(24)]
        for day in (date(2026, 6, 1), date(2026, 6, 2), date(2026, 7, 1)):
            settlement.update_day(self.conn, day, history, rce, pv_extra_load_factor=0.0)
        self.assertIsNone(settlement.update_day(self.conn, date(2026, 7, 2), history, rce))

        months = settlement.monthly(self.conn)
        self.assertEqual([m['period'] for m in months], ['2026-06', '2026-07'])
        june = months[0]
        self.assertEqual(june['days'], 2)
        self.assertAlmostEqual(june['export_credit_pln'], 2.0 * 0.5 * 1.23)
        self.assertAlmostEqual(june['net_import_pln'], 1.0)
        self.assertEqual(len(settlement.daily(self.conn, '2026-06')), 2)

    def test_failed_rce_fetch_is_retried_later(self):
        self._hour(_epoch(2026, 6, 1, 12), 0.0, 2.0, 1.0, 3.0)
        history = settlement.TariffHistory([settlement.TariffPeriod(date(2024, 1, 1), flat_price=1.0)])

        def failing(day):
            raise RuntimeError("PSE down")
        totals = settlement.update_day(self.conn, date(2026, 6, 1), history, failing)
        self.assertEqual(totals['export_price_missing_hours'], 1)
        self.assertIn(date(2026, 6, 1), settlement.days_needing_update(self.conn, date(2026, 6, 10)))

    def test_day_bounds_cover_25_hours_on_fall_back_day(self):
        start, end = settlement.day_bounds(date(2026, 10, 25))
        self.assertEqual(end - start, 25 * 3600)


class PaybackTest(unittest.TestCase):
    def test_projects_payback_from_last_year_rate(self):
        today = date(2026, 1, 1)
        rows = [((today - timedelta(days=i)).isoformat(), 10.0) for i in range(1, 366)]
        result = settlement.payback(rows, investment_pln=7300.0, today=today)
        self.assertAlmostEqual(result['total_pln'], 3650.0)
        self.assertAlmostEqual(result['per_year_pln'], 3650.0)
        self.assertEqual(result['payback_date'], (today + timedelta(days=365)).isoformat())


class ExpectedDaysTest(unittest.TestCase):
    def test_caps_at_first_day_and_today(self):
        self.assertEqual(settlement.expected_days('2025-07', date(2026, 1, 1), '2024-08-12'), 31)
        self.assertEqual(settlement.expected_days('2024-08', date(2026, 1, 1), '2024-08-12'), 20)
        self.assertEqual(settlement.expected_days('2026-11', date(2026, 11, 23), '2024-08-12'), 23)
        self.assertEqual(settlement.expected_days('2025-12', date(2026, 1, 1), None), 31)
