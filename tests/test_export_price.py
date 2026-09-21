import os
import tempfile
import unittest
from datetime import date
from zoneinfo import ZoneInfo

import rce_storage
import export_price

WARSAW = ZoneInfo('Europe/Warsaw')


class BuildExportPricePayloadTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        rce_storage.RCE_DB_PATH = self.db_path

    def tearDown(self):
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def _store(self, business_date, periods_and_prices):
        conn = rce_storage.init_db()
        rce_storage.store_prices(conn, business_date, periods_and_prices)
        conn.close()

    def test_applies_vat_bonus_and_converts_to_kwh(self):
        self._store('2026-07-15', [('00:00', 400.0), ('00:15', 400.0), ('24:00', 400.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        # 400 PLN/MWh -> 0.4 zl/kWh, x1.23 VAT bonus = 0.492
        self.assertAlmostEqual(payload['raw_today'][0]['value'], 0.492, places=6)

    def test_tomorrow_omitted_when_not_yet_cached(self):
        self._store('2026-07-15', [('00:00', 400.0), ('24:00', 400.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertEqual(payload['raw_tomorrow'], [])

    def test_tomorrow_included_once_cached(self):
        self._store('2026-07-15', [('00:00', 400.0), ('24:00', 400.0)])
        self._store('2026-07-16', [('00:00', 500.0), ('24:00', 500.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertGreater(len(payload['raw_tomorrow']), 0)

    def test_timestamps_are_timezone_aware(self):
        self._store('2026-07-15', [('00:00', 400.0), ('24:00', 400.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertRegex(payload['raw_today'][0]['start'], r'\+\d{2}:\d{2}$')

    def test_dst_fall_back_ambiguous_hour_gets_distinct_offsets(self):
        # 2026-10-25 is a real Polish DST fall-back Sunday: local time
        # 02:00-02:59 occurs twice, once at +02:00 (CEST, plain '02:15')
        # and once at +01:00 (CET, disambiguated '02a:15').
        self._store('2026-10-25', [
            ('02:00', 400.0),
            ('02:15', 400.0),
            ('02a:00', 400.0),
            ('02a:15', 400.0),
            ('24:00', 400.0),
        ])
        payload = export_price.build_export_price_payload(date(2026, 10, 25), WARSAW)

        bands_by_start = {band['start']: band for band in payload['raw_today']}
        first_occurrence = bands_by_start['2026-10-25T02:15:00+02:00']
        second_occurrence = bands_by_start['2026-10-25T02:15:00+01:00']

        self.assertNotEqual(first_occurrence['start'], second_occurrence['start'])

    def test_default_granularity_is_15min(self):
        self._store('2026-07-15', [
            ('00:00', 100.0), ('00:15', 200.0), ('00:30', 300.0), ('00:45', 400.0),
            ('24:00', 400.0),
        ])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertEqual(len(payload['raw_today']), 4)

    def test_hourly_granularity_averages_the_four_quarters(self):
        self._store('2026-07-15', [
            ('00:00', 100.0), ('00:15', 200.0), ('00:30', 300.0), ('00:45', 400.0),
            ('01:00', 500.0), ('01:15', 500.0), ('01:30', 500.0), ('01:45', 500.0),
            ('24:00', 500.0),
        ])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW, granularity='hourly')

        self.assertEqual(len(payload['raw_today']), 2)
        # mean(100,200,300,400) = 250 PLN/MWh -> 0.25 zl/kWh, x1.23 = 0.3075
        self.assertAlmostEqual(payload['raw_today'][0]['value'], 0.3075, places=6)
        self.assertEqual(payload['raw_today'][0]['start'][11:16], '00:00')
        self.assertEqual(payload['raw_today'][0]['end'][11:16], '01:00')
        self.assertEqual(payload['raw_today'][1]['start'][11:16], '01:00')
        # the last hour's end is the '24:00' sentinel, i.e. next day 00:00
        self.assertTrue(payload['raw_today'][1]['end'].startswith('2026-07-16T00:00:00'))

    def test_hourly_granularity_on_dst_fall_back_keeps_both_ambiguous_hours_distinct(self):
        self._store('2026-10-25', [
            ('02:00', 100.0), ('02:15', 200.0), ('02:30', 300.0), ('02:45', 400.0),
            ('02a:00', 500.0), ('02a:15', 500.0), ('02a:30', 500.0), ('02a:45', 500.0),
            ('24:00', 500.0),
        ])
        payload = export_price.build_export_price_payload(date(2026, 10, 25), WARSAW, granularity='hourly')

        bands_by_start = {band['start']: band for band in payload['raw_today']}
        first_occurrence = bands_by_start['2026-10-25T02:00:00+02:00']
        second_occurrence = bands_by_start['2026-10-25T02:00:00+01:00']

        # mean(100,200,300,400) = 250 -> 0.3075; mean(500,500,500,500) = 500 -> 0.615
        self.assertAlmostEqual(first_occurrence['value'], 0.3075, places=6)
        self.assertAlmostEqual(second_occurrence['value'], 0.615, places=6)

    def test_negative_price_defaults_to_zero(self):
        self._store('2026-07-15', [('00:00', -50.0), ('24:00', -50.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertEqual(payload['raw_today'][0]['value'], 0.0)

    def test_negative_price_raw_publishes_true_value_without_vat_bonus(self):
        self._store('2026-07-15', [('00:00', -50.0), ('24:00', -50.0)])
        payload = export_price.build_export_price_payload(
            date(2026, 7, 15), WARSAW, negative_prices='raw')

        # -50 PLN/MWh -> -0.05 zl/kWh, no VAT bonus applied to a loss
        self.assertAlmostEqual(payload['raw_today'][0]['value'], -0.05, places=6)

    def test_positive_price_unaffected_by_negative_prices_setting(self):
        self._store('2026-07-15', [('00:00', 400.0), ('24:00', 400.0)])
        payload = export_price.build_export_price_payload(
            date(2026, 7, 15), WARSAW, negative_prices='raw')

        self.assertAlmostEqual(payload['raw_today'][0]['value'], 0.492, places=6)

    def test_hourly_average_negative_then_negative_handling_applied(self):
        # mean(-100, -100, 100, 100) = 0 - not negative, so the VAT bonus
        # still applies to the averaged (non-negative) hourly value even
        # though two of the four quarters were individually negative.
        self._store('2026-07-15', [
            ('00:00', -100.0), ('00:15', -100.0), ('00:30', 100.0), ('00:45', 100.0),
            ('24:00', 100.0),
        ])
        payload = export_price.build_export_price_payload(
            date(2026, 7, 15), WARSAW, granularity='hourly', negative_prices='raw')

        self.assertAlmostEqual(payload['raw_today'][0]['value'], 0.0, places=6)


class ExportValueRoundingTest(unittest.TestCase):
    def test_positive_value_rounded_to_4_decimal_places(self):
        # 298.032520325 / 1000 * 1.23 = 0.36657999999975 unrounded - float
        # noise from the division/multiplication, not real RCE precision.
        self.assertEqual(export_price.export_value(298.032520325, 'zero'), 0.3666)

    def test_raw_negative_value_rounded_to_4_decimal_places(self):
        self.assertEqual(export_price.export_value(-298.032520325, 'raw'), -0.298)


if __name__ == '__main__':
    unittest.main()
