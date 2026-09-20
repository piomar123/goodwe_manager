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

        self.assertRegex(payload['raw_today'][0]['from'], r'\+\d{2}:\d{2}$')

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

        bands_by_from = {band['from']: band for band in payload['raw_today']}
        first_occurrence = bands_by_from['2026-10-25T02:15:00+02:00']
        second_occurrence = bands_by_from['2026-10-25T02:15:00+01:00']

        self.assertNotEqual(first_occurrence['from'], second_occurrence['from'])


if __name__ == '__main__':
    unittest.main()
