import os
import tempfile
import unittest

import rce_storage


class RceStorageTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)  # sqlite3.connect creates it fresh
        self.conn = rce_storage.init_db(self.db_path)

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_init_db_is_idempotent(self):
        # calling it again on the same file must not raise
        rce_storage.init_db(self.db_path).close()

    def test_is_cached_false_when_no_marker(self):
        self.assertFalse(rce_storage.is_cached(self.conn, '2026-01-01'))

    def test_store_prices_then_is_cached_true(self):
        series = [('00:00', 100.0), ('00:15', 110.0), ('24:00', 110.0)]
        rce_storage.store_prices(self.conn, '2026-01-01', series)
        self.assertTrue(rce_storage.is_cached(self.conn, '2026-01-01'))

    def test_store_prices_then_get_cached_prices_round_trips(self):
        series = [('00:00', 100.0), ('00:15', 110.5), ('00:30', 90.25), ('24:00', 90.25)]
        rce_storage.store_prices(self.conn, '2026-01-01', series)
        result = rce_storage.get_cached_prices(self.conn, '2026-01-01')
        self.assertEqual(result, series)

    def test_get_cached_prices_only_returns_the_requested_date(self):
        rce_storage.store_prices(self.conn, '2026-01-01', [('00:00', 1.0), ('24:00', 1.0)])
        rce_storage.store_prices(self.conn, '2026-01-02', [('00:00', 2.0), ('24:00', 2.0)])
        result = rce_storage.get_cached_prices(self.conn, '2026-01-02')
        self.assertEqual(result, [('00:00', 2.0), ('24:00', 2.0)])

    def test_store_prices_is_idempotent_via_insert_or_replace(self):
        rce_storage.store_prices(self.conn, '2026-01-01', [('00:00', 1.0)])
        rce_storage.store_prices(self.conn, '2026-01-01', [('00:00', 2.0)])
        result = rce_storage.get_cached_prices(self.conn, '2026-01-01')
        self.assertEqual(result, [('00:00', 2.0)])

    def test_dst_spring_92_periods_caches_correctly(self):
        # spring-forward day: 92 quarter-hour periods instead of the usual 96
        series = [(f'{h:02}:{m:02}', float(h * 4 + m // 15)) for h in range(23) for m in (0, 15, 30, 45)]
        series.append(('24:00', series[-1][1]))
        self.assertEqual(len(series), 93)
        rce_storage.store_prices(self.conn, '2026-03-29', series)
        self.assertTrue(rce_storage.is_cached(self.conn, '2026-03-29'))
        self.assertEqual(rce_storage.get_cached_prices(self.conn, '2026-03-29'), series)

    def test_dst_fall_100_periods_caches_correctly(self):
        # fall-back day: 100 quarter-hour periods instead of the usual 96
        # (a normal day's 96, plus one repeated hour's worth of 4 quarters).
        # The repeated hour uses synthetic non-colliding labels ('23:46'..
        # '23:49', sorting between '23:45' and '24:00') since real PSE
        # period-label text for the repeated hour isn't specified by this
        # design - this test only exercises the storage layer's ability to
        # hold a 100-period business_date under the (business_date, period)
        # primary key and get_cached_prices' ORDER BY period returning rows
        # in the same order they were stored, not real PSE collision
        # behavior for that hour.
        series = [(f'{h:02}:{m:02}', float(h * 4 + m // 15)) for h in range(24) for m in (0, 15, 30, 45)]
        series += [(f'23:{46 + i}', 99.0) for i in range(4)]
        series.append(('24:00', series[-1][1]))
        self.assertEqual(len(series), 101)
        rce_storage.store_prices(self.conn, '2026-10-25', series)
        self.assertTrue(rce_storage.is_cached(self.conn, '2026-10-25'))
        self.assertEqual(rce_storage.get_cached_prices(self.conn, '2026-10-25'), series)


if __name__ == '__main__':
    unittest.main()
