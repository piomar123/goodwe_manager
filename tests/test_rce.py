import os
import tempfile
import unittest
from datetime import date
from unittest.mock import patch

import rce
import rce_storage


class GetRce15MinTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        patcher = patch.object(rce_storage, 'RCE_DB_PATH', self.db_path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_cache_miss_calls_live_fetch_and_writes_through(self):
        series = [('00:00', 100.0), ('00:15', 110.0), ('24:00', 110.0)]
        with patch.object(rce, 'query_pse_rce_15min', return_value=series) as mock_fetch:
            result = rce.get_rce_15min(date(2026, 1, 1))
        self.assertEqual(result, series)
        mock_fetch.assert_called_once_with(date(2026, 1, 1))
        conn = rce_storage.init_db()
        try:
            self.assertTrue(rce_storage.is_cached(conn, '2026-01-01'))
            self.assertEqual(rce_storage.get_cached_prices(conn, '2026-01-01'), series)
        finally:
            conn.close()

    def test_cache_hit_does_not_call_live_fetch(self):
        conn = rce_storage.init_db()
        series = [('00:00', 50.0), ('24:00', 50.0)]
        rce_storage.store_prices(conn, '2026-01-02', series)
        conn.close()

        with patch.object(rce, 'query_pse_rce_15min') as mock_fetch:
            result = rce.get_rce_15min(date(2026, 1, 2))
        mock_fetch.assert_not_called()
        self.assertEqual(result, series)

    def test_query_pse_rce_uses_the_cache(self):
        # query_pse_rce (hourly average) must go through get_rce_15min,
        # not call query_pse_rce_15min directly - so a second call for the
        # same date is served from cache with no network call.
        series = [(f'{h:02}:00', float(h)) for h in range(24)]
        series.append(('24:00', 23.0))
        with patch.object(rce, 'query_pse_rce_15min', return_value=series) as mock_fetch:
            rce.query_pse_rce(date(2026, 1, 3))
            rce.query_pse_rce(date(2026, 1, 3))
        mock_fetch.assert_called_once_with(date(2026, 1, 3))


if __name__ == '__main__':
    unittest.main()
