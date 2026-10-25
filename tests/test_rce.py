import json
import os
import tempfile
import unittest
from datetime import date
from unittest.mock import patch

import matplotlib
matplotlib.use('Agg')

import rce
import rce_storage

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')


def load_pse_fixture(business_date):
    """Real PSE rce-pln responses (period, period_utc, dtime_utc,
    rce_pln) for the 2026 DST days, saved from the live API."""
    with open(os.path.join(FIXTURES, f'pse_rce_{business_date}.json')) as f:
        return json.load(f)


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


class ConvertToSeries15MinTest(unittest.TestCase):
    def test_normal_periods_keep_the_start_time(self):
        response_data = {'value': [
            {'period': '00:00 - 00:15', 'period_utc': '22:00 - 22:15', 'dtime_utc': '2026-07-14 22:15:00', 'rce_pln': 1.0},
            {'period': '00:15 - 00:30', 'period_utc': '22:15 - 22:30', 'dtime_utc': '2026-07-14 22:30:00', 'rce_pln': 2.0},
        ]}
        self.assertEqual(
            rce.convert_to_series_15min(response_data),
            [('00:00', 1.0), ('00:15', 2.0), ('24:00', 2.0)],
        )

    def test_period_ending_at_utc_midnight(self):
        # period_utc "23:45 - 24:00": the length must wrap, not go negative
        response_data = {'value': [
            {'period': '01:45 - 02:00', 'period_utc': '23:45 - 24:00', 'dtime_utc': '2026-07-15 00:00:00', 'rce_pln': 1.0},
        ]}
        self.assertEqual(rce.convert_to_series_15min(response_data)[0][0], '01:45')

    def test_dst_fall_back_labels_come_from_utc(self):
        # PSE's own labels for the repeated hour are "03:00 - 02a:15", ...,
        # "02a:45 - 03a:00", "03a:00 - 03:15": taking their start as the
        # label read the first quarter as 03:00 CET (an hour late) and
        # sorted "03a:00" after "03:45".
        labels = [label for label, _ in rce.convert_to_series_15min(load_pse_fixture('2026-10-25'))]

        self.assertEqual(len(labels), 101)
        self.assertEqual(labels[8:14], ['02:00', '02:15', '02:30', '02:45', '02a:00', '02a:15'])
        self.assertEqual(labels[14:18], ['02a:30', '02a:45', '03:00', '03:15'])
        self.assertEqual(len(set(labels)), len(labels))
        # rce_storage returns rows ORDER BY period, so text order must be time order
        self.assertEqual(sorted(labels), labels)

    def test_dst_spring_forward_labels_come_from_utc(self):
        # PSE labels the quarter after 01:45 "02:00 - 03:15"; it starts at 03:00 CEST
        labels = [label for label, _ in rce.convert_to_series_15min(load_pse_fixture('2026-03-29'))]

        self.assertEqual(len(labels), 93)
        self.assertEqual(labels[7:10], ['01:45', '03:00', '03:15'])
        self.assertEqual(sorted(labels), labels)


class PlotRceTest(unittest.TestCase):
    def test_xlim_matches_normal_day_length(self):
        series = [(f'{h:02}:{m:02}', 1.0) for h in range(24) for m in (0, 15, 30, 45)]
        series.append(('24:00', 1.0))
        fig = rce.plot_rce(series, '2026-06-01')
        self.assertEqual(fig.gca().get_xlim(), (0.0, len(series) - 1))

    def test_dst_fall_back_day_is_not_clipped_by_xlim(self):
        # 100 periods (25-hour day) plus the appended '24:00' = 101 points -
        # xlim must stretch to cover all of them, not stay hardcoded at 96,
        # or the last ~hour of the chart is invisible.
        series = [(f'{h:02}:{m:02}', 1.0) for h in range(24) for m in (0, 15, 30, 45)]
        series += [('02a:15', 1.0), ('02a:30', 1.0), ('02a:45', 1.0), ('03a:00', 1.0)]
        series.append(('24:00', 1.0))
        fig = rce.plot_rce(series, '2026-10-25')
        axes = fig.gca()
        self.assertEqual(axes.get_xlim(), (0.0, len(series) - 1))
        last_plotted_position = len(axes.get_lines()[0].get_xdata()) - 1
        self.assertLessEqual(last_plotted_position, axes.get_xlim()[1])

    def test_dst_spring_forward_day_xlim_matches_shorter_series(self):
        # 92 periods (23-hour day) plus the appended '24:00' = 93 points.
        series = [(f'{h:02}:{m:02}', 1.0) for h in range(23) for m in (0, 15, 30, 45)]
        series.append(('24:00', 1.0))
        fig = rce.plot_rce(series, '2026-03-29')
        self.assertEqual(fig.gca().get_xlim(), (0.0, len(series) - 1))


if __name__ == '__main__':
    unittest.main()
