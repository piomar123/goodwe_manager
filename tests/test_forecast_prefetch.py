import os
import tempfile
import unittest
from datetime import datetime, time as dtime
from unittest.mock import patch

import forecast_history
import forecast_prefetch


class NextWakeTimeTest(unittest.TestCase):
    def test_picks_the_soonest_slot_later_today(self):
        now = datetime(2026, 1, 1, 7, 0)
        result = forecast_prefetch.next_wake_time(now, wake_times=(dtime(6, 0), dtime(10, 0), dtime(14, 15), dtime(18, 0)))
        self.assertEqual(result, datetime(2026, 1, 1, 10, 0))

    def test_rolls_to_tomorrows_first_slot_after_the_last_one_today(self):
        now = datetime(2026, 1, 1, 19, 0)
        result = forecast_prefetch.next_wake_time(now, wake_times=(dtime(6, 0), dtime(10, 0), dtime(14, 15), dtime(18, 0)))
        self.assertEqual(result, datetime(2026, 1, 2, 6, 0))


class WakeScheduleTest(unittest.TestCase):
    def test_forecast_wake_times_has_three_slots(self):
        self.assertEqual(len(forecast_prefetch.FORECAST_WAKE_TIMES), 3)

    def test_actuals_wake_time_is_not_one_of_the_forecast_slots(self):
        self.assertNotIn(forecast_prefetch.ACTUALS_WAKE_TIME, forecast_prefetch.FORECAST_WAKE_TIMES)

    def test_actuals_wake_time_is_after_every_forecast_slot(self):
        # Pins the specific schedule chosen (23:00, after 06:00/11:00/21:00)
        # - not a hard design requirement (exact actuals timing isn't
        # critical, see forecast_prefetch.py's module docstring), just a
        # regression guard on the concrete choice.
        self.assertTrue(all(forecast_prefetch.ACTUALS_WAKE_TIME > t for t in forecast_prefetch.FORECAST_WAKE_TIMES))


class FetchAndStoreTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = forecast_history.init_db(self.db_path)

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    @patch('forecast_prefetch.forecast.fetch_pv_production_forecast_combined_hourly_kwh')
    def test_fetch_and_store_meteosource_writes_a_snapshot(self, mock_fetch):
        mock_fetch.return_value = {'07:00': 1.5}
        forecast_prefetch.fetch_and_store_meteosource(self.conn, '2026-01-01')
        mock_fetch.assert_called_once_with('2026-01-01')
        times = forecast_history.get_fetch_times(self.conn, '2026-01-01')
        self.assertEqual(len(times), 1)
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', times[0]), {'07:00': 1.5})

    @patch.dict(os.environ, {'SOLCAST_SITE_EAST_ID': 'east-1', 'SOLCAST_SITE_WEST_ID': 'west-1'})
    @patch('forecast_prefetch.solcast.fetch_solcast_forecast_30min')
    def test_fetch_and_store_solcast_sums_sites_and_writes_a_snapshot_per_date(self, mock_fetch):
        def fake_fetch(resource_id):
            if resource_id == 'east-1':
                return {'2026-01-01': {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}}
            return {'2026-01-01': {'07:00': {'c10': 0.5, 'c50': 1.0, 'c90': 1.5}}}
        mock_fetch.side_effect = fake_fetch

        forecast_prefetch.fetch_and_store_solcast(self.conn)

        times = forecast_history.get_fetch_times(self.conn, '2026-01-01')
        self.assertEqual(len(times), 1)
        self.assertEqual(
            forecast_history.get_snapshot(self.conn, 'solcast', '2026-01-01', times[0]),
            {'07:00': {'c10': 1.5, 'c50': 3.0, 'c90': 4.5}},
        )

    @patch.dict(os.environ, {'SOLCAST_SITE_EAST_ID': 'east-1', 'SOLCAST_SITE_WEST_ID': 'west-1'})
    @patch('forecast_prefetch.solcast.fetch_solcast_estimated_actuals_30min')
    def test_fetch_and_store_solcast_actuals_sums_sites_and_writes_a_snapshot_per_date(self, mock_fetch):
        def fake_fetch(resource_id):
            if resource_id == 'east-1':
                return {'2026-01-01': {'07:00': 0.5}}
            return {'2026-01-01': {'07:00': 0.25}}
        mock_fetch.side_effect = fake_fetch

        forecast_prefetch.fetch_and_store_solcast_actuals(self.conn)

        times = forecast_history.get_fetch_times(self.conn, '2026-01-01')
        self.assertEqual(len(times), 1)
        self.assertEqual(
            forecast_history.get_snapshot(self.conn, 'solcast_actuals', '2026-01-01', times[0]),
            {'07:00': 0.75},
        )


if __name__ == '__main__':
    unittest.main()
