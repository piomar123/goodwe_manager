import os
import tempfile
import unittest
from datetime import datetime, time as dtime
from unittest.mock import ANY, call, patch

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


class LastWakeTimeTest(unittest.TestCase):
    def test_picks_the_most_recent_slot_already_passed_today(self):
        now = datetime(2026, 1, 1, 12, 0)
        result = forecast_prefetch.last_wake_time(now, wake_times=(dtime(6, 0), dtime(10, 0), dtime(14, 15), dtime(18, 0)))
        self.assertEqual(result, datetime(2026, 1, 1, 10, 0))

    def test_rolls_back_to_yesterdays_last_slot_before_any_slot_today(self):
        now = datetime(2026, 1, 1, 3, 0)
        result = forecast_prefetch.last_wake_time(now, wake_times=(dtime(6, 0), dtime(10, 0), dtime(14, 15), dtime(18, 0)))
        self.assertEqual(result, datetime(2025, 12, 31, 18, 0))

    def test_a_slot_exactly_at_now_counts_as_already_passed(self):
        now = datetime(2026, 1, 1, 6, 0)
        result = forecast_prefetch.last_wake_time(now, wake_times=(dtime(6, 0), dtime(10, 0)))
        self.assertEqual(result, datetime(2026, 1, 1, 6, 0))


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

    @patch('forecast_prefetch.forecast.fetch_pv_production_forecast_combined_hourly_kwh')
    def test_fetch_and_store_meteosource_uses_the_given_now_as_fetched_at(self, mock_fetch):
        mock_fetch.return_value = {'07:00': 1.5}
        forecast_prefetch.fetch_and_store_meteosource(self.conn, '2026-01-01', now=12345)
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 12345), {'07:00': 1.5})

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
    @patch('forecast_prefetch.solcast.fetch_solcast_forecast_30min')
    def test_fetch_and_store_solcast_uses_the_given_now_for_every_date(self, mock_fetch):
        mock_fetch.return_value = {'2026-01-01': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}, '2026-01-02': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        forecast_prefetch.fetch_and_store_solcast(self.conn, now=12345)
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-01'), [12345])
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-02'), [12345])

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

    @patch.dict(os.environ, {'SOLCAST_SITE_EAST_ID': 'east-1', 'SOLCAST_SITE_WEST_ID': 'west-1'})
    @patch('forecast_prefetch.solcast.fetch_solcast_estimated_actuals_30min')
    def test_fetch_and_store_solcast_actuals_with_max_date_excludes_dates_at_or_after_it(self, mock_fetch):
        def fake_fetch(resource_id):
            return {'2026-01-01': {'07:00': 0.5}, '2026-01-02': {'07:00': 0.5}}
        mock_fetch.side_effect = fake_fetch

        forecast_prefetch.fetch_and_store_solcast_actuals(self.conn, max_date='2026-01-02')

        self.assertEqual(forecast_history.get_latest_merged(self.conn, 'solcast_actuals', '2026-01-01'), {'07:00': 1.0})
        self.assertEqual(forecast_history.get_latest_merged(self.conn, 'solcast_actuals', '2026-01-02'), {})

    @patch.dict(os.environ, {'SOLCAST_SITE_EAST_ID': 'east-1', 'SOLCAST_SITE_WEST_ID': 'west-1'})
    @patch('forecast_prefetch.solcast.fetch_solcast_estimated_actuals_30min')
    def test_fetch_and_store_solcast_actuals_uses_the_given_now_for_every_date(self, mock_fetch):
        mock_fetch.return_value = {'2026-01-01': {'07:00': 0.5}, '2026-01-02': {'07:00': 0.5}}
        forecast_prefetch.fetch_and_store_solcast_actuals(self.conn, now=12345)
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-01'), [12345])
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-02'), [12345])


class RunCatchUpTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        self.conn = forecast_history.init_db(self.db_path)
        self.now = datetime(2026, 1, 2, 8, 0)  # between the 06:00 and 11:00 forecast slots

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    @patch('forecast_prefetch.fetch_and_store_solcast_actuals')
    @patch('forecast_prefetch.fetch_and_store_solcast')
    @patch('forecast_prefetch.fetch_and_store_meteosource')
    def test_fetches_everything_when_nothing_is_fresh(self, mock_meteosource, mock_solcast, mock_actuals):
        forecast_prefetch.run_catch_up(self.conn, self.now)
        self.assertEqual(mock_meteosource.call_args_list, [
            call(self.conn, '2026-01-02', now=ANY),
            call(self.conn, '2026-01-03', now=ANY),
        ])
        mock_solcast.assert_called_once_with(self.conn, now=ANY)
        mock_actuals.assert_called_once_with(self.conn, max_date='2026-01-02', now=ANY)

    @patch('forecast_prefetch.fetch_and_store_solcast_actuals')
    @patch('forecast_prefetch.fetch_and_store_solcast')
    @patch('forecast_prefetch.fetch_and_store_meteosource')
    def test_meteosource_and_solcast_share_the_same_fetched_at_when_both_stale(self, mock_meteosource, mock_solcast, mock_actuals):
        # Two separate API round-trips (Meteosource, then Solcast east+west)
        # drifting a few seconds apart used to give them different
        # fetched_at values, splitting one wake-up into two dropdown
        # entries where each source is only present in one - see
        # run_catch_up's/the module docstring's note on this.
        forecast_prefetch.run_catch_up(self.conn, self.now)
        solcast_now = mock_solcast.call_args.kwargs['now']
        meteosource_nows = [c.kwargs['now'] for c in mock_meteosource.call_args_list]
        self.assertEqual(meteosource_nows, [solcast_now, solcast_now])

    @patch('forecast_prefetch.fetch_and_store_solcast_actuals')
    @patch('forecast_prefetch.fetch_and_store_solcast')
    @patch('forecast_prefetch.fetch_and_store_meteosource')
    def test_skips_sources_already_fresh_since_the_last_passed_wake_time(self, mock_meteosource, mock_solcast, mock_actuals):
        # A snapshot written after last_wake_time(now, FORECAST_WAKE_TIMES)
        # (today's 06:00) means solcast is fresh and meteosource is fresh
        # for *today* - but meteosource's staleness is checked per-date
        # (see has_fetched_date_since), so tomorrow's still-missing
        # snapshot must still be fetched even though solcast doesn't need
        # to piggyback it. Only actuals (last passed slot: yesterday's
        # 23:00) is otherwise stale.
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-02', {'07:00': 1.0}, now=int(datetime(2026, 1, 2, 6, 5).timestamp()))
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-02', {'07:00': {'c10': 1, 'c50': 2, 'c90': 3}}, now=int(datetime(2026, 1, 2, 6, 5).timestamp()))

        forecast_prefetch.run_catch_up(self.conn, self.now)

        mock_meteosource.assert_called_once_with(self.conn, '2026-01-03', now=ANY)
        mock_solcast.assert_not_called()
        mock_actuals.assert_called_once_with(self.conn, max_date='2026-01-02', now=ANY)

    @patch('forecast_prefetch.fetch_and_store_solcast_actuals')
    @patch('forecast_prefetch.fetch_and_store_solcast')
    @patch('forecast_prefetch.fetch_and_store_meteosource')
    def test_meteosource_fetches_alongside_solcast_even_when_individually_fresh(self, mock_meteosource, mock_solcast, mock_actuals):
        # Meteosource has no quota to conserve, unlike Solcast - it should
        # piggyback on Solcast's fetch cycle even when its own staleness
        # check alone would have skipped it, so the two stay aligned on one
        # shared fetched_at.
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-02', {'07:00': 1.0}, now=int(datetime(2026, 1, 2, 6, 5).timestamp()))

        forecast_prefetch.run_catch_up(self.conn, self.now)

        solcast_now = mock_solcast.call_args.kwargs['now']
        meteosource_nows = [c.kwargs['now'] for c in mock_meteosource.call_args_list]
        self.assertEqual(meteosource_nows, [solcast_now, solcast_now])

    @patch('forecast_prefetch.fetch_and_store_solcast_actuals')
    @patch('forecast_prefetch.fetch_and_store_solcast')
    @patch('forecast_prefetch.fetch_and_store_meteosource')
    def test_solcast_does_not_fetch_just_because_meteosource_is_stale(self, mock_meteosource, mock_solcast, mock_actuals):
        # The reverse doesn't hold - Solcast's own staleness gate is
        # untouched, since it's the one with quota to conserve.
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-02', {'07:00': {'c10': 1, 'c50': 2, 'c90': 3}}, now=int(datetime(2026, 1, 2, 6, 5).timestamp()))

        forecast_prefetch.run_catch_up(self.conn, self.now)

        # stale on its own - fetched for both today and tomorrow
        self.assertEqual(mock_meteosource.call_count, 2)
        mock_solcast.assert_not_called()

    @patch('forecast_prefetch.fetch_and_store_solcast_actuals')
    @patch('forecast_prefetch.fetch_and_store_solcast')
    @patch('forecast_prefetch.fetch_and_store_meteosource')
    def test_fetch_failure_is_logged_and_does_not_stop_the_other_catch_up_fetches(self, mock_meteosource, mock_solcast, mock_actuals):
        mock_meteosource.side_effect = RuntimeError("boom")
        forecast_prefetch.run_catch_up(self.conn, self.now)
        self.assertEqual(mock_meteosource.call_count, 2)  # today and tomorrow both attempted despite failure
        mock_solcast.assert_called_once()
        mock_actuals.assert_called_once()


if __name__ == '__main__':
    unittest.main()
