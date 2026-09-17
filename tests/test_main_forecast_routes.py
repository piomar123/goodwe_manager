import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

import forecast_history
import history
import storage
from sensors import sensor_columns

import main


class ForecastSummaryAccuracyDeltaGatingTest(unittest.TestCase):
    """Covers the /forecast route's actual_total gating in get_forecast():
    the accuracy delta (Δ) must only be shown for the merged "Latest" view
    (fetched_at omitted), never for a specific historical snapshot - a
    snapshot fetched early in the day may only cover that day's remaining
    hours (Solcast/Meteosource fetches are forward-looking), so comparing
    its partial forecast total against the full-day actual total would be
    misleading. Uses real temp DBs (not mocks) for both data.db and
    forecast_history.db so this exercises get_forecast()'s actual gating
    logic end to end, same spirit as test_main_backfill.py's DB setup.
    """

    DATE = '2026-01-05'  # a fully elapsed past date relative to "today" in tests

    def setUp(self):
        main.app.testing = True
        self.client = main.app.test_client()

        fd, self.data_db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.data_db_path)
        storage.init_db_sync(self.data_db_path, sensor_columns()).close()
        self._orig_data_db_path = storage.DATA_DB_PATH
        storage.DATA_DB_PATH = self.data_db_path

        fd, self.forecast_db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.forecast_db_path)
        self._orig_forecast_db_path = forecast_history.FORECAST_HISTORY_DB_PATH
        forecast_history.FORECAST_HISTORY_DB_PATH = self.forecast_db_path
        forecast_history.init_db(self.forecast_db_path).close()

        # Complete (24h) actual telemetry for DATE, straight into
        # hourly_summary (the only table _get_actual_hourly_pv_kwh reads).
        conn = storage.init_db_sync(self.data_db_path, sensor_columns())
        try:
            day = datetime.strptime(self.DATE, '%Y-%m-%d').date()
            day_start_epoch, _ = history.date_range_to_epoch(day, day)
            for hour in range(24):
                conn.execute(
                    "INSERT INTO hourly_summary (hour_start, pv_kwh) VALUES (?, ?)",
                    (day_start_epoch + hour * 3600, 1.0),
                )
            conn.commit()
        finally:
            conn.close()

        # Two forecast_history snapshots for Meteosource: an early one that
        # only covers the first 6 hours of the day (a realistic
        # forward-looking partial fetch), and a later one that covers all
        # 24 - so the merged "Latest" view is complete, but the early
        # snapshot alone (queried via fetched_at) stays partial.
        fh_conn = forecast_history.init_db(self.forecast_db_path)
        try:
            partial_payload = {f'{h:02d}:00': 1.0 for h in range(6)}
            full_payload = {f'{h:02d}:00': 1.0 for h in range(24)}
            self.partial_fetched_at = forecast_history.write_snapshot(
                fh_conn, 'meteosource', self.DATE, partial_payload, now=1000)
            forecast_history.write_snapshot(fh_conn, 'meteosource', self.DATE, full_payload, now=2000)
        finally:
            fh_conn.close()

    def tearDown(self):
        storage.DATA_DB_PATH = self._orig_data_db_path
        forecast_history.FORECAST_HISTORY_DB_PATH = self._orig_forecast_db_path
        for path in (self.data_db_path, self.forecast_db_path):
            for suffix in ('', '-wal', '-shm'):
                p = path + suffix
                if os.path.exists(p):
                    os.remove(p)

    def test_specific_historical_snapshot_shows_no_delta(self):
        resp = self.client.get(f'/forecast?date={self.DATE}&fetched_at={self.partial_fetched_at}')
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertNotIn('Δ', body)

    def test_latest_merged_view_shows_delta_for_the_same_date(self):
        # Companion case, same date/data, just without fetched_at - pins
        # that it's specifically `fetched_at is None` gating the delta,
        # not something else (e.g. forecast completeness alone).
        resp = self.client.get(f'/forecast?date={self.DATE}')
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn('Δ', body)


class ForecastHourlyJsonRouteTest(unittest.TestCase):
    def setUp(self):
        main.app.testing = True
        self.client = main.app.test_client()

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[2000, 1000])
    @patch('main.forecast_history.get_latest_merged')
    def test_returns_meteosource_solcast_and_fetch_times(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        def fake_merged(conn, source, date):
            if source == 'meteosource':
                return {'07:00': 1.5}
            elif source == 'solcast':
                return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
            return {}
        mock_merged.side_effect = fake_merged

        resp = self.client.get('/forecast/hourly.json?date=2026-01-01')

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data['meteosource']['available'])
        self.assertEqual(data['meteosource']['hours'], [{'time': '07:00', 'kwh': 1.5}])
        self.assertTrue(data['solcast']['available'])
        self.assertEqual(data['solcast']['periods'], [{'time': '07:00', 'c10': 1.0, 'c50': 2.0, 'c90': 3.0}])
        self.assertEqual(data['fetch_times'], [2000, 1000])

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.write_snapshot')
    @patch('main.forecast.fetch_pv_production_forecast_combined_hourly_kwh', return_value={})
    @patch('main.forecast_history.get_fetch_times', return_value=[])
    @patch('main.forecast_history.get_latest_merged', return_value={})
    def test_solcast_unavailable_when_no_snapshot_exists(self, mock_merged, mock_fetch_times, mock_live_fetch, mock_write, mock_partial, mock_actual):
        # meteosource merged is empty too, so the route's live-fallback path
        # (Task 5's _read_forecast_payload) fires for meteosource - mocked
        # here so this test doesn't make a real network call.
        resp = self.client.get('/forecast/hourly.json?date=2099-01-01')
        data = resp.get_json()
        self.assertFalse(data['solcast']['available'])
        self.assertEqual(data['solcast']['periods'], [])

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[1000])
    @patch('main.forecast_history.get_snapshot')
    def test_specific_fetched_at_uses_get_snapshot_not_merged(self, mock_snapshot, mock_fetch_times, mock_partial, mock_actual):
        # source-aware, not one shared return value: the route's Solcast
        # branch does `**v` per period, which would blow up on a plain
        # {"07:00": 1.5} float value if both sources returned the same
        # Meteosource-shaped payload.
        def fake_snapshot(conn, source, date, fetched_at):
            if source == 'meteosource':
                return {'07:00': 1.5}
            elif source == 'solcast':
                return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
            return {}
        mock_snapshot.side_effect = fake_snapshot

        resp = self.client.get('/forecast/hourly.json?date=2026-01-01&fetched_at=1000')
        data = resp.get_json()
        self.assertEqual(data['meteosource']['hours'], [{'time': '07:00', 'kwh': 1.5}])
        self.assertEqual(data['solcast']['periods'], [{'time': '07:00', 'c10': 1.0, 'c50': 2.0, 'c90': 3.0}])
        mock_snapshot.assert_any_call(unittest.mock.ANY, 'meteosource', '2026-01-01', 1000)

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[1000])
    @patch('main.forecast_history.get_snapshot')
    def test_meteosource_unavailable_for_a_fetched_at_with_no_matching_snapshot(self, mock_snapshot, mock_fetch_times, mock_partial, mock_actual):
        # A selected fetched_at is an exact-match lookup, not merged (see
        # test_specific_fetched_at_uses_get_snapshot_not_merged) - Solcast
        # can have a snapshot at a timestamp Meteosource never wrote one at
        # (two independent API round-trips within one wake-up, or a
        # solcast-only catch-up fetch). No live-fallback here either, since
        # that only fires for the merged "Latest" view (fetched_at is None).
        def fake_snapshot(conn, source, date, fetched_at):
            if source == 'solcast':
                return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
            return {}
        mock_snapshot.side_effect = fake_snapshot

        resp = self.client.get('/forecast/hourly.json?date=2026-01-01&fetched_at=1000')
        data = resp.get_json()
        self.assertFalse(data['meteosource']['available'])
        self.assertEqual(data['meteosource']['hours'], [])
        self.assertTrue(data['solcast']['available'])

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[1000])
    @patch('main.forecast_history.get_snapshot')
    @patch('main.forecast_history.get_merged_since')
    def test_solcast_actuals_for_a_specific_fetched_at_uses_merged_since_that_days_start(
            self, mock_merged_since, mock_snapshot, mock_fetch_times, mock_partial, mock_actual):
        # Actuals are fetched once/day and don't change once measured -
        # unlike meteosource/solcast, a selected fetched_at must not be an
        # exact-match get_snapshot lookup for solcast_actuals (that would
        # spuriously show "unavailable" for any fetched_at earlier in the
        # day than that day's once-daily actuals fetch). Instead it should
        # show whatever's freshest from that fetched_at's calendar day
        # onward.
        def fake_snapshot(conn, source, date, fetched_at):
            if source == 'meteosource':
                return {'07:00': 1.5}
            elif source == 'solcast':
                return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
            return {}
        mock_snapshot.side_effect = fake_snapshot
        mock_merged_since.return_value = {'07:00': 0.75}

        # fetched_at=1000 -> 1970-01-01 00:16:40 UTC; midnight that day is epoch 0.
        resp = self.client.get('/forecast/hourly.json?date=2020-01-01&fetched_at=1000')

        data = resp.get_json()
        self.assertTrue(data['solcast_actuals']['available'])
        self.assertEqual(data['solcast_actuals']['periods'], [{'time': '07:00', 'kwh': 0.75}])
        mock_merged_since.assert_called_once()
        args = mock_merged_since.call_args.args
        self.assertEqual(args[1], 'solcast_actuals')
        self.assertEqual(args[2], '2020-01-01')
        self.assertLessEqual(args[3], 1000)
        mock_snapshot.assert_any_call(unittest.mock.ANY, 'meteosource', '2020-01-01', 1000)
        for call in mock_snapshot.call_args_list:
            self.assertNotEqual(call.args[1], 'solcast_actuals')

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[])
    @patch('main.forecast_history.get_latest_merged')
    def test_returns_solcast_actuals_for_a_past_date(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        def fake_merged(conn, source, date):
            # meteosource must return non-empty here too, otherwise
            # _read_forecast_payload's live-fallback fires a real network
            # call to meteosource.com for this (out-of-range) past date.
            if source == 'meteosource':
                return {'07:00': 1.5}
            elif source == 'solcast_actuals':
                return {'07:00': 0.75}
            return {}
        mock_merged.side_effect = fake_merged

        resp = self.client.get('/forecast/hourly.json?date=2020-01-01')  # safely in the past

        data = resp.get_json()
        self.assertTrue(data['solcast_actuals']['available'])
        self.assertEqual(data['solcast_actuals']['periods'], [{'time': '07:00', 'kwh': 0.75}])

    @patch('main._get_actual_hourly_pv_kwh', return_value={})
    @patch('main._get_actual_pv_kwh_so_far_this_hour', return_value=None)
    @patch('main.forecast_history.get_fetch_times', return_value=[])
    @patch('main.forecast_history.get_latest_merged')
    def test_omits_solcast_actuals_for_todays_date(self, mock_merged, mock_fetch_times, mock_partial, mock_actual):
        # Even if a snapshot exists (e.g. a stray/manual fetch), today's
        # date must not surface it - see this plan's Global Constraints and
        # spec §4's past-dates-only gating.
        def fake_merged(conn, source, date):
            # meteosource must return non-empty here too, otherwise
            # _read_forecast_payload's live-fallback fires a real network
            # call to meteosource.com.
            if source == 'meteosource':
                return {'07:00': 1.5}
            elif source == 'solcast_actuals':
                return {'07:00': 0.75}
            return {}
        mock_merged.side_effect = fake_merged

        today = datetime.now().strftime('%Y-%m-%d')
        resp = self.client.get(f'/forecast/hourly.json?date={today}')

        data = resp.get_json()
        self.assertFalse(data['solcast_actuals']['available'])
        self.assertEqual(data['solcast_actuals']['periods'], [])
        for call in mock_merged.call_args_list:
            self.assertNotEqual(call.args[1], 'solcast_actuals')


if __name__ == '__main__':
    unittest.main()
