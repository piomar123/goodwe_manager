import unittest
from unittest.mock import patch

import main


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
            return {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
        mock_merged.side_effect = fake_merged

        resp = self.client.get('/forecast/hourly.json?date=2026-01-01')

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
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
            return {'07:00': 1.5} if source == 'meteosource' else {'07:00': {'c10': 1.0, 'c50': 2.0, 'c90': 3.0}}
        mock_snapshot.side_effect = fake_snapshot

        resp = self.client.get('/forecast/hourly.json?date=2026-01-01&fetched_at=1000')
        data = resp.get_json()
        self.assertEqual(data['meteosource']['hours'], [{'time': '07:00', 'kwh': 1.5}])
        self.assertEqual(data['solcast']['periods'], [{'time': '07:00', 'c10': 1.0, 'c50': 2.0, 'c90': 3.0}])
        mock_snapshot.assert_any_call(unittest.mock.ANY, 'meteosource', '2026-01-01', 1000)


if __name__ == '__main__':
    unittest.main()
