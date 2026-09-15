import os
import unittest
from unittest.mock import patch, MagicMock

import solcast


def _fake_response(forecasts):
    resp = MagicMock()
    resp.json.return_value = {'forecasts': forecasts}
    resp.raise_for_status.return_value = None
    return resp


def _fake_actuals_response(estimated_actuals):
    resp = MagicMock()
    resp.json.return_value = {'estimated_actuals': estimated_actuals}
    resp.raise_for_status.return_value = None
    return resp


class FetchSolcastForecast30MinTest(unittest.TestCase):
    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_parses_periods_into_local_date_and_hhmm_buckets(self, mock_get):
        # period_end is the period's END, in UTC; period covers
        # [period_end - 30min, period_end). CEST (UTC+2) in September, so
        # 2026-09-14T12:00:00Z -> local 14:00, period start local 13:30.
        mock_get.return_value = _fake_response([
            {'period_end': '2026-09-14T12:00:00.0000000Z', 'pv_estimate10': 1.0, 'pv_estimate': 2.0, 'pv_estimate90': 3.0},
        ])

        result = solcast.fetch_solcast_forecast_30min('site-123')

        self.assertEqual(result, {'2026-09-14': {'13:30': {'c10': 0.5, 'c50': 1.0, 'c90': 1.5}}})

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_uses_api_key_from_env_and_pt30m_period(self, mock_get):
        mock_get.return_value = _fake_response([])
        solcast.fetch_solcast_forecast_30min('site-123')
        args, kwargs = mock_get.call_args
        self.assertIn('site-123', args[0])
        self.assertEqual(kwargs['params']['api_key'], 'test-key')
        self.assertEqual(kwargs['params']['period'], 'PT30M')

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_api_key_raises(self):
        with self.assertRaises(AssertionError):
            solcast.fetch_solcast_forecast_30min('site-123')

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_periods_spanning_a_local_midnight_land_in_the_right_date(self, mock_get):
        # 2026-09-14T22:00:00Z -> local 2026-09-15 00:00 (CEST, UTC+2);
        # period start local 2026-09-14 23:30.
        mock_get.return_value = _fake_response([
            {'period_end': '2026-09-14T22:00:00.0000000Z', 'pv_estimate10': 0, 'pv_estimate': 0, 'pv_estimate90': 0},
        ])
        result = solcast.fetch_solcast_forecast_30min('site-123')
        self.assertEqual(list(result.keys()), ['2026-09-14'])
        self.assertEqual(list(result['2026-09-14'].keys()), ['23:30'])


class SumSitesTest(unittest.TestCase):
    def test_sums_two_sites_matching_dates_and_periods(self):
        east = {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        west = {'2026-09-14': {'07:00': {'c10': 0.05, 'c50': 0.1, 'c90': 0.2}}}
        result = solcast.sum_sites(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': {'c10': 0.15, 'c50': 0.3, 'c90': 0.5}}})

    def test_period_present_in_only_one_site_is_treated_as_zero_for_the_other(self):
        east = {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        west = {'2026-09-14': {}}
        result = solcast.sum_sites(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}})

    def test_date_present_in_only_one_site_is_kept(self):
        east = {'2026-09-14': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        west = {'2026-09-15': {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}}
        result = solcast.sum_sites(east, west)
        self.assertEqual(set(result.keys()), {'2026-09-14', '2026-09-15'})


class FetchSolcastEstimatedActuals30MinTest(unittest.TestCase):
    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_parses_periods_into_local_date_and_hhmm_buckets(self, mock_get):
        # Same period_end/local-time conversion as the forecast endpoint's
        # own test - CEST (UTC+2) in September.
        mock_get.return_value = _fake_actuals_response([
            {'pv_estimate': 2.0, 'period_end': '2026-09-14T12:00:00.0000000Z', 'period': 'PT30M'},
        ])

        result = solcast.fetch_solcast_estimated_actuals_30min('site-123')

        self.assertEqual(result, {'2026-09-14': {'13:30': 1.0}})

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_uses_api_key_hours_and_pt30m_period(self, mock_get):
        mock_get.return_value = _fake_actuals_response([])
        solcast.fetch_solcast_estimated_actuals_30min('site-123', hours=48)
        args, kwargs = mock_get.call_args
        self.assertIn('site-123', args[0])
        self.assertIn('estimated_actuals', args[0])
        self.assertEqual(kwargs['params']['api_key'], 'test-key')
        self.assertEqual(kwargs['params']['period'], 'PT30M')
        self.assertEqual(kwargs['params']['hours'], 48)

    @patch.dict(os.environ, {'SOLCAST_API_KEY': 'test-key'})
    @patch('solcast.requests.get')
    def test_defaults_to_168_hours(self, mock_get):
        mock_get.return_value = _fake_actuals_response([])
        solcast.fetch_solcast_estimated_actuals_30min('site-123')
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['hours'], 168)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_api_key_raises(self):
        with self.assertRaises(AssertionError):
            solcast.fetch_solcast_estimated_actuals_30min('site-123')


class SumSitesFlatTest(unittest.TestCase):
    def test_sums_two_sites_matching_dates_and_periods(self):
        east = {'2026-09-14': {'07:00': 0.2}}
        west = {'2026-09-14': {'07:00': 0.1}}
        result = solcast.sum_sites_flat(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': 0.3}})

    def test_period_present_in_only_one_site_is_treated_as_zero_for_the_other(self):
        east = {'2026-09-14': {'07:00': 0.2}}
        west = {'2026-09-14': {}}
        result = solcast.sum_sites_flat(east, west)
        self.assertEqual(result, {'2026-09-14': {'07:00': 0.2}})

    def test_date_present_in_only_one_site_is_kept(self):
        east = {'2026-09-14': {'07:00': 0.2}}
        west = {'2026-09-15': {'07:00': 0.2}}
        result = solcast.sum_sites_flat(east, west)
        self.assertEqual(set(result.keys()), {'2026-09-14', '2026-09-15'})


if __name__ == '__main__':
    unittest.main()
