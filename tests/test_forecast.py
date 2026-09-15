import unittest
from unittest.mock import patch

import forecast


class FetchCombinedHourlyKwhTest(unittest.TestCase):
    @patch('forecast.fetch_pv_production_forecast_hourly_kwh')
    def test_sums_both_orientations_by_local_hour(self, mock_fetch):
        # epoch millis for 2026-01-01 07:00 and 08:00, read via
        # utcfromtimestamp per forecast.py's own documented convention
        def fake_fetch(date, orientation):
            base = {90: [(1767250800000, 0.5), (1767254400000, 0.9)],
                    270: [(1767250800000, 0.2), (1767254400000, 0.3)]}
            return base[orientation]
        mock_fetch.side_effect = fake_fetch

        result = forecast.fetch_pv_production_forecast_combined_hourly_kwh('2026-01-01')

        self.assertEqual(result, {'07:00': 0.7, '08:00': 1.2})
        self.assertEqual(mock_fetch.call_args_list, [
            unittest.mock.call('2026-01-01', 90),
            unittest.mock.call('2026-01-01', 270),
        ])

    @patch('forecast.fetch_pv_production_forecast_hourly_kwh')
    def test_missing_orientation_hour_treated_as_zero(self, mock_fetch):
        def fake_fetch(date, orientation):
            return [(1767250800000, 0.5)] if orientation == 90 else []
        mock_fetch.side_effect = fake_fetch

        result = forecast.fetch_pv_production_forecast_combined_hourly_kwh('2026-01-01')

        self.assertEqual(result, {'07:00': 0.5})


if __name__ == '__main__':
    unittest.main()
