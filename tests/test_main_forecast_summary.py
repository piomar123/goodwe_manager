import unittest

from main import _accuracy_delta_pct, _build_forecast_summary


class AccuracyDeltaPctTest(unittest.TestCase):
    def test_forecast_higher_than_actual_is_a_positive_delta(self):
        self.assertEqual(_accuracy_delta_pct(10.0, 8.0), 25)

    def test_forecast_lower_than_actual_is_a_negative_delta(self):
        self.assertEqual(_accuracy_delta_pct(6.0, 8.0), -25)

    def test_zero_actual_total_returns_none(self):
        self.assertIsNone(_accuracy_delta_pct(10.0, 0.0))

    def test_none_actual_total_returns_none(self):
        self.assertIsNone(_accuracy_delta_pct(10.0, None))


class BuildForecastSummaryTest(unittest.TestCase):
    def test_meteosource_only_no_solcast_no_actual(self):
        summary = _build_forecast_summary(10.0, None, None)
        self.assertEqual(summary, "Meteosource: 10.0 kWh")

    def test_meteosource_and_solcast_no_actual(self):
        summary = _build_forecast_summary(10.0, (3.0, 5.0, 7.0), None)
        self.assertEqual(summary, "Meteosource: 10.0 kWh\nSolcast: 5.0 (3.0-7.0) kWh")

    def test_meteosource_and_solcast_with_actual_shows_both_deltas(self):
        summary = _build_forecast_summary(10.0, (4.0, 6.0, 10.0), 8.0)
        self.assertEqual(
            summary,
            "Meteosource: 10.0 kWh (Δ +25% vs actual)\nSolcast: 6.0 (4.0-10.0) kWh (Δ -25% vs actual)",
        )

    def test_meteosource_only_with_actual_shows_one_delta_and_no_solcast_line(self):
        summary = _build_forecast_summary(10.0, None, 8.0)
        self.assertEqual(summary, "Meteosource: 10.0 kWh (Δ +25% vs actual)")


if __name__ == '__main__':
    unittest.main()
