import unittest
from datetime import date
from zoneinfo import ZoneInfo

import pv_forecast_payload

WARSAW = ZoneInfo('Europe/Warsaw')


class BuildDetailedForecastTest(unittest.TestCase):
    def test_converts_fields_and_period_start_shape(self):
        periods = {
            '06:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3},
            '06:30': {'c10': 0.4, 'c50': 0.5, 'c90': 0.6},
        }
        entries = pv_forecast_payload.build_detailed_forecast(periods, date(2026, 7, 15), WARSAW)

        self.assertEqual(len(entries), 2)
        first = entries[0]
        self.assertEqual(first['period_start'], '2026-07-15T06:00:00+02:00')
        self.assertEqual(first['pv_estimate'], 0.2)
        self.assertEqual(first['pv_estimate10'], 0.1)
        self.assertEqual(first['pv_estimate90'], 0.3)

    def test_empty_periods_returns_empty_list(self):
        self.assertEqual(pv_forecast_payload.build_detailed_forecast({}, date(2026, 7, 15), WARSAW), [])

    def test_entries_sorted_by_time_regardless_of_input_order(self):
        periods = {
            '12:30': {'c10': 0.0, 'c50': 0.0, 'c90': 0.0},
            '06:00': {'c10': 0.0, 'c50': 0.0, 'c90': 0.0},
            '09:15': {'c10': 0.0, 'c50': 0.0, 'c90': 0.0},
        }
        entries = pv_forecast_payload.build_detailed_forecast(periods, date(2026, 7, 15), WARSAW)

        times = [e['period_start'][11:16] for e in entries]
        self.assertEqual(times, ['06:00', '09:15', '12:30'])

    def test_dst_spring_forward_day_periods_get_differing_offsets(self):
        # 2026-03-29 is Poland's DST spring-forward day: local clocks jump
        # from 02:00 straight to 03:00, so 01:30 is still CET (+01:00) and
        # 03:00 onward is already CEST (+02:00) - a real 48h forecast
        # spanning this transition must reflect that offset change, not a
        # single fixed offset for the whole day.
        periods = {
            '01:30': {'c10': 0.0, 'c50': 0.0, 'c90': 0.0},
            '03:00': {'c10': 0.0, 'c50': 0.0, 'c90': 0.0},
        }
        entries = pv_forecast_payload.build_detailed_forecast(periods, date(2026, 3, 29), WARSAW)

        entries_by_time = {e['period_start'][11:16]: e['period_start'] for e in entries}
        self.assertEqual(entries_by_time['01:30'], '2026-03-29T01:30:00+01:00')
        self.assertEqual(entries_by_time['03:00'], '2026-03-29T03:00:00+02:00')


if __name__ == '__main__':
    unittest.main()
