import unittest

from sensors import CalculatedValuesEvaluator, SELECTED_SENSORS, sensor_columns


class CalculatedValuesEvaluatorTest(unittest.TestCase):
    def test_first_sample_becomes_its_own_hour_start_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        sample = {
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        }

        calculated = evaluator.calculate_values(sample)

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_hourly_meter_export'], '0.00')
        self.assertEqual(calculated['_hourly_meter_import'], '0.00')
        self.assertEqual(calculated['_hourly_load'], '0.0')

    def test_running_totals_accumulate_within_the_same_hour(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 14:30:05',
            'meter_e_total_exp': '103.5',
            'meter_e_total_imp': '51.0',
            'e_load_total': '14.0',
        })

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_hourly_meter_export'], '3.50')
        self.assertEqual(calculated['_hourly_meter_import'], '1.00')
        self.assertEqual(calculated['_hourly_load'], '4.0')

    def test_new_hour_resets_the_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 15:00:02',
            'meter_e_total_exp': '110.0',
            'meter_e_total_imp': '55.0',
            'e_load_total': '20.0',
        })

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 15:00:02')
        self.assertEqual(calculated['_hourly_meter_export'], '0.00')

    def test_seed_hour_start_restores_a_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.seed_hour_start({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 14:15:00',
            'meter_e_total_exp': '101.0',
            'meter_e_total_imp': '50.5',
            'e_load_total': '12.0',
        })

        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_hourly_meter_export'], '1.00')

    def test_seed_hour_start_with_none_leaves_evaluator_at_cold_start(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.seed_hour_start(None)

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 14:15:00',
            'meter_e_total_exp': '101.0',
            'meter_e_total_imp': '50.5',
            'e_load_total': '12.0',
        })

        # no baseline was restored, so this first sample becomes the baseline
        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-28 14:15:00')
        self.assertEqual(calculated['_hourly_meter_export'], '0.00')


class SensorColumnsTest(unittest.TestCase):
    def test_covers_every_selected_sensor_plus_calculated_headers(self):
        columns = sensor_columns()
        column_names = [name for name, _ in columns]

        self.assertEqual(len(columns), len(SELECTED_SENSORS) + 4)
        self.assertEqual(column_names[:len(SELECTED_SENSORS)], SELECTED_SENSORS)
        self.assertIn('_hourly_meter_export', column_names)

    def test_label_columns_are_text_everything_else_is_real(self):
        columns = dict(sensor_columns())

        self.assertEqual(columns['timestamp'], 'TEXT')
        self.assertEqual(columns['pv1_mode_label'], 'TEXT')
        self.assertEqual(columns['ppv'], 'REAL')
        self.assertEqual(columns['battery_soc'], 'REAL')


if __name__ == '__main__':
    unittest.main()
