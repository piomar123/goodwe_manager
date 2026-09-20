import unittest

from sensors import CalculatedValuesEvaluator, DB_SENSORS, SELECTED_SENSORS, sensor_columns, db_row


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


class DailyCalculatedValuesTest(unittest.TestCase):
    def test_first_sample_becomes_its_own_day_start_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        sample = {
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        }

        calculated = evaluator.calculate_values(sample)

        self.assertEqual(calculated['_day_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_daily_meter_export'], '0.00')
        self.assertEqual(calculated['_daily_meter_import'], '0.00')
        self.assertEqual(calculated['_daily_load'], '0.0')

    def test_running_totals_accumulate_within_the_same_day_across_hour_boundaries(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        # crosses an hour boundary (14 -> 15) but stays the same day
        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 15:30:05',
            'meter_e_total_exp': '108.0',
            'meter_e_total_imp': '53.0',
            'e_load_total': '18.0',
        })

        self.assertEqual(calculated['_day_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_daily_meter_export'], '8.00')
        self.assertEqual(calculated['_daily_meter_import'], '3.00')
        self.assertEqual(calculated['_daily_load'], '8.0')

    def test_new_day_resets_the_daily_baseline_but_not_the_hourly_one_independently(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 23:30:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-29 00:05:00',
            'meter_e_total_exp': '101.0',
            'meter_e_total_imp': '50.5',
            'e_load_total': '10.5',
        })

        self.assertEqual(calculated['_day_start_timestamp'], '2026-08-29 00:05:00')
        self.assertEqual(calculated['_daily_meter_export'], '0.00')
        # the hour also rolled over here, so the hourly baseline resets too
        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-29 00:05:00')

    def test_seed_day_start_restores_a_prior_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.seed_day_start({
            'timestamp': '2026-08-28 00:00:03',
            'meter_e_total_exp': '90.0',
            'meter_e_total_imp': '40.0',
            'e_load_total': '5.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 10:00:00',
            'meter_e_total_exp': '95.0',
            'meter_e_total_imp': '42.0',
            'e_load_total': '9.0',
        })

        self.assertEqual(calculated['_daily_meter_export'], '5.00')
        self.assertEqual(calculated['_daily_meter_import'], '2.00')
        self.assertEqual(calculated['_daily_load'], '4.0')


class SensorColumnsTest(unittest.TestCase):
    def test_covers_every_db_sensor_plus_calculated_headers(self):
        columns = sensor_columns()
        column_names = [name for name, _ in columns]

        self.assertEqual(len(columns), len(DB_SENSORS) + 8)
        self.assertEqual(column_names[:len(DB_SENSORS)], DB_SENSORS)
        self.assertIn('_hourly_meter_export', column_names)

    def test_excludes_daily_reset_counters_kept_ephemeral_for_mqtt_only(self):
        column_names = {name for name, _ in sensor_columns()}

        for ephemeral in ('e_day_exp', 'e_day_imp', 'e_load_day', 'e_bat_charge_day', 'e_bat_discharge_day'):
            self.assertNotIn(ephemeral, column_names)
        # e_total is the lifetime counter, not a daily-reset one - stays in the DB.
        self.assertIn('e_total', column_names)
        self.assertIn('e_total', DB_SENSORS)

    def test_label_columns_are_text_everything_else_is_real(self):
        columns = dict(sensor_columns())

        self.assertEqual(columns['timestamp'], 'TEXT')
        self.assertEqual(columns['pv1_mode_label'], 'TEXT')
        self.assertEqual(columns['ppv'], 'REAL')
        self.assertEqual(columns['battery_soc'], 'REAL')


class DbRowTest(unittest.TestCase):
    def test_narrows_to_db_sensors_and_calculated_headers_only(self):
        full = {name: f'v_{name}' for name in SELECTED_SENSORS}
        full |= {name: f'c_{name}' for name in CalculatedValuesEvaluator.headers()}
        full['e_day_exp'] = 'should be dropped'

        narrowed = db_row(full)

        self.assertNotIn('e_day_exp', narrowed)
        self.assertIn('e_total', narrowed)
        self.assertEqual(set(narrowed.keys()), set(DB_SENSORS) | set(CalculatedValuesEvaluator.headers()))


if __name__ == '__main__':
    unittest.main()
