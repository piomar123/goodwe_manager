"""
tests/test_control_io.py
ControlWriter against a fake inverter with an injectable clock - covers
diff-only writes, ordering, delayed read-back, retries, and failed reads.
"""
import unittest
from datetime import date

import control_io
from tests.control_fakes import (AUTO, CHARGE_2K, FREEZE_CHARGE_60, FREEZE_EXPORT, Clock, FakeInverter, base_values,
                                 run_steps)


class WriteOrderTest(unittest.TestCase):
    def test_entering_forced_mode_sets_power_first(self):
        self.assertEqual(control_io.write_order(CHARGE_2K, AUTO), ['ems_power_limit', 'ems_mode'])

    def test_leaving_forced_mode_sets_mode_first(self):
        self.assertEqual(control_io.write_order(AUTO, CHARGE_2K), ['ems_mode', 'ems_power_limit'])

    def test_zeroing_currents_first_restoring_last(self):
        self.assertEqual(control_io.write_order(FREEZE_EXPORT, CHARGE_2K),
                         ['battery_charge_current', 'ems_mode', 'ems_power_limit'])
        self.assertEqual(control_io.write_order(CHARGE_2K, FREEZE_EXPORT),
                         ['ems_power_limit', 'ems_mode', 'battery_charge_current'])

    def test_floor_raised_first_and_lowered_last(self):
        self.assertEqual(control_io.write_order(FREEZE_CHARGE_60, CHARGE_2K),
                         ['battery_discharge_depth', 'ems_mode', 'ems_power_limit'])
        self.assertEqual(control_io.write_order(CHARGE_2K, FREEZE_CHARGE_60),
                         ['ems_power_limit', 'ems_mode', 'battery_discharge_depth'])
        self.assertEqual(control_io.write_order(FREEZE_EXPORT, FREEZE_CHARGE_60),
                         ['battery_charge_current', 'battery_discharge_depth'])

    def test_unknown_floor_readback_counts_as_restricting(self):
        self.assertEqual(control_io.write_order(AUTO, {**AUTO, 'battery_discharge_depth': None}),
                         ['battery_discharge_depth'])

    def test_current_tolerance(self):
        self.assertEqual(control_io.write_order(AUTO, {**AUTO, 'battery_charge_current': 19.02}), [])


class ControlWriterTest(unittest.TestCase):
    def make(self, values, delay=0.0, shadow=False):
        clock = Clock()
        inv = FakeInverter(clock, values, delay=delay)
        return clock, inv, control_io.ControlWriter(inv, now_fn=clock, shadow=shadow)

    def test_no_writes_when_already_applied(self):
        clock, inv, w = self.make(base_values())
        run_steps(w, clock, AUTO, 30)
        self.assertEqual(inv.writes, [])
        self.assertTrue(w.applied(AUTO))
        self.assertEqual(w.readback['work_mode'], 3)

    def test_writes_only_diffs_once(self):
        clock, inv, w = self.make(base_values())
        run_steps(w, clock, CHARGE_2K, 30)
        self.assertEqual(inv.writes, [('ems_power_limit', 2000), ('ems_mode', 11)])
        self.assertTrue(w.applied(CHARGE_2K))
        self.assertEqual(w.writes_today, 2)
        self.assertIsNone(w.last_error)

    def test_delayed_readback_within_attempts_is_not_an_error(self):
        clock, inv, w = self.make(base_values(), delay=8.0)
        run_steps(w, clock, FREEZE_EXPORT, 20)
        self.assertTrue(w.applied(FREEZE_EXPORT))
        self.assertIsNone(w.last_error)
        self.assertLessEqual(len(inv.writes), 3)

    def test_never_applied_sets_error_and_backs_off(self):
        clock, inv, w = self.make(base_values(), delay=10_000)
        run_steps(w, clock, FREEZE_EXPORT, 30)  # writes at t=0, 3, 6; error at t=9, back-off until t=69
        self.assertIn('battery_charge_current', w.last_error)
        writes_before = len(inv.writes)
        run_steps(w, clock, FREEZE_EXPORT, 30)  # t=31..61
        self.assertEqual(len(inv.writes), writes_before)  # back-off holds
        run_steps(w, clock, FREEZE_EXPORT, 20)  # t=62..82
        self.assertGreater(len(inv.writes), writes_before)  # retried after 60 s

    def test_new_desired_bypasses_backoff(self):
        clock, inv, w = self.make(base_values(), delay=10_000)
        run_steps(w, clock, FREEZE_EXPORT, 12)  # error at t=9, back-off until t=69
        self.assertIsNotNone(w.last_error)
        writes_before = len(inv.writes)
        run_steps(w, clock, FREEZE_CHARGE_60, 0)  # one step
        self.assertEqual(inv.writes[writes_before:], [('battery_discharge_depth', 60)])

    def test_failed_write_stops_the_rest_of_the_order(self):
        clock, inv, w = self.make(base_values())
        inv.failing_writes = {'ems_power_limit'}
        run_steps(w, clock, CHARGE_2K, 0)  # one step
        self.assertEqual(inv.writes, [('ems_power_limit', 2000)])  # never ems_mode 11 on a stale setpoint
        self.assertIn('ems_power_limit', w.last_error)

    def test_failed_reads_are_skipped_and_no_write_without_readback_change(self):
        clock, inv, w = self.make(base_values())
        run_steps(w, clock, AUTO, 2)
        inv.failing = set(control_io.REPORTED_SETTINGS)
        run_steps(w, clock, AUTO, 40)  # must not raise
        self.assertEqual(inv.writes, [])
        self.assertEqual(w.readback['ems_mode'], 1)  # last good value kept

    def test_startup_reverts_leftover_freeze_export(self):
        clock, inv, w = self.make(base_values(battery_charge_current=0.0, ems_mode=3, ems_power_limit=1000))
        run_steps(w, clock, AUTO, 10)
        self.assertEqual(inv.writes, [('ems_mode', 1), ('ems_power_limit', 0), ('battery_charge_current', 19.0)])
        self.assertTrue(w.applied(AUTO))

    def test_startup_reverts_leftover_freeze_charge_floor(self):
        clock, inv, w = self.make(base_values(battery_discharge_depth=60))
        run_steps(w, clock, AUTO, 10)
        self.assertEqual(inv.writes, [('battery_discharge_depth', 14)])
        self.assertTrue(w.applied(AUTO))

    def test_shadow_never_writes(self):
        clock, inv, w = self.make(base_values(), shadow=True)
        run_steps(w, clock, CHARGE_2K, 30)
        self.assertEqual(inv.writes, [])
        self.assertFalse(w.applied(CHARGE_2K))

    def test_none_desired_only_reads(self):
        clock, inv, w = self.make(base_values())
        run_steps(w, clock, None, 5)
        self.assertEqual(inv.writes, [])
        self.assertEqual(w.readback['soc_upper_limit'], 100)
        self.assertFalse(w.applied(None))

    def test_write_counter_resets_daily_and_warns(self):
        days = [date(2026, 9, 26)]
        clock = Clock()
        inv = FakeInverter(clock, base_values())
        w = control_io.ControlWriter(inv, shadow=False, now_fn=clock, today_fn=lambda: days[0], max_writes_per_day=1)
        with self.assertLogs('control_io', level='WARNING'):
            run_steps(w, clock, CHARGE_2K, 10)
        self.assertEqual(w.writes_today, 2)
        days[0] = date(2026, 9, 27)
        run_steps(w, clock, CHARGE_2K, 2)
        self.assertEqual(w.writes_today, 0)


if __name__ == '__main__':
    unittest.main()
