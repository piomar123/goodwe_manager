"""
tests/test_shadow_scan_guard.py
ShadowScanGuard with a fake inverter and clock - shadow scan off while
off-grid, the previous value back after the on-grid cooldown, flapping
grids, persistence across restarts, and inverter errors.
"""
import asyncio
import json
import os
import tempfile
import unittest
from datetime import timedelta

import shadow_scan_guard
from shadow_scan_guard import GuardConfig, ShadowScanGuard
from tests.control_fakes import Clock, FakeInverter

COOLDOWN = timedelta(minutes=15)


def run(coro):
    return asyncio.run(coro)


class ConfigTest(unittest.TestCase):
    def test_off_by_default(self):
        self.assertIsNone(shadow_scan_guard.config_from_env({}))
        self.assertIsNone(shadow_scan_guard.config_from_env({'OFF_GRID_SHADOW_SCAN_GUARD': 'off'}))
        self.assertIsNone(shadow_scan_guard.config_from_env({'OFF_GRID_SHADOW_SCAN_GUARD': ''}))

    def test_on_with_default_cooldown(self):
        cfg = shadow_scan_guard.config_from_env({'OFF_GRID_SHADOW_SCAN_GUARD': 'on'})
        self.assertEqual(cfg, GuardConfig(cooldown=timedelta(minutes=15)))

    def test_custom_cooldown(self):
        cfg = shadow_scan_guard.config_from_env({'OFF_GRID_SHADOW_SCAN_GUARD': 'ON',
                                                 'OFF_GRID_SHADOW_SCAN_COOLDOWN_MIN': '30'})
        self.assertEqual(cfg.cooldown, timedelta(minutes=30))

    def test_invalid_values_fail(self):
        with self.assertRaises(ValueError):
            shadow_scan_guard.config_from_env({'OFF_GRID_SHADOW_SCAN_GUARD': 'yes'})
        with self.assertRaises(ValueError):
            shadow_scan_guard.config_from_env({'OFF_GRID_SHADOW_SCAN_GUARD': 'on',
                                               'OFF_GRID_SHADOW_SCAN_COOLDOWN_MIN': '-1'})


class GuardTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.inv = FakeInverter(self.clock, {'shadow_scan': 1}, delay=1.0)
        self.tmp = tempfile.TemporaryDirectory()
        self.state_path = os.path.join(self.tmp.name, 'shadow_scan_guard.json')
        self.guard = self.make_guard()

    def tearDown(self):
        self.tmp.cleanup()

    def make_guard(self):
        return ShadowScanGuard(GuardConfig(COOLDOWN), state_path=self.state_path, mono_fn=self.clock)

    def tick(self, off_grid, seconds=1, guard=None):
        """Advance the clock second by second, stepping the guard each time
        like the 1 Hz poll loop."""
        for _ in range(seconds):
            run((guard or self.guard).step(self.inv, off_grid))
            self.clock.t += 1

    def test_on_grid_does_nothing(self):
        self.tick(False, 60)
        self.assertEqual(self.inv.writes, [])

    def test_off_grid_turns_it_off_once(self):
        self.tick(True, 60)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0)])
        self.assertEqual(self.inv.values['shadow_scan'], 0)

    def test_restored_only_after_cooldown(self):
        self.tick(True, 30)
        self.tick(False, int(COOLDOWN.total_seconds()) - 5)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0)])
        self.tick(False, 30)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0), ('shadow_scan', 1)])
        self.assertEqual(self.inv.values['shadow_scan'], 1)
        self.tick(False, 600)
        self.assertEqual(len(self.inv.writes), 2)

    def test_flapping_grid_restarts_cooldown(self):
        self.tick(True, 30)
        self.tick(False, 600)
        self.tick(True, 10)  # drops again 10 min after coming back
        self.tick(False, 600)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0)])  # 20 min since first return, not 15 stable
        self.tick(False, 330)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0), ('shadow_scan', 1)])

    def test_already_off_is_left_off(self):
        self.inv.values['shadow_scan'] = 0
        self.tick(True, 30)
        self.tick(False, int(COOLDOWN.total_seconds()) + 30)
        self.assertEqual(self.inv.writes, [])
        self.assertEqual(self.inv.values['shadow_scan'], 0)
        with open(self.state_path) as f:
            self.assertEqual(json.load(f), {'holding': False, 'restore': None})

    def test_next_outage_restores_the_value_set_in_between(self):
        self.inv.values['shadow_scan'] = 0
        self.tick(True, 30)
        self.tick(False, int(COOLDOWN.total_seconds()) + 30)
        self.inv.values['shadow_scan'] = 1  # turned on by hand on the /config page
        self.tick(True, 30)
        self.tick(False, int(COOLDOWN.total_seconds()) + 30)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0), ('shadow_scan', 1)])

    def test_other_previous_value_is_restored(self):
        self.inv.values['shadow_scan'] = 3
        self.tick(True, 30)
        self.tick(False, int(COOLDOWN.total_seconds()) + 30)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0), ('shadow_scan', 3)])

    def test_state_is_saved_while_holding(self):
        self.tick(True, 30)
        with open(self.state_path) as f:
            self.assertEqual(json.load(f), {'holding': True, 'restore': 1})
        self.tick(False, int(COOLDOWN.total_seconds()) + 30)
        with open(self.state_path) as f:
            self.assertEqual(json.load(f), {'holding': False, 'restore': None})

    def test_restart_during_outage_still_restores(self):
        self.tick(True, 30)
        restarted = self.make_guard()  # e.g. a deploy while off-grid
        self.tick(True, 30, guard=restarted)
        self.tick(False, int(COOLDOWN.total_seconds()) + 30, guard=restarted)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0), ('shadow_scan', 1)])

    def test_restart_after_grid_return_waits_full_cooldown(self):
        self.tick(True, 30)
        self.tick(False, 600)
        restarted = self.make_guard()
        self.tick(False, int(COOLDOWN.total_seconds()) - 5, guard=restarted)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0)])
        self.tick(False, 30, guard=restarted)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0), ('shadow_scan', 1)])

    def test_state_is_loaded_on_first_step_not_at_construction(self):
        # main.py builds the guard at import, before logging is configured:
        # loading (and its log lines) must wait for the poll loop.
        self.tick(True, 30)
        with self.assertLogs('shadow_scan_guard', level='INFO') as logs:
            restarted = self.make_guard()
            shadow_scan_guard.logger.info('marker')  # assertLogs needs at least one record
        self.assertEqual(logs.output, ['INFO:shadow_scan_guard:marker'])
        with self.assertLogs('shadow_scan_guard', level='INFO') as logs:
            self.tick(True, 1, guard=restarted)
        self.assertIn('resumed', logs.output[0])

    def test_unreadable_state_file_is_ignored(self):
        with open(self.state_path, 'w') as f:
            f.write('not json')
        guard = self.make_guard()
        self.tick(False, 30, guard=guard)
        self.assertEqual(self.inv.writes, [])

    def test_read_failure_is_retried_without_writing(self):
        self.inv.failing.add('shadow_scan')
        self.tick(True, 30)
        self.assertEqual(self.inv.writes, [])
        self.inv.failing.clear()
        self.tick(True, 120)
        self.assertEqual(self.inv.writes, [('shadow_scan', 0)])

    def test_write_failure_is_retried(self):
        self.inv.failing_writes.add('shadow_scan')
        self.tick(True, 30)
        self.assertGreaterEqual(len(self.inv.writes), 1)
        self.inv.failing_writes.clear()
        self.tick(True, 1200)
        self.assertEqual(self.inv.values['shadow_scan'], 0)

    def test_register_that_never_takes_the_value_backs_off(self):
        self.inv.delay = 10 ** 9  # write acked, never visible
        self.tick(True, 3600)
        self.assertLessEqual(len(self.inv.writes), 12)

    def test_reconnect_uses_the_new_inverter(self):
        self.tick(True, 30)
        new_inv = FakeInverter(self.clock, {'shadow_scan': 0})
        self.inv = new_inv
        self.tick(False, int(COOLDOWN.total_seconds()) + 30)
        self.assertEqual(new_inv.writes, [('shadow_scan', 1)])


if __name__ == '__main__':
    unittest.main()
