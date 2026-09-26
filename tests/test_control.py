"""
tests/test_control.py
Pure-logic tests for control.py - no inverter, no MQTT, fixed clock.
"""
import json
import unittest
from datetime import datetime, timedelta, timezone

import control
from control import Command, CommandError, Mode

NOW = datetime(2026, 9, 26, 14, 0, tzinfo=timezone(timedelta(hours=2)))


def cmd_json(**fields) -> str:
    return json.dumps(fields)


class ParseCommandTest(unittest.TestCase):
    def test_charge_with_expires_at(self):
        cmd = control.parse_command(cmd_json(mode='charge', power_w=3000, target_soc=80, source='predbat',
                                             expires_at=(NOW + timedelta(minutes=15)).isoformat()), NOW)
        self.assertEqual(cmd, Command(Mode.CHARGE, 3000, 80, 'predbat', NOW + timedelta(minutes=15)))

    def test_ttl_s_is_converted_to_expires_at(self):
        cmd = control.parse_command(cmd_json(mode='freeze_charge', ttl_s=900), NOW)
        self.assertEqual(cmd.expires_at, NOW + timedelta(seconds=900))

    def test_expires_at_in_another_offset_is_accepted(self):
        utc = (NOW + timedelta(minutes=10)).astimezone(timezone.utc).isoformat()
        cmd = control.parse_command(cmd_json(mode='freeze_export', expires_at=utc), NOW)
        self.assertEqual(cmd.expires_at, NOW + timedelta(minutes=10))

    def test_auto_needs_no_expiry(self):
        cmd = control.parse_command(cmd_json(mode='auto', source='predbat', stop='charge'), NOW)
        self.assertEqual((cmd.mode, cmd.expires_at, cmd.stop), (Mode.AUTO, None, 'charge'))

    def test_power_is_ignored_for_unpowered_modes(self):
        cmd = control.parse_command(cmd_json(mode='freeze_charge', power_w=500, target_soc=50, ttl_s=60), NOW)
        self.assertEqual((cmd.power_w, cmd.target_soc), (None, None))

    def test_rejections(self):
        cases = {
            'not json': 'nope',
            'not an object': '[1]',
            'unknown mode': cmd_json(mode='boost', ttl_s=60),
            'charge without power': cmd_json(mode='charge', ttl_s=60),
            'zero power': cmd_json(mode='export', power_w=0, ttl_s=60),
            'bool power': cmd_json(mode='export', power_w=True, ttl_s=60),
            'target above 100': cmd_json(mode='charge', power_w=1000, target_soc=101, ttl_s=60),
            'missing expiry': cmd_json(mode='freeze_charge'),
            'naive expires_at': cmd_json(mode='freeze_charge', expires_at='2026-09-26T14:10:00'),
            'past expires_at': cmd_json(mode='freeze_charge', expires_at=(NOW - timedelta(seconds=1)).isoformat()),
            'too far': cmd_json(mode='freeze_charge', ttl_s=3601),
            'negative ttl': cmd_json(mode='freeze_charge', ttl_s=-5),
            'bad stop': cmd_json(mode='auto', stop='everything'),
            'stop on non-auto': cmd_json(mode='charge', power_w=1000, ttl_s=60, stop='charge'),
        }
        for name, payload in cases.items():
            with self.subTest(name):
                with self.assertRaises(CommandError):
                    control.parse_command(payload, NOW)

    def test_bytes_payload(self):
        cmd = control.parse_command(b'{"mode": "auto"}', NOW)
        self.assertEqual(cmd.mode, Mode.AUTO)

    def test_same_request_ignores_expiry_and_id(self):
        a = Command(Mode.CHARGE, 3000, 80, 'predbat', NOW, id='1')
        b = Command(Mode.CHARGE, 3000, 80, 'predbat', NOW + timedelta(minutes=5), id='2')
        self.assertTrue(a.same_request(b))
        self.assertFalse(a.same_request(Command(Mode.CHARGE, 2500, 80, 'predbat', NOW)))
        self.assertFalse(a.same_request(None))


class MakeOverrideTest(unittest.TestCase):
    def test_form_values_are_parsed(self):
        cmd = control.make_override('export', '1500', '40', '30', NOW)
        self.assertEqual(cmd, Command(Mode.EXPORT, 1500, 40, 'dashboard', NOW + timedelta(minutes=30)))

    def test_blank_optional_fields(self):
        cmd = control.make_override('freeze_charge', '', '', '15', NOW)
        self.assertEqual((cmd.power_w, cmd.target_soc), (None, None))

    def test_duration_bounds(self):
        for bad in ('14', '721', 'x'):
            with self.subTest(bad), self.assertRaises(CommandError):
                control.make_override('auto', '', '', bad, NOW)

    def test_charge_needs_power(self):
        with self.assertRaises(CommandError):
            control.make_override('charge', '', '', '60', NOW)


class ConfigFromEnvTest(unittest.TestCase):
    def test_off_by_default(self):
        self.assertIsNone(control.config_from_env({}))
        self.assertIsNone(control.config_from_env({'CONTROL_MODE': 'OFF'}))

    def test_on_with_currents(self):
        cfg = control.config_from_env({'CONTROL_MODE': 'on', 'CONTROL_CHARGE_CURRENT_A': '19',
                                       'CONTROL_DISCHARGE_CURRENT_A': '18.5', 'CONTROL_MIN_SOC': '14'})
        self.assertEqual(cfg, control.ControlConfig('on', 19.0, 18.5, 14, 3400, 300))

    def test_overrides(self):
        cfg = control.config_from_env({'CONTROL_MODE': 'shadow', 'CONTROL_CHARGE_CURRENT_A': '19',
                                       'CONTROL_DISCHARGE_CURRENT_A': '19', 'CONTROL_MIN_SOC': '10',
                                       'CONTROL_MAX_BATTERY_W': '3000', 'CONTROL_MAX_WRITES_PER_DAY': '100'})
        self.assertEqual((cfg.min_soc, cfg.max_battery_w, cfg.max_writes_per_day), (10, 3000, 100))

    def test_invalid(self):
        ok = {'CONTROL_MODE': 'on', 'CONTROL_CHARGE_CURRENT_A': '19', 'CONTROL_DISCHARGE_CURRENT_A': '19',
              'CONTROL_MIN_SOC': '14'}
        cases = [
            {'CONTROL_MODE': 'maybe'},
            {'CONTROL_MODE': 'on'},
            {'CONTROL_MODE': 'on', 'CONTROL_CHARGE_CURRENT_A': '19'},
            {**ok, 'CONTROL_CHARGE_CURRENT_A': '0'},
            {**ok, 'CONTROL_CHARGE_CURRENT_A': '26'},
            {k: v for k, v in ok.items() if k != 'CONTROL_MIN_SOC'},
            {**ok, 'CONTROL_MIN_SOC': '101'},
            {**ok, 'CONTROL_MIN_SOC': '14.5'},
        ]
        for env in cases:
            with self.subTest(env), self.assertRaises(ValueError):
                control.config_from_env(env)


class SampleTest(unittest.TestCase):
    def test_from_runtime(self):
        s = control.Sample.from_runtime({'battery_soc': 55, 'vbattery1': '195.2', 'battery_charge_limit': 18,
                                         'battery_discharge_limit': None})
        self.assertEqual(s, control.Sample(55.0, 195.2, 18.0, None))

    def test_out_of_range_soc_is_none(self):
        self.assertIsNone(control.Sample.from_runtime({'battery_soc': 250}).soc)
        self.assertIsNone(control.Sample.from_runtime({'battery_soc': 'None'}).soc)

    def test_off_grid_detection(self):
        # runtime grid_mode: 0 not connected, 1 connected, 2 fault;
        # runtime work_mode 2 = Normal (Off-Grid). Missing values = on-grid.
        cases = [({}, False), ({'grid_mode': 1}, False), ({'grid_mode': 1, 'work_mode': 1}, False),
                 ({'grid_mode': 2}, True), ({'grid_mode': 0}, True), ({'grid_mode': 1, 'work_mode': 2}, True),
                 ({'grid_mode': '2.0', 'work_mode': '2.0'}, True)]
        for data, expected in cases:
            with self.subTest(data):
                self.assertEqual(control.Sample.from_runtime(data).off_grid, expected)


CFG = control.ControlConfig('on', 19.0, 18.5, 14, 3400, 300)
S = control.Sample


def at(seconds: float) -> datetime:
    return NOW + timedelta(seconds=seconds)


def charge(power=3000, target=None, minutes=15, source='predbat') -> Command:
    return Command(Mode.CHARGE, power, target, source, NOW + timedelta(minutes=minutes))


class ExecutorModesTest(unittest.TestCase):
    def test_no_command_is_auto_with_user_currents(self):
        ex = control.Executor(CFG)
        self.assertEqual(ex.tick(S(50), NOW), {'ems_mode': 1, 'ems_power_limit': 0, 'battery_charge_current': 19.0,
                                                'battery_discharge_current': 18.5, 'battery_discharge_depth': 14})
        self.assertEqual(ex.snapshot(NOW)['reason'], 'no command')

    def test_mode_table(self):
        cases = [
            (charge(2000), (11, 2000, 19.0, 18.5, 14)),
            (Command(Mode.EXPORT, 1500, None, 'p', at(600)), (3, 1500, 19.0, 18.5, 14)),
            (Command(Mode.FREEZE_CHARGE, expires_at=at(600)), (1, 0, 19.0, 18.5, 50)),
            (Command(Mode.FREEZE_EXPORT, expires_at=at(600)), (1, 0, 0, 18.5, 14)),
        ]
        for cmd, expected in cases:
            with self.subTest(cmd.mode):
                ex = control.Executor(CFG)
                ex.submit(cmd)
                self.assertEqual(tuple(ex.tick(S(50), NOW).values()), expected)

    def test_expiry_reverts_to_auto(self):
        ex = control.Executor(CFG)
        ex.submit(charge(minutes=1))
        ex.tick(S(50), NOW)
        self.assertEqual(ex.tick(S(50), at(60))['ems_mode'], 1)
        self.assertEqual(ex.snapshot(at(60))['reason'], 'command expired')
        self.assertEqual(ex.snapshot(at(61))['reason'], 'command expired')

    def test_resend_refreshes_expiry(self):
        ex = control.Executor(CFG)
        ex.submit(charge(minutes=1))
        ex.submit(Command(Mode.CHARGE, 3000, None, 'predbat', at(600)))
        self.assertEqual(ex.tick(S(50), at(120))['ems_mode'], 11)

    def test_unscoped_auto_clears_any_command(self):
        ex = control.Executor(CFG)
        ex.submit(charge())
        ex.submit(Command(Mode.AUTO, source='predbat'))
        self.assertEqual(ex.tick(S(50), NOW)['ems_mode'], 1)

    def test_scoped_stop_clears_its_own_domain(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.FREEZE_CHARGE, expires_at=at(600)))
        ex.submit(Command(Mode.AUTO, stop='charge'))
        self.assertEqual(ex.tick(S(50), NOW)['battery_discharge_depth'], 14)

    def test_opposite_scoped_stop_is_ignored(self):
        ex = control.Executor(CFG)
        ex.submit(charge())
        ex.submit(Command(Mode.AUTO, source='predbat', stop='export'))
        ex.submit(charge())
        self.assertEqual(ex.tick(S(50), NOW)['ems_mode'], 11)
        self.assertEqual(ex.snapshot(NOW)['mode'], 'charge')


class ExecutorPowerTest(unittest.TestCase):
    def test_clamped_to_config_max(self):
        ex = control.Executor(CFG)
        ex.submit(charge(8000))
        self.assertEqual(ex.tick(S(50), NOW)['ems_power_limit'], 3400)
        self.assertTrue(ex.snapshot(NOW)['power_clamped'])

    def test_clamped_to_live_bms_limit_per_direction(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.EXPORT, 3400, None, 'p', at(600)))
        self.assertEqual(ex.tick(S(50, 190.0, 18.0, 10.0), NOW)['ems_power_limit'], 1900)

    def test_raised_to_minimum(self):
        ex = control.Executor(CFG)
        ex.submit(charge(20))
        self.assertEqual(ex.tick(S(50), NOW)['ems_power_limit'], 100)


class ExecutorTargetsTest(unittest.TestCase):
    def test_charge_target_needs_30s_then_freezes(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=80))
        self.assertEqual(ex.tick(S(79), at(0))['ems_mode'], 11)
        self.assertEqual(ex.tick(S(80), at(5))['ems_mode'], 11)
        self.assertEqual(ex.tick(S(80), at(34))['ems_mode'], 11)
        desired = ex.tick(S(80), at(35))
        self.assertEqual((desired['ems_mode'], desired['battery_discharge_depth']), (1, 80))
        self.assertEqual(ex.snapshot(at(35))['effective_mode'], 'freeze_charge')
        self.assertEqual(ex.snapshot(at(35))['reason'], 'target_soc reached')

    def test_charge_hold_hysteresis(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=50))
        ex.tick(S(60), NOW)  # already met on first sample -> freeze at once
        self.assertEqual(ex.tick(S(48), at(10))['battery_discharge_depth'], 60)  # floor never goes down
        desired = ex.tick(S(46), at(20))
        self.assertEqual((desired['ems_mode'], desired['battery_discharge_depth']), (11, 14))

    def test_hold_charge_target_below_soc_applies_immediately(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=40))
        self.assertEqual(ex.tick(S(55), NOW)['battery_discharge_depth'], 55)

    def test_export_target_reached_goes_auto_and_stays(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.EXPORT, 2000, 30, 'p', at(900)))
        ex.tick(S(31), at(0))
        ex.tick(S(30), at(1))
        self.assertEqual(ex.tick(S(30), at(31))['ems_mode'], 1)
        self.assertEqual(ex.tick(S(35), at(60))['ems_mode'], 1)

    def test_single_garbage_soc_sample_does_not_latch(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=80))
        ex.tick(S(60), at(0))
        ex.tick(S(100), at(5))
        ex.tick(S(61), at(10))
        self.assertEqual(ex.tick(S(100), at(40))['ems_mode'], 11)
        ex.set_reserve(25)
        ex.submit(Command(Mode.AUTO))
        ex.tick(S(60), at(50))
        ex.tick(S(0), at(55))
        self.assertEqual(ex.tick(S(60), at(90))['battery_discharge_depth'], 14)

    def test_none_soc_keeps_debounce_running(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=80))
        ex.tick(S(70), at(0))
        ex.tick(S(80), at(1))
        ex.tick(S(None), at(20))
        self.assertEqual(ex.tick(S(80), at(31))['battery_discharge_depth'], 80)

    def test_new_command_resets_latch(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=50))
        ex.tick(S(60), NOW)
        ex.submit(charge(target=90))
        self.assertEqual(ex.tick(S(60), at(1))['ems_mode'], 11)


class ExecutorReserveTest(unittest.TestCase):
    def test_reserve_freezes_after_30s_and_releases_with_hysteresis(self):
        ex = control.Executor(CFG)
        ex.set_reserve(25)
        ex.tick(S(26), at(0))
        ex.tick(S(25), at(1))
        self.assertEqual(ex.tick(S(25), at(31))['battery_discharge_depth'], 25)
        self.assertEqual(ex.snapshot(at(31))['reason'], 'reserve')
        self.assertEqual(ex.tick(S(26), at(40))['battery_discharge_depth'], 25)
        self.assertEqual(ex.tick(S(27), at(50))['battery_discharge_depth'], 14)

    def test_reserve_does_not_touch_forced_modes(self):
        ex = control.Executor(CFG)
        ex.set_reserve(25)
        ex.submit(Command(Mode.EXPORT, 2000, None, 'p', at(900)))
        ex.tick(S(20), at(0))
        self.assertEqual(ex.tick(S(20), at(60))['ems_mode'], 3)

    def test_reserve_cleared(self):
        ex = control.Executor(CFG)
        ex.set_reserve(25)
        ex.set_reserve(None)
        ex.tick(S(10), at(0))
        self.assertEqual(ex.tick(S(10), at(60))['battery_discharge_depth'], 14)


class ExecutorOverrideTest(unittest.TestCase):
    def test_override_wins_then_expires_back_to_command(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.EXPORT, 2000, None, 'predbat', at(3000)))
        ex.set_override(Command(Mode.FREEZE_CHARGE, source='dashboard', expires_at=at(900)))
        self.assertEqual(ex.tick(S(50), at(0))['battery_discharge_depth'], 50)
        snap = ex.snapshot(at(0))
        self.assertEqual((snap['mode'], snap['effective_mode']), ('export', 'freeze_charge'))
        self.assertEqual(snap['override']['mode'], 'freeze_charge')
        self.assertEqual(ex.tick(S(50), at(900))['ems_mode'], 3)
        self.assertIsNone(ex.snapshot(at(900))['override'])

    def test_clear_override(self):
        ex = control.Executor(CFG)
        ex.set_override(Command(Mode.FREEZE_EXPORT, source='dashboard', expires_at=at(900)))
        ex.clear_override()
        self.assertEqual(ex.tick(S(50), at(0))['battery_charge_current'], 19.0)


AUTO_SETTINGS = {'ems_mode': 1, 'ems_power_limit': 0, 'battery_charge_current': 19.0, 'battery_discharge_current': 18.5,
                 'battery_discharge_depth': 14}


class ExecutorOffGridTest(unittest.TestCase):
    def test_off_grid_restores_auto_over_everything(self):
        cmds = [Command(Mode.FREEZE_CHARGE, expires_at=at(900)), Command(Mode.FREEZE_EXPORT, expires_at=at(900)),
                Command(Mode.EXPORT, 2000, None, 'p', at(900)), charge(2000, 90)]
        for cmd in cmds:
            with self.subTest(cmd.mode):
                ex = control.Executor(CFG)
                ex.submit(cmd)
                self.assertEqual(ex.tick(S(50, off_grid=True), NOW), AUTO_SETTINGS)
                snap = ex.snapshot(NOW)
                self.assertEqual((snap['reason'], snap['off_grid'], snap['mode']), ('off-grid', True, cmd.mode.value))

    def test_off_grid_beats_override_and_reserve(self):
        ex = control.Executor(CFG)
        ex.set_reserve(60)
        ex.set_override(Command(Mode.FREEZE_CHARGE, source='dashboard', expires_at=at(900)))
        ex.tick(S(50, off_grid=True), at(0))
        self.assertEqual(ex.tick(S(50, off_grid=True), at(60)), AUTO_SETTINGS)

    def test_back_on_grid_needs_60s_in_a_row(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.FREEZE_CHARGE, expires_at=at(900)))
        ex.tick(S(50, off_grid=True), at(0))
        self.assertEqual(ex.tick(S(50), at(30))['battery_discharge_depth'], 14)
        self.assertEqual(ex.tick(S(50, off_grid=True), at(40))['battery_discharge_depth'], 14)
        self.assertEqual(ex.tick(S(50), at(50))['battery_discharge_depth'], 14)  # timer restarts here
        self.assertEqual(ex.tick(S(50), at(109))['battery_discharge_depth'], 14)
        self.assertEqual(ex.tick(S(48), at(110))['battery_discharge_depth'], 48)  # floor from the SoC now
        self.assertFalse(ex.snapshot(at(110))['off_grid'])

    def test_command_still_expires_while_off_grid(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.FREEZE_CHARGE, expires_at=at(30)))
        ex.tick(S(50, off_grid=True), at(0))
        ex.tick(S(50, off_grid=True), at(31))
        ex.tick(S(50), at(40))
        self.assertEqual(ex.tick(S(50), at(100)), AUTO_SETTINGS)
        self.assertEqual(ex.snapshot(at(100))['reason'], 'command expired')


class ExecutorFreezeFloorTest(unittest.TestCase):
    def frozen(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.FREEZE_CHARGE, expires_at=at(3600)))
        return ex

    def test_floor_is_current_soc_rounded_down(self):
        ex = self.frozen()
        self.assertEqual(ex.tick(S(55.7), NOW)['battery_discharge_depth'], 55)
        self.assertEqual(ex.snapshot(NOW)['freeze_floor'], 55)

    def test_floor_follows_rising_soc_in_3_point_steps_and_never_drops(self):
        ex = self.frozen()
        ex.tick(S(55), at(0))
        self.assertEqual(ex.tick(S(57.9), at(10))['battery_discharge_depth'], 55)
        self.assertEqual(ex.tick(S(58.2), at(20))['battery_discharge_depth'], 58)
        self.assertEqual(ex.tick(S(56), at(30))['battery_discharge_depth'], 58)
        self.assertEqual(ex.tick(S(None), at(40))['battery_discharge_depth'], 58)

    def test_floor_never_below_min_soc(self):
        ex = self.frozen()
        self.assertEqual(ex.tick(S(9), NOW)['battery_discharge_depth'], 14)

    def test_freeze_charge_waits_for_soc(self):
        ex = self.frozen()
        self.assertEqual(ex.tick(S(None), at(0)), AUTO_SETTINGS)
        snap = ex.snapshot(at(0))
        self.assertEqual((snap['effective_mode'], snap['reason'], snap['freeze_floor']), ('auto', 'waiting for SoC', None))
        self.assertEqual(ex.tick(S(40), at(1))['battery_discharge_depth'], 40)

    def test_last_known_soc_is_used_after_a_failed_read(self):
        ex = control.Executor(CFG)
        ex.tick(S(62), at(0))
        ex.submit(Command(Mode.FREEZE_CHARGE, expires_at=at(3600)))
        self.assertEqual(ex.tick(S(None), at(1))['battery_discharge_depth'], 62)
        self.assertEqual(ex.last_soc, 62)

    def test_leaving_restores_min_and_reentry_takes_the_new_soc(self):
        ex = self.frozen()
        ex.tick(S(70), at(0))
        ex.submit(Command(Mode.AUTO, stop='charge'))
        self.assertEqual(ex.tick(S(65), at(10))['battery_discharge_depth'], 14)
        self.assertIsNone(ex.snapshot(at(10))['freeze_floor'])
        ex.submit(Command(Mode.FREEZE_CHARGE, expires_at=at(3600)))
        self.assertEqual(ex.tick(S(65), at(20))['battery_discharge_depth'], 65)

    def test_min_soc_hold_after_a_freeze_near_min(self):
        ex = control.Executor(CFG)
        ex.set_reserve(16)
        ex.tick(S(16), at(0))
        self.assertEqual(ex.tick(S(16), at(30))['battery_discharge_depth'], 16)
        self.assertFalse(ex.min_soc_hold)
        self.assertEqual(ex.tick(S(18), at(40))['battery_discharge_depth'], 14)  # reserve released at 16 + 2
        self.assertTrue(ex.min_soc_hold)
        ex.tick(S(19), at(50))
        self.assertFalse(ex.min_soc_hold)

    def test_no_min_soc_hold_when_freeze_ends_higher(self):
        ex = self.frozen()
        ex.tick(S(40), at(0))
        ex.submit(Command(Mode.AUTO))
        ex.tick(S(40), at(1))
        self.assertFalse(ex.min_soc_hold)


class ExecutorSnapshotTest(unittest.TestCase):
    def test_snapshot_fields(self):
        ex = control.Executor(control.ControlConfig('shadow', 19.0, 18.5, 14))
        ex.submit(charge(3000, 80))
        ex.tick(S(50), NOW)
        ex.reject('invalid JSON: x')
        snap = ex.snapshot(NOW)
        self.assertEqual(snap['mode'], 'charge')
        self.assertEqual(snap['power_w'], 3000)
        self.assertEqual(snap['power_applied_w'], 3000)
        self.assertEqual(snap['target_soc'], 80)
        self.assertEqual(snap['source'], 'predbat')
        self.assertEqual(snap['expires_at'], (NOW + timedelta(minutes=15)).isoformat())
        self.assertEqual(snap['since'], NOW.isoformat())
        self.assertEqual(snap['command_error'], 'invalid JSON: x')
        self.assertTrue(snap['shadow'])
        self.assertIsNone(snap['freeze_floor'])

    def test_accepted_command_clears_command_error(self):
        ex = control.Executor(CFG)
        ex.reject('bad')
        ex.submit(charge())
        self.assertIsNone(ex.snapshot(NOW)['command_error'])


class ComputeWarningsTest(unittest.TestCase):
    def test_all_warnings(self):
        w = control.compute_warnings({'soc_upper_limit': 90, 'work_mode': 0}, [True, False, True, False], 15, 3)
        self.assertEqual(w, ['soc_upper_limit is 90 (expected 100)', 'work_mode changed from 3 to 0',
                             'eco slot 1 is enabled', 'eco slot 3 is enabled',
                             'reserve 15% is below 20% (BMS SoC is unreliable there)'])

    def test_no_warnings(self):
        self.assertEqual(control.compute_warnings({'soc_upper_limit': 100, 'work_mode': 3}, [False] * 4, 25, 3), [])
        self.assertEqual(control.compute_warnings({}, None, None, None), [])

    def test_min_soc_hold_warning(self):
        self.assertEqual(control.compute_warnings({}, None, None, None, min_soc_hold=14),
                         ['discharge may stay blocked until SoC reaches 19% (the inverter resumes 5 points '
                          'above its minimum SoC after a freeze)'])


if __name__ == '__main__':
    unittest.main()
