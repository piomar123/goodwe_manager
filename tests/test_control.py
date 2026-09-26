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


if __name__ == '__main__':
    unittest.main()
