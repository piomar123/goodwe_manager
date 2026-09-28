"""
tests/test_control_runtime.py
ControlRuntime with a fake inverter and fake MQTT - message handling,
state publishing cadence, eco-slot warnings, and the Predbat paired
stop/start pattern.
"""
import asyncio
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import control
import control_runtime
from tests.control_fakes import Clock, FakeInverter, base_values

T0 = datetime(2026, 9, 26, 14, 0, tzinfo=timezone(timedelta(hours=2)))
CFG = control.ControlConfig('on', 19.0, 18.5, 14)
RUNTIME = {'battery_soc': 55, 'vbattery1': 195, 'battery_charge_limit': 18, 'battery_discharge_limit': 18}


class FakeMqtt:
    def __init__(self):
        self.states = []

    async def publish_control_state(self, state):
        self.states.append(state)


def make(eco_on=(False, False, False, False), counter_path=None):
    clock = Clock()
    inv = FakeInverter(clock, base_values())
    for i, on in enumerate(eco_on, start=1):
        inv.values[f'eco_mode_{i}'] = SimpleNamespace(on_off=-1 if on else 0)
    mqtt = FakeMqtt()
    rt = control_runtime.ControlRuntime(CFG, mqtt, now_fn=lambda: T0 + timedelta(seconds=clock.t), mono_fn=clock,
                                        counter_path=counter_path)
    rt.attach(inv)
    return clock, inv, mqtt, rt


def run(rt, clock, seconds, data=RUNTIME):
    async def go():
        end = clock.t + seconds
        while clock.t <= end:
            await rt.step(data)
            clock.t += 1.0
    asyncio.run(go())


def publish(rt, **fields):
    rt.on_mqtt_message('control/set', json.dumps(fields).encode())


class ControlRuntimeTest(unittest.TestCase):
    def test_command_is_applied_and_state_reports_it(self):
        clock, inv, mqtt, rt = make()
        publish(rt, mode='charge', power_w=2000, ttl_s=900, source='predbat')
        run(rt, clock, 10)
        self.assertIn(('ems_mode', 11), inv.writes)
        state = mqtt.states[-1]
        self.assertEqual((state['mode'], state['effective_mode'], state['applied']), ('charge', 'charge', True))
        self.assertEqual(state['registers']['ems_power_limit'], 2000)
        self.assertEqual(state['writes_today'], 2)

    def test_paired_stop_start_each_cycle_causes_no_writes(self):
        clock, inv, mqtt, rt = make()
        publish(rt, mode='charge', power_w=2000, ttl_s=900, source='predbat')
        run(rt, clock, 10)
        writes = len(inv.writes)
        for _ in range(3):  # three Predbat cycles
            publish(rt, mode='auto', stop='export', source='predbat')
            run(rt, clock, 1)
            publish(rt, mode='charge', power_w=2000, ttl_s=900, source='predbat')
            run(rt, clock, 300)
        self.assertEqual(len(inv.writes), writes)

    def test_invalid_command_is_reported_and_current_kept(self):
        clock, inv, mqtt, rt = make()
        publish(rt, mode='charge', power_w=2000, ttl_s=900)
        rt.on_mqtt_message('control/set', b'{bad')
        run(rt, clock, 5)
        self.assertEqual(mqtt.states[-1]['effective_mode'], 'charge')
        self.assertIn('invalid JSON', mqtt.states[-1]['last_error'])

    def test_reserve_message(self):
        clock, inv, mqtt, rt = make()
        rt.on_mqtt_message('control/reserve/set', b'25')
        run(rt, clock, 1)
        self.assertEqual(mqtt.states[-1]['reserve_soc'], 25)
        for bad in (b'abc', b'150'):
            rt.on_mqtt_message('control/reserve/set', bad)
        run(rt, clock, 1)
        self.assertEqual(mqtt.states[-1]['reserve_soc'], 25)
        rt.on_mqtt_message('control/reserve/set', b'')
        run(rt, clock, 1)
        self.assertIsNone(mqtt.states[-1]['reserve_soc'])

    def test_state_published_on_change_and_every_10s(self):
        clock, inv, mqtt, rt = make()
        run(rt, clock, 25)
        self.assertEqual(len(mqtt.states), 3)  # t=0, t=10, t=20

    def test_eco_slot_warning(self):
        clock, inv, mqtt, rt = make(eco_on=(True, False, False, False))
        run(rt, clock, 1)
        self.assertIn('eco slot 1 is enabled', mqtt.states[-1]['warnings'])

    def test_override(self):
        clock, inv, mqtt, rt = make()
        rt.set_override(control.make_override('freeze_charge', '', '', '30', T0))
        run(rt, clock, 5)
        self.assertEqual(mqtt.states[-1]['override']['mode'], 'freeze_charge')
        self.assertIn(('battery_discharge_depth', 55), inv.writes)
        rt.clear_override()
        run(rt, clock, 10)
        self.assertIsNone(mqtt.states[-1]['override'])

    def test_min_soc_hold_warning_after_freeze_near_min(self):
        clock, inv, mqtt, rt = make()
        low = {**RUNTIME, 'battery_soc': 16}
        rt.set_override(control.make_override('freeze_charge', '', '', '30', T0))
        run(rt, clock, 2, data=low)
        rt.clear_override()
        run(rt, clock, 1, data=low)
        self.assertIn('discharge may stay blocked until SoC reaches 19%', ' '.join(mqtt.states[-1]['warnings']))

    def test_off_grid_onset_retries_a_backed_off_restore_at_once(self):
        clock = Clock()
        inv = FakeInverter(clock, base_values(battery_charge_current=0.0), delay=10_000)
        rt = control_runtime.ControlRuntime(CFG, FakeMqtt(), now_fn=lambda: T0 + timedelta(seconds=clock.t),
                                            mono_fn=clock)
        rt.attach(inv)
        run(rt, clock, 12)  # auto restore of the charge current fails, writer backs off
        writes_before = len(inv.writes)
        run(rt, clock, 0, data={**RUNTIME, 'grid_mode': 2, 'work_mode': 2})
        self.assertEqual(inv.writes[writes_before:], [('battery_charge_current', 19.0)])

    def test_write_counter_survives_an_inverter_reconnect(self):
        clock, inv, mqtt, rt = make()
        publish(rt, mode='charge', power_w=2000, ttl_s=900, source='predbat')
        run(rt, clock, 10)
        rt.attach(inv)
        run(rt, clock, 1)
        self.assertEqual(mqtt.states[-1]['writes_today'], 2)

    def test_step_never_raises(self):
        clock, inv, mqtt, rt = make()

        async def broken(*a):
            raise RuntimeError('mqtt down')
        mqtt.publish_control_state = broken
        run(rt, clock, 2)  # must not raise
        self.assertIsNotNone(rt.last_state)

    def test_unattached_step_reports_without_writing(self):
        mqtt = FakeMqtt()
        rt = control_runtime.ControlRuntime(CFG, mqtt, now_fn=lambda: T0, mono_fn=lambda: 0.0)
        state = asyncio.run(rt.step(RUNTIME))
        self.assertFalse(state['applied'])


class WriteCounterPersistenceTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, 'control_writes.json')

    def tearDown(self):
        self.dir.cleanup()

    def test_write_count_survives_a_process_restart(self):
        clock, inv, mqtt, rt = make(counter_path=self.path)
        publish(rt, mode='charge', power_w=2000, ttl_s=900, source='predbat')
        run(rt, clock, 10)
        written = mqtt.states[-1]['writes_today']
        self.assertGreater(written, 0)
        _, _, mqtt2, rt2 = make(counter_path=self.path)  # a fresh process
        run(rt2, Clock(), 0)
        self.assertEqual(mqtt2.states[-1]['writes_today'], written)

    def test_count_from_an_earlier_day_is_not_restored(self):
        with open(self.path, 'w') as f:
            json.dump({'date': '2000-01-01', 'writes': 250}, f)
        clock, inv, mqtt, rt = make(counter_path=self.path)
        run(rt, clock, 0)
        self.assertEqual(mqtt.states[-1]['writes_today'], 0)

    def test_corrupt_counter_file_starts_from_zero(self):
        with open(self.path, 'w') as f:
            f.write('{not json')
        clock, inv, mqtt, rt = make(counter_path=self.path)
        publish(rt, mode='charge', power_w=2000, ttl_s=900, source='predbat')
        run(rt, clock, 10)
        self.assertGreater(mqtt.states[-1]['writes_today'], 0)
        with open(self.path) as f:
            self.assertEqual(json.load(f)['writes'], mqtt.states[-1]['writes_today'])

    def test_unwritable_counter_path_does_not_break_control(self):
        clock, inv, mqtt, rt = make(counter_path=os.path.join(self.dir.name, 'missing', 'x.json'))
        publish(rt, mode='charge', power_w=2000, ttl_s=900, source='predbat')
        run(rt, clock, 10)
        self.assertIn(('ems_mode', 11), inv.writes)


if __name__ == '__main__':
    unittest.main()
