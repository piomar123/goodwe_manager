"""
tests/control_fakes.py
Fake goodwe inverter and clock shared by the control tests. Import as
`from tests.control_fakes import ...` (works both for
`python -m unittest discover -s tests` and `python -m unittest tests.x`,
since the repo root is on sys.path in both).
"""
import asyncio

USER = {'battery_charge_current': 19.0, 'battery_discharge_current': 18.5, 'battery_discharge_depth': 14}
AUTO = {'ems_mode': 1, 'ems_power_limit': 0, **USER}
FREEZE_EXPORT = {**AUTO, 'battery_charge_current': 0}
FREEZE_CHARGE_60 = {**AUTO, 'battery_discharge_depth': 60}
CHARGE_2K = {'ems_mode': 11, 'ems_power_limit': 2000, **USER}


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class FakeInverter:
    """Stores values; a write becomes visible after `delay` seconds of the
    shared clock. Reads of names in `failing` raise."""

    def __init__(self, clock, values=None, delay=0.0):
        self.clock = clock
        self.values = dict(values or {})
        self.pending = []  # (visible_at, name, value)
        self.delay = delay
        self.writes = []
        self.failing = set()

    async def read_setting(self, name):
        if name in self.failing:
            raise TimeoutError('no response')
        for item in list(self.pending):
            visible_at, n, v = item
            if self.clock.t >= visible_at:
                self.values[n] = v
                self.pending.remove(item)
        return self.values.get(name)

    async def write_setting(self, name, value):
        self.writes.append((name, value))
        self.pending.append((self.clock.t + self.delay, name, value))


def base_values(**over):
    v = {**AUTO, 'soc_upper_limit': 100, 'work_mode': 3}
    v.update(over)
    return v


def run_steps(writer, clock, desired, seconds, tick=1.0):
    """Call writer.step(desired) once per simulated second from clock.t to
    clock.t + seconds inclusive; leaves clock.t one tick past the end."""
    async def go():
        end = clock.t + seconds
        while clock.t <= end:
            await writer.step(desired)
            clock.t += tick
    asyncio.run(go())
