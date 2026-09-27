"""
control_io.py
Applies control.Executor's desired settings through the (single) goodwe
connection: writes only what differs, verifies by read-back, retries, and
never raises into the poll loop. Some settings only show a new value a few
seconds after the write is acked, hence the delayed verification.
"""
import datetime
import logging
import time
from typing import Callable, Optional

from control import EMS_AUTO, MODE_SETTINGS, REPORTED_SETTINGS

logger = logging.getLogger(__name__)

_CURRENTS = ('battery_charge_current', 'battery_discharge_current')
_LIMITS = _CURRENTS + ('battery_discharge_depth',)
_TOLERANCE = {name: 0.05 for name in _CURRENTS}
MAX_BACKOFF_S = 3600.0  # a register that acks but never takes the value: ~3 writes an hour, not a minute


def _matches(name: str, want, have) -> bool:
    if have is None or want is None:
        return False
    return abs(float(want) - float(have)) <= _TOLERANCE.get(name, 0)


def _restricts(name: str, want, have) -> bool:
    if name == 'battery_discharge_depth':
        return have is None or float(want) > float(have)  # floor going up
    return want == 0  # current going to 0


def write_order(desired: dict, readback: dict) -> list:
    """Differing settings in safe order: restricting limits first (a current
    going to 0, the minimum SoC going up - entering a freeze), then EMS
    (setpoint before mode when entering a forced mode, mode before setpoint
    when going back to AUTO), then relaxing limits (currents restored, the
    minimum SoC lowered)."""
    differing = [n for n in MODE_SETTINGS if not _matches(n, desired[n], readback.get(n))]
    restrict = [n for n in _LIMITS if n in differing and _restricts(n, desired[n], readback.get(n))]
    relax = [n for n in _LIMITS if n in differing and n not in restrict]
    ems = [n for n in ('ems_mode', 'ems_power_limit') if n in differing]
    first = 'ems_mode' if desired['ems_mode'] == EMS_AUTO else 'ems_power_limit'
    ems.sort(key=lambda n: 0 if n == first else 1)
    return restrict + ems + relax


class ControlWriter:
    def __init__(self, inverter, *, shadow: bool, max_writes_per_day: int = 300,
                 now_fn: Callable[[], float] = time.monotonic,
                 today_fn: Callable[[], datetime.date] = datetime.date.today,
                 read_interval_s: float = 10.0, verify_delay_s: float = 3.0,
                 max_attempts: int = 3, error_backoff_s: float = 60.0):
        self._inverter = inverter
        self._shadow = shadow
        self._max_writes = max_writes_per_day
        self._now = now_fn
        self._today_fn = today_fn
        self._read_interval = read_interval_s
        self._verify_delay = verify_delay_s
        self._max_attempts = max_attempts
        self._error_backoff = error_backoff_s
        self._backoff_s = error_backoff_s  # doubles per failed round, up to MAX_BACKOFF_S
        self.readback: dict = {}
        self.last_error: Optional[str] = None
        self.writes_today = 0
        self._today = today_fn()
        self._next_read = 0.0
        self._verify_at: Optional[float] = None
        self._backoff_until: Optional[float] = None
        self._attempts: dict = {}
        self._warned_writes = False
        self._last_desired: Optional[dict] = None

    def restart(self) -> None:
        """Forget retries, back-off and pending verification - for events
        (going off-grid) that must be written now even when the desired
        values didn't change."""
        self._attempts.clear()
        self._backoff_until = None
        self._backoff_s = self._error_backoff
        self._verify_at = None

    def carry_state_from(self, other: 'ControlWriter') -> None:
        """Keep the daily write count, retries and back-off across inverter
        reconnects - a flaky link must not buy a stuck register fresh writes."""
        self._today, self.writes_today, self._warned_writes = other._today, other.writes_today, other._warned_writes
        self._attempts, self._backoff_until, self._backoff_s = dict(other._attempts), other._backoff_until, other._backoff_s
        self._last_desired, self.last_error = other._last_desired, other.last_error

    def applied(self, desired: Optional[dict]) -> bool:
        return desired is not None and all(_matches(n, desired[n], self.readback.get(n)) for n in MODE_SETTINGS)

    async def step(self, desired: Optional[dict]) -> None:
        now = self._now()
        self._roll_day()
        if now >= self._next_read:
            await self._read_all(now)
        if desired is None:
            return
        if desired != self._last_desired:
            # New target (e.g. off-grid restore): start fresh - don't let a
            # back-off or pending verification from the old target delay it.
            self._last_desired = dict(desired)
            self.restart()
        if self.applied(desired):
            self._attempts.clear()
            self._backoff_until = None
            self._backoff_s = self._error_backoff
            self.last_error = None
            return
        if self._shadow:
            return
        if self._verify_at is not None and now < self._verify_at:
            return
        if self._backoff_until is not None and now < self._backoff_until:
            return
        order = write_order(desired, self.readback)
        for name in order:
            attempts = self._attempts.get(name, 0)
            if attempts >= self._max_attempts:
                self.last_error = (f'{name}: not applied after {attempts} attempts '
                                   f'(wanted {desired[name]}, read {self.readback.get(name)})')
                logger.warning(f'Control write failed: {self.last_error}')
                self._backoff_until = now + self._backoff_s
                self._backoff_s = min(self._backoff_s * 2, MAX_BACKOFF_S)
                self._attempts.clear()
                return
            self._attempts[name] = attempts + 1
            self._count_write()
            try:
                await self._inverter.write_setting(name, desired[name])
                logger.info(f'Control write {name} = {desired[name]}')
            except Exception as e:
                self.last_error = f'{name}: write failed: {e}'
                logger.warning(f'Control write {name} failed: {e}')
                break  # the rest of the order may rely on this one - retry it all after verification
        self._verify_at = self._next_read = now + self._verify_delay

    async def _read_all(self, now: float) -> None:
        for name in REPORTED_SETTINGS:
            try:
                self.readback[name] = await self._inverter.read_setting(name)
            except Exception as e:
                logger.debug(f'Control read of {name} failed: {e}')
        self._next_read = now + self._read_interval
        self._verify_at = None

    def _count_write(self) -> None:
        self.writes_today += 1
        if self.writes_today > self._max_writes and not self._warned_writes:
            self._warned_writes = True
            logger.warning(f'Control writes today ({self.writes_today}) exceed {self._max_writes} - check for flapping')

    def _roll_day(self) -> None:
        today = self._today_fn()
        if today != self._today:
            self._today, self.writes_today, self._warned_writes = today, 0, False
