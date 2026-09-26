"""
control.py
Optimizer-neutral battery control executor - pure logic, no I/O. Turns
commands (MQTT or dashboard) plus runtime samples into desired values for
the inverter's control settings. See
docs/superpowers/specs/2026-09-26-predbat-control-executor-design.md for
the measured inverter behaviour every rule here is based on.
"""
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Mapping, Optional


class Mode(str, Enum):
    AUTO = 'auto'
    CHARGE = 'charge'
    EXPORT = 'export'
    FREEZE_CHARGE = 'freeze_charge'
    FREEZE_EXPORT = 'freeze_export'


EMS_AUTO = 1
EMS_DISCHARGE_PV = 3
EMS_CHARGE_BATTERY = 11

# The five settings every mode fully specifies, and the ones only reported.
# battery_discharge_depth is the raw on-grid minimum SoC % (SolarGo shows it
# inverted as DoD); off-grid the inverter uses its separate offline minimum.
MODE_SETTINGS = ('ems_mode', 'ems_power_limit', 'battery_charge_current', 'battery_discharge_current',
                 'battery_discharge_depth')
REPORTED_SETTINGS = MODE_SETTINGS + ('soc_upper_limit', 'work_mode')

POWERED_MODES = (Mode.CHARGE, Mode.EXPORT)
STOP_DOMAINS = {'charge': (Mode.CHARGE, Mode.FREEZE_CHARGE), 'export': (Mode.EXPORT, Mode.FREEZE_EXPORT)}
MIN_POWER_W = 100
MAX_EXPIRY = timedelta(minutes=60)
OVERRIDE_MIN = timedelta(minutes=15)
OVERRIDE_MAX = timedelta(hours=12)
MAX_INVERTER_CURRENT_A = 25.0


class CommandError(ValueError):
    pass


@dataclass(frozen=True)
class Command:
    mode: Mode
    power_w: Optional[int] = None
    target_soc: Optional[int] = None
    source: str = 'unknown'
    expires_at: Optional[datetime] = None
    stop: Optional[str] = None
    id: Optional[str] = None

    def same_request(self, other: Optional['Command']) -> bool:
        """True when other asks for the same thing (expiry/id may differ) -
        a re-sent command then only refreshes the expiry."""
        return other is not None and (self.mode, self.power_w, self.target_soc, self.source, self.stop) == \
            (other.mode, other.power_w, other.target_soc, other.source, other.stop)


def _number(value: Any, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CommandError(f'{what} must be a number')
    return value


def parse_command(payload, now: datetime) -> Command:
    """Validate a control/set payload. now must be timezone-aware."""
    try:
        data = json.loads(payload)
    except (TypeError, ValueError) as e:
        raise CommandError(f'invalid JSON: {e}') from e
    if not isinstance(data, dict):
        raise CommandError('command must be a JSON object')
    try:
        mode = Mode(data.get('mode'))
    except ValueError:
        raise CommandError(f"unknown mode: {data.get('mode')!r}") from None

    power = target = None
    if mode in POWERED_MODES:
        power = _number(data.get('power_w'), 'power_w')
        if power <= 0:
            raise CommandError(f'{mode.value} needs a positive power_w')
        power = int(power)
        if data.get('target_soc') is not None:
            target = _number(data['target_soc'], 'target_soc')
            if not 0 <= target <= 100:
                raise CommandError('target_soc must be 0-100')
            target = int(target)

    stop = data.get('stop')
    if stop is not None and (mode is not Mode.AUTO or stop not in STOP_DOMAINS):
        raise CommandError('stop must be "charge" or "export" and only goes with mode auto')

    expires = None
    if data.get('expires_at') is not None:
        try:
            expires = datetime.fromisoformat(data['expires_at'])
        except (TypeError, ValueError):
            raise CommandError(f"invalid expires_at: {data['expires_at']!r}") from None
        if expires.tzinfo is None:
            raise CommandError('expires_at needs a timezone offset')
    elif data.get('ttl_s') is not None:
        expires = now + timedelta(seconds=_number(data['ttl_s'], 'ttl_s'))
    if expires is None and mode is not Mode.AUTO:
        raise CommandError(f'{mode.value} needs expires_at or ttl_s')
    if expires is not None:
        if expires <= now:
            raise CommandError('expiry is in the past')
        if expires > now + MAX_EXPIRY:
            raise CommandError('expiry is more than 60 min ahead')

    source = str(data.get('source') or 'unknown')[:32]
    cid = None if data.get('id') is None else str(data['id'])[:64]
    return Command(mode, power, target, source, expires, stop, cid)


def make_override(mode: str, power_w, target_soc, duration_min, now: datetime) -> Command:
    """Build a dashboard override from form values (strings, blanks allowed)."""
    try:
        duration = timedelta(minutes=int(duration_min))
    except (TypeError, ValueError):
        raise CommandError('duration must be whole minutes') from None
    if not OVERRIDE_MIN <= duration <= OVERRIDE_MAX:
        raise CommandError('duration must be 15 min - 12 h')
    fields: dict = {'mode': mode, 'source': 'dashboard', 'ttl_s': 1}
    if power_w not in (None, ''):
        fields['power_w'] = float(power_w)
    if target_soc not in (None, ''):
        fields['target_soc'] = float(target_soc)
    cmd = parse_command(json.dumps(fields), now)  # reuse all field validation
    return Command(cmd.mode, cmd.power_w, cmd.target_soc, 'dashboard', now + duration)


@dataclass(frozen=True)
class ControlConfig:
    mode: str  # 'shadow' or 'on' ('off' means no ControlConfig at all)
    charge_current_a: float
    discharge_current_a: float
    min_soc: int  # normal battery_discharge_depth, restored whenever not frozen
    max_battery_w: int = 3400
    max_writes_per_day: int = 300


def config_from_env(env: Mapping[str, str]) -> Optional[ControlConfig]:
    mode = (env.get('CONTROL_MODE') or 'off').strip().lower()
    if mode == 'off':
        return None
    if mode not in ('shadow', 'on'):
        raise ValueError(f'CONTROL_MODE must be off, shadow or on, not {mode!r}')

    def current(key: str) -> float:
        raw = env.get(key)
        if not raw:
            raise ValueError(f'{key} is required when CONTROL_MODE={mode}')
        value = float(raw)
        if not 0 < value <= MAX_INVERTER_CURRENT_A:
            raise ValueError(f'{key} must be > 0 and <= {MAX_INVERTER_CURRENT_A} A')
        return value

    raw_min = env.get('CONTROL_MIN_SOC')
    if not raw_min:
        raise ValueError(f'CONTROL_MIN_SOC is required when CONTROL_MODE={mode}')
    min_soc = int(raw_min)
    if not 0 <= min_soc <= 100:
        raise ValueError('CONTROL_MIN_SOC must be 0-100')

    return ControlConfig(mode, current('CONTROL_CHARGE_CURRENT_A'), current('CONTROL_DISCHARGE_CURRENT_A'), min_soc,
                         int(env.get('CONTROL_MAX_BATTERY_W') or 3400),
                         int(env.get('CONTROL_MAX_WRITES_PER_DAY') or 300))


def _float_or_none(value) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class Sample:
    soc: Optional[float] = None
    battery_v: Optional[float] = None
    bms_charge_limit_a: Optional[float] = None
    bms_discharge_limit_a: Optional[float] = None
    off_grid: bool = False

    @staticmethod
    def from_runtime(data: dict) -> 'Sample':
        soc = _float_or_none(data.get('battery_soc'))
        if soc is not None and not 0 <= soc <= 100:
            soc = None
        # Runtime (not the work_mode *setting*): grid_mode 0 not connected /
        # 2 fault, work_mode 2 Normal (Off-Grid) - the history shows outages
        # as grid_mode 2 + work_mode 2.
        grid_mode = _float_or_none(data.get('grid_mode'))
        work_mode = _float_or_none(data.get('work_mode'))
        off_grid = (grid_mode is not None and grid_mode != 1) or work_mode == 2
        return Sample(soc, _float_or_none(data.get('vbattery1')), _float_or_none(data.get('battery_charge_limit')),
                      _float_or_none(data.get('battery_discharge_limit')), off_grid)
