"""
control.py
Optimizer-neutral battery control executor - pure logic, no I/O. Turns
commands (MQTT or dashboard) plus runtime samples into desired values for
the inverter's control settings. See
docs/superpowers/notes/2026-09-26-predbat-control-path-brainstorm-state.md
for the measured inverter behaviour every rule here is based on.
"""
import json
import math
from collections import deque
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
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise CommandError(f'{what} must be a finite number')
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
            raw = data['expires_at']
            if isinstance(raw, str) and raw.endswith('Z'):
                raw = raw[:-1] + '+00:00'  # Python < 3.11 fromisoformat doesn't take 'Z'
            expires = datetime.fromisoformat(raw)
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
    max_battery_w: int = 3600
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
                         int(env.get('CONTROL_MAX_BATTERY_W') or 3600),
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


TARGET_DEBOUNCE = timedelta(seconds=30)
CHARGE_TARGET_HYSTERESIS = 3
RESERVE_HYSTERESIS = 2
RESERVE_WARN_BELOW = 20
OFF_GRID_RELEASE = timedelta(seconds=60)
FLOOR_STEP = 3  # freeze floor follows a rising SoC in steps of this many points
MIN_FLOOR_SAMPLES = 3  # freeze floor needs this many SoC samples (startup, after reconnect gaps)
INVERTER_RESUME_MARGIN = 5  # inverter discharges again only this far above its minimum SoC


class _Debounce:
    """Condition must hold for TARGET_DEBOUNCE. A None SoC sample neither
    confirms nor breaks it (callers skip update for None)."""

    def __init__(self):
        self._since: Optional[datetime] = None

    def update(self, condition: bool, now: datetime) -> bool:
        if not condition:
            self._since = None
            return False
        if self._since is None:
            self._since = now
        return now - self._since >= TARGET_DEBOUNCE

    def reset(self):
        self._since = None


class Executor:
    """Decides the desired settings each tick. Not thread-safe - call it only
    from the asyncio loop thread (MQTT reader, poll loop, and Flask routes via
    run_coroutine_threadsafe all run there)."""

    def __init__(self, config: ControlConfig):
        self._config = config
        self._command: Optional[Command] = None
        self._override: Optional[Command] = None
        self._reserve: Optional[int] = None
        self._idle_reason = 'no command'
        self._command_error: Optional[str] = None
        self._effective = Mode.AUTO
        self._reason = 'no command'
        self._since: Optional[datetime] = None
        self._power_applied: Optional[int] = None
        self._clamped = False
        self._reset_latches()
        self._reserve_latched = False
        self._reserve_deb = _Debounce()
        self._reserve_release_deb = _Debounce()
        self._off_grid = False
        self._on_grid_since: Optional[datetime] = None
        self._bms_limits_w: tuple = (None, None)  # informational only, never applied
        self._last_soc: Optional[float] = None
        self._soc_window: deque = deque()  # (time, soc) of the last TARGET_DEBOUNCE
        self._floor: Optional[int] = None
        self._min_soc_hold = False

    # --- inputs ---------------------------------------------------------
    def submit(self, cmd: Command) -> None:
        self._command_error = None
        if cmd.stop is not None:
            if self._command is not None and self._command.mode in STOP_DOMAINS[cmd.stop]:
                self._clear_command('stopped by ' + cmd.source)
            return
        old = self._command
        self._command = cmd
        if old is not None and (cmd.mode, cmd.target_soc, cmd.source) == (old.mode, old.target_soc, old.source):
            return  # a re-send (or only power_w changed, e.g. low-power rates): keep the hold
        if self._override is None:  # while overridden, the latches belong to the override
            self._reset_latches()

    def reject(self, error: str) -> None:
        self._command_error = error

    def set_override(self, cmd: Command) -> None:
        self._override = cmd
        self._reset_latches()

    def clear_override(self) -> None:
        self._override = None
        self._reset_latches()

    def set_reserve(self, soc: Optional[int]) -> None:
        self._reserve = soc
        self._reserve_latched = False
        self._reserve_deb.reset()
        self._reserve_release_deb.reset()

    @property
    def reserve(self) -> Optional[int]:
        return self._reserve

    @property
    def last_soc(self) -> Optional[float]:
        return self._last_soc

    @property
    def min_soc_hold(self) -> bool:
        return self._min_soc_hold

    # --- decision -------------------------------------------------------
    def tick(self, sample: Sample, now: datetime) -> dict:
        if sample.soc is not None:
            self._last_soc = sample.soc
            self._soc_window.append((now, sample.soc))
        # By age, but always keep the last few: after a reading gap the first
        # (possibly garbage) sample must not be the only one the floor sees.
        while len(self._soc_window) > MIN_FLOOR_SAMPLES and now - self._soc_window[0][0] > TARGET_DEBOUNCE:
            self._soc_window.popleft()
        active, reason = self._active(now)  # always: keeps expiry running off-grid too
        if self._update_off_grid(sample.off_grid, now):
            # Backup side depends on the battery: never keep a current at 0.
            active, mode, reason = None, Mode.AUTO, 'off-grid'
        else:
            mode, reason = self._targets(active, sample, now, reason)
            mode, reason = self._apply_reserve(mode, sample, now, reason)
        if mode is Mode.FREEZE_CHARGE and self._floor is None and len(self._soc_window) < MIN_FLOOR_SAMPLES:
            # A guessed floor above the real SoC would make the inverter's DoD
            # Holding charge from the grid.
            mode, reason = Mode.AUTO, 'waiting for SoC'
        self._update_floor(mode)
        power = self._clamp(active.power_w if active is not None and mode in POWERED_MODES else None, mode, sample)
        self._bms_limits_w = tuple(None if not (a and sample.battery_v and a > 0 and sample.battery_v > 0)
                                   else int(a * sample.battery_v)
                                   for a in (sample.bms_charge_limit_a, sample.bms_discharge_limit_a))
        if mode is not self._effective or self._since is None:
            self._since = now
        self._effective, self._reason, self._power_applied = mode, reason, power
        return self._settings(mode, power)

    def _steady_soc(self) -> float:
        """Lowest SoC of the last TARGET_DEBOUNCE (last known if none): a
        single garbage high sample must never set a floor above the real SoC
        (DoD Holding would then charge from the grid); a garbage low one only
        makes the floor more permissive."""
        return min(soc for _, soc in self._soc_window)

    def _update_floor(self, mode: Mode) -> None:
        min_soc, soc = self._config.min_soc, self._last_soc
        if mode is Mode.FREEZE_CHARGE:
            whole = int(self._steady_soc())
            if self._floor is None:
                self._floor = max(min_soc, whole)
            elif whole >= self._floor + FLOOR_STEP:
                self._floor = whole  # keep surplus PV charge; never lowered
            self._min_soc_hold = False
            return
        if self._floor is not None and soc is not None and soc < min_soc + INVERTER_RESUME_MARGIN:
            self._min_soc_hold = True
        elif soc is not None and soc >= min_soc + INVERTER_RESUME_MARGIN:
            self._min_soc_hold = False
        self._floor = None

    def _update_off_grid(self, off_grid: bool, now: datetime) -> bool:
        if off_grid:
            self._off_grid, self._on_grid_since = True, None
            return True
        if self._off_grid:
            if self._on_grid_since is None:
                self._on_grid_since = now
            if now - self._on_grid_since < OFF_GRID_RELEASE:
                return True
            self._off_grid, self._on_grid_since = False, None
        return False

    def _clear_command(self, reason: str) -> None:
        self._command = None
        self._idle_reason = reason
        if self._override is None:
            self._reset_latches()

    def _reset_latches(self) -> None:
        self._charge_latched = self._export_latched = False
        self._charge_deb, self._export_deb = _Debounce(), _Debounce()
        self._charge_release_deb = _Debounce()
        self._fresh = True  # first SoC sample of a new command skips the debounce

    def _active(self, now: datetime):
        if self._override is not None:
            if self._override.expires_at <= now:
                self._override = None
                self._reset_latches()
            else:
                return self._override, 'dashboard override'
        if self._command is not None and self._command.expires_at is not None and self._command.expires_at <= now:
            self._clear_command('command expired')
        if self._command is None or self._command.mode is Mode.AUTO:
            if self._command is not None:
                return None, f'auto from {self._command.source}'
            return None, self._idle_reason
        return self._command, f'command from {self._command.source}'

    def _targets(self, active: Optional[Command], sample: Sample, now: datetime, reason: str):
        if active is None:
            return Mode.AUTO, reason
        soc, target, fresh = sample.soc, active.target_soc, self._fresh
        if soc is not None:
            self._fresh = False
        if target is None or soc is None and not (self._charge_latched or self._export_latched):
            return active.mode, reason
        if active.mode is Mode.CHARGE:
            if self._charge_latched:
                if soc is not None and self._charge_release_deb.update(soc < target - CHARGE_TARGET_HYSTERESIS, now):
                    self._charge_latched = False
                    self._charge_deb.reset()
                    self._charge_release_deb.reset()
                    return Mode.CHARGE, reason
                return Mode.FREEZE_CHARGE, 'target_soc reached'
            if (fresh and soc >= target) or self._charge_deb.update(soc >= target, now):
                self._charge_latched = True
                return Mode.FREEZE_CHARGE, 'target_soc reached'
        if active.mode is Mode.EXPORT:
            if self._export_latched:
                return Mode.AUTO, 'target_soc reached'
            if (fresh and soc <= target) or self._export_deb.update(soc <= target, now):
                self._export_latched = True
                return Mode.AUTO, 'target_soc reached'
        return active.mode, reason

    def _apply_reserve(self, mode: Mode, sample: Sample, now: datetime, reason: str):
        if mode is not Mode.AUTO or self._reserve is None:
            self._reserve_latched = False
            self._reserve_deb.reset()
            self._reserve_release_deb.reset()
            return mode, reason
        soc = sample.soc
        if self._reserve_latched:
            if soc is not None and self._reserve_release_deb.update(soc >= self._reserve + RESERVE_HYSTERESIS, now):
                self._reserve_latched = False
                self._reserve_deb.reset()
                self._reserve_release_deb.reset()
                return mode, reason
            return Mode.FREEZE_CHARGE, 'reserve'
        if soc is not None and self._reserve_deb.update(soc <= self._reserve, now):
            self._reserve_latched = True
            return Mode.FREEZE_CHARGE, 'reserve'
        return mode, reason

    def _clamp(self, power: Optional[int], mode: Mode, sample: Sample) -> Optional[int]:
        if power is None:
            self._clamped = False
            return None
        # Only the fixed config maximum: the inverter enforces the live BMS
        # limit itself, and following it made the setpoint track voltage
        # jitter (a write every second while the BMS tapers).
        clamped = max(MIN_POWER_W, min(power, self._config.max_battery_w))
        self._clamped = clamped != power
        return clamped

    def _settings(self, mode: Mode, power: Optional[int]) -> dict:
        c, d, m = self._config.charge_current_a, self._config.discharge_current_a, self._config.min_soc
        ems, limit, charge_a, depth = {
            Mode.AUTO: (EMS_AUTO, 0, c, m),
            Mode.CHARGE: (EMS_CHARGE_BATTERY, power, c, m),
            Mode.EXPORT: (EMS_DISCHARGE_PV, power, c, m),
            Mode.FREEZE_CHARGE: (EMS_AUTO, 0, c, self._floor),
            Mode.FREEZE_EXPORT: (EMS_AUTO, 0, 0, m),
        }[mode]
        return {'ems_mode': ems, 'ems_power_limit': limit, 'battery_charge_current': charge_a,
                'battery_discharge_current': d, 'battery_discharge_depth': depth}

    # --- reporting ------------------------------------------------------
    def snapshot(self, now: datetime) -> dict:
        cmd = self._command
        iso = lambda t: None if t is None else t.isoformat()
        return {
            'mode': cmd.mode.value if cmd is not None else Mode.AUTO.value,
            'effective_mode': self._effective.value,
            'power_w': cmd.power_w if cmd is not None else None,
            'power_applied_w': self._power_applied,
            'power_clamped': self._clamped,
            'target_soc': cmd.target_soc if cmd is not None else None,
            'source': cmd.source if cmd is not None else None,
            'expires_at': iso(cmd.expires_at) if cmd is not None else None,
            'since': iso(self._since),
            'override': None if self._override is None else {
                'mode': self._override.mode.value, 'power_w': self._override.power_w,
                'target_soc': self._override.target_soc, 'until': iso(self._override.expires_at)},
            'reserve_soc': self._reserve,
            'reason': self._reason,
            'command_error': self._command_error,
            'shadow': self._config.mode == 'shadow',
            'off_grid': self._off_grid,
            'bms_charge_limit_w': self._bms_limits_w[0],
            'bms_discharge_limit_w': self._bms_limits_w[1],
            'freeze_floor': self._floor,
        }


def compute_warnings(readback: dict, eco_slots_enabled: Optional[list], reserve: Optional[int],
                     initial_work_mode: Optional[int], min_soc_hold: Optional[int] = None) -> list:
    warnings = []
    upper = readback.get('soc_upper_limit')
    if upper is not None and upper != 100:
        warnings.append(f'soc_upper_limit is {upper} (expected 100)')
    work_mode = readback.get('work_mode')
    if initial_work_mode is not None and work_mode is not None and work_mode != initial_work_mode:
        warnings.append(f'work_mode changed from {initial_work_mode} to {work_mode}')
    for i, enabled in enumerate(eco_slots_enabled or [], start=1):
        if enabled:
            warnings.append(f'eco slot {i} is enabled')
    if reserve is not None and reserve < RESERVE_WARN_BELOW:
        warnings.append(f'reserve {reserve}% is below {RESERVE_WARN_BELOW}% (BMS SoC is unreliable there)')
    if min_soc_hold is not None:
        warnings.append(f'discharge may stay blocked until SoC reaches {min_soc_hold + INVERTER_RESUME_MARGIN}% '
                        f'(the inverter resumes {INVERTER_RESUME_MARGIN} points above its minimum SoC after a freeze)')
    return warnings
