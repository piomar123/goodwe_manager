# Predbat Control Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Predbat (or any optimizer) drive the GoodWe GW8KN-ET through goodwe_manager via MQTT commands, with read-back acknowledgement, fail-safe expiry, a timed dashboard override and a software reserve.

**Architecture:** A pure `control.Executor` turns commands + runtime samples into desired values for four inverter settings; `control_io.ControlWriter` writes only the differences through the existing single goodwe connection and verifies them by read-back; `control_runtime.ControlRuntime` glues both to MQTT and the 1 Hz poll loop in `main.py`. Everything runs on the existing asyncio loop thread; Flask routes reach it via `run_coroutine_threadsafe`.

**Tech Stack:** Python 3.12, goodwe 0.4.10, aiomqtt 2.5.1, Flask, `unittest` (`python -m unittest discover -s tests`).

**Spec:** `docs/superpowers/specs/2026-09-26-predbat-control-executor-design.md` (read it first; measured inverter behaviour and the reasons behind every rule are there and in `docs/superpowers/notes/2026-09-26-predbat-control-path-brainstorm-state.md`).

## Global Constraints

- Settings are addressed by goodwe library ids only: `ems_mode`, `ems_power_limit`, `battery_charge_current`, `battery_discharge_current`, `soc_upper_limit`, `work_mode`, `eco_mode_1..4`. No raw register addresses in code.
- EMS mode values: AUTO = 1, DISCHARGE_PV = 3, CHARGE_BATTERY = 11.
- Mode table (every mode sets all four): `auto` = (1, 0, user, user); `charge` P = (11, P, user, user); `export` P = (3, P, user, user); `freeze_charge` = (1, 0, user, 0); `freeze_export` = (1, 0, 0, user). Order of values: `ems_mode`, `ems_power_limit`, `battery_charge_current`, `battery_discharge_current`.
- "user" currents come from `CONTROL_CHARGE_CURRENT_A` / `CONTROL_DISCHARGE_CURRENT_A` (float A, 0 < x ≤ 25), never captured from the inverter.
- `CONTROL_MODE` = `off` (default; no subscriptions, no writes, no behaviour change) | `shadow` (compute + publish state with `"shadow": true`, never write) | `on`.
- `CONTROL_MAX_BATTERY_W` default 3400; `CONTROL_MAX_WRITES_PER_DAY` default 300 (log warning only).
- Commands: topic `<prefix>/control/set`, QoS 1, not retained; `expires_at` (ISO 8601 with offset) or `ttl_s`, max 60 min ahead, required unless `mode` is `auto`; power clamped to `[100, min(CONTROL_MAX_BATTERY_W, BMS limit A × battery V)]`.
- Scoped stop: `{"mode":"auto","stop":"charge"|"export"}` only clears a command in that domain.
- SoC targets: 30 s debounce, except on the first SoC sample after a new command; charge target reached → `freeze_charge`, back to `charge` below target − 3; export target reached → `auto`.
- Software reserve: topic `<prefix>/control/reserve/set` (retained, integer %); in effective `auto`, SoC ≤ reserve (30 s) → `freeze_charge` until SoC ≥ reserve + 2; warn below 20 %.
- Writer: read the six reported settings every 10 s and 3 s after writes; up to 3 attempts per setting, then `last_error` and 60 s back-off; failed reads are skipped, never raised.
- State topic `<prefix>/control/state` retained, published on change and at least every 10 s.
- Override from the dashboard: 15 min - 12 h, memory only.
- MQTT or control failures must never stop inverter polling (same rule as the existing bridge: catch, log, continue).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. Predbat sends `discharge_stop` + `charge_start` (or `charge_stop` + `discharge_start`) every cycle - the executor must not bounce through `auto` or write anything when the resulting command is unchanged. Pinned in Task 2 (`test_opposite_scoped_stop_is_ignored`) and Task 5 (`test_paired_stop_start_each_cycle_causes_no_writes`).
2. A single garbage SoC sample (e.g. 0 or 100 for one poll) must not trip a target or the reserve. Pinned in Task 2 (`test_single_garbage_soc_sample_does_not_latch`).
3. The MQTT broker drops and comes back: control topics must be re-subscribed and the retained reserve re-delivered. Pinned in Task 4 (`test_resubscribes_after_reconnect`).
4. The dongle stops answering for ~20 s mid-cycle: the writer must neither raise into the poll loop nor spam writes. Pinned in Task 3 (`test_failed_reads_are_skipped_and_no_write_without_readback_change`).
5. The manager restarts while the inverter was left in a freeze (a current at 0) or forced mode: the first steps must restore `auto` + user currents in the safe order. Pinned in Task 3 (`test_startup_reverts_leftover_freeze_export`).

---

## File Structure

| File | Responsibility |
|---|---|
| `control.py` (create) | Pure logic: `Mode`, `Command`, `parse_command`, `make_override`, `ControlConfig`, `config_from_env`, `Sample`, `Executor`, `compute_warnings`. No I/O, no asyncio. |
| `control_io.py` (create) | `write_order`, `ControlWriter`: diff writes + read-back verification against any object with `async read_setting(name)` / `async write_setting(name, value)` (the goodwe `Inverter`). |
| `control_runtime.py` (create) | `ControlRuntime`: MQTT message handling, per-iteration step, eco-slot checks, state building and publishing, override entry points. |
| `mqtt_bridge.py` (modify) | Control topic subscription + reader task, re-subscribe on reconnect, `publish_control_state`. |
| `main.py` (modify) | Config parsing, runtime creation/attach, poll-loop call, SSE `_control`, Flask `/control/*` routes, config page data. |
| `templates/index.html`, `templates/config.html` (modify) | Control panel + override form; read-only control settings on the config page. |
| `MQTT_TOPICS.md`, `README.MD`, `.env.example`, `CHANGELOG.md` (modify) | Topics, HA entities, HA script, Predbat apps.yaml example, env keys. |
| `tests/test_control.py`, `tests/test_control_io.py`, `tests/test_control_runtime.py`, `tests/test_main_control_routes.py` (create), `tests/test_mqtt_bridge.py` (modify) | Tests. |

---

### Task 1: Commands, config and samples (`control.py`, part 1)

**Files:**
- Create: `control.py`
- Test: `tests/test_control.py`

**Interfaces:**
- Produces:
  - `class Mode(str, Enum)`: `AUTO='auto'`, `CHARGE='charge'`, `EXPORT='export'`, `FREEZE_CHARGE='freeze_charge'`, `FREEZE_EXPORT='freeze_export'`
  - constants `EMS_AUTO=1`, `EMS_DISCHARGE_PV=3`, `EMS_CHARGE_BATTERY=11`, `MODE_SETTINGS`, `REPORTED_SETTINGS`
  - `class CommandError(ValueError)`
  - `@dataclass(frozen=True) class Command(mode: Mode, power_w: int|None=None, target_soc: int|None=None, source: str='unknown', expires_at: datetime|None=None, stop: str|None=None, id: str|None=None)` with `same_request(other) -> bool`
  - `parse_command(payload: bytes|str, now: datetime) -> Command` (raises `CommandError`)
  - `make_override(mode: str, power_w: str|int|None, target_soc: str|int|None, duration_min: str|int, now: datetime) -> Command` (raises `CommandError`)
  - `@dataclass(frozen=True) class ControlConfig(mode: str, charge_current_a: float, discharge_current_a: float, max_battery_w: int=3400, max_writes_per_day: int=300)`
  - `config_from_env(env: Mapping[str, str]) -> ControlConfig | None` (raises `ValueError`)
  - `@dataclass(frozen=True) class Sample(soc, battery_v, bms_charge_limit_a, bms_discharge_limit_a)` (all `float|None`) with `Sample.from_runtime(data: dict) -> Sample`

- [ ] **Step 1: Write the failing tests**

```python
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
                                       'CONTROL_DISCHARGE_CURRENT_A': '18.5'})
        self.assertEqual(cfg, control.ControlConfig('on', 19.0, 18.5, 3400, 300))

    def test_overrides(self):
        cfg = control.config_from_env({'CONTROL_MODE': 'shadow', 'CONTROL_CHARGE_CURRENT_A': '19',
                                       'CONTROL_DISCHARGE_CURRENT_A': '19', 'CONTROL_MAX_BATTERY_W': '3000',
                                       'CONTROL_MAX_WRITES_PER_DAY': '100'})
        self.assertEqual((cfg.max_battery_w, cfg.max_writes_per_day), (3000, 100))

    def test_invalid(self):
        cases = [
            {'CONTROL_MODE': 'maybe'},
            {'CONTROL_MODE': 'on'},
            {'CONTROL_MODE': 'on', 'CONTROL_CHARGE_CURRENT_A': '19'},
            {'CONTROL_MODE': 'on', 'CONTROL_CHARGE_CURRENT_A': '0', 'CONTROL_DISCHARGE_CURRENT_A': '19'},
            {'CONTROL_MODE': 'on', 'CONTROL_CHARGE_CURRENT_A': '26', 'CONTROL_DISCHARGE_CURRENT_A': '19'},
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


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_control -v`
Expected: FAIL / ERROR with `ModuleNotFoundError: No module named 'control'`

- [ ] **Step 3: Write the implementation**

```python
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

# The four settings every mode fully specifies, and the ones only reported.
MODE_SETTINGS = ('ems_mode', 'ems_power_limit', 'battery_charge_current', 'battery_discharge_current')
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

    return ControlConfig(mode, current('CONTROL_CHARGE_CURRENT_A'), current('CONTROL_DISCHARGE_CURRENT_A'),
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

    @staticmethod
    def from_runtime(data: dict) -> 'Sample':
        soc = _float_or_none(data.get('battery_soc'))
        if soc is not None and not 0 <= soc <= 100:
            soc = None
        return Sample(soc, _float_or_none(data.get('vbattery1')), _float_or_none(data.get('battery_charge_limit')),
                      _float_or_none(data.get('battery_discharge_limit')))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_control -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add control.py tests/test_control.py
git commit -m "Add control command parsing, config and runtime sample

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Executor state machine and warnings (`control.py`, part 2)

**Files:**
- Modify: `control.py` (append)
- Test: `tests/test_control.py` (append)

**Interfaces:**
- Consumes: everything from Task 1.
- Produces:
  - `class Executor(config: ControlConfig)` with
    - `submit(cmd: Command) -> None`
    - `reject(error: str) -> None`
    - `set_override(cmd: Command) -> None`, `clear_override() -> None`
    - `set_reserve(soc: int|None) -> None`, property `reserve -> int|None`
    - `tick(sample: Sample, now: datetime) -> dict[str, int|float]` (keys = `MODE_SETTINGS`)
    - `snapshot(now: datetime) -> dict` (keys: `mode`, `effective_mode`, `power_w`, `power_applied_w`, `power_clamped`, `target_soc`, `source`, `expires_at`, `since`, `override`, `reserve_soc`, `reason`, `command_error`, `shadow`)
  - `compute_warnings(readback: dict, eco_slots_enabled: list[bool]|None, reserve: int|None, initial_work_mode: int|None) -> list[str]`

- [ ] **Step 1: Write the failing tests** (append to `tests/test_control.py`, above the `if __name__` block)

```python
CFG = control.ControlConfig('on', 19.0, 18.5, 3400, 300)
S = control.Sample


def at(seconds: float) -> datetime:
    return NOW + timedelta(seconds=seconds)


def charge(power=3000, target=None, minutes=15, source='predbat') -> Command:
    return Command(Mode.CHARGE, power, target, source, NOW + timedelta(minutes=minutes))


class ExecutorModesTest(unittest.TestCase):
    def test_no_command_is_auto_with_user_currents(self):
        ex = control.Executor(CFG)
        self.assertEqual(ex.tick(S(50), NOW), {'ems_mode': 1, 'ems_power_limit': 0,
                                                'battery_charge_current': 19.0, 'battery_discharge_current': 18.5})
        self.assertEqual(ex.snapshot(NOW)['reason'], 'no command')

    def test_mode_table(self):
        cases = [
            (charge(2000), (11, 2000, 19.0, 18.5)),
            (Command(Mode.EXPORT, 1500, None, 'p', at(600)), (3, 1500, 19.0, 18.5)),
            (Command(Mode.FREEZE_CHARGE, expires_at=at(600)), (1, 0, 19.0, 0)),
            (Command(Mode.FREEZE_EXPORT, expires_at=at(600)), (1, 0, 0, 18.5)),
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
        self.assertEqual(ex.tick(S(50), NOW)['battery_discharge_current'], 18.5)

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
        self.assertEqual((desired['ems_mode'], desired['battery_discharge_current']), (1, 0))
        self.assertEqual(ex.snapshot(at(35))['effective_mode'], 'freeze_charge')
        self.assertEqual(ex.snapshot(at(35))['reason'], 'target_soc reached')

    def test_charge_hold_hysteresis(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=50))
        ex.tick(S(60), NOW)  # already met on first sample -> freeze at once
        self.assertEqual(ex.tick(S(48), at(10))['battery_discharge_current'], 0)
        self.assertEqual(ex.tick(S(46), at(20))['ems_mode'], 11)

    def test_hold_charge_target_below_soc_applies_immediately(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=40))
        self.assertEqual(ex.tick(S(55), NOW)['battery_discharge_current'], 0)

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
        self.assertEqual(ex.tick(S(60), at(90))['battery_discharge_current'], 18.5)

    def test_none_soc_keeps_debounce_running(self):
        ex = control.Executor(CFG)
        ex.submit(charge(target=80))
        ex.tick(S(70), at(0))
        ex.tick(S(80), at(1))
        ex.tick(S(None), at(20))
        self.assertEqual(ex.tick(S(80), at(31))['battery_discharge_current'], 0)

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
        self.assertEqual(ex.tick(S(25), at(31))['battery_discharge_current'], 0)
        self.assertEqual(ex.snapshot(at(31))['reason'], 'reserve')
        self.assertEqual(ex.tick(S(26), at(40))['battery_discharge_current'], 0)
        self.assertEqual(ex.tick(S(27), at(50))['battery_discharge_current'], 18.5)

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
        self.assertEqual(ex.tick(S(10), at(60))['battery_discharge_current'], 18.5)


class ExecutorOverrideTest(unittest.TestCase):
    def test_override_wins_then_expires_back_to_command(self):
        ex = control.Executor(CFG)
        ex.submit(Command(Mode.EXPORT, 2000, None, 'predbat', at(3000)))
        ex.set_override(Command(Mode.FREEZE_CHARGE, source='dashboard', expires_at=at(900)))
        self.assertEqual(ex.tick(S(50), at(0))['battery_discharge_current'], 0)
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


class ExecutorSnapshotTest(unittest.TestCase):
    def test_snapshot_fields(self):
        ex = control.Executor(control.ControlConfig('shadow', 19.0, 18.5))
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_control -v`
Expected: ERROR `AttributeError: module 'control' has no attribute 'Executor'`

- [ ] **Step 3: Write the implementation** (append to `control.py`)

```python
TARGET_DEBOUNCE = timedelta(seconds=30)
CHARGE_TARGET_HYSTERESIS = 3
RESERVE_HYSTERESIS = 2
RESERVE_WARN_BELOW = 20


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

    # --- inputs ---------------------------------------------------------
    def submit(self, cmd: Command) -> None:
        self._command_error = None
        if cmd.stop is not None:
            if self._command is not None and self._command.mode in STOP_DOMAINS[cmd.stop]:
                self._clear_command('stopped by ' + cmd.source)
            return
        if cmd.same_request(self._command):
            self._command = cmd  # refresh expiry/id, keep latches
            return
        self._command = cmd
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

    @property
    def reserve(self) -> Optional[int]:
        return self._reserve

    # --- decision -------------------------------------------------------
    def tick(self, sample: Sample, now: datetime) -> dict:
        active, reason = self._active(now)
        mode, reason = self._targets(active, sample, now, reason)
        mode, reason = self._apply_reserve(mode, sample, now, reason)
        power = self._clamp(active.power_w if active is not None and mode in POWERED_MODES else None, mode, sample)
        if mode is not self._effective or self._since is None:
            self._since = now
        self._effective, self._reason, self._power_applied = mode, reason, power
        return self._settings(mode, power)

    def _clear_command(self, reason: str) -> None:
        self._command = None
        self._idle_reason = reason
        self._reset_latches()

    def _reset_latches(self) -> None:
        self._charge_latched = self._export_latched = False
        self._charge_deb, self._export_deb = _Debounce(), _Debounce()
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
                if soc is not None and soc < target - CHARGE_TARGET_HYSTERESIS:
                    self._charge_latched = False
                    self._charge_deb.reset()
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
            return mode, reason
        soc = sample.soc
        if self._reserve_latched:
            if soc is not None and soc >= self._reserve + RESERVE_HYSTERESIS:
                self._reserve_latched = False
                self._reserve_deb.reset()
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
        limit = self._config.max_battery_w
        amps = sample.bms_charge_limit_a if mode is Mode.CHARGE else sample.bms_discharge_limit_a
        if amps and sample.battery_v and amps > 0 and sample.battery_v > 0:
            limit = min(limit, int(amps * sample.battery_v))
        clamped = max(MIN_POWER_W, min(power, limit))
        self._clamped = clamped != power
        return clamped

    def _settings(self, mode: Mode, power: Optional[int]) -> dict:
        c, d = self._config.charge_current_a, self._config.discharge_current_a
        return {
            Mode.AUTO: {'ems_mode': EMS_AUTO, 'ems_power_limit': 0, 'battery_charge_current': c, 'battery_discharge_current': d},
            Mode.CHARGE: {'ems_mode': EMS_CHARGE_BATTERY, 'ems_power_limit': power, 'battery_charge_current': c, 'battery_discharge_current': d},
            Mode.EXPORT: {'ems_mode': EMS_DISCHARGE_PV, 'ems_power_limit': power, 'battery_charge_current': c, 'battery_discharge_current': d},
            Mode.FREEZE_CHARGE: {'ems_mode': EMS_AUTO, 'ems_power_limit': 0, 'battery_charge_current': c, 'battery_discharge_current': 0},
            Mode.FREEZE_EXPORT: {'ems_mode': EMS_AUTO, 'ems_power_limit': 0, 'battery_charge_current': 0, 'battery_discharge_current': d},
        }[mode]

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
        }


def compute_warnings(readback: dict, eco_slots_enabled: Optional[list], reserve: Optional[int],
                     initial_work_mode: Optional[int]) -> list:
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
    return warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_control -v`
Expected: all PASS. If `test_single_garbage_soc_sample_does_not_latch` fails, check that a `False` condition in `_Debounce.update` resets `_since` - the 61 % sample at t=10 must break the debounce started by the 100 % sample.

- [ ] **Step 5: Commit**

```bash
git add control.py tests/test_control.py
git commit -m "Add control executor: precedence, scoped stops, SoC targets, reserve, clamp

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Control writer (`control_io.py`)

**Files:**
- Create: `control_io.py`
- Create: `tests/control_fakes.py` (fake inverter + clock, shared with Task 5)
- Test: `tests/test_control_io.py`

**Interfaces:**
- Consumes: `control.MODE_SETTINGS`, `control.REPORTED_SETTINGS`, `control.EMS_AUTO`.
- Produces:
  - `write_order(desired: dict, readback: dict) -> list[str]`
  - `class ControlWriter(inverter, *, shadow: bool, max_writes_per_day: int = 300, now_fn=time.monotonic, today_fn=datetime.date.today, read_interval_s=10.0, verify_delay_s=3.0, max_attempts=3, error_backoff_s=60.0)` with
    - `async step(desired: dict | None) -> None`
    - `applied(desired: dict | None) -> bool`
    - attributes `readback: dict`, `last_error: str | None`, `writes_today: int`

- [ ] **Step 1: Write the fakes and the failing tests**

`tests/control_fakes.py`:

```python
"""
tests/control_fakes.py
Fake goodwe inverter and clock shared by the control tests. Import as
`from tests.control_fakes import ...` (works both for
`python -m unittest discover -s tests` and `python -m unittest tests.x`,
since the repo root is on sys.path in both).
"""
import asyncio

USER = {'battery_charge_current': 19.0, 'battery_discharge_current': 18.5}
AUTO = {'ems_mode': 1, 'ems_power_limit': 0, **USER}
FREEZE_EXPORT = {'ems_mode': 1, 'ems_power_limit': 0, 'battery_charge_current': 0, 'battery_discharge_current': 18.5}
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
```

`tests/test_control_io.py`:

```python
"""
tests/test_control_io.py
ControlWriter against a fake inverter with an injectable clock - covers
diff-only writes, ordering, delayed read-back, retries, and failed reads.
"""
import unittest
from datetime import date

import control_io
from tests.control_fakes import AUTO, CHARGE_2K, FREEZE_EXPORT, Clock, FakeInverter, base_values, run_steps


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_control_io -v`
Expected: ERROR `ModuleNotFoundError: No module named 'control_io'`

- [ ] **Step 3: Write the implementation**

```python
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
_TOLERANCE = {name: 0.05 for name in _CURRENTS}


def _matches(name: str, want, have) -> bool:
    if have is None or want is None:
        return False
    return abs(float(want) - float(have)) <= _TOLERANCE.get(name, 0)


def write_order(desired: dict, readback: dict) -> list:
    """Differing settings in safe order: currents going to 0 first (enter a
    freeze), then EMS (setpoint before mode when entering a forced mode, mode
    before setpoint when going back to AUTO), then currents being restored."""
    differing = [n for n in MODE_SETTINGS if not _matches(n, desired[n], readback.get(n))]
    to_zero = [n for n in _CURRENTS if n in differing and desired[n] == 0]
    restore = [n for n in _CURRENTS if n in differing and desired[n] != 0]
    ems = [n for n in ('ems_mode', 'ems_power_limit') if n in differing]
    first = 'ems_mode' if desired['ems_mode'] == EMS_AUTO else 'ems_power_limit'
    ems.sort(key=lambda n: 0 if n == first else 1)
    return to_zero + ems + restore


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
        self.readback: dict = {}
        self.last_error: Optional[str] = None
        self.writes_today = 0
        self._today = today_fn()
        self._next_read = 0.0
        self._verify_at: Optional[float] = None
        self._backoff_until: Optional[float] = None
        self._attempts: dict = {}
        self._warned_writes = False

    def applied(self, desired: Optional[dict]) -> bool:
        return desired is not None and all(_matches(n, desired[n], self.readback.get(n)) for n in MODE_SETTINGS)

    async def step(self, desired: Optional[dict]) -> None:
        now = self._now()
        self._roll_day()
        if now >= self._next_read:
            await self._read_all(now)
        if desired is None:
            return
        if self.applied(desired):
            self._attempts.clear()
            self._backoff_until = None
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
                self._backoff_until = now + self._error_backoff
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_control_io -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add control_io.py tests/control_fakes.py tests/test_control_io.py
git commit -m "Add control writer with diff-only writes and read-back verification

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: MQTT control subscription (`mqtt_bridge.py`)

**Files:**
- Modify: `mqtt_bridge.py` (constructor, `_do_connect`, `publish_offline_and_disconnect`, new methods)
- Test: `tests/test_mqtt_bridge.py` (extend `FakeMqttClient`, add `MqttBridgeControlTest`)

**Interfaces:**
- Produces:
  - `MqttBridge.set_control_handler(handler: Callable[[str, bytes], None]) -> None` - `handler(topic_suffix, payload)` for `control/set` and `control/reserve/set`, called on the asyncio loop.
  - `MqttBridge.publish_control_state(state: dict) -> Awaitable[None]` - retained `control/state`.
  - `mqtt_bridge.CONTROL_TOPICS = ('control/set', 'control/reserve/set')`

- [ ] **Step 1: Write the failing tests**

In `tests/test_mqtt_bridge.py`, extend `FakeMqttClient` (keep existing members):

```python
class FakeMessage:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


class FakeMqttClient:
    def __init__(self):
        self.published = []
        self.connected = False
        self.disconnected = False
        self.aenter_count = 0
        self.subscribed = []
        self._inbox = None

    @property
    def inbox(self):
        # Created lazily inside the running loop - asyncio.Queue() at
        # construction time fails on Python < 3.10 (no current event loop).
        if self._inbox is None:
            self._inbox = asyncio.Queue()
        return self._inbox

    async def __aenter__(self):
        self.connected = True
        self.aenter_count += 1
        return self

    async def __aexit__(self, *exc_info):
        self.disconnected = True

    async def publish(self, topic, payload, retain=False):
        self.published.append((topic, payload, retain))

    async def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    @property
    def messages(self):
        return self._iterate()

    async def _iterate(self):
        while True:
            item = await self.inbox.get()
            if isinstance(item, Exception):
                raise item
            yield item
```

Append a test class:

```python
class MqttBridgeControlTest(unittest.TestCase):
    def test_subscribes_and_dispatches_control_messages(self):
        async def go():
            client = FakeMqttClient()
            bridge = mqtt_bridge.MqttBridge(host='broker', client_factory=lambda **kw: client)
            received = []
            bridge.set_control_handler(lambda suffix, payload: received.append((suffix, payload)))
            await bridge.connect()
            self.assertEqual(client.subscribed, [('goodwe/control/set', 1), ('goodwe/control/reserve/set', 1)])
            await client.inbox.put(FakeMessage('goodwe/control/set', b'{"mode":"auto"}'))
            await client.inbox.put(FakeMessage('goodwe/control/reserve/set', b'25'))
            await asyncio.sleep(0.01)
            self.assertEqual(received, [('control/set', b'{"mode":"auto"}'), ('control/reserve/set', b'25')])
            await bridge.publish_offline_and_disconnect()
        asyncio.run(go())

    def test_no_subscription_without_handler(self):
        async def go():
            client = FakeMqttClient()
            bridge = mqtt_bridge.MqttBridge(host='broker', client_factory=lambda **kw: client)
            await bridge.connect()
            self.assertEqual(client.subscribed, [])
        asyncio.run(go())

    def test_handler_exception_does_not_stop_reader(self):
        async def go():
            client = FakeMqttClient()
            bridge = mqtt_bridge.MqttBridge(host='broker', client_factory=lambda **kw: client)
            received = []

            def handler(suffix, payload):
                if payload == b'boom':
                    raise ValueError('boom')
                received.append(payload)
            bridge.set_control_handler(handler)
            await bridge.connect()
            await client.inbox.put(FakeMessage('goodwe/control/set', b'boom'))
            await client.inbox.put(FakeMessage('goodwe/control/set', b'ok'))
            await asyncio.sleep(0.01)
            self.assertEqual(received, [b'ok'])
            await bridge.publish_offline_and_disconnect()
        asyncio.run(go())

    def test_resubscribes_after_reconnect(self):
        async def go():
            clients = [FakeMqttClient(), FakeMqttClient()]
            factory = iter(clients)
            now = [0.0]
            bridge = mqtt_bridge.MqttBridge(host='broker', client_factory=lambda **kw: next(factory),
                                            reconnect_interval_seconds=0, now_fn=lambda: now[0])
            received = []
            bridge.set_control_handler(lambda s, p: received.append((s, p)))
            await bridge.connect()
            await clients[0].inbox.put(ConnectionError('broker gone'))
            await asyncio.sleep(0.01)
            now[0] = 100.0
            await bridge.publish_telemetry({'x': '1'})  # triggers the reconnect path
            self.assertEqual(clients[1].subscribed[0], ('goodwe/control/set', 1))
            await clients[1].inbox.put(FakeMessage('goodwe/control/reserve/set', b'30'))
            await asyncio.sleep(0.01)
            self.assertEqual(received, [('control/reserve/set', b'30')])
            await bridge.publish_offline_and_disconnect()
        asyncio.run(go())

    def test_publish_control_state_is_retained(self):
        async def go():
            client = FakeMqttClient()
            bridge = mqtt_bridge.MqttBridge(host='broker', client_factory=lambda **kw: client)
            await bridge.connect()
            await bridge.publish_control_state({'mode': 'auto'})
            self.assertIn(('goodwe/control/state', '{"mode": "auto"}', True), client.published)
        asyncio.run(go())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_mqtt_bridge -v`
Expected: new tests ERROR with `AttributeError: 'MqttBridge' object has no attribute 'set_control_handler'`; existing tests still PASS.

- [ ] **Step 3: Implement**

In `mqtt_bridge.py`:

1. Add `import asyncio` to the imports and, below `logger = ...`:

```python
CONTROL_TOPICS = ('control/set', 'control/reserve/set')
```

2. At the end of `__init__` add:

```python
        self._control_handler: Optional[Callable[[str, bytes], None]] = None
        self._reader_task: Optional[asyncio.Task] = None
```

3. Add methods (next to the other `publish_*` methods):

```python
    def set_control_handler(self, handler: Callable[[str, bytes], None]) -> None:
        """Subscribe to the control topics on every (re)connect and call
        handler(topic_suffix, payload) for each message, on the asyncio loop.
        Must be set before connect() - main.py does it at import time."""
        self._control_handler = handler

    async def publish_control_state(self, state: dict) -> None:
        await self._publish('control/state', json.dumps(state), retain=True)

    async def _subscribe_control(self, client) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
        for suffix in CONTROL_TOPICS:
            await client.subscribe(self._topic(suffix), qos=1)
        self._reader_task = asyncio.create_task(self._read_control_messages(client))

    async def _read_control_messages(self, client) -> None:
        prefix = self._prefix + '/'
        try:
            async for message in client.messages:
                topic = str(getattr(message.topic, 'value', message.topic))
                suffix = topic[len(prefix):] if topic.startswith(prefix) else topic
                try:
                    self._control_handler(suffix, message.payload)
                except Exception as e:
                    logger.warning(f'Control message on {topic} failed: {e}')
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Same recovery path as a failed publish: drop the client so the
            # next ~1 Hz telemetry publish reconnects (and re-subscribes).
            logger.warning(f'MQTT control subscription ended: {e}')
            if self._client is client:
                self._client = None
```

4. In `_do_connect`, after `await self._publish('bridge/status', 'online', retain=True)` add:

```python
        if self._control_handler is not None:
            try:
                await self._subscribe_control(client)
            except Exception as e:
                logger.warning(f'Could not subscribe to control topics: {e}')
                self._client = None
```

5. In `publish_offline_and_disconnect`, before `await self._publish('bridge/status', 'offline', retain=True)` add:

```python
        if self._reader_task is not None:
            self._reader_task.cancel()
            self._reader_task = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_mqtt_bridge -v`
Expected: all PASS (old and new).

- [ ] **Step 5: Commit**

```bash
git add mqtt_bridge.py tests/test_mqtt_bridge.py
git commit -m "MQTT bridge: control topic subscription with re-subscribe on reconnect

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Control runtime glue (`control_runtime.py`)

**Files:**
- Create: `control_runtime.py`
- Test: `tests/test_control_runtime.py`

**Interfaces:**
- Consumes: `control.*` (Tasks 1-2), `control_io.ControlWriter` (Task 3), `tests/control_fakes.py` (Task 3), an MQTT object with `async publish_control_state(state: dict)` (Task 4).
- Produces: `class ControlRuntime(config: ControlConfig, mqtt, *, now_fn=lambda: datetime.now().astimezone(), mono_fn=time.monotonic, eco_check_interval_s=300.0, state_interval_s=10.0)` with
  - `attach(inverter) -> None` (new writer per inverter connection; executor persists)
  - `on_mqtt_message(topic_suffix: str, payload: bytes) -> None`
  - `async step(runtime_data: dict) -> dict` (returns the state document; never raises)
  - `set_override(cmd: Command) -> None`, `clear_override() -> None`
  - property `last_state -> dict | None`

- [ ] **Step 1: Write the failing tests**

```python
"""
tests/test_control_runtime.py
ControlRuntime with a fake inverter and fake MQTT - message handling,
state publishing cadence, eco-slot warnings, and the Predbat paired
stop/start pattern.
"""
import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import control
import control_runtime
from tests.control_fakes import Clock, FakeInverter, base_values

T0 = datetime(2026, 9, 26, 14, 0, tzinfo=timezone(timedelta(hours=2)))
CFG = control.ControlConfig('on', 19.0, 18.5)
RUNTIME = {'battery_soc': 55, 'vbattery1': 195, 'battery_charge_limit': 18, 'battery_discharge_limit': 18}


class FakeMqtt:
    def __init__(self):
        self.states = []

    async def publish_control_state(self, state):
        self.states.append(state)


def make(eco_on=(False, False, False, False)):
    clock = Clock()
    inv = FakeInverter(clock, base_values())
    for i, on in enumerate(eco_on, start=1):
        inv.values[f'eco_mode_{i}'] = SimpleNamespace(on_off=-1 if on else 0)
    mqtt = FakeMqtt()
    rt = control_runtime.ControlRuntime(CFG, mqtt, now_fn=lambda: T0 + timedelta(seconds=clock.t), mono_fn=clock)
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
        self.assertIn(('battery_discharge_current', 0), inv.writes)
        rt.clear_override()
        run(rt, clock, 10)
        self.assertIsNone(mqtt.states[-1]['override'])

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


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_control_runtime -v`
Expected: ERROR `ModuleNotFoundError: No module named 'control_runtime'`

- [ ] **Step 3: Write the implementation**

```python
"""
control_runtime.py
Glue between control.Executor, control_io.ControlWriter, MQTT and main.py's
1 Hz poll loop. Every method runs on the asyncio loop thread; step() never
raises, so a control problem can't stop inverter polling.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Callable, Optional

from control import Command, CommandError, ControlConfig, Executor, Sample, compute_warnings, parse_command
from control_io import ControlWriter

logger = logging.getLogger(__name__)


class ControlRuntime:
    def __init__(self, config: ControlConfig, mqtt, *,
                 now_fn: Callable[[], datetime] = lambda: datetime.now().astimezone(),
                 mono_fn: Callable[[], float] = time.monotonic,
                 eco_check_interval_s: float = 300.0, state_interval_s: float = 10.0):
        self._config = config
        self._mqtt = mqtt
        self._now = now_fn
        self._mono = mono_fn
        self._eco_interval = eco_check_interval_s
        self._state_interval = state_interval_s
        self.executor = Executor(config)
        self._writer: Optional[ControlWriter] = None
        self._inverter = None
        self._eco_enabled: Optional[list] = None
        self._next_eco = 0.0
        self._initial_work_mode: Optional[int] = None
        self._last_state: Optional[dict] = None
        self._next_state = 0.0

    @property
    def last_state(self) -> Optional[dict]:
        return self._last_state

    def attach(self, inverter) -> None:
        """Called after every inverter (re)connect - the goodwe object is new
        each time, the executor (commands, override, reserve) is kept."""
        self._inverter = inverter
        self._writer = ControlWriter(inverter, shadow=self._config.mode == 'shadow',
                                     max_writes_per_day=self._config.max_writes_per_day, now_fn=self._mono)
        self._next_eco = 0.0

    def on_mqtt_message(self, topic_suffix: str, payload: bytes) -> None:
        if topic_suffix == 'control/set':
            try:
                self.executor.submit(parse_command(payload, self._now()))
            except CommandError as e:
                logger.warning(f'Rejected control command: {e}')
                self.executor.reject(str(e))
        elif topic_suffix == 'control/reserve/set':
            text = payload.decode(errors='replace').strip() if isinstance(payload, bytes) else str(payload).strip()
            if text == '':
                self.executor.set_reserve(None)
                return
            try:
                value = int(float(text))
            except ValueError:
                logger.warning(f'Ignored reserve {text!r}: not a number')
                return
            if not 0 <= value <= 100:
                logger.warning(f'Ignored reserve {value}: outside 0-100')
                return
            self.executor.set_reserve(value)

    def set_override(self, cmd: Command) -> None:
        self.executor.set_override(cmd)

    def clear_override(self) -> None:
        self.executor.clear_override()

    async def step(self, runtime_data: dict) -> dict:
        now = self._now()
        desired = None
        try:
            desired = self.executor.tick(Sample.from_runtime(runtime_data), now)
            if self._writer is not None:
                await self._writer.step(desired)
                if self._initial_work_mode is None:
                    self._initial_work_mode = self._writer.readback.get('work_mode')
                await self._maybe_read_eco()
        except Exception as e:
            logger.warning(f'Control step failed: {e}')
        state = self._build_state(now, desired)
        await self._maybe_publish(state)
        return state

    async def _maybe_read_eco(self) -> None:
        mono = self._mono()
        if mono < self._next_eco:
            return
        self._next_eco = mono + self._eco_interval
        enabled = []
        for i in range(1, 5):
            try:
                slot = await self._inverter.read_setting(f'eco_mode_{i}')
                enabled.append(slot is not None and slot.on_off < 0)
            except Exception as e:
                logger.debug(f'Could not read eco_mode_{i}: {e}')
                return  # keep the previous list rather than a partial one
        self._eco_enabled = enabled

    def _build_state(self, now: datetime, desired: Optional[dict]) -> dict:
        snap = self.executor.snapshot(now)
        writer = self._writer
        readback = dict(writer.readback) if writer is not None else {}
        state = {k: v for k, v in snap.items() if k != 'command_error'}
        state.update({
            'applied': writer.applied(desired) if writer is not None else False,
            'registers': readback,
            'last_error': (writer.last_error if writer is not None else None) or snap['command_error'],
            'warnings': compute_warnings(readback, self._eco_enabled, self.executor.reserve, self._initial_work_mode),
            'writes_today': writer.writes_today if writer is not None else 0,
        })
        return state

    async def _maybe_publish(self, state: dict) -> None:
        mono = self._mono()
        if state == self._last_state and mono < self._next_state:
            return
        self._last_state = state
        self._next_state = mono + self._state_interval
        try:
            await asyncio.wait_for(self._mqtt.publish_control_state(state), timeout=5)
        except Exception as e:
            logger.warning(f'Could not publish control state: {e}')
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_control_runtime -v`
Expected: all PASS. If `test_state_published_on_change_and_every_10s` counts more than 3, check that `since` in the snapshot doesn't change on unchanged ticks (it's only updated when the effective mode changes).

- [ ] **Step 5: Commit**

```bash
git add control_runtime.py tests/test_control_runtime.py
git commit -m "Add control runtime glue: MQTT handling, state publishing, eco warnings

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Wire into `main.py` (config, poll loop, SSE, routes)

**Files:**
- Modify: `main.py` (config block near line 57; after `mqtt = ...` near line 433; `_get_inverter_data` after the inverter connect and inside the `while True` loop; new routes after `update_config`; `get_config`)
- Test: `tests/test_main_control_routes.py`

**Interfaces:**
- Consumes: `control.config_from_env`, `control.make_override`, `control.CommandError`, `control_runtime.ControlRuntime`, `mqtt.set_control_handler`.
- Produces: module globals `CONTROL_CONFIG`, `control_runtime_instance`; routes `GET /control/state`, `POST /control/override`, `POST /control/override/clear`; SSE payload key `_control`; `get_config` passes `control_state` to the template.

- [ ] **Step 1: Write the failing tests**

```python
"""
tests/test_main_control_routes.py
Flask /control/* routes - the runtime itself is covered by
tests/test_control_runtime.py; here only the HTTP layer and its guards.
"""
import concurrent.futures
import os
import unittest
from unittest import mock

os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import control
import main


def _run_now(coro) -> concurrent.futures.Future:
    """Stand-in for asyncio_thread.run_coroutine_threadsafe: run the coroutine
    to completion synchronously."""
    import asyncio
    future: concurrent.futures.Future = concurrent.futures.Future()
    future.set_result(asyncio.run(coro))
    return future


class ControlRoutesDisabledTest(unittest.TestCase):
    def test_routes_404_when_control_off(self):
        with mock.patch.object(main, 'control_runtime_instance', None):
            client = main.app.test_client()
            self.assertEqual(client.get('/control/state').status_code, 404)
            self.assertEqual(client.post('/control/override', data={}).status_code, 404)
            self.assertEqual(client.post('/control/override/clear').status_code, 404)


class ControlRoutesEnabledTest(unittest.TestCase):
    def setUp(self):
        self.runtime = mock.Mock()
        self.runtime.last_state = {'mode': 'auto'}
        patches = [
            mock.patch.object(main, 'control_runtime_instance', self.runtime),
            mock.patch.object(main.asyncio_thread, 'run_coroutine_threadsafe', side_effect=_run_now),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.client = main.app.test_client()

    def test_state(self):
        response = self.client.get('/control/state')
        self.assertEqual(response.get_json(), {'mode': 'auto'})

    def test_override_valid(self):
        response = self.client.post('/control/override', data={'mode': 'export', 'power_w': '1500',
                                                               'target_soc': '40', 'duration_min': '30'})
        self.assertEqual(response.status_code, 302)
        cmd = self.runtime.set_override.call_args.args[0]
        self.assertEqual((cmd.mode, cmd.power_w, cmd.target_soc, cmd.source),
                         (control.Mode.EXPORT, 1500, 40, 'dashboard'))

    def test_override_invalid_is_400(self):
        response = self.client.post('/control/override', data={'mode': 'charge', 'power_w': '',
                                                               'target_soc': '', 'duration_min': '30'})
        self.assertEqual(response.status_code, 400)
        self.runtime.set_override.assert_not_called()

    def test_clear(self):
        response = self.client.post('/control/override/clear')
        self.assertEqual(response.status_code, 302)
        self.runtime.clear_override.assert_called_once()


class ControlConfigTest(unittest.TestCase):
    def test_default_env_leaves_control_off(self):
        self.assertIsNone(control.config_from_env({}))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_main_control_routes -v`
Expected: FAIL/ERROR - `main` has no attribute `control_runtime_instance`.

- [ ] **Step 3: Implement**

1. Imports at the top of `main.py` (with the other local imports):

```python
import control
import control_runtime
```

2. After the `RCE_EXPORT_NEGATIVE_PRICES = ...` line:

```python
# Invalid CONTROL_* values fail startup on purpose - better than silently
# running without the control the user configured.
CONTROL_CONFIG = control.config_from_env(os.environ)
```

3. Right after `mqtt = mqtt_bridge.MqttBridge(...)`:

```python
control_runtime_instance: Optional[control_runtime.ControlRuntime] = None
if CONTROL_CONFIG is not None:
    control_runtime_instance = control_runtime.ControlRuntime(CONTROL_CONFIG, mqtt)
    mqtt.set_control_handler(control_runtime_instance.on_mqtt_message)
    logger.info(f'Battery control enabled in {CONTROL_CONFIG.mode} mode')
```

4. In `AsyncioThread._get_inverter_data`, right after `logger.info(f'Connected to the inverter')`:

```python
        if control_runtime_instance is not None:
            control_runtime_instance.attach(self._inverter)
```

5. In the same method's `while True:` loop, change the `announce_payload` construction to include the previous control state, and run the control step after the telemetry publish block (before the day-rollover check):

```python
                announce_payload = sensors_data_with_calculated | {
                    '_read_duration_seconds': round(read_done - read_start, 3),
                    '_server_received_at': server_received_at,
                }
                if control_runtime_instance is not None and control_runtime_instance.last_state is not None:
                    # Previous iteration's state - running the control step
                    # before announcing would delay every SSE update by its
                    # register reads.
                    announce_payload['_control'] = control_runtime_instance.last_state
```

and, after the `if mqtt.enabled: ... publish_telemetry ...` block:

```python
                if control_runtime_instance is not None:
                    await control_runtime_instance.step(inverter_runtime)
```

6. New routes after `update_config`:

```python
async def _control_call(fn, *args):
    return fn(*args)


def _require_control():
    if control_runtime_instance is None:
        flask.abort(404)


@app.get('/control/state')
def get_control_state():
    _require_control()
    return flask.jsonify(control_runtime_instance.last_state or {})


@app.post('/control/override')
def set_control_override():
    _require_control()
    try:
        cmd = control.make_override(request.form.get('mode', ''), request.form.get('power_w'),
                                    request.form.get('target_soc'), request.form.get('duration_min'),
                                    datetime.now().astimezone())
    except (control.CommandError, ValueError) as e:
        return flask.Response(f'Invalid override: {e}', status=400)
    logger.info(f'Dashboard override: {cmd}')
    asyncio_thread.run_coroutine_threadsafe(_control_call(control_runtime_instance.set_override, cmd)).result(timeout=10)
    return flask.redirect('/')


@app.post('/control/override/clear')
def clear_control_override():
    _require_control()
    logger.info('Dashboard override cleared')
    asyncio_thread.run_coroutine_threadsafe(_control_call(control_runtime_instance.clear_override)).result(timeout=10)
    return flask.redirect('/')
```

7. In `get_config`, pass the control state:

```python
    return flask.render_template('config.html', settings=settings,
                                 control_state=control_runtime_instance.last_state if control_runtime_instance else None)
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests.test_main_control_routes -v` then the full suite `python -m unittest discover -s tests`
Expected: all PASS (existing tests unaffected with `CONTROL_MODE` unset).

- [ ] **Step 5: Manual dry-run check**

Run: `CONTROL_MODE=shadow CONTROL_CHARGE_CURRENT_A=19 CONTROL_DISCHARGE_CURRENT_A=19 python main.py --dry-run` and open the LAN URL (e.g. http://192.168.1.x:5000/control/state).
Expected: `{}` (no inverter loop in dry-run), log line `Battery control enabled in shadow mode`. Then `CONTROL_MODE=bogus python main.py --dry-run` must exit with `ValueError: CONTROL_MODE must be off, shadow or on`.

- [ ] **Step 6: Commit**

```bash
git add main.py tests/test_main_control_routes.py
git commit -m "Wire battery control into the poll loop, SSE and /control routes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Dashboard panel and config page

**Files:**
- Modify: `templates/index.html` (new row before the `<hr>` debug row; JS in the `onmessage` handler)
- Modify: `templates/config.html` (read-only section at the end of the container)

**Interfaces:**
- Consumes: SSE `_control` (Task 6), `POST /control/override`, `POST /control/override/clear`, template var `control_state`.

- [ ] **Step 1: Add the panel markup** to `templates/index.html`, directly before `<div class="row">\n      <hr>`:

```html
    <div class="row" id="control-panel" style="display: none">
      <div class="col">
        <h4>Battery control <span id="control-shadow" class="badge bg-secondary" style="display: none">shadow</span></h4>
        <div>Mode: <b id="control-effective"></b> <span id="control-reason" class="text-muted"></span></div>
        <div>Command: <span id="control-command"></span></div>
        <div>Applied: <span id="control-applied" class="badge"></span> <span id="control-error" class="text-danger"></span></div>
        <div>Reserve: <span id="control-reserve"></span></div>
        <div id="control-warnings" class="text-warning"></div>
        <div class="text-muted small">Settings: <span id="control-registers"></span> · writes today <span id="control-writes"></span></div>
        <div id="control-override" class="mt-1"></div>
        <form class="row row-cols-lg-auto g-2 align-items-center mt-1" method="post" action="/control/override">
          <div class="col-12">
            <select class="form-select form-select-sm" name="mode">
              <option value="auto">auto</option>
              <option value="charge">charge</option>
              <option value="export">export</option>
              <option value="freeze_charge">freeze charge</option>
              <option value="freeze_export">freeze export</option>
            </select>
          </div>
          <div class="col-12"><input class="form-control form-control-sm" name="power_w" type="number" min="100" placeholder="power W"></div>
          <div class="col-12"><input class="form-control form-control-sm" name="target_soc" type="number" min="0" max="100" placeholder="target %"></div>
          <div class="col-12"><input class="form-control form-control-sm" name="duration_min" type="number" min="15" max="720" value="60" required> min</div>
          <div class="col-12"><button class="btn btn-sm btn-warning" type="submit">Override</button></div>
        </form>
        <form method="post" action="/control/override/clear" class="mt-1">
          <button class="btn btn-sm btn-outline-secondary" type="submit">Clear override</button>
        </form>
      </div>
    </div>
```

- [ ] **Step 2: Add the render function** in the first `<script>` block (after the variable declarations at its top):

```javascript
    function renderControl(c) {
      const panel = document.getElementById('control-panel');
      if (!c) { panel.style.display = 'none'; return; }
      panel.style.display = '';
      const text = (id, v) => { document.getElementById(id).textContent = v; };
      text('control-effective', c.effective_mode);
      text('control-reason', c.reason ? `(${c.reason})` : '');
      document.getElementById('control-shadow').style.display = c.shadow ? '' : 'none';
      const cmd = c.mode === 'auto' && !c.source ? 'none'
        : `${c.mode}${c.power_w ? ' ' + c.power_w + ' W' : ''}${c.power_clamped ? ' (clamped to ' + c.power_applied_w + ' W)' : ''}` +
          `${c.target_soc != null ? ' → ' + c.target_soc + '%' : ''} from ${c.source}` +
          `${c.expires_at ? ' until ' + new Date(c.expires_at).toLocaleTimeString() : ''}`;
      text('control-command', cmd);
      const applied = document.getElementById('control-applied');
      applied.textContent = c.applied ? 'yes' : 'no';
      applied.className = 'badge ' + (c.applied ? 'bg-success' : 'bg-danger');
      text('control-error', c.last_error || '');
      text('control-reserve', c.reserve_soc != null ? c.reserve_soc + '%' : 'not set');
      text('control-warnings', (c.warnings || []).map(w => '⚠ ' + w).join(' · '));
      text('control-registers', Object.entries(c.registers || {}).map(([k, v]) => `${k}=${v}`).join(', '));
      text('control-writes', c.writes_today);
      text('control-override', c.override
        ? `Override: ${c.override.mode}${c.override.power_w ? ' ' + c.override.power_w + ' W' : ''} until ${new Date(c.override.until).toLocaleTimeString()}`
        : '');
    }
```

- [ ] **Step 3: Call it from `eventSource.onmessage`** - after `let data = JSON.parse(e.data);` add `renderControl(data['_control']);`, and change the debug dump line so objects don't print as `[object Object]`:

```javascript
      eventTarget.innerHTML = Object.entries(data)
        .map(([k, v]) => `${k}: ${typeof v === 'object' && v !== null ? JSON.stringify(v) : v}<br>`).join('');
```

- [ ] **Step 4: Config page section** - in `templates/config.html`, before the closing `</div>` of the container:

```html
  {% if control_state %}
  <h2 class="mt-4">Battery control (read-only)</h2>
  <table class="table table-sm w-auto">
    <tr><th>Effective mode</th><td>{{ control_state['effective_mode'] }}</td></tr>
    <tr><th>Applied</th><td>{{ 'yes' if control_state['applied'] else 'no' }}</td></tr>
    {% for name, value in control_state['registers'].items() %}
    <tr><th><code>{{ name }}</code></th><td>{{ value }}</td></tr>
    {% endfor %}
    {% for w in control_state['warnings'] %}
    <tr><th>Warning</th><td class="text-warning">{{ w }}</td></tr>
    {% endfor %}
  </table>
  {% endif %}
```

- [ ] **Step 5: Verify manually**

Run the app in dry-run with the SSE fed by hand is not possible (no loop), so verify on the Pi in `shadow` mode during Task 9 step 2; for now check the templates render: `python -m unittest discover -s tests` (Flask template syntax errors surface in `test_main_*` route tests that render `config.html`) and open `/` in dry-run to confirm the page loads with the panel hidden.

- [ ] **Step 6: Commit**

```bash
git add templates/index.html templates/config.html
git commit -m "Dashboard: battery control panel with timed override

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Documentation (topics, HA entities, Predbat config, env)

**Files:**
- Modify: `MQTT_TOPICS.md`, `README.MD`, `.env.example`, `CHANGELOG.md`

- [ ] **Step 1: `.env.example`** - append:

```
# Battery control from Predbat/other optimizers via MQTT (see MQTT_TOPICS.md).
# off (default) | shadow (publish decisions, never write) | on
CONTROL_MODE=off
# Your normal inverter battery current limits (A), restored after every freeze.
# Required when CONTROL_MODE is not off.
CONTROL_CHARGE_CURRENT_A=
CONTROL_DISCHARGE_CURRENT_A=
CONTROL_MAX_BATTERY_W=3400
CONTROL_MAX_WRITES_PER_DAY=300
```

- [ ] **Step 2: `README.MD`** - add a "Battery control" section after the MQTT bridge section: what `CONTROL_MODE` does, the five modes with a one-line behaviour each (copy the spec's measured-behaviour table), the fail-safe rules (expiry → auto, restart → auto, no inverter watchdog so a dead Pi leaves the last mode), "disable eco slots before `on`", "use the dashboard override to `auto` before changing settings in SolarGo", and a link to `MQTT_TOPICS.md`.

- [ ] **Step 3: `MQTT_TOPICS.md`** - add sections:

````markdown
## `control/set` (subscribed, QoS 1, not retained) - only when `CONTROL_MODE` is `shadow` or `on`

```json
{"mode": "charge", "power_w": 3000, "target_soc": 80, "source": "predbat", "ttl_s": 900}
```

| Field | Meaning |
|---|---|
| `mode` | `auto`, `charge`, `export`, `freeze_charge`, `freeze_export` |
| `power_w` | required for `charge`/`export`; clamped to 100 W … min(`CONTROL_MAX_BATTERY_W`, live BMS limit) |
| `target_soc` | optional for `charge`/`export`: charge → `freeze_charge` once reached; export → `auto` once reached |
| `ttl_s` / `expires_at` | one required unless `mode` is `auto`; max 3600 s / 60 min ahead. Re-send before it runs out (Predbat: `repeat: true`) |
| `stop` | with `mode: auto` only: `charge` or `export` - clears the current command only if it is in that domain |
| `source`, `id` | free text, echoed in the state |

Invalid commands are rejected (the current one stays) and reported in `control/state.last_error`.

## `control/reserve/set` (subscribed, retained)

Integer SoC % software reserve; empty payload clears it. In `auto`, SoC at or below it (for 30 s) switches to `freeze_charge` until SoC is 2 points above.

## `control/state` (retained, on change and every 10 s)

```json
{"mode": "charge", "effective_mode": "freeze_charge", "power_w": 3000, "power_applied_w": 3000,
 "power_clamped": false, "target_soc": 80, "source": "predbat", "expires_at": "2026-09-26T15:10:00+02:00",
 "since": "2026-09-26T14:58:12+02:00", "override": null, "reserve_soc": 25, "reason": "target_soc reached",
 "shadow": false, "applied": true, "last_error": null, "warnings": [], "writes_today": 14,
 "registers": {"ems_mode": 1, "ems_power_limit": 0, "battery_charge_current": 19.0,
               "battery_discharge_current": 0, "soc_upper_limit": 100, "work_mode": 3}}
```

`applied` is true only when the last read-back of all four mode settings matches what `effective_mode` needs.

## Home Assistant + Predbat example

```yaml
mqtt:
  sensor:
    - name: "Goodwe Control Mode"
      unique_id: goodwe_control_mode
      state_topic: "goodwe/control/state"
      value_template: "{{ value_json.effective_mode }}"
      json_attributes_topic: "goodwe/control/state"
  binary_sensor:
    - name: "Goodwe Control Applied"
      unique_id: goodwe_control_applied
      state_topic: "goodwe/control/state"
      value_template: "{{ 'ON' if value_json.applied else 'OFF' }}"
  number:
    - name: "Goodwe Reserve"
      unique_id: goodwe_reserve
      command_topic: "goodwe/control/reserve/set"
      state_topic: "goodwe/control/state"
      value_template: "{{ value_json.reserve_soc | int(0) }}"
      min: 0
      max: 100
      unit_of_measurement: "%"
      retain: true

script:
  goodwe_control:
    alias: "GoodWe control command"
    fields:
      mode: {}
      power: {}
      target_soc: {}
      stop: {}
    sequence:
      - action: mqtt.publish
        data:
          topic: "goodwe/control/set"
          qos: 1
          payload: >-
            {{ {'mode': mode, 'source': 'predbat', 'ttl_s': 900}
               | combine({'power_w': power | int} if power is defined and power not in ('', None) else {})
               | combine({'target_soc': target_soc | int} if target_soc is defined and target_soc not in ('', None) else {})
               | combine({'stop': stop} if stop is defined and stop else {})
               | to_json }}
```

Predbat `apps.yaml` (custom inverter section):

```yaml
  inverter:
    has_target_soc: true
    support_charge_freeze: true
    support_discharge_freeze: true
    charge_control_immediate: true
    has_timed_pause: false
  reserve: number.goodwe_reserve
  charge_start_service:
    service: script.goodwe_control
    mode: charge
    power: "{power}"
    target_soc: "{target_soc}"
    repeat: true
  charge_freeze_service:
    service: script.goodwe_control
    mode: freeze_charge
    repeat: true
  charge_stop_service:
    service: script.goodwe_control
    mode: auto
    stop: charge
    repeat: true
  discharge_start_service:
    service: script.goodwe_control
    mode: export
    power: "{power}"
    target_soc: "{target_soc}"
    repeat: true
  discharge_freeze_service:
    service: script.goodwe_control
    mode: freeze_export
    repeat: true
  discharge_stop_service:
    service: script.goodwe_control
    mode: auto
    stop: export
    repeat: true
```

Battery model settings from the spec: `best_soc_min` 20 % and `best_soc_keep` 25 % of `soc_max`, `set_charge_low_power` / `set_export_low_power` on.
````

- [ ] **Step 4: `CHANGELOG.md`** - add an "Unreleased" entry: "Battery control executor (off by default): MQTT `control/set`/`control/reserve/set`/`control/state`, dashboard override, `CONTROL_*` env keys. No behaviour change unless `CONTROL_MODE` is set."

- [ ] **Step 5: Commit**

```bash
git add MQTT_TOPICS.md README.MD .env.example CHANGELOG.md
git commit -m "Document battery control topics, HA entities and Predbat config

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Rollout and live acceptance (supervised, on the Pi)

No code. Each step needs the user present; stop and ask before any step that writes to the inverter.

- [ ] **Step 1: Deploy with control off.** Merge the PR, `git pull` on `raspberry4.local` in `/home/piomar/goodwe_manager/` (check `git status` first - the checkout has local changes), `systemctl --user restart goodwe_manager`. Expected: dashboard unchanged, no `control/state` topic.

- [ ] **Step 2: Shadow.** Set in `.env`: `CONTROL_MODE=shadow`, `CONTROL_CHARGE_CURRENT_A=19`, `CONTROL_DISCHARGE_CURRENT_A=19`; restart. Deploy the HA MQTT entities and `script.goodwe_control` from `MQTT_TOPICS.md` in the `home-assistant-raspberry4` repo; point Predbat's `apps.yaml` at the service templates and set Predbat to control mode (not read-only). Use Predbat's manual plan overrides to force, one after another: charge, freeze charge, export, freeze export, demand. Expected for each: `control/state.effective_mode` and `registers` show the matching desired values, `applied` stays `false` (shadow never writes), `writes_today` stays 0, Predbat's paired stop/start calls don't change `effective_mode` between cycles. About an hour.

- [ ] **Step 3: Disable eco slots** on the `/eco` page (user action); check `control/state.warnings` has no eco-slot entries.

- [ ] **Step 4: Live acceptance** (user present, PV surplus preferred). Set `CONTROL_MODE=on`, restart, and with Predbat paused (read-only) publish by hand from the Pi (`mosquitto_pub -h <broker> -t goodwe/control/set -q 1 -m '<json>'`), 60 s per check:
  1. `{"mode":"charge","power_w":1000,"ttl_s":300,"source":"test"}` → battery ≈ −1000 W, `applied` true within ~10 s.
  2. `{"mode":"export","power_w":1000,"ttl_s":300,"source":"test"}` → battery ≈ +1000 W.
  3. `{"mode":"freeze_charge","ttl_s":300,"source":"test"}` → no discharge.
  4. `{"mode":"freeze_export","ttl_s":300,"source":"test"}` → no charging, surplus exported.
  5. `{"mode":"charge","power_w":1000,"ttl_s":60,"source":"test"}` then wait 70 s → back to `auto`, currents at 19 A.
  6. `{"mode":"freeze_export","ttl_s":600,"source":"test"}` then `systemctl --user restart goodwe_manager` → within ~10 s of restart `auto` + user currents.
  7. Dashboard override `freeze_charge` 15 min while sending `export` commands → override wins, state shows both.
  Record results in the notes file.

- [ ] **Step 5: Enable Predbat control** (Predbat out of read-only) and watch the first charge/export window; check `writes_today` at the end of the day (expected 20-60).

- [ ] **Step 6: Later (after a week stable):** lower the inverter DoD (`battery_discharge_depth`) and let the Predbat-driven reserve manage the floor; add the HA alert "`bridge/status` offline while `control/state.effective_mode != auto`".
