# Pylontech BMS Poller Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An optional goodwe_manager module that polls the Pylontech Force H2
BMS every 60 s through its SolarMan logger. It stores each sample in `bms.db`
and publishes it on MQTT `goodwe/bms`.

**Architecture:**

- Pure decoding and the async `BmsPoller` live in `bms_poller.py`. Storage is
  in `bms_storage.py`. `MqttBridge` gets one new publish method.
- `main.py` starts the poller as another task on the existing asyncio loop,
  only when `BMS_LOGGER_HOST` and `BMS_LOGGER_SERIAL` are set.
- Every network call has a 5 s timeout, so the inverter loop can never stall.

**Tech Stack:** Python 3.12, asyncio, `pysolarmanv5` 3.0.6
(`PySolarmanV5Async`), `aiosqlite`, `unittest`.

**Spec:** `docs/superpowers/specs/2026-09-29-pylontech-bms-poller-design.md`

## Global Constraints

- Optional module:
  - Missing `BMS_LOGGER_HOST` or `BMS_LOGGER_SERIAL` → disabled, one INFO
    line "BMS poller disabled", no task, no `bms.db`.
  - `pysolarmanv5` is never imported when disabled.
- Invalid serial, port or slave ID (not an integer) → one ERROR line, module
  disabled, goodwe_manager still starts.
- Env defaults: `BMS_LOGGER_PORT` 8899, `BMS_SLAVE_ID` 1, `BMS_POLL_SECONDS`
  60, minimum 10 (clamped, with a warning).
- Read-only: only Modbus function 3 (read holding registers). Never write to
  the BMS or the logger.
- Connecting and each read have a 5 s timeout (`asyncio.wait_for`).
- A poll is all-or-nothing: no row, no payload and no partial sample on any
  failure.
- Plausibility checks:
  - SoC and SOH are in 0-100;
  - pack voltage is in 100-500 V;
  - there's at least 1 cell, and every cell is in 2.0-4.0 V;
  - temperatures are in -30 to 80 °C;
  - module voltages add up to within 2 % of the pack voltage.
- Backoff:
  - After 3 consecutive failures the interval doubles per failure, capped at
    600 s.
  - One success resets it.
  - The client is recreated after every failed poll.
- Logging:
  - INFO on failures 1-2;
  - WARNING on the 3rd;
  - DEBUG after that;
  - INFO on recovery, with the failure count and outage duration.
- `bms.db`:
  - sits next to `data.db` (`BMS_DB_PATH = 'bms.db'`);
  - `PRAGMA journal_mode = WAL`;
  - table `bms_history`;
  - `timestamp_epoch INTEGER`, indexed;
  - `timestamp` is local `YYYY-MM-DD HH:MM:SS` at poll start.
- MQTT `goodwe/bms`:
  - not retained;
  - JSON with numeric values (not strings);
  - `raw_1100` is not published.
- Dependencies are pinned: `pysolarmanv5==3.0.6`, `umodbus==1.0.4`.
- The poller doesn't start in `--dry-run`, same as the inverter poll.
- Don't touch inverter polling, the executor, `data.db` or existing topics.

## Review Focus

1. **Winter temperatures.** A cell temperature below 0 °C arrives as a
   register value ≥ 32768 and must decode as negative, not 6553 °C. Tested in
   Task 1.
2. **Short Modbus response.** The logger returning fewer registers than asked
   must fail the poll cleanly (no IndexError, no partial row). Tested in
   Task 3.
3. **Failing `on_sample`.** A handler that raises (DB locked, MQTT bug) must
   not stop the poll loop or count as a BMS failure. Tested in Task 3.
4. **Shutdown during a read.** Cancelling the poller while a read is pending
   must end the task promptly and close the client. It must not be swallowed
   as a failed poll. Tested in Task 3.
5. **Unwritable `bms.db`.** If the file can't be opened, log an ERROR and
   disable the poller; goodwe_manager keeps running. Tested in Task 6.

## File Structure

| File | Responsibility |
|---|---|
| `bms_poller.py` (new) | register map constants, `BmsSample`, `decode()`, plausibility checks, `BmsConfig` + `load_bms_config()`, `default_client_factory()`, `BmsPoller`, `sample_to_payload()` |
| `bms_storage.py` (new) | `BMS_DB_PATH`, `init_db_async()`, `insert_sample()` |
| `mqtt_bridge.py` | `publish_bms()` |
| `main.py` | `BMS_CONFIG`, `AsyncioThread._create_loop_tasks()`, `_run_bms_poller()`, `_bms_sample_handler()` |
| `tests/test_bms_poller.py`, `tests/test_bms_storage.py`, `tests/test_main_bms.py` (new) | tests |
| `tests/test_mqtt_bridge.py` | publish test |
| `requirements.txt`, `.env.example`, `.gitignore`, `MQTT_TOPICS.md`, `CHANGELOG.md` | docs and config |

Run all commands from the worktree root. The venv lives in the main
checkout: use `../../venv/bin/python`.

---

### Task 1: Register decoding and plausibility checks

**Files:**
- Create: `bms_poller.py`
- Test: `tests/test_bms_poller.py`

**Interfaces:**
- Produces:
  - `BmsSample` (frozen dataclass, fields below);
  - `BmsDecodeError(ValueError)`;
  - `decode(summary: list[int], cells: list[int], when: datetime) -> BmsSample`,
    which raises `BmsDecodeError`;
  - constants `SUMMARY_START=0x1100`, `SUMMARY_COUNT=64`,
    `CELLS_START=0x1500`, `CELLS_END=0x1600`, `CHUNK=32`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_bms_poller.py`:

```python
"""
tests/test_bms_poller.py
Decoding is tested against real register dumps read from the Pylontech
Force H2 BMS through the SolarMan logger on 2026-09-29 (SolarMan app showed
cell temperatures 27.2 / 25.7 °C and SOH 97 % at the time).
"""
import unittest
from datetime import datetime

import bms_poller

SUMMARY = (
    [1027, 0, 0, 1965, 0, 0, 360, 33, 679, 2160, 0, 1850, 1740, 65535, 63686, 3]
    + [3276, 3273, 3, 24, 280, 250, 30, 22, 9825, 9824, 0, 1, 272, 257, 1, 0]
    + [97, 0, 2347, 0, 2652, 0, 4021, 0, 2652, 0, 4046, 0, 4942, 0, 4902, 0]
    + [0, 0, 0, 0, 0, 0, 2, 60, 0, 0, 0, 0, 0, 0, 0, 0]
)
CELLS = (
    [3276, 3275, 3276, 3277, 3275, 3276, 3276, 3276, 3275, 3276, 3276, 3276, 3275, 3276, 3276, 3276]
    + [3277, 3277, 3275, 3276, 3275, 3276, 3276, 3275, 3273, 3276, 3276, 3276, 3277, 3275, 3276, 3275]
    + [3277, 3276, 3275, 3276, 3277, 3274, 3275, 3274, 3275, 3275, 3275, 3276, 3277, 3277, 3275, 3277]
    + [3274, 3275, 3277, 3276, 3275, 3277, 3276, 3276, 3277, 3277, 3274, 3275, 0, 0, 0, 0]
)
WHEN = datetime(2026, 9, 29, 8, 45, 30)


def with_reg(block, addr, value, start=bms_poller.SUMMARY_START):
    changed = list(block)
    changed[addr - start] = value
    return changed


class DecodeTest(unittest.TestCase):
    def test_decodes_the_real_dump(self):
        s = bms_poller.decode(SUMMARY, CELLS, WHEN)

        self.assertEqual(s.timestamp, '2026-09-29 08:45:30')
        self.assertEqual(s.timestamp_epoch, int(WHEN.timestamp()))
        self.assertAlmostEqual(s.pack_voltage, 196.5)
        self.assertAlmostEqual(s.bms_temperature, 36.0)
        self.assertEqual(s.soc, 33)
        self.assertEqual(s.soh, 97)
        self.assertAlmostEqual(s.cell_voltage_max, 3.276)
        self.assertAlmostEqual(s.cell_voltage_min, 3.273)
        self.assertEqual(s.cell_voltage_max_id, 3)
        self.assertEqual(s.cell_voltage_min_id, 24)
        self.assertAlmostEqual(s.cell_temp_max, 27.2)
        self.assertAlmostEqual(s.cell_temp_min, 25.7)
        self.assertEqual(s.module_voltages, [98.25, 98.24])
        self.assertEqual(len(s.cell_mv), 60)
        self.assertEqual(s.cell_mv[:3], [3276, 3275, 3276])
        self.assertEqual(s.raw_1100, SUMMARY)

    def test_negative_temperatures_are_signed(self):
        summary = with_reg(with_reg(SUMMARY, 0x111C, 65536 - 25), 0x111D, 65536 - 52)

        s = bms_poller.decode(summary, CELLS, WHEN)

        self.assertAlmostEqual(s.cell_temp_max, -2.5)
        self.assertAlmostEqual(s.cell_temp_min, -5.2)

    def test_trailing_cells_after_the_first_zero_are_ignored(self):
        s = bms_poller.decode(SUMMARY, CELLS[:60] + [0, 3300, 3300], WHEN)
        self.assertEqual(len(s.cell_mv), 60)

    def test_module_voltages_stop_at_four_slots(self):
        # A 4-module pack: 0x1118-0x111B all set. Reading must stop there,
        # not run on into 0x111C (cell temperature 27.2 -> "0.272 V").
        summary = with_reg(with_reg(with_reg(SUMMARY, 0x1103, 3930), 0x111A, 9825), 0x111B, 9826)

        s = bms_poller.decode(summary, CELLS, WHEN)

        self.assertEqual(s.module_voltages, [98.25, 98.24, 98.25, 98.26])

    def test_wrong_summary_length_is_rejected(self):
        with self.assertRaisesRegex(bms_poller.BmsDecodeError, 'summary'):
            bms_poller.decode(SUMMARY[:32], CELLS, WHEN)


class PlausibilityTest(unittest.TestCase):
    def assert_rejected(self, summary=SUMMARY, cells=CELLS, match=''):
        with self.assertRaisesRegex(bms_poller.BmsDecodeError, match):
            bms_poller.decode(summary, cells, WHEN)

    def test_soc_above_100(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1107, 120), match='soc')

    def test_soh_above_100(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1120, 101), match='soh')

    def test_pack_voltage_out_of_range(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1103, 50), match='pack_voltage')

    def test_no_cells(self):
        self.assert_rejected(cells=[0] * 32, match='cell')

    def test_cell_out_of_range(self):
        self.assert_rejected(cells=[1500] + CELLS[1:], match='cell')

    def test_temperature_out_of_range(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1106, 900), match='temperature')

    def test_module_sum_off_by_more_than_two_percent(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1119, 9000), match='module')

    def test_no_modules(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1118, 0), match='module')


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `../../venv/bin/python -m unittest tests.test_bms_poller -v`
Expected: ERROR `ModuleNotFoundError: No module named 'bms_poller'`

- [ ] **Step 3: Implement the decoding**

Create `bms_poller.py`:

```python
"""
bms_poller.py
Optional poller for a Pylontech Force H2 BMS, read through the SolarMan
Wi-Fi logger attached to it (SolarMan V5 protocol, Modbus slave 1, holding
registers only). See
docs/superpowers/specs/2026-09-29-pylontech-bms-poller-design.md for the
register map and how each field was confirmed.

No import-time side effects: pysolarmanv5 is only imported by
default_client_factory, so a setup without the battery never loads it.
"""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List

logger = logging.getLogger(__name__)

SUMMARY_START = 0x1100
SUMMARY_COUNT = 64
CELLS_START = 0x1500
CELLS_END = 0x1600  # exclusive; only 0x1500-0x153F is verified to respond
CHUNK = 32
MAX_MODULE_SLOTS = 4  # 0x1118-0x111B; 0x111C onwards holds temperatures


class BmsDecodeError(ValueError):
    pass


@dataclass(frozen=True)
class BmsSample:
    timestamp: str
    timestamp_epoch: int
    pack_voltage: float
    bms_temperature: float
    soc: int
    soh: int
    cell_voltage_max: float
    cell_voltage_min: float
    cell_voltage_max_id: int
    cell_voltage_min_id: int
    cell_temp_max: float
    cell_temp_min: float
    module_voltages: List[float]
    cell_mv: List[int]
    raw_1100: List[int]


def _signed(value: int) -> int:
    return value - 0x10000 if value >= 0x8000 else value


def decode(summary: List[int], cells: List[int], when: datetime) -> BmsSample:
    """Decode the 0x1100-0x113F summary block and the cell block read from
    0x1500 into a BmsSample, then run the plausibility checks. Raises
    BmsDecodeError if anything doesn't fit."""
    if len(summary) != SUMMARY_COUNT:
        raise BmsDecodeError(f'summary block has {len(summary)} registers, expected {SUMMARY_COUNT}')

    def reg(addr: int) -> int:
        return summary[addr - SUMMARY_START]

    modules = []
    for i in range(MAX_MODULE_SLOTS):
        value = reg(0x1118 + i)
        if value == 0:
            break
        modules.append(value / 100)
    cell_mv = []
    for value in cells:
        if value == 0:
            break
        cell_mv.append(value)

    sample = BmsSample(
        timestamp=when.strftime('%Y-%m-%d %H:%M:%S'),
        timestamp_epoch=int(when.timestamp()),
        pack_voltage=reg(0x1103) / 10,
        bms_temperature=_signed(reg(0x1106)) / 10,
        soc=reg(0x1107),
        soh=reg(0x1120),
        cell_voltage_max=reg(0x1110) / 1000,
        cell_voltage_min=reg(0x1111) / 1000,
        cell_voltage_max_id=reg(0x1112),
        cell_voltage_min_id=reg(0x1113),
        cell_temp_max=_signed(reg(0x111C)) / 10,
        cell_temp_min=_signed(reg(0x111D)) / 10,
        module_voltages=modules,
        cell_mv=cell_mv,
        raw_1100=list(summary),
    )
    _check_plausible(sample)
    return sample


def _check_plausible(s: BmsSample) -> None:
    if not 0 <= s.soc <= 100:
        raise BmsDecodeError(f'soc {s.soc} outside 0-100')
    if not 0 <= s.soh <= 100:
        raise BmsDecodeError(f'soh {s.soh} outside 0-100')
    if not 100 <= s.pack_voltage <= 500:
        raise BmsDecodeError(f'pack_voltage {s.pack_voltage} V outside 100-500')
    if not s.cell_mv:
        raise BmsDecodeError('no cell voltages')
    bad = [mv for mv in s.cell_mv if not 2000 <= mv <= 4000]
    if bad:
        raise BmsDecodeError(f'cell voltage {bad[0]} mV outside 2000-4000')
    for name in ('bms_temperature', 'cell_temp_max', 'cell_temp_min'):
        value = getattr(s, name)
        if not -30 <= value <= 80:
            raise BmsDecodeError(f'{name} temperature {value} °C outside -30..80')
    if not s.module_voltages:
        raise BmsDecodeError('no module voltages')
    total = sum(s.module_voltages)
    if abs(total - s.pack_voltage) > 0.02 * s.pack_voltage:
        raise BmsDecodeError(f'module voltages sum {total:.2f} V, pack reads {s.pack_voltage} V')
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `../../venv/bin/python -m unittest tests.test_bms_poller -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add bms_poller.py tests/test_bms_poller.py
git commit -m "BMS poller: decode Pylontech registers with plausibility checks

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Config loading and the client factory

**Files:**
- Modify: `bms_poller.py`
- Test: `tests/test_bms_poller.py`

**Interfaces:**
- Produces:
  - `BmsConfig(host: str, serial: int, port: int = 8899, slave_id: int = 1, poll_seconds: int = 60)`
    (frozen dataclass);
  - `load_bms_config(env: Mapping[str, str]) -> Optional[BmsConfig]`;
  - `default_client_factory(config: BmsConfig)`, which returns an
    unconnected `PySolarmanV5Async`;
  - constants `READ_TIMEOUT_S = 5.0`, `MIN_POLL_SECONDS = 10`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bms_poller.py` (above the `if __name__` line), and add
`import subprocess`, `import sys` at the top:

```python
class LoadConfigTest(unittest.TestCase):
    def test_disabled_without_host_or_serial(self):
        for env in ({}, {'BMS_LOGGER_HOST': '192.168.1.221'}, {'BMS_LOGGER_SERIAL': '4060493924'},
                    {'BMS_LOGGER_HOST': ' ', 'BMS_LOGGER_SERIAL': '4060493924'}):
            with self.assertLogs('bms_poller', 'INFO') as logs:
                self.assertIsNone(bms_poller.load_bms_config(env))
            self.assertIn('BMS poller disabled', logs.output[0])

    def test_defaults(self):
        cfg = bms_poller.load_bms_config({'BMS_LOGGER_HOST': '192.168.1.221', 'BMS_LOGGER_SERIAL': '4060493924'})
        self.assertEqual(cfg, bms_poller.BmsConfig('192.168.1.221', 4060493924, 8899, 1, 60))

    def test_overrides(self):
        cfg = bms_poller.load_bms_config({'BMS_LOGGER_HOST': 'h', 'BMS_LOGGER_SERIAL': '7',
                                          'BMS_LOGGER_PORT': '9000', 'BMS_SLAVE_ID': '2', 'BMS_POLL_SECONDS': '30'})
        self.assertEqual(cfg, bms_poller.BmsConfig('h', 7, 9000, 2, 30))

    def test_invalid_number_disables_with_error(self):
        for key in ('BMS_LOGGER_SERIAL', 'BMS_LOGGER_PORT', 'BMS_SLAVE_ID', 'BMS_POLL_SECONDS'):
            env = {'BMS_LOGGER_HOST': 'h', 'BMS_LOGGER_SERIAL': '7', key: 'abc'}
            with self.assertLogs('bms_poller', 'ERROR'):
                self.assertIsNone(bms_poller.load_bms_config(env))

    def test_poll_interval_is_clamped_to_ten_seconds(self):
        with self.assertLogs('bms_poller', 'WARNING'):
            cfg = bms_poller.load_bms_config({'BMS_LOGGER_HOST': 'h', 'BMS_LOGGER_SERIAL': '7', 'BMS_POLL_SECONDS': '5'})
        self.assertEqual(cfg.poll_seconds, 10)

    def test_disabled_config_never_imports_pysolarmanv5(self):
        # Subprocess: other tests may import pysolarmanv5 into this process.
        code = ("import sys, bms_poller; bms_poller.load_bms_config({}); "
                "sys.exit(1 if 'pysolarmanv5' in sys.modules else 0)")
        result = subprocess.run([sys.executable, '-c', code], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `../../venv/bin/python -m unittest tests.test_bms_poller.LoadConfigTest -v`
Expected: FAIL/ERROR `AttributeError: module 'bms_poller' has no attribute 'load_bms_config'`

- [ ] **Step 3: Implement**

In `bms_poller.py`, change the typing import to
`from typing import List, Mapping, Optional` and add after the constants:

```python
READ_TIMEOUT_S = 5.0
MIN_POLL_SECONDS = 10  # protects the logger (it also uploads to the SolarMan cloud)


@dataclass(frozen=True)
class BmsConfig:
    host: str
    serial: int
    port: int = 8899
    slave_id: int = 1
    poll_seconds: int = 60


def load_bms_config(env: Mapping[str, str]) -> Optional[BmsConfig]:
    """BmsConfig from BMS_* environment variables, or None (module off) when
    BMS_LOGGER_HOST / BMS_LOGGER_SERIAL aren't set or a number doesn't parse.
    Never raises - a bad BMS config must not stop goodwe_manager starting."""
    host = (env.get('BMS_LOGGER_HOST') or '').strip()
    serial = (env.get('BMS_LOGGER_SERIAL') or '').strip()
    if not host or not serial:
        logger.info('BMS poller disabled (BMS_LOGGER_HOST / BMS_LOGGER_SERIAL not set)')
        return None
    try:
        config = BmsConfig(
            host=host,
            serial=int(serial),
            port=int(env.get('BMS_LOGGER_PORT') or 8899),
            slave_id=int(env.get('BMS_SLAVE_ID') or 1),
            poll_seconds=int(env.get('BMS_POLL_SECONDS') or 60),
        )
    except ValueError as e:
        logger.error(f'BMS poller disabled, invalid BMS_* setting: {e}')
        return None
    if config.poll_seconds < MIN_POLL_SECONDS:
        logger.warning(f'BMS_POLL_SECONDS={config.poll_seconds} is below {MIN_POLL_SECONDS}, using {MIN_POLL_SECONDS}')
        config = BmsConfig(config.host, config.serial, config.port, config.slave_id, MIN_POLL_SECONDS)
    return config


def default_client_factory(config: BmsConfig):
    """An unconnected SolarMan V5 client. The import lives here so
    pysolarmanv5 is only loaded when the module is enabled."""
    from pysolarmanv5 import PySolarmanV5Async
    return PySolarmanV5Async(config.host, config.serial, port=config.port, mb_slave_id=config.slave_id,
                             socket_timeout=READ_TIMEOUT_S, auto_reconnect=False)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `../../venv/bin/python -m unittest tests.test_bms_poller -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add bms_poller.py tests/test_bms_poller.py
git commit -m "BMS poller: config from BMS_* env vars, lazy client factory

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The async poller (reads, timeouts, backoff, logging)

**Files:**
- Modify: `bms_poller.py`
- Test: `tests/test_bms_poller.py`

**Interfaces:**
- Consumes: `decode`, `BmsConfig`, `default_client_factory`, the constants
  from Tasks 1-2.
- Produces:
  - `BmsPoller(config, on_sample, client_factory=default_client_factory, now_fn=datetime.now, sleep=asyncio.sleep, timeout_s=READ_TIMEOUT_S)`,
    where `on_sample` is `async (BmsSample) -> None`;
  - `async BmsPoller.poll_once() -> Optional[BmsSample]`;
  - `async BmsPoller.run() -> None` (loops until cancelled);
  - the `BmsPoller.interval` property (float, seconds);
  - `sample_to_payload(sample: BmsSample) -> dict`.

**The client protocol used** (what `PySolarmanV5Async` provides):
- `async connect()`;
- `async disconnect()`;
- `async read_holding_registers(register_addr, quantity) -> list[int]`.

An address the BMS doesn't have raises umodbus's `IllegalDataAddressError`.
It's matched by class name, so umodbus is never imported when the module is
disabled.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bms_poller.py` (add `import asyncio`, `import time`
at the top):

```python
class IllegalDataAddressError(Exception):
    """Same class name as umodbus.exceptions.IllegalDataAddressError."""


class FakeClient:
    """Serves registers from a dict {addr: value}. `hang` makes calls never
    return; `fail_at` raises on reads starting at that address."""

    def __init__(self, registers, hang=False, fail_at=None, hang_connect=False, short=False):
        self.registers = registers
        self.hang = hang
        self.hang_connect = hang_connect
        self.fail_at = fail_at
        self.short = short
        self.connected = False
        self.disconnected = False
        self.reads = []

    async def connect(self):
        if self.hang_connect:
            await asyncio.Event().wait()
        self.connected = True

    async def disconnect(self):
        self.disconnected = True

    async def read_holding_registers(self, register_addr, quantity):
        self.reads.append((register_addr, quantity))
        if self.hang:
            await asyncio.Event().wait()
        if self.fail_at == register_addr:
            raise OSError('connection reset')
        if register_addr not in self.registers:
            raise IllegalDataAddressError()
        values = [self.registers.get(register_addr + i, 0) for i in range(quantity)]
        return values[:-1] if self.short else values


def bms_registers(cells=CELLS):
    regs = {bms_poller.SUMMARY_START + i: v for i, v in enumerate(SUMMARY)}
    regs.update({bms_poller.CELLS_START + i: v for i, v in enumerate(cells)})
    return regs


CONFIG = bms_poller.BmsConfig('h', 7)


class PollerHarness:
    def __init__(self, clients, timeout_s=0.05):
        self.clients = list(clients)
        self.created = []
        self.samples = []

        def factory(config):
            client = self.clients.pop(0)
            self.created.append(client)
            return client

        async def on_sample(sample):
            self.samples.append(sample)

        self.poller = bms_poller.BmsPoller(CONFIG, on_sample, client_factory=factory,
                                           now_fn=lambda: WHEN, timeout_s=timeout_s)


class PollOnceTest(unittest.TestCase):
    def test_success_calls_on_sample_once(self):
        h = PollerHarness([FakeClient(bms_registers())])
        sample = asyncio.run(h.poller.poll_once())
        self.assertEqual(h.samples, [sample])
        self.assertEqual(sample.soh, 97)
        # summary in 2 chunks, cells stop after the chunk containing a zero
        self.assertEqual(h.created[0].reads, [(0x1100, 32), (0x1120, 32), (0x1500, 32), (0x1520, 32)])

    def test_failed_second_read_gives_no_sample(self):
        h = PollerHarness([FakeClient(bms_registers(), fail_at=0x1120)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))
        self.assertEqual(h.samples, [])

    def test_short_response_fails_the_poll(self):
        h = PollerHarness([FakeClient(bms_registers(), short=True)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))
        self.assertEqual(h.samples, [])

    def test_hanging_read_times_out(self):
        h = PollerHarness([FakeClient(bms_registers(), hang=True)])
        started = time.monotonic()
        self.assertIsNone(asyncio.run(h.poller.poll_once()))
        self.assertLess(time.monotonic() - started, 1.0)

    def test_hanging_connect_times_out(self):
        h = PollerHarness([FakeClient(bms_registers(), hang_connect=True)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))

    def test_invalid_data_fails_the_poll(self):
        regs = bms_registers()
        regs[0x1107] = 120  # SoC 120
        h = PollerHarness([FakeClient(regs)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))

    def test_cells_filling_a_chunk_continue_until_illegal_address(self):
        cells = [3300] * 64  # no zero in 0x1500-0x153F; 0x1540 doesn't exist
        h = PollerHarness([FakeClient(bms_registers(cells))])
        sample = asyncio.run(h.poller.poll_once())
        self.assertEqual(len(sample.cell_mv), 64)

    def test_illegal_address_on_first_cell_chunk_fails(self):
        regs = {bms_poller.SUMMARY_START + i: v for i, v in enumerate(SUMMARY)}
        h = PollerHarness([FakeClient(regs)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))

    def test_new_client_after_a_failure(self):
        bad = FakeClient(bms_registers(), fail_at=0x1100)
        good = FakeClient(bms_registers())
        h = PollerHarness([bad, good])

        async def two_polls():
            await h.poller.poll_once()
            return await h.poller.poll_once()

        self.assertIsNotNone(asyncio.run(two_polls()))
        self.assertTrue(bad.disconnected)
        self.assertEqual(h.created, [bad, good])

    def test_failing_on_sample_does_not_count_as_failure(self):
        h = PollerHarness([FakeClient(bms_registers())])

        async def broken(sample):
            raise RuntimeError('db locked')

        h.poller._on_sample = broken
        with self.assertLogs('bms_poller', 'WARNING'):
            self.assertIsNotNone(asyncio.run(h.poller.poll_once()))
        self.assertEqual(h.poller.interval, 60)


class BackoffAndLoggingTest(unittest.TestCase):
    def test_backoff_sequence_and_reset(self):
        h = PollerHarness([FakeClient({}, fail_at=0x1100) for _ in range(7)] + [FakeClient(bms_registers())])
        intervals = []

        async def polls():
            for _ in range(7):
                await h.poller.poll_once()
                intervals.append(h.poller.interval)
            await h.poller.poll_once()

        with self.assertLogs('bms_poller', 'DEBUG'):
            asyncio.run(polls())
        self.assertEqual(intervals, [60, 60, 120, 240, 480, 600, 600])
        self.assertEqual(h.poller.interval, 60)

    def test_log_levels(self):
        h = PollerHarness([FakeClient({}, fail_at=0x1100) for _ in range(4)] + [FakeClient(bms_registers())])

        async def polls():
            for _ in range(5):
                await h.poller.poll_once()

        with self.assertLogs('bms_poller', 'DEBUG') as logs:
            asyncio.run(polls())
        levels = [r.levelname for r in logs.records]
        self.assertEqual(levels, ['INFO', 'INFO', 'WARNING', 'DEBUG', 'INFO'])
        self.assertIn('recovered after 4', logs.records[-1].getMessage())


class RunLoopTest(unittest.TestCase):
    def test_does_not_block_the_event_loop(self):
        # The inverter poll shares this loop: a never-answering logger must
        # not delay other tasks.
        clients = [FakeClient(bms_registers(), hang=True) for _ in range(50)]
        h = PollerHarness(clients, timeout_s=0.1)
        h.poller._sleep = lambda seconds: asyncio.sleep(0.01)
        gaps = []

        async def ticker():
            last = time.monotonic()
            for _ in range(40):
                await asyncio.sleep(0.01)
                now = time.monotonic()
                gaps.append(now - last)
                last = now

        async def main():
            task = asyncio.create_task(h.poller.run())
            await ticker()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        asyncio.run(main())
        self.assertLess(max(gaps), 0.05)

    def test_cancel_during_read_ends_promptly_and_closes_client(self):
        client = FakeClient(bms_registers(), hang=True)
        h = PollerHarness([client], timeout_s=10)

        async def main():
            task = asyncio.create_task(h.poller.run())
            await asyncio.sleep(0.05)
            task.cancel()
            started = time.monotonic()
            with self.assertRaises(asyncio.CancelledError):
                await task
            return time.monotonic() - started

        self.assertLess(asyncio.run(main()), 0.5)
        self.assertTrue(client.disconnected)
        self.assertEqual(h.poller._failures, 0)


class PayloadTest(unittest.TestCase):
    def test_payload_has_numbers_and_no_raw_block(self):
        payload = bms_poller.sample_to_payload(bms_poller.decode(SUMMARY, CELLS, WHEN))
        self.assertNotIn('raw_1100', payload)
        self.assertEqual(payload['timestamp'], '2026-09-29 08:45:30')
        self.assertEqual(payload['soh'], 97)
        self.assertIsInstance(payload['cell_temp_max'], float)
        self.assertEqual(len(payload['cell_mv']), 60)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `../../venv/bin/python -m unittest tests.test_bms_poller -v`
Expected: the new tests ERROR with `AttributeError: module 'bms_poller' has no attribute 'BmsPoller'`.

- [ ] **Step 3: Implement**

In `bms_poller.py`, add `import asyncio`,
`from dataclasses import asdict, dataclass` and
`from typing import Awaitable, Callable, List, Mapping, Optional`. Then
append:

```python
BACKOFF_AFTER_FAILURES = 3
MAX_INTERVAL_S = 600


class PollFailed(Exception):
    pass


def sample_to_payload(sample: BmsSample) -> dict:
    """MQTT payload: every decoded field as a JSON number; the raw register
    block stays in bms.db only."""
    payload = asdict(sample)
    del payload['raw_1100']
    return payload


class BmsPoller:
    """Polls the BMS every config.poll_seconds until cancelled. Every
    network call is bounded by timeout_s - this runs on the same asyncio loop
    as the inverter poll, so nothing here may block or hang."""

    def __init__(self, config: BmsConfig, on_sample: Callable[[BmsSample], Awaitable[None]],
                 client_factory=default_client_factory, now_fn=datetime.now, sleep=asyncio.sleep,
                 timeout_s: float = READ_TIMEOUT_S):
        self._config = config
        self._on_sample = on_sample
        self._client_factory = client_factory
        self._now_fn = now_fn
        self._sleep = sleep
        self._timeout_s = timeout_s
        self._client = None
        self._failures = 0
        self._failing_since: Optional[datetime] = None

    @property
    def interval(self) -> float:
        if self._failures < BACKOFF_AFTER_FAILURES:
            return self._config.poll_seconds
        return min(self._config.poll_seconds * 2 ** (self._failures - BACKOFF_AFTER_FAILURES + 1), MAX_INTERVAL_S)

    async def run(self) -> None:
        try:
            while True:
                await self.poll_once()
                await self._sleep(self.interval)
        finally:
            await self._close_client()

    async def poll_once(self) -> Optional[BmsSample]:
        when = self._now_fn()
        try:
            if self._client is None:
                self._client = self._client_factory(self._config)
                await asyncio.wait_for(self._client.connect(), self._timeout_s)
            summary = await self._read(SUMMARY_START, SUMMARY_COUNT)
            cells = await self._read_cells()
            sample = decode(summary, cells, when)
        except Exception as e:  # CancelledError is a BaseException: shutdown passes through
            await self._record_failure(e, when)
            return None
        self._record_success(when)
        try:
            await self._on_sample(sample)
        except Exception as e:
            logger.warning(f'BMS sample handler failed: {e!r}')
        return sample

    async def _read(self, start: int, count: int) -> List[int]:
        values: List[int] = []
        for addr in range(start, start + count, CHUNK):
            quantity = min(CHUNK, start + count - addr)
            chunk = await asyncio.wait_for(
                self._client.read_holding_registers(register_addr=addr, quantity=quantity), self._timeout_s)
            if len(chunk) != quantity:
                raise PollFailed(f'short response at {addr:#06x}: {len(chunk)} of {quantity} registers')
            values.extend(chunk)
        return values

    async def _read_cells(self) -> List[int]:
        cells: List[int] = []
        for addr in range(CELLS_START, CELLS_END, CHUNK):
            try:
                chunk = await self._read(addr, CHUNK)
            except Exception as e:
                # Past the first chunk, an address the BMS doesn't have just
                # means the cell list ended (only 0x1500-0x153F is verified).
                if addr != CELLS_START and type(e).__name__ == 'IllegalDataAddressError':
                    break
                raise
            cells.extend(chunk)
            if 0 in chunk:
                break
        return cells

    async def _record_failure(self, error: Exception, when: datetime) -> None:
        await self._close_client()
        self._failures += 1
        if self._failures == 1:
            self._failing_since = when
        reason = f'{type(error).__name__}: {error}' if str(error) else type(error).__name__
        if self._failures < BACKOFF_AFTER_FAILURES:
            logger.info(f'BMS poll failed ({reason}), attempt {self._failures}')
        elif self._failures == BACKOFF_AFTER_FAILURES:
            logger.warning(f'BMS poll failed {self._failures} times in a row ({reason}); '
                           f'backing off, next try in {self.interval:.0f} s')
        else:
            logger.debug(f'BMS poll failed ({reason}), attempt {self._failures}, next try in {self.interval:.0f} s')

    def _record_success(self, when: datetime) -> None:
        if self._failures:
            outage = when - self._failing_since if self._failing_since else None
            logger.info(f'BMS poll recovered after {self._failures} failed polls'
                        + (f' ({outage.total_seconds():.0f} s)' if outage is not None else ''))
        self._failures = 0
        self._failing_since = None

    async def _close_client(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        try:
            await asyncio.wait_for(client.disconnect(), self._timeout_s)
        except Exception:
            pass
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `../../venv/bin/python -m unittest tests.test_bms_poller -v`
Expected: all PASS. If `test_cancel_during_read_ends_promptly_and_closes_client`
fails on `client.disconnected`: the `finally` in `run()` must still await
`_close_client()` after cancellation. `FakeClient.disconnect` returns
immediately, so that works as written.

- [ ] **Step 5: Commit**

```bash
git add bms_poller.py tests/test_bms_poller.py
git commit -m "BMS poller: async poll loop with timeouts, backoff and transition logging

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `bms.db` storage

**Files:**
- Create: `bms_storage.py`
- Test: `tests/test_bms_storage.py`
- Modify: `.gitignore` (after the `/forecast_history.db-shm` line)

**Interfaces:**
- Consumes: `bms_poller.BmsSample`.
- Produces:
  - `BMS_DB_PATH = 'bms.db'`;
  - `async init_db_async(path: Optional[str] = None) -> aiosqlite.Connection`;
  - `async insert_sample(conn: aiosqlite.Connection, sample: BmsSample) -> None`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_bms_storage.py`:

```python
import asyncio
import json
import os
import sqlite3
import tempfile
import unittest

import bms_poller
import bms_storage
from tests.test_bms_poller import CELLS, SUMMARY, WHEN


class BmsStorageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'bms.db')

    def tearDown(self):
        self.tmp.cleanup()

    def test_creates_table_index_and_wal(self):
        async def go():
            conn = await bms_storage.init_db_async(self.path)
            await conn.close()

        asyncio.run(go())
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
            indexes = [r[1] for r in db.execute("PRAGMA index_list('bms_history')")]
            self.assertIn('idx_bms_history_timestamp_epoch', indexes)

    def test_row_round_trips(self):
        sample = bms_poller.decode(SUMMARY, CELLS, WHEN)

        async def go():
            conn = await bms_storage.init_db_async(self.path)
            await bms_storage.insert_sample(conn, sample)
            await conn.close()

        asyncio.run(go())
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT * FROM bms_history').fetchone()
        self.assertEqual(row['timestamp'], '2026-09-29 08:45:30')
        self.assertEqual(row['timestamp_epoch'], int(WHEN.timestamp()))
        self.assertAlmostEqual(row['cell_temp_max'], 27.2)
        self.assertEqual(row['soh'], 97)
        self.assertEqual(json.loads(row['module_voltages']), [98.25, 98.24])
        self.assertEqual(len(json.loads(row['cell_mv'])), 60)
        self.assertEqual(json.loads(row['raw_1100']), SUMMARY)

    def test_default_path_is_looked_up_at_call_time(self):
        orig = bms_storage.BMS_DB_PATH
        bms_storage.BMS_DB_PATH = self.path
        try:
            async def go():
                conn = await bms_storage.init_db_async()
                await conn.close()
            asyncio.run(go())
        finally:
            bms_storage.BMS_DB_PATH = orig
        self.assertTrue(os.path.exists(self.path))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `../../venv/bin/python -m unittest tests.test_bms_storage -v`
Expected: ERROR `ModuleNotFoundError: No module named 'bms_storage'`

- [ ] **Step 3: Implement**

Create `bms_storage.py`:

```python
"""
bms_storage.py
bms.db: one row per accepted Pylontech BMS sample (see bms_poller.py). A
separate file from data.db so the optional BMS module never touches the
inverter database or queues behind its 1 Hz writes.
"""
import json
from typing import Optional

import aiosqlite

from bms_poller import BmsSample

BMS_DB_PATH = 'bms.db'

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS bms_history (
        id INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL,
        timestamp_epoch INTEGER NOT NULL,
        pack_voltage REAL,
        bms_temperature REAL,
        soc REAL,
        soh REAL,
        cell_voltage_max REAL,
        cell_voltage_min REAL,
        cell_voltage_max_id INTEGER,
        cell_voltage_min_id INTEGER,
        cell_temp_max REAL,
        cell_temp_min REAL,
        module_voltages TEXT,
        cell_mv TEXT,
        raw_1100 TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_bms_history_timestamp_epoch ON bms_history (timestamp_epoch)",
)

_COLUMNS = ('timestamp', 'timestamp_epoch', 'pack_voltage', 'bms_temperature', 'soc', 'soh',
            'cell_voltage_max', 'cell_voltage_min', 'cell_voltage_max_id', 'cell_voltage_min_id',
            'cell_temp_max', 'cell_temp_min', 'module_voltages', 'cell_mv', 'raw_1100')
_JSON_COLUMNS = {'module_voltages', 'cell_mv', 'raw_1100'}


async def init_db_async(path: Optional[str] = None) -> aiosqlite.Connection:
    """`path` defaults to BMS_DB_PATH, looked up at call time so tests can
    monkeypatch it (same convention as forecast_history.init_db)."""
    conn = await aiosqlite.connect(path or BMS_DB_PATH)
    try:
        await conn.execute("PRAGMA journal_mode = WAL")
        for statement in _SCHEMA:
            await conn.execute(statement)
        await conn.commit()
    except Exception:
        await conn.close()
        raise
    return conn


async def insert_sample(conn: aiosqlite.Connection, sample: BmsSample) -> None:
    values = [json.dumps(getattr(sample, c)) if c in _JSON_COLUMNS else getattr(sample, c) for c in _COLUMNS]
    await conn.execute(
        f"INSERT INTO bms_history ({', '.join(_COLUMNS)}) VALUES ({', '.join('?' * len(_COLUMNS))})", values)
    await conn.commit()
```

Add to `.gitignore` after `/forecast_history.db-shm`:

```
/bms.db
/bms.db-wal
/bms.db-shm
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `../../venv/bin/python -m unittest tests.test_bms_storage -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add bms_storage.py tests/test_bms_storage.py .gitignore
git commit -m "BMS poller: bms.db storage

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: MQTT `goodwe/bms`

**Files:**
- Modify: `mqtt_bridge.py` (next to `publish_pv_forecast`)
- Modify: `MQTT_TOPICS.md` (add a section after `telemetry`)
- Test: `tests/test_mqtt_bridge.py`

**Interfaces:**
- Produces: `async MqttBridge.publish_bms(payload: dict) -> None`, which
  publishes to `<prefix>/bms`, not retained.

- [ ] **Step 1: Write the failing test**

In `tests/test_mqtt_bridge.py`, in the test class whose `setUp` builds the
bridge with `capturing_client_factory` (the one containing
`test_publish_telemetry_is_not_retained`), add:

```python
    def test_publish_bms_is_not_retained_and_keeps_numbers(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_bms({'soh': 97, 'cell_temp_max': 27.2, 'cell_mv': [3276, 3275]}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/bms')
        self.assertFalse(retain)
        self.assertEqual(json.loads(payload), {'soh': 97, 'cell_temp_max': 27.2, 'cell_mv': [3276, 3275]})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `../../venv/bin/python -m unittest tests.test_mqtt_bridge -v`
Expected: ERROR `AttributeError: 'MqttBridge' object has no attribute 'publish_bms'`

- [ ] **Step 3: Implement**

In `mqtt_bridge.py`, after `publish_pv_forecast`:

```python
    async def publish_bms(self, payload: dict) -> None:
        """Pylontech BMS sample (bms_poller.sample_to_payload) - numbers stay
        numbers, unlike the string-valued telemetry payload."""
        await self._publish('bms', json.dumps(payload), retain=False)
```

In `MQTT_TOPICS.md`, after the `telemetry` section:

````markdown
## `bms` (not retained, every `BMS_POLL_SECONDS`, default 60 s)

Only published when the optional Pylontech BMS poller is enabled
(`BMS_LOGGER_HOST` / `BMS_LOGGER_SERIAL` in `.env`). One accepted BMS
sample, with **real JSON numbers** (unlike `telemetry`):

```json
{"timestamp": "2026-09-29 08:45:30", "timestamp_epoch": 1790664330,
 "pack_voltage": 196.5, "bms_temperature": 36.0, "soc": 33, "soh": 97,
 "cell_voltage_max": 3.276, "cell_voltage_min": 3.273,
 "cell_voltage_max_id": 3, "cell_voltage_min_id": 24,
 "cell_temp_max": 27.2, "cell_temp_min": 25.7,
 "module_voltages": [98.25, 98.24], "cell_mv": [3276, 3275, ...]}
```

`bms_temperature` is the BMS's own sensor (the same 36 °C the inverter
reports as `battery_temperature`); `cell_temp_max`/`_min` are the cells.
Nothing is published while the BMS can't be read - use `expire_after` on
HA sensors.
````

- [ ] **Step 4: Run the tests to verify they pass**

Run: `../../venv/bin/python -m unittest tests.test_mqtt_bridge -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add mqtt_bridge.py MQTT_TOPICS.md tests/test_mqtt_bridge.py
git commit -m "BMS poller: publish samples on goodwe/bms

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Wire into `main.py`, config, dependencies, changelog

**Files:**
- Modify: `main.py`:
  - imports;
  - config block after `MQTT_TOPIC_PREFIX`;
  - `AsyncioThread.run()` (≈ line 142);
  - new methods on `AsyncioThread`;
  - module-level `_bms_sample_handler` after `mqtt = ...`.
- Modify: `requirements.txt`, `.env.example`, `CHANGELOG.md`
- Test: `tests/test_main_bms.py`

**Interfaces:**
- Consumes:
  - `bms_poller.load_bms_config`, `bms_poller.BmsPoller`,
    `bms_poller.sample_to_payload`;
  - `bms_storage.init_db_async`, `bms_storage.insert_sample`,
    `bms_storage.BMS_DB_PATH`;
  - `mqtt.publish_bms`.
- Produces:
  - `main.BMS_CONFIG: Optional[BmsConfig]`;
  - `AsyncioThread._create_loop_tasks(loop)`;
  - `async AsyncioThread._run_bms_poller(config)`;
  - `main._bms_sample_handler(conn) -> async (BmsSample) -> None`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_main_bms.py`:

```python
"""
tests/test_main_bms.py
main.py wiring of the optional BMS poller: only started when configured and
not in --dry-run, never allowed to take goodwe_manager down with it.
"""
import asyncio
import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault('INVERTER_IP', '127.0.0.1')

import bms_poller
import main
from tests.test_bms_poller import CELLS, SUMMARY, WHEN

CONFIG = bms_poller.BmsConfig('h', 7)


class FakeLoop:
    def __init__(self):
        self.coros = []

    def create_task(self, coro):
        self.coros.append(coro.__qualname__)
        coro.close()


class CreateLoopTasksTest(unittest.TestCase):
    def setUp(self):
        self._orig = (main.dry_run, main.BMS_CONFIG)

    def tearDown(self):
        main.dry_run, main.BMS_CONFIG = self._orig

    def tasks(self):
        loop = FakeLoop()
        main.asyncio_thread._create_loop_tasks(loop)
        return loop.coros

    def test_bms_task_started_when_configured(self):
        main.dry_run, main.BMS_CONFIG = False, CONFIG
        self.assertIn('AsyncioThread._run_bms_poller', self.tasks())

    def test_no_bms_task_when_disabled(self):
        main.dry_run, main.BMS_CONFIG = False, None
        self.assertNotIn('AsyncioThread._run_bms_poller', self.tasks())

    def test_no_bms_task_in_dry_run(self):
        main.dry_run, main.BMS_CONFIG = True, CONFIG
        self.assertEqual(self.tasks(), [])


class BmsSampleHandlerTest(unittest.TestCase):
    def test_db_failure_still_publishes(self):
        sample = bms_poller.decode(SUMMARY, CELLS, WHEN)
        publish = mock.AsyncMock()
        with mock.patch('bms_storage.insert_sample', mock.AsyncMock(side_effect=RuntimeError('locked'))), \
                mock.patch.object(main.mqtt, 'publish_bms', publish), \
                self.assertLogs('main', 'WARNING'):
            asyncio.run(main._bms_sample_handler(conn=object())(sample))
        publish.assert_awaited_once()
        self.assertNotIn('raw_1100', publish.await_args.args[0])


class RunBmsPollerTest(unittest.TestCase):
    def test_unopenable_db_logs_error_and_returns(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch('bms_storage.BMS_DB_PATH', os.path.join(tmp, 'missing-dir', 'bms.db')), \
                self.assertLogs('main', 'ERROR') as logs:
            asyncio.run(main.asyncio_thread._run_bms_poller(CONFIG))
        self.assertIn('BMS poller disabled', logs.output[0])


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `../../venv/bin/python -m unittest tests.test_main_bms -v`
Expected: ERROR `AttributeError: module 'main' has no attribute 'BMS_CONFIG'`

- [ ] **Step 3: Implement**

In `main.py`:

a) Imports, in the local-module block (alphabetical, before `import control`):

```python
import bms_poller
import bms_storage
```

b) After the `MQTT_TOPIC_PREFIX = ...` line:

```python
# Optional Pylontech BMS poller (read through its SolarMan logger) - None
# unless BMS_LOGGER_HOST and BMS_LOGGER_SERIAL are set; see bms_poller.py.
BMS_CONFIG = bms_poller.load_bms_config(os.environ)
```

c) Replace the body of `AsyncioThread.run()`'s `try:` block, and add the new
methods right after `run()`:

```python
    def run(self):
        loop = asyncio.new_event_loop()
        self._asyncio_loop = loop
        asyncio.set_event_loop(loop)
        try:
            self._create_loop_tasks(loop)
            loop.run_forever()
        finally:
            self._drain_and_close_loop(loop)
            logger.info("Finished the asyncio loop")

    def _create_loop_tasks(self, loop) -> None:
        if dry_run:
            return
        loop.create_task(self._get_inverter_data_with_retry())
        if BMS_CONFIG is not None:
            loop.create_task(self._run_bms_poller(BMS_CONFIG))

    async def _run_bms_poller(self, config: bms_poller.BmsConfig) -> None:
        """Runs until the loop shuts down (cancelled by _drain_and_close_loop).
        Any failure here stays here - the BMS is optional, the inverter poll
        on the same loop is not."""
        try:
            conn = await bms_storage.init_db_async()
        except Exception as e:
            logger.error(f'BMS poller disabled: cannot open {bms_storage.BMS_DB_PATH}: {e!r}')
            return
        logger.info(f'BMS poller started ({config.host}:{config.port}, every {config.poll_seconds} s)')
        try:
            await bms_poller.BmsPoller(config, _bms_sample_handler(conn)).run()
        finally:
            await conn.close()
```

d) After `mqtt = mqtt_bridge.MqttBridge(...)`:

```python
def _bms_sample_handler(conn):
    async def on_sample(sample: bms_poller.BmsSample) -> None:
        try:
            await bms_storage.insert_sample(conn, sample)
        except Exception as e:
            logger.warning(f'BMS sample not stored: {e!r}')
        await mqtt.publish_bms(bms_poller.sample_to_payload(sample))
    return on_sample
```

`requirements.txt`: add in alphabetical position:

```
pysolarmanv5==3.0.6
```

between `pyparsing` and `python-dateutil`, and

```
umodbus==1.0.4
```

after `typing_extensions`.

`.env.example`: append:

```
# Optional: Pylontech BMS via its SolarMan Wi-Fi logger (cell temperatures,
# SOH, cell voltages -> bms.db and MQTT goodwe/bms). Leave empty to disable.
BMS_LOGGER_HOST=
BMS_LOGGER_SERIAL=
BMS_LOGGER_PORT=8899
BMS_SLAVE_ID=1
BMS_POLL_SECONDS=60
```

`CHANGELOG.md`: at the top of `## Unreleased` → `### Changed`:

```markdown
- Optional Pylontech BMS poller: with `BMS_LOGGER_HOST` / `BMS_LOGGER_SERIAL`
  set in `.env`, cell temperatures, SOH, and every cell's voltage are read
  from the BMS through its SolarMan logger every 60 s, stored in `bms.db`,
  and published on MQTT `goodwe/bms`. Run `pip install -r requirements.txt`
  after pulling (new dependency `pysolarmanv5`). Without those settings
  nothing changes.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `../../venv/bin/python -m unittest tests.test_main_bms -v`
Expected: all PASS.

- [ ] **Step 5: Run the whole suite**

Run: `../../venv/bin/python -m unittest discover -s tests`
Expected: OK. The original 488 tests plus the new ones must all pass,
especially `test_main_dry_run_guards` (`run()` behaviour in dry-run is
unchanged).

- [ ] **Step 6: Commit**

```bash
git add main.py requirements.txt .env.example CHANGELOG.md tests/test_main_bms.py
git commit -m "BMS poller: start it from main.py when configured

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Deploy on the Pi and verify (manual, with the user)

Not code. Do these after the branch is reviewed, timed between executor
commands. Check `sensor.goodwe_control_mode` first: a restart drops the
active command until Predbat's next call.

- [ ] The Pi runs `predbat-control-executor` plus a stale merge of the
  closed PR #41. PR #40 is merged, so switch it to `main` once this branch
  is merged. Before that, to test, use a detached checkout of
  `origin/bms-poller`. Then run `venv/bin/pip install -r requirements.txt`.
- [ ] Check the logger's current IP (discovery broadcast on UDP 48899 from
  `wlan0`) and add `BMS_LOGGER_HOST=<ip>` and
  `BMS_LOGGER_SERIAL=4060493924` to `.env`.
- [ ] Restart. The log shows "BMS poller started". Within 2 min, `bms.db`
  has rows whose `cell_temp_max`/`_min` and `soh` match the SolarMan app.
- [ ] `inverter_history` rows per minute stay about 60 (compare the hour
  before and after).
- [ ] The next day: the SolarMan app kept updating, and there are no
  repeated backoff WARNINGs in `manager.log`.
- [ ] During a charge and during an export, read `raw_1100[4:6]`
  (0x1104/0x1105) from `bms.db` and compare with GoodWe `ibattery1`. If one
  of them matches current, open a follow-up to add the column plus a
  backfill from `raw_1100`.
