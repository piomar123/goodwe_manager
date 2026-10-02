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
import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Awaitable, Callable, List, Mapping, Optional

logger = logging.getLogger(__name__)

SUMMARY_START = 0x1100
SUMMARY_COUNT = 64
CELLS_START = 0x1500
CELLS_END = 0x1600  # exclusive; only 0x1500-0x153F is verified to respond
CHUNK = 32
CELLS_PER_MODULE = 30  # Force H2 module: 30 LFP cells (98.25 V / 30 = 3.275 V)
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


class BmsDecodeError(ValueError):
    pass


STATES = {1: 'charge', 2: 'discharge', 3: 'idle'}  # 0x1100 bits 0-2; other values not seen yet


@dataclass(frozen=True)
class BmsSample:
    """Units: V, A (current positive = charging), °C, %, kWh. Ids are 0-based
    (cell 0-59, module 0-1 on a 2-module pack)."""
    timestamp: str
    timestamp_epoch: int
    state: str
    pack_voltage: float
    current: float
    bms_temperature: float
    soc: int
    soh: int
    remaining_capacity: float
    cycle_count: int
    charge_voltage_limit: float
    charge_current_limit: float
    discharge_voltage_limit: float
    discharge_current_limit: float
    cell_voltage_max: float
    cell_voltage_min: float
    cell_voltage_max_id: int
    cell_voltage_min_id: int
    cell_temp_max: float
    cell_temp_min: float
    cell_temp_max_id: int
    cell_temp_min_id: int
    module_voltage_max: float
    module_voltage_min: float
    module_voltage_max_id: int
    module_voltage_min_id: int
    module_temp_max: float
    module_temp_min: float
    module_temp_max_id: int
    module_temp_min_id: int
    charge_today_wh: int
    discharge_today_wh: int
    charge_total_kwh: int
    discharge_total_kwh: int
    fully_charged: bool
    module_voltages: List[float]
    cell_mv: List[int]
    raw_1100: List[int]


def _signed(value: int) -> int:
    return value - 0x10000 if value >= 0x8000 else value


def _signed32(high: int, low: int) -> int:
    value = (high << 16) | low
    return value - 0x100000000 if value >= 0x80000000 else value


def decode(summary: List[int], cells: List[int], when: datetime) -> BmsSample:
    """Decode the 0x1100-0x113F summary block and the cell block read from
    0x1500 into a BmsSample, then run the plausibility checks. Raises
    BmsDecodeError if anything doesn't fit."""
    if len(summary) != SUMMARY_COUNT:
        raise BmsDecodeError(f'summary block has {len(summary)} registers, expected {SUMMARY_COUNT}')

    def reg(addr: int) -> int:
        return summary[addr - SUMMARY_START]

    def reg32(addr: int) -> int:
        return _signed32(reg(addr), reg(addr + 1))

    cell_mv = []
    for value in cells:
        if value == 0:
            break
        cell_mv.append(value)
    # Per-module voltages are summed from the cells: the BMS only reports
    # the max/min module (0x1118/0x1119) and their ids (0x111A/0x111B).
    modules = [sum(cell_mv[i:i + CELLS_PER_MODULE]) / 1000 for i in range(0, len(cell_mv), CELLS_PER_MODULE)]
    status = reg(0x1100) & 0x7

    sample = BmsSample(
        timestamp=when.strftime('%Y-%m-%d %H:%M:%S'),
        timestamp_epoch=int(when.timestamp()),
        state=STATES.get(status, f'unknown ({status})'),
        pack_voltage=reg(0x1103) / 10,
        current=reg32(0x1104) / 100,
        bms_temperature=_signed(reg(0x1106)) / 10,
        soc=reg(0x1107),
        soh=reg(0x1120),
        remaining_capacity=reg32(0x1121) / 1000,  # Wh in the register
        cycle_count=reg(0x1108),
        charge_voltage_limit=reg(0x1109) / 10,
        charge_current_limit=reg32(0x110A) / 100,
        discharge_voltage_limit=reg(0x110C) / 10,
        discharge_current_limit=abs(reg32(0x110D)) / 100,  # the BMS reports it negative
        cell_voltage_max=reg(0x1110) / 1000,
        cell_voltage_min=reg(0x1111) / 1000,
        cell_voltage_max_id=reg(0x1112),
        cell_voltage_min_id=reg(0x1113),
        cell_temp_max=_signed(reg(0x1114)) / 10,
        cell_temp_min=_signed(reg(0x1115)) / 10,
        cell_temp_max_id=reg(0x1116),
        cell_temp_min_id=reg(0x1117),
        module_voltage_max=reg(0x1118) / 100,
        module_voltage_min=reg(0x1119) / 100,
        module_voltage_max_id=reg(0x111A),
        module_voltage_min_id=reg(0x111B),
        module_temp_max=_signed(reg(0x111C)) / 10,
        module_temp_min=_signed(reg(0x111D)) / 10,
        module_temp_max_id=reg(0x111E),
        module_temp_min_id=reg(0x111F),
        charge_today_wh=reg32(0x1123),
        discharge_today_wh=reg32(0x1125),
        charge_total_kwh=reg32(0x112B),
        discharge_total_kwh=reg32(0x112D),
        fully_charged=bool(reg(0x1138)),
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
    for name in ('bms_temperature', 'cell_temp_max', 'cell_temp_min', 'module_temp_max', 'module_temp_min'):
        value = getattr(s, name)
        if not -30 <= value <= 80:
            raise BmsDecodeError(f'{name} temperature {value} °C outside -30..80')
    total = sum(s.cell_mv) / 1000
    if abs(total - s.pack_voltage) > 0.02 * s.pack_voltage:
        raise BmsDecodeError(f'cells sum {total:.2f} V, pack reads {s.pack_voltage} V')
    for name, expected in (('module_voltage_max', max(s.module_voltages)),
                           ('module_voltage_min', min(s.module_voltages))):
        value = getattr(s, name)
        if abs(value - expected) > 0.02 * expected:
            raise BmsDecodeError(f'{name} {value} V, the cells sum to {expected:.2f} V')


BACKOFF_AFTER_FAILURES = 3
MAX_INTERVAL_S = 600


class PollFailed(Exception):
    pass


def sample_to_payload(sample: BmsSample) -> dict:
    """MQTT payload: every decoded field (numbers, `state` a string,
    `fully_charged` a bool); the raw register block stays in bms.db only."""
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
