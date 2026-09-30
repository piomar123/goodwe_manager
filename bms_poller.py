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
from typing import List, Mapping, Optional

logger = logging.getLogger(__name__)

SUMMARY_START = 0x1100
SUMMARY_COUNT = 64
CELLS_START = 0x1500
CELLS_END = 0x1600  # exclusive; only 0x1500-0x153F is verified to respond
CHUNK = 32
MAX_MODULE_SLOTS = 4  # 0x1118-0x111B; 0x111C onwards holds temperatures
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
