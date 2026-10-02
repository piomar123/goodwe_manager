"""
bms_storage.py
bms.db: one row per accepted Pylontech BMS sample (see bms_poller.py). A
separate file from data.db so the optional BMS module never touches the
inverter database or queues behind its 1 Hz writes.
"""
import json
import logging
from datetime import datetime
from typing import Optional

import aiosqlite

from bms_poller import BmsDecodeError, BmsSample, decode

logger = logging.getLogger(__name__)

BMS_DB_PATH = 'bms.db'

# 1: first deployed version (2026-10-01). 2: full register map - current,
# limits, counters, module max/min; cell_temp_* moved from 0x111C/D (module
# temperatures) to 0x1114/5, module_voltages now summed from the cells.
SCHEMA_VERSION = 2

# Column -> SQLite type, in BmsSample field order (id is the primary key).
_COLUMN_TYPES = {
    'timestamp': 'TEXT NOT NULL',
    'timestamp_epoch': 'INTEGER NOT NULL',
    'state': 'TEXT',
    'pack_voltage': 'REAL',
    'current': 'REAL',
    'bms_temperature': 'REAL',
    'soc': 'REAL',
    'soh': 'REAL',
    'remaining_capacity': 'REAL',
    'cycle_count': 'INTEGER',
    'charge_voltage_limit': 'REAL',
    'charge_current_limit': 'REAL',
    'discharge_voltage_limit': 'REAL',
    'discharge_current_limit': 'REAL',
    'cell_voltage_max': 'REAL',
    'cell_voltage_min': 'REAL',
    'cell_voltage_max_id': 'INTEGER',
    'cell_voltage_min_id': 'INTEGER',
    'cell_temp_max': 'REAL',
    'cell_temp_min': 'REAL',
    'cell_temp_max_id': 'INTEGER',
    'cell_temp_min_id': 'INTEGER',
    'module_voltage_max': 'REAL',
    'module_voltage_min': 'REAL',
    'module_voltage_max_id': 'INTEGER',
    'module_voltage_min_id': 'INTEGER',
    'module_temp_max': 'REAL',
    'module_temp_min': 'REAL',
    'module_temp_max_id': 'INTEGER',
    'module_temp_min_id': 'INTEGER',
    'charge_today_wh': 'INTEGER',
    'discharge_today_wh': 'INTEGER',
    'charge_total_kwh': 'INTEGER',
    'discharge_total_kwh': 'INTEGER',
    'fully_charged': 'INTEGER',
    'module_voltages': 'TEXT',  # JSON array of V
    'cell_mv': 'TEXT',  # JSON array of mV
    'raw_1100': 'TEXT',  # JSON array, all 64 registers 0x1100-0x113F
}
_COLUMNS = tuple(_COLUMN_TYPES)
_JSON_COLUMNS = {'module_voltages', 'cell_mv', 'raw_1100'}

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS bms_history (id INTEGER PRIMARY KEY, "
    + ', '.join(f'{c} {t}' for c, t in _COLUMN_TYPES.items()) + ")",
    "CREATE INDEX IF NOT EXISTS idx_bms_history_timestamp_epoch ON bms_history (timestamp_epoch)",
)


def _values(sample: BmsSample, columns) -> list:
    return [json.dumps(getattr(sample, c)) if c in _JSON_COLUMNS else getattr(sample, c) for c in columns]


async def _migrate(conn: aiosqlite.Connection) -> None:
    """Bring an older bms.db up to SCHEMA_VERSION: add the missing columns,
    then re-decode every row from its raw registers and cells (the spec's
    rule: a newly decoded column also fills in past rows)."""
    async with conn.execute("PRAGMA table_info('bms_history')") as cur:
        existing = {row[1] async for row in cur}
    for column, sql_type in _COLUMN_TYPES.items():
        if column not in existing:
            await conn.execute(f"ALTER TABLE bms_history ADD COLUMN {column} {sql_type.replace(' NOT NULL', '')}")
    redecoded = total = 0
    updated = [c for c in _COLUMNS if c not in ('timestamp', 'timestamp_epoch')]
    async with conn.execute("SELECT id, timestamp, raw_1100, cell_mv FROM bms_history") as cur:
        rows = await cur.fetchall()
    for row_id, timestamp, raw, cells in rows:
        total += 1
        try:
            sample = decode(json.loads(raw), json.loads(cells), datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'))
        except (BmsDecodeError, ValueError, TypeError):
            continue
        await conn.execute(f"UPDATE bms_history SET {', '.join(f'{c} = ?' for c in updated)} WHERE id = ?",
                           _values(sample, updated) + [row_id])
        redecoded += 1
    logger.info(f'bms.db schema {SCHEMA_VERSION}: re-decoded {redecoded} of {total} rows from raw registers')


async def init_db_async(path: Optional[str] = None) -> aiosqlite.Connection:
    """`path` defaults to BMS_DB_PATH, looked up at call time so tests can
    monkeypatch it (same convention as forecast_history.init_db)."""
    conn = await aiosqlite.connect(path or BMS_DB_PATH)
    try:
        await conn.execute("PRAGMA journal_mode = WAL")
        async with conn.execute("PRAGMA user_version") as cur:
            version = (await cur.fetchone())[0]
        async with conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'bms_history'") as cur:
            had_table = await cur.fetchone() is not None
        for statement in _SCHEMA:
            await conn.execute(statement)
        if had_table and version < SCHEMA_VERSION:
            await _migrate(conn)
        await conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        await conn.commit()
    except Exception:
        await conn.close()
        raise
    return conn


async def insert_sample(conn: aiosqlite.Connection, sample: BmsSample) -> None:
    await conn.execute(
        f"INSERT INTO bms_history ({', '.join(_COLUMNS)}) VALUES ({', '.join('?' * len(_COLUMNS))})",
        _values(sample, _COLUMNS))
    await conn.commit()
