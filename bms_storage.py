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
