"""
SQLite storage for inverter telemetry: raw per-sample history
(inverter_history) and derived per-hour totals (hourly_summary).
See docs/superpowers/specs/2026-08-27-sqlite-storage-design.md.
"""
import sqlite3
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Tuple

import aiosqlite

DATA_DB_PATH = 'data.db'


def parse_timestamp_epoch(timestamp: str) -> int:
    """Converts a sensor 'YYYY-MM-DD HH:MM:SS' local-time string into a unix
    epoch integer. Done in Python (not SQL) because the timestamp is local
    time, and SQLite's unixepoch() assumes UTC input.
    """
    return int(datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').timestamp())


def current_hour_bounds(now: datetime) -> Tuple[int, int]:
    """Returns (start_epoch, end_epoch) for the hour containing `now`."""
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    start_epoch = int(hour_start.timestamp())
    return start_epoch, start_epoch + 3600


def _column_ddl(columns: Iterable[Tuple[str, str]]) -> str:
    return ',\n            '.join(f'{name} {sql_type}' for name, sql_type in columns)


def build_ddl_statements(columns: list) -> list:
    return [
        "PRAGMA journal_mode = WAL",
        "PRAGMA auto_vacuum = INCREMENTAL",
        f"""
        CREATE TABLE IF NOT EXISTS inverter_history (
            id INTEGER PRIMARY KEY,
            timestamp_epoch INTEGER NOT NULL,
            {_column_ddl(columns)}
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_inverter_history_timestamp_epoch "
        "ON inverter_history (timestamp_epoch)",
        """
        CREATE TABLE IF NOT EXISTS hourly_summary (
            hour_start INTEGER PRIMARY KEY,
            meter_export_kwh REAL,
            meter_import_kwh REAL,
            load_kwh REAL,
            pv_kwh REAL,
            battery_charge_kwh REAL,
            battery_discharge_kwh REAL
        )
        """,
    ]


def init_db_sync(path: str, columns: list) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    for statement in build_ddl_statements(columns):
        conn.execute(statement)
    conn.commit()
    return conn


async def init_db_async(path: str, columns: list) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(path)
    for statement in build_ddl_statements(columns):
        await conn.execute(statement)
    await conn.commit()
    return conn


def _row_with_epoch(row: Mapping[str, Any]) -> dict:
    full_row = dict(row)
    full_row['timestamp_epoch'] = parse_timestamp_epoch(row['timestamp'])
    return full_row


def _insert_sql(column_names: list) -> str:
    placeholders = ', '.join('?' for _ in column_names)
    columns_sql = ', '.join(column_names)
    return f"INSERT INTO inverter_history ({columns_sql}) VALUES ({placeholders})"


def insert_sample_sync(conn: sqlite3.Connection, row: Mapping[str, Any]) -> None:
    full_row = _row_with_epoch(row)
    column_names = list(full_row.keys())
    conn.execute(_insert_sql(column_names), [full_row[c] for c in column_names])
    conn.commit()


async def insert_sample_async(conn: aiosqlite.Connection, row: Mapping[str, Any]) -> None:
    full_row = _row_with_epoch(row)
    column_names = list(full_row.keys())
    await conn.execute(_insert_sql(column_names), [full_row[c] for c in column_names])
    await conn.commit()


_HOUR_START_QUERY = (
    "SELECT * FROM inverter_history "
    "WHERE timestamp_epoch >= ? AND timestamp_epoch < ? "
    "ORDER BY timestamp_epoch ASC LIMIT 1"
)


def get_current_hour_start_sample(conn: sqlite3.Connection, hour_start_epoch: int,
                                  hour_end_epoch: int) -> Optional[dict]:
    cursor = conn.execute(_HOUR_START_QUERY, (hour_start_epoch, hour_end_epoch))
    row = cursor.fetchone()
    if row is None:
        return None
    column_names = [d[0] for d in cursor.description]
    return dict(zip(column_names, row))


async def get_current_hour_start_sample_async(conn: aiosqlite.Connection, hour_start_epoch: int,
                                               hour_end_epoch: int) -> Optional[dict]:
    async with conn.execute(_HOUR_START_QUERY, (hour_start_epoch, hour_end_epoch)) as cursor:
        row = await cursor.fetchone()
        if row is None:
            return None
        column_names = [d[0] for d in cursor.description]
        return dict(zip(column_names, row))
