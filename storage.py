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


_HOURLY_METRIC_COLUMNS = [
    ('meter_e_total_exp', 'meter_export_kwh'),
    ('meter_e_total_imp', 'meter_import_kwh'),
    ('e_load_total', 'load_kwh'),
    ('e_day', 'pv_kwh'),
    ('e_bat_charge_total', 'battery_charge_kwh'),
    ('e_bat_discharge_total', 'battery_discharge_kwh'),
]


def find_hours_needing_backfill(conn: sqlite3.Connection) -> list:
    cursor = conn.execute("""
        WITH buckets AS (
            SELECT DISTINCT (timestamp_epoch / 3600) * 3600 AS bucket FROM inverter_history
        )
        SELECT bucket FROM buckets b
        WHERE bucket NOT IN (SELECT hour_start FROM hourly_summary)
          AND EXISTS (SELECT 1 FROM buckets nxt WHERE nxt.bucket = b.bucket + 3600)
        ORDER BY bucket
    """)
    return [row[0] for row in cursor.fetchall()]


def _max_counters(conn: sqlite3.Connection, start_epoch: int, end_epoch: int) -> Optional[dict]:
    source_columns = [source for source, _ in _HOURLY_METRIC_COLUMNS]
    select_sql = ', '.join(f'MAX({c})' for c in source_columns)
    row = conn.execute(
        f"SELECT {select_sql} FROM inverter_history WHERE timestamp_epoch >= ? AND timestamp_epoch < ?",
        (start_epoch, end_epoch),
    ).fetchone()
    if row is None or all(v is None for v in row):
        return None
    return dict(zip(source_columns, row))


def backfill_hourly_summary(conn: sqlite3.Connection) -> int:
    """Derives hourly_summary rows from inverter_history for every hour that
    has data in the following hour (proving it's complete) and doesn't have
    a hourly_summary row yet. If there's no data at all in the preceding
    hour, the row is still inserted (so it's not reprocessed every run) but
    with NULL metrics, since a true diff can't be computed without a prior
    baseline. Returns the number of hours backfilled.

    pv_kwh (sourced from e_day) is a special case: e_day resets to 0 right
    after local midnight, unlike the other lifetime-cumulative counters, so
    the hour whose start crosses a calendar-day boundary uses e_day's
    current-hour value alone rather than diffing against the previous
    (different day's) value.
    """
    backfilled = 0
    for hour_start in find_hours_needing_backfill(conn):
        current = _max_counters(conn, hour_start, hour_start + 3600)
        previous = _max_counters(conn, hour_start - 3600, hour_start)
        # fromtimestamp() (no tzinfo) uses the host's local timezone, the
        # exact symmetric inverse of parse_timestamp_epoch()'s naive-local
        # strptime(...).timestamp() - so .date() reflects the same local
        # calendar day the inverter itself resets e_day at. This requires
        # the host's system timezone to be set to the inverter's own
        # timezone (Europe/Warsaw); see the Global Constraints note.
        crosses_midnight = (
            datetime.fromtimestamp(hour_start).date() != datetime.fromtimestamp(hour_start - 3600).date()
        )
        metrics = {}
        for source, target in _HOURLY_METRIC_COLUMNS:
            if current is None:
                metrics[target] = None
            elif source == 'e_day' and crosses_midnight:
                metrics[target] = current[source]
            elif previous is None:
                metrics[target] = None
            elif current[source] is None or previous[source] is None:
                metrics[target] = None
            else:
                metrics[target] = current[source] - previous[source]
        conn.execute(
            """
            INSERT OR REPLACE INTO hourly_summary
                (hour_start, meter_export_kwh, meter_import_kwh, load_kwh, pv_kwh,
                 battery_charge_kwh, battery_discharge_kwh)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (hour_start, metrics['meter_export_kwh'], metrics['meter_import_kwh'], metrics['load_kwh'],
             metrics['pv_kwh'], metrics['battery_charge_kwh'], metrics['battery_discharge_kwh']),
        )
        conn.commit()
        backfilled += 1
    return backfilled
