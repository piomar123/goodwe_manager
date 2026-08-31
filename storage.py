"""
SQLite storage for inverter telemetry: raw per-sample history
(inverter_history) and derived per-hour totals (hourly_summary).
See docs/superpowers/specs/2026-08-27-sqlite-storage-design.md.
"""
import json
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


_HOURLY_SUMMARY_COLUMNS = [
    ('meter_export_kwh', 'REAL'),
    ('meter_import_kwh', 'REAL'),
    ('load_kwh', 'REAL'),
    ('pv_kwh', 'REAL'),
    ('battery_charge_kwh', 'REAL'),
    ('battery_discharge_kwh', 'REAL'),
    ('sample_count', 'INTEGER'),
    ('vgrid_min', 'REAL'),
    ('vgrid_max', 'REAL'),
    ('vgrid2_min', 'REAL'),
    ('vgrid2_max', 'REAL'),
    ('vgrid3_min', 'REAL'),
    ('vgrid3_max', 'REAL'),
    ('fgrid_min', 'REAL'),
    ('fgrid_max', 'REAL'),
    ('fgrid2_min', 'REAL'),
    ('fgrid2_max', 'REAL'),
    ('fgrid3_min', 'REAL'),
    ('fgrid3_max', 'REAL'),
    ('inverter_temp_min', 'REAL'),
    ('inverter_temp_max', 'REAL'),
    ('battery_temp_min', 'REAL'),
    ('battery_temp_max', 'REAL'),
    ('work_mode_breakdown', 'TEXT'),
]


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
        f"""
        CREATE TABLE IF NOT EXISTS hourly_summary (
            hour_start INTEGER PRIMARY KEY,
            {_column_ddl(_HOURLY_SUMMARY_COLUMNS)}
        )
        """,
    ]


def _missing_columns(existing_column_names: Iterable[str], columns: list) -> list:
    """Returns the (name, sql_type) pairs from `columns` that aren't already
    present in `existing_column_names`, preserving `columns`' order.
    """
    existing = set(existing_column_names)
    return [(name, sql_type) for name, sql_type in columns if name not in existing]


def _reconcile_table_columns_sync(conn: sqlite3.Connection, table: str, columns: list) -> None:
    """Adds any column present in `columns` but missing from `table` (e.g.
    after SELECTED_SENSORS grows, or hourly_summary gains a new derived
    metric), so an existing data.db doesn't start raising 'no such column'
    on every insert. CREATE TABLE IF NOT EXISTS alone can't do this, since
    it's a no-op once the table already exists.
    """
    existing = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
    for name, sql_type in _missing_columns(existing, columns):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")


async def _reconcile_table_columns_async(conn: aiosqlite.Connection, table: str, columns: list) -> None:
    cursor = await conn.execute(f"PRAGMA table_info({table})")
    existing = [row[1] for row in await cursor.fetchall()]
    for name, sql_type in _missing_columns(existing, columns):
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")


def init_db_sync(path: str, columns: list) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    for statement in build_ddl_statements(columns):
        conn.execute(statement)
    _reconcile_table_columns_sync(conn, 'inverter_history', columns)
    _reconcile_table_columns_sync(conn, 'hourly_summary', _HOURLY_SUMMARY_COLUMNS)
    conn.commit()
    return conn


async def init_db_async(path: str, columns: list) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(path)
    for statement in build_ddl_statements(columns):
        await conn.execute(statement)
    await _reconcile_table_columns_async(conn, 'inverter_history', columns)
    await _reconcile_table_columns_async(conn, 'hourly_summary', _HOURLY_SUMMARY_COLUMNS)
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


def insert_sample_sync(conn: sqlite3.Connection, row: Mapping[str, Any], commit: bool = True) -> None:
    full_row = _row_with_epoch(row)
    column_names = list(full_row.keys())
    conn.execute(_insert_sql(column_names), [full_row[c] for c in column_names])
    if commit:
        conn.commit()


_DEFAULT_BATCH_SIZE = 5000


def insert_samples_batch(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]], commit: bool = True,
                          batch_size: int = _DEFAULT_BATCH_SIZE) -> None:
    """Bulk equivalent of insert_sample_sync(), for callers inserting many
    rows at once (e.g. CSV migration) rather than one sample per call. The
    per-row path is fine for live polling (~1 row/second), but at millions
    of rows its per-call SQL-string rebuild and Python-level loop overhead
    dominates - conn.executemany() with a single precomputed SQL string
    avoids both by preparing the statement once.

    Rows are grouped into chunks of `batch_size` and passed to
    conn.executemany() one chunk at a time, rather than materializing the
    whole `rows` iterable into one parameter list up front. A real
    migration file can be 300k+ rows - building the entire parameter list
    at once pushed a 3.7GB Raspberry Pi to the edge of OOM during an actual
    migration run (RSS hit 3.3GB, swap exhausted). Chunking bounds peak
    extra memory to O(batch_size) regardless of the input's total size.

    Every row must have the same set of keys (true for rows read from a
    single CSV file's DictReader) - the column list is taken from the
    first row only. A no-op for an empty `rows`.
    """
    column_names = None
    sql = None
    chunk = []
    for row in rows:
        full_row = _row_with_epoch(row)
        if column_names is None:
            column_names = list(full_row.keys())
            sql = _insert_sql(column_names)
        chunk.append([full_row[c] for c in column_names])
        if len(chunk) >= batch_size:
            conn.executemany(sql, chunk)
            chunk = []
    if chunk:
        conn.executemany(sql, chunk)
    if commit:
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


def find_hours_needing_backfill(conn: sqlite3.Connection, full_rescan: bool = False) -> list:
    """Every hour at or after the watermark (the last hour already
    backfilled) is scanned for buckets, rather than the whole table, so this
    stays cheap as inverter_history grows towards the retention window's
    full 180 days - everything before the watermark is already backfilled
    (with real values or documented NULLs) and will never need revisiting.
    COALESCE(..., 0) makes a fresh/empty hourly_summary fall back to
    scanning everything, matching the original full-table behavior for the
    initial migration case. Uses the existing timestamp_epoch index for a
    bounded range scan instead of a full-table DISTINCT.

    full_rescan=True ignores the watermark and scans the whole table - for
    one-off fixes where a gap sits *behind* the watermark and would
    otherwise never be revisited: backdated data imported after the live
    watermark already advanced past it, or a hourly_summary row manually
    deleted to force a recompute (e.g. after an aggregation-logic change)
    without also clearing every row after it.
    """
    watermark_sql = (
        "SELECT 0 AS start_epoch" if full_rescan
        else "SELECT COALESCE(MAX(hour_start), 0) AS start_epoch FROM hourly_summary"
    )
    cursor = conn.execute(f"""
        WITH watermark AS ({watermark_sql}),
        buckets AS (
            SELECT DISTINCT (timestamp_epoch / 3600) * 3600 AS bucket
            FROM inverter_history, watermark
            WHERE timestamp_epoch >= watermark.start_epoch
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


_QUALITY_STAT_COLUMNS = [
    'sample_count',
    'vgrid_min', 'vgrid_max', 'vgrid2_min', 'vgrid2_max', 'vgrid3_min', 'vgrid3_max',
    'fgrid_min', 'fgrid_max', 'fgrid2_min', 'fgrid2_max', 'fgrid3_min', 'fgrid3_max',
    'inverter_temp_min', 'inverter_temp_max', 'battery_temp_min', 'battery_temp_max',
]

# Each phase is aggregated independently (rather than a combined
# worst-case across phases), so a NULL phase (e.g. a single-phase
# inverter never populating vgrid2/vgrid3) just yields NULL for that
# phase's columns without affecting the others.
_QUALITY_STATS_QUERY = """
    SELECT COUNT(*),
        MIN(vgrid), MAX(vgrid), MIN(vgrid2), MAX(vgrid2), MIN(vgrid3), MAX(vgrid3),
        MIN(fgrid), MAX(fgrid), MIN(fgrid2), MAX(fgrid2), MIN(fgrid3), MAX(fgrid3),
        MIN(temperature), MAX(temperature),
        MIN(battery_temperature), MAX(battery_temperature)
    FROM inverter_history WHERE timestamp_epoch >= ? AND timestamp_epoch < ?
"""


def _hour_quality_stats(conn: sqlite3.Connection, start_epoch: int, end_epoch: int) -> dict:
    """Returns sample_count plus per-hour min/max grid-quality and
    temperature stats. Unlike _max_counters, these aren't diffed against
    the previous hour - they're computed directly over the hour's own
    samples, so they're available even for an hour with no usable baseline.
    """
    row = conn.execute(_QUALITY_STATS_QUERY, (start_epoch, end_epoch)).fetchone()
    return dict(zip(_QUALITY_STAT_COLUMNS, row))


def _hour_work_mode_breakdown(conn: sqlite3.Connection, start_epoch: int, end_epoch: int) -> Optional[str]:
    """Returns a JSON object mapping each work_mode_label seen in the hour
    to its sample count (e.g. '{"Normal (On-Grid)": 58, "Fault": 2}'), so a
    brief fault doesn't get silently outvoted by a single majority-label
    summary. None if the hour has no samples.
    """
    rows = conn.execute(
        "SELECT work_mode_label, COUNT(*) FROM inverter_history "
        "WHERE timestamp_epoch >= ? AND timestamp_epoch < ? GROUP BY work_mode_label",
        (start_epoch, end_epoch),
    ).fetchall()
    if not rows:
        return None
    return json.dumps(dict(rows))


def backfill_hourly_summary(conn: sqlite3.Connection, full_rescan: bool = False) -> int:
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

    full_rescan is passed straight through to find_hours_needing_backfill -
    see its docstring for when a one-off full rescan is needed instead of
    the normal watermark-bounded scan.
    """
    backfilled = 0
    for hour_start in find_hours_needing_backfill(conn, full_rescan=full_rescan):
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
        quality = _hour_quality_stats(conn, hour_start, hour_start + 3600)
        work_mode_breakdown = _hour_work_mode_breakdown(conn, hour_start, hour_start + 3600)
        conn.execute(
            """
            INSERT OR REPLACE INTO hourly_summary
                (hour_start, meter_export_kwh, meter_import_kwh, load_kwh, pv_kwh,
                 battery_charge_kwh, battery_discharge_kwh, sample_count,
                 vgrid_min, vgrid_max, vgrid2_min, vgrid2_max, vgrid3_min, vgrid3_max,
                 fgrid_min, fgrid_max, fgrid2_min, fgrid2_max, fgrid3_min, fgrid3_max,
                 inverter_temp_min, inverter_temp_max, battery_temp_min, battery_temp_max,
                 work_mode_breakdown)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (hour_start, metrics['meter_export_kwh'], metrics['meter_import_kwh'], metrics['load_kwh'],
             metrics['pv_kwh'], metrics['battery_charge_kwh'], metrics['battery_discharge_kwh'],
             quality['sample_count'],
             quality['vgrid_min'], quality['vgrid_max'], quality['vgrid2_min'], quality['vgrid2_max'],
             quality['vgrid3_min'], quality['vgrid3_max'],
             quality['fgrid_min'], quality['fgrid_max'], quality['fgrid2_min'], quality['fgrid2_max'],
             quality['fgrid3_min'], quality['fgrid3_max'],
             quality['inverter_temp_min'], quality['inverter_temp_max'],
             quality['battery_temp_min'], quality['battery_temp_max'],
             work_mode_breakdown),
        )
        conn.commit()
        backfilled += 1
    return backfilled
