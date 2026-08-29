"""Query-building logic for the read-only /history viewer. Kept Flask-free
so it's independently unit-testable — see the Global Constraints note in
docs/superpowers/plans/2026-08-29-history-viewer.md about why main.py can't
be imported by the test suite.
"""
import sqlite3
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

# Allow-list of "presentable" inverter_history columns, in display order.
# Deliberately a curated subset of the ~109 raw sensor fields — the rest
# are internal/diagnostic codes not meaningful to browse (see spec's
# "Interactive history viewer" section).
RAW_COLUMNS = (
    'timestamp', 'ppv', 'ppv1', 'ppv2', 'vpv1', 'vpv2', 'ipv1', 'ipv2',
    'pgrid', 'vgrid', 'igrid', 'fgrid', 'load_ptotal', 'house_consumption',
    'battery_soc', 'pbattery1', 'vbattery1', 'ibattery1', 'battery_temperature',
    'e_day', 'e_total_exp', 'e_total_imp', 'e_load_total',
    'meter_e_total_exp', 'meter_e_total_imp',
    'e_bat_charge_total', 'e_bat_discharge_total',
    'work_mode_label', 'operation_mode', 'grid_in_out_label',
    'temperature', 'temperature_air', 'rssi',
)

DEFAULT_RAW_COLUMNS = (
    'timestamp', 'ppv', 'ppv1', 'ppv2', 'pgrid', 'load_ptotal',
    'battery_soc', 'pbattery1', 'e_day', 'work_mode_label',
)

HOURLY_COLUMNS = (
    'hour_start', 'meter_export_kwh', 'meter_import_kwh', 'load_kwh',
    'pv_kwh', 'battery_charge_kwh', 'battery_discharge_kwh',
)

ALLOWED_LIMITS = (50, 100, 250, 500)
DEFAULT_LIMIT = 100


def resolve_raw_columns(requested: Optional[Iterable[str]]) -> list:
    """Filters `requested` down to RAW_COLUMNS, in RAW_COLUMNS' canonical
    order (not request order) so column position never depends on how a
    URL or localStorage value happens to be ordered. Unknown names are
    dropped silently. Falls back to DEFAULT_RAW_COLUMNS if nothing usable
    is left. 'timestamp' is always present, prepended if not requested,
    since a time-series table without it isn't meaningful.
    """
    if requested is None:
        return list(DEFAULT_RAW_COLUMNS)
    requested_set = set(requested)
    filtered = [c for c in RAW_COLUMNS if c in requested_set]
    if not filtered:
        return list(DEFAULT_RAW_COLUMNS)
    if 'timestamp' not in filtered:
        filtered = ['timestamp'] + filtered
    return filtered


def resolve_limit(value: Optional[str]) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    return parsed if parsed in ALLOWED_LIMITS else DEFAULT_LIMIT


def resolve_offset(value: Optional[str]) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0


def parse_date_or_default(value: Optional[str], default: date) -> date:
    if not value:
        return default
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        return default


def default_date_range(today: date) -> tuple:
    """Last 7 days, inclusive of today."""
    return today - timedelta(days=6), today


def date_range_to_epoch(start_date: date, end_date: date) -> tuple:
    """(start_epoch, end_epoch) where end_epoch is EXCLUSIVE - the start of
    the day after end_date - so callers can use `>= start AND < end`
    consistently with storage.py's hour-bucket convention. Both computed in
    naive local time via datetime(...).timestamp(), the same convention
    storage.py's parse_timestamp_epoch()/backfill_hourly_summary() use, so
    this requires the same host-timezone constraint (see Global
    Constraints).
    """
    start_epoch = int(datetime(start_date.year, start_date.month, start_date.day).timestamp())
    end_of_range = end_date + timedelta(days=1)
    end_epoch = int(datetime(end_of_range.year, end_of_range.month, end_of_range.day).timestamp())
    return start_epoch, end_epoch


def fetch_inverter_rows(conn: sqlite3.Connection, columns: list, start_epoch: int, end_epoch: int,
                        limit: int, offset: int) -> tuple:
    """Returns (rows, has_more). `columns` must already be the output of
    resolve_raw_columns() - re-validated here against RAW_COLUMNS as a
    defense-in-depth check before string-interpolating them into SQL,
    since column names can't be parameterized with `?` placeholders.
    """
    for column in columns:
        if column not in RAW_COLUMNS:
            raise ValueError(f"Column not in the allow-list: {column!r}")
    columns_sql = ', '.join(columns)
    cursor = conn.execute(
        f"SELECT {columns_sql} FROM inverter_history "
        "WHERE timestamp_epoch >= ? AND timestamp_epoch < ? "
        "ORDER BY timestamp_epoch DESC LIMIT ? OFFSET ?",
        (start_epoch, end_epoch, limit + 1, offset),
    )
    fetched = cursor.fetchall()
    has_more = len(fetched) > limit
    rows = [dict(zip(columns, row)) for row in fetched[:limit]]
    return rows, has_more


def fetch_hourly_rows(conn: sqlite3.Connection, start_epoch: int, end_epoch: int,
                      limit: int, offset: int) -> tuple:
    """Returns (rows, has_more). hour_start is formatted as a local
    'YYYY-MM-DD HH:00' string for display (see Global Constraints re: host
    timezone), replacing the raw epoch int.
    """
    columns_sql = ', '.join(HOURLY_COLUMNS)
    cursor = conn.execute(
        f"SELECT {columns_sql} FROM hourly_summary "
        "WHERE hour_start >= ? AND hour_start < ? "
        "ORDER BY hour_start DESC LIMIT ? OFFSET ?",
        (start_epoch, end_epoch, limit + 1, offset),
    )
    fetched = cursor.fetchall()
    has_more = len(fetched) > limit
    rows = []
    for row in fetched[:limit]:
        row_dict = dict(zip(HOURLY_COLUMNS, row))
        row_dict['hour_start'] = datetime.fromtimestamp(row_dict['hour_start']).strftime('%Y-%m-%d %H:00')
        rows.append(row_dict)
    return rows, has_more
