"""
One-off migration of legacy data-*.csv files into data.db (inverter_history).
Safe to re-run: already-migrated files are skipped via the csv_migration_log
manifest table. Never deletes the source CSV files - keep them on disk at
least as long as inverter_history's own retention window (180 days) past a
successful, verified migration.

Usage: python _migrate_csv_to_sqlite.py [--dry-run] [--csv-dir DIR] [--db-path PATH]
"""
import argparse
import csv
import logging
import sqlite3
import time
from pathlib import Path

import storage
from sensors import sensor_columns

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {'timestamp', 'meter_e_total_exp', 'meter_e_total_imp', 'e_load_total'}


def ensure_migration_log_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS csv_migration_log (
            filename TEXT PRIMARY KEY,
            row_count INTEGER,
            migrated_at INTEGER,
            status TEXT
        )
    """)
    conn.commit()


def already_migrated(conn: sqlite3.Connection, filename: str) -> bool:
    row = conn.execute(
        "SELECT status FROM csv_migration_log WHERE filename = ?", (filename,)
    ).fetchone()
    return row is not None and row[0] == 'done'


def read_csv_rows(csv_path: Path) -> list:
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return []
        if not REQUIRED_COLUMNS.issubset(set(reader.fieldnames)):
            return []
        return list(reader)


def _record_migration(conn: sqlite3.Connection, filename: str, row_count: int, status: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO csv_migration_log (filename, row_count, migrated_at, status) "
        "VALUES (?, ?, ?, ?)",
        (filename, row_count, int(time.time()), status),
    )
    conn.commit()


def migrate_file(conn: sqlite3.Connection, csv_path: Path, dry_run: bool) -> str:
    filename = csv_path.name
    if already_migrated(conn, filename):
        logger.info(f"Skipping {filename}: already migrated")
        return 'skipped'

    rows = read_csv_rows(csv_path)
    if not rows:
        logger.warning(f"Skipping {filename}: empty or missing required columns")
        return 'skipped'

    if dry_run:
        logger.info(f"[dry-run] Would import {len(rows)} rows from {filename}")
        return 'dry-run'

    valid_columns = {name for name, _ in sensor_columns()}
    inserted = 0
    try:
        for row in rows:
            filtered_row = {k: v for k, v in row.items() if k in valid_columns}
            storage.insert_sample_sync(conn, filtered_row, commit=False)
            inserted += 1
    except sqlite3.Error as e:
        logger.error(f"{filename}: insert failed after {inserted}/{len(rows)} rows: {e}")
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        try:
            _record_migration(conn, filename, len(rows), 'error')
        except sqlite3.Error:
            # the database is contested enough that even recording the
            # failure failed - migrate_file still reports 'error' to its
            # caller rather than crashing; the file stays unmarked in the
            # manifest, so a later re-run will retry it from scratch
            logger.error(f"{filename}: could not record migration failure in the manifest (database still locked)")
        return 'error'

    # nothing is committed yet - the whole file's rows are still pending in
    # this transaction, so a failed verification below can still be rolled
    # back cleanly rather than leaving partial/incorrect data in place
    epoch_first = storage.parse_timestamp_epoch(rows[0]['timestamp'])
    epoch_last = storage.parse_timestamp_epoch(rows[-1]['timestamp'])
    range_start, range_end = min(epoch_first, epoch_last), max(epoch_first, epoch_last)
    db_row_count = conn.execute(
        "SELECT COUNT(*) FROM inverter_history WHERE timestamp_epoch BETWEEN ? AND ?",
        (range_start, range_end),
    ).fetchone()[0]

    first_row_db = conn.execute(
        "SELECT meter_e_total_exp FROM inverter_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
        (rows[0]['timestamp'],),
    ).fetchone()
    last_row_db = conn.execute(
        "SELECT meter_e_total_exp FROM inverter_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
        (rows[-1]['timestamp'],),
    ).fetchone()
    verified = (
        db_row_count == len(rows)
        and first_row_db is not None
        and str(first_row_db[0]) == str(float(rows[0]['meter_e_total_exp']))
        and last_row_db is not None
        and str(last_row_db[0]) == str(float(rows[-1]['meter_e_total_exp']))
    )

    if verified:
        conn.commit()
        status = 'done'
        logger.info(f"{filename}: imported and verified {inserted} rows")
    else:
        conn.rollback()
        status = 'error'
        logger.error(
            f"{filename}: verification failed (db_row_count={db_row_count}/{len(rows)}, "
            "spot-check mismatch) - rolled back"
        )

    _record_migration(conn, filename, len(rows), status)
    return status


def main():
    parser = argparse.ArgumentParser(description="Migrate legacy data-*.csv files into data.db")
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--csv-dir', default='.')
    parser.add_argument('--db-path', default=storage.DATA_DB_PATH)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    conn = storage.init_db_sync(args.db_path, sensor_columns())
    ensure_migration_log_table(conn)

    csv_paths = sorted(Path(args.csv_dir).glob('data-*.csv'))
    logger.info(f"Found {len(csv_paths)} CSV files in {args.csv_dir}")

    results = {}
    for csv_path in csv_paths:
        try:
            status = migrate_file(conn, csv_path, args.dry_run)
        except Exception as e:
            # migrate_file's own try/except only covers sqlite3.Error around
            # the insert loop; a ValueError (garbage CSV cell) or
            # UnicodeDecodeError (unexpected file encoding) elsewhere in it
            # would otherwise propagate out of main() and abort every
            # remaining file. There's no clean status to record in the
            # csv_migration_log manifest here (migrate_file didn't complete
            # normally), so this just counts the file as an error and moves on.
            logger.error(f"{csv_path.name}: unexpected error, skipping: {e}")
            status = 'error'
        results[status] = results.get(status, 0) + 1

    logger.info(f"Migration summary: {results}")

    if not args.dry_run and results.get('error', 0) == 0:
        backfilled = storage.backfill_hourly_summary(conn)
        logger.info(f"Backfilled {backfilled} hourly_summary rows")
    elif results.get('error', 0):
        logger.warning("Skipping hourly_summary backfill because some files had errors - fix and re-run first")

    conn.close()


if __name__ == '__main__':
    main()
