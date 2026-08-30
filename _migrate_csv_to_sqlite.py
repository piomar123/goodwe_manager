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
from typing import Optional

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
            status TEXT,
            starting_max_id INTEGER
        )
    """)
    # existing data.db files predate starting_max_id (added for crash
    # recovery - see _mark_pending()/_pending_starting_max_id() below)
    storage._reconcile_table_columns_sync(conn, 'csv_migration_log', [('starting_max_id', 'INTEGER')])
    conn.commit()


def already_migrated(conn: sqlite3.Connection, filename: str) -> bool:
    row = conn.execute(
        "SELECT status FROM csv_migration_log WHERE filename = ?", (filename,)
    ).fetchone()
    return row is not None and row[0] == 'done'


# Rows are streamed and committed in chunks of this size, rather than
# loading a whole CSV file into memory and deferring one commit until the
# end. On the Raspberry Pi this script actually runs on (3.7GB RAM), that
# combination - full-file row list plus a single file-spanning transaction
# - drove RSS to 2.3-3.3GB and exhausted swap on this codebase's largest
# real files (~300k rows/200MB+), reproduced live during an actual
# migration run. Committing per-chunk bounds both the row list AND
# SQLite's in-flight transaction/WAL growth to O(_MIGRATION_CHUNK_SIZE)
# regardless of the file's total size.
_MIGRATION_CHUNK_SIZE = 5000


def _validate_header(fieldnames) -> bool:
    return fieldnames is not None and REQUIRED_COLUMNS.issubset(set(fieldnames))


def _record_migration(conn: sqlite3.Connection, filename: str, row_count: int, status: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO csv_migration_log (filename, row_count, migrated_at, status) "
        "VALUES (?, ?, ?, ?)",
        (filename, row_count, int(time.time()), status),
    )
    conn.commit()


def _delete_rows_from(conn: sqlite3.Connection, starting_after_id: int) -> None:
    """Removes exactly the rows this migrate_file() call itself inserted
    (id > starting_after_id), leaving any pre-existing data in the same
    timestamp range untouched. Used instead of conn.rollback() because
    chunked commits mean a failure partway through no longer has a single
    open transaction to roll back - some chunks may already be committed.
    """
    conn.execute("DELETE FROM inverter_history WHERE id > ?", (starting_after_id,))
    conn.commit()


def _mark_pending(conn: sqlite3.Connection, filename: str, starting_max_id: int) -> None:
    """Records that migrate_file() is about to start committing chunks for
    `filename`, and the id watermark to clean up from if this attempt itself
    never reaches a final 'done'/'error' status (e.g. the process is killed
    outright - SIGKILL/OOM/power loss - which the try/except around the
    insert loop below can't catch). _record_migration()'s INSERT OR REPLACE
    overwrites this with the real outcome once the attempt completes
    normally, so a 'pending' row only survives to be seen again if this
    exact attempt crashed.
    """
    conn.execute(
        "INSERT OR REPLACE INTO csv_migration_log (filename, row_count, migrated_at, status, starting_max_id) "
        "VALUES (?, NULL, ?, 'pending', ?)",
        (filename, int(time.time()), starting_max_id),
    )
    conn.commit()


def _pending_starting_max_id(conn: sqlite3.Connection, filename: str) -> Optional[int]:
    row = conn.execute(
        "SELECT starting_max_id FROM csv_migration_log WHERE filename = ? AND status = 'pending'",
        (filename,),
    ).fetchone()
    return row[0] if row else None


def migrate_file(conn: sqlite3.Connection, csv_path: Path, dry_run: bool) -> str:
    filename = csv_path.name
    if already_migrated(conn, filename):
        logger.info(f"Skipping {filename}: already migrated")
        return 'skipped'

    valid_columns = {name for name, _ in sensor_columns()}

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        if not _validate_header(reader.fieldnames):
            logger.warning(f"Skipping {filename}: empty or missing required columns")
            return 'skipped'

        if dry_run:
            row_count = sum(1 for _ in reader)
            if row_count == 0:
                logger.warning(f"Skipping {filename}: empty or missing required columns")
                return 'skipped'
            logger.info(f"[dry-run] Would import {row_count} rows from {filename}")
            return 'dry-run'

        # a stale 'pending' entry means a previous attempt at this exact
        # file crashed outright (SIGKILL/OOM/power loss) after committing
        # some chunks but before reaching a final 'done'/'error' status -
        # clean up whatever it left behind before starting fresh, rather
        # than appending this attempt's rows on top of orphaned ones
        stale_starting_max_id = _pending_starting_max_id(conn, filename)
        if stale_starting_max_id is not None:
            logger.warning(f"{filename}: found a pending (crashed) previous attempt - cleaning up and retrying")
            _delete_rows_from(conn, stale_starting_max_id)

        row_count = 0
        first_row = None
        last_row = None
        chunk = []
        try:
            starting_max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM inverter_history").fetchone()[0]
            _mark_pending(conn, filename, starting_max_id)
            for row in reader:
                if first_row is None:
                    first_row = row
                last_row = row
                chunk.append({k: v for k, v in row.items() if k in valid_columns})
                row_count += 1
                if len(chunk) >= _MIGRATION_CHUNK_SIZE:
                    storage.insert_samples_batch(conn, chunk, commit=True)
                    chunk = []
            if chunk:
                storage.insert_samples_batch(conn, chunk, commit=True)
        except sqlite3.Error as e:
            logger.error(f"{filename}: insert failed after {row_count} rows: {e}")
            try:
                _delete_rows_from(conn, starting_max_id)
            except sqlite3.Error:
                pass
            try:
                _record_migration(conn, filename, row_count, 'error')
            except sqlite3.Error:
                # the database is contested enough that even recording the
                # failure failed - migrate_file still reports 'error' to its
                # caller rather than crashing; the file stays unmarked in the
                # manifest, so a later re-run will retry it from scratch
                logger.error(f"{filename}: could not record migration failure in the manifest (database still locked)")
            return 'error'

    if row_count == 0:
        logger.warning(f"Skipping {filename}: empty or missing required columns")
        return 'skipped'

    epoch_first = storage.parse_timestamp_epoch(first_row['timestamp'])
    epoch_last = storage.parse_timestamp_epoch(last_row['timestamp'])
    range_start, range_end = min(epoch_first, epoch_last), max(epoch_first, epoch_last)
    db_row_count = conn.execute(
        "SELECT COUNT(*) FROM inverter_history WHERE timestamp_epoch BETWEEN ? AND ?",
        (range_start, range_end),
    ).fetchone()[0]

    first_row_db = conn.execute(
        "SELECT meter_e_total_exp FROM inverter_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
        (first_row['timestamp'],),
    ).fetchone()
    last_row_db = conn.execute(
        "SELECT meter_e_total_exp FROM inverter_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
        (last_row['timestamp'],),
    ).fetchone()
    verified = (
        db_row_count == row_count
        and first_row_db is not None
        and str(first_row_db[0]) == str(float(first_row['meter_e_total_exp']))
        and last_row_db is not None
        and str(last_row_db[0]) == str(float(last_row['meter_e_total_exp']))
    )

    if verified:
        status = 'done'
        logger.info(f"{filename}: imported and verified {row_count} rows")
    else:
        _delete_rows_from(conn, starting_max_id)
        status = 'error'
        logger.error(
            f"{filename}: verification failed (db_row_count={db_row_count}/{row_count}, "
            "spot-check mismatch) - deleted this file's rows"
        )

    _record_migration(conn, filename, row_count, status)
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
