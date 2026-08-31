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


def _row_epoch_if_usable(row: dict) -> Optional[int]:
    """Returns the row's parsed timestamp_epoch if every REQUIRED_COLUMNS
    value is present and usable, else None. A process killed mid-write
    (SIGKILL/OOM/power loss) leaves its CSV file's last row truncated and
    zero-padded by the filesystem, rather than a clean row - real examples
    from this codebase's own data: a run of NUL bytes with no delimiters at
    all (landing entirely in the 'timestamp' field, every other field
    defaulting to None), or a line cut short mid-field (DictReader's
    restval default fills the missing trailing fields with None too).
    Skipping just that one row recovers everything else in the file,
    instead of discarding an otherwise-complete file over its last line.
    """
    try:
        epoch = storage.parse_timestamp_epoch(row['timestamp'])
        float(row['meter_e_total_exp'])
        float(row['meter_e_total_imp'])
        float(row['e_load_total'])
        return epoch
    except (ValueError, TypeError, KeyError):
        return None


# Columns compared to decide whether a row already in inverter_history at
# the same timestamp is a harmless duplicate or a genuine clash. Chosen
# from REQUIRED_COLUMNS plus e_day - between them, an instantaneous reading
# (ppv) and several cumulative counters (meter_e_total_exp/imp, e_day,
# e_load_total) reliably distinguish "same physical sample recorded twice"
# from "different point in the inverter's own timeline mislabeled onto the
# same wall-clock second" (confirmed against real data - see the migration
# analysis this recovery path exists for).
RECONCILE_COMPARE_COLUMNS = ['ppv', 'meter_e_total_exp', 'meter_e_total_imp', 'e_day', 'e_load_total']


def _values_match(row: dict, db_values: tuple) -> bool:
    file_values = tuple(row.get(c) for c in RECONCILE_COMPARE_COLUMNS)
    try:
        file_norm = tuple(str(float(v)) if v not in (None, '') else None for v in file_values)
    except (ValueError, TypeError):
        return False  # an unparseable compare value is never a silent match
    db_norm = tuple(str(v) if v is not None else None for v in db_values)
    return file_norm == db_norm


def _reconcile_chunk(conn: sqlite3.Connection, chunk: list, valid_columns: set, filename: str) -> dict:
    """chunk: list of (epoch, row) pairs. Looks up existing DB values only
    for this chunk's own epochs (IN(...), not a BETWEEN range) - a file
    spanning many days can have a DB-side range containing millions of
    unrelated rows from every other file in that span; a range fetch would
    load all of them into memory regardless of this chunk's actual size.
    """
    epochs = [epoch for epoch, _ in chunk]
    placeholders = ', '.join('?' for _ in epochs)
    select_cols = ', '.join(RECONCILE_COMPARE_COLUMNS)
    existing = {
        epoch: values for epoch, *values in conn.execute(
            f"SELECT timestamp_epoch, {select_cols} FROM inverter_history WHERE timestamp_epoch IN ({placeholders})",
            epochs,
        )
    }

    to_insert = []
    duplicate = 0
    clashes = []
    for epoch, row in chunk:
        db_values = existing.get(epoch)
        if db_values is None:
            to_insert.append({k: v for k, v in row.items() if k in valid_columns})
        elif _values_match(row, db_values):
            duplicate += 1
        else:
            clashes.append({
                'timestamp': row['timestamp'],
                'file_values': {c: row.get(c) for c in RECONCILE_COMPARE_COLUMNS},
                'db_values': dict(zip(RECONCILE_COMPARE_COLUMNS, db_values)),
            })

    if to_insert:
        storage.insert_samples_batch(conn, to_insert, commit=True)
    return {'inserted': len(to_insert), 'duplicate': duplicate, 'clashes': clashes}


def reconcile_file(conn: sqlite3.Connection, csv_path: Path, chunk_size: int = 500) -> dict:
    """Per-row recovery for a file that already failed migrate_file()'s
    whole-file verification. Unlike migrate_file(), a single bad or
    ambiguous row never costs the rest of the file: each row is decided
    independently -
      - inserted, if its timestamp doesn't exist in inverter_history yet
      - skipped as a harmless duplicate, if it exists with identical values
      - skipped and reported as a clash, if it exists with DIFFERENT
        values - existing DB data is NEVER overwritten, so a clash can only
        ever mean losing this file's version of that one row, never
        corrupting what's already there.

    Naturally safe to interrupt and re-run: every row's fate is decided by
    its own current DB state, not a whole-file transaction, so a partial
    run just leaves already-processed rows looking like duplicates on the
    next attempt - no separate crash-recovery bookkeeping needed (contrast
    with migrate_file()'s 'pending' manifest state).

    Returns {'status': 'recovered'|'skipped', 'inserted': int,
    'duplicate': int, 'clashes': [{'timestamp', 'file_values', 'db_values'}]}.
    """
    filename = csv_path.name
    valid_columns = {name for name, _ in sensor_columns()}
    totals = {'inserted': 0, 'duplicate': 0, 'clashes': []}

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        if not _validate_header(reader.fieldnames):
            return {'status': 'skipped', **totals}

        chunk = []
        row_iter = iter(reader)
        while True:
            try:
                row = next(row_iter)
            except StopIteration:
                break
            except csv.Error as e:
                logger.warning(f"{filename}: skipping malformed line: {e}")
                continue
            epoch = _row_epoch_if_usable(row)
            if epoch is None:
                logger.warning(f"{filename}: skipping malformed row (likely a truncated trailing write)")
                continue
            chunk.append((epoch, row))
            if len(chunk) >= chunk_size:
                result = _reconcile_chunk(conn, chunk, valid_columns, filename)
                totals['inserted'] += result['inserted']
                totals['duplicate'] += result['duplicate']
                totals['clashes'].extend(result['clashes'])
                chunk = []
        if chunk:
            result = _reconcile_chunk(conn, chunk, valid_columns, filename)
            totals['inserted'] += result['inserted']
            totals['duplicate'] += result['duplicate']
            totals['clashes'].extend(result['clashes'])

    for clash in totals['clashes']:
        logger.warning(
            f"{filename}: clash at {clash['timestamp']} - kept existing DB value "
            f"(file had {clash['file_values']}, DB has {clash['db_values']})"
        )

    return {'status': 'recovered', **totals}


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
            row_count = 0
            row_iter = iter(reader)
            while True:
                try:
                    row = next(row_iter)
                except StopIteration:
                    break
                except csv.Error:
                    continue
                if _row_epoch_if_usable(row) is not None:
                    row_count += 1
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
        min_epoch = None
        max_epoch = None
        min_row = None
        max_row = None
        chunk = []
        try:
            starting_max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM inverter_history").fetchone()[0]
            _mark_pending(conn, filename, starting_max_id)
            # driven manually rather than `for row in reader` - a
            # truncated/corrupted line (see _row_epoch_if_usable) can make
            # the underlying csv module itself raise mid-iteration (e.g.
            # "line contains NUL"), which a plain for-loop can't recover
            # from and continue past
            row_iter = iter(reader)
            while True:
                try:
                    row = next(row_iter)
                except StopIteration:
                    break
                except csv.Error as e:
                    logger.warning(f"{filename}: skipping malformed line: {e}")
                    continue
                epoch = _row_epoch_if_usable(row)
                if epoch is None:
                    logger.warning(f"{filename}: skipping malformed row (likely a truncated trailing write)")
                    continue
                if min_epoch is None or epoch < min_epoch:
                    min_epoch, min_row = epoch, row
                if max_epoch is None or epoch > max_epoch:
                    max_epoch, max_row = epoch, row
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
        logger.warning(f"Skipping {filename}: every row was malformed (or the file was empty/missing columns)")
        # _mark_pending() already wrote a 'pending' row for this filename -
        # without overwriting it here, this file would show 'pending'
        # forever (never 'done' or 'error'), since already_migrated() only
        # checks for 'done' - so every future run retries it, marks it
        # 'pending' again, and hits this exact same path again
        _record_migration(conn, filename, 0, 'skipped')
        return 'skipped'

    # min_epoch/max_epoch are the true extremes across every usable row,
    # not just the first/last row in file order - a mid-file clock jump
    # (confirmed against real data from this codebase's own migration) can
    # otherwise put an earlier or later timestamp somewhere in the middle,
    # which a first-row/last-row range would silently miss and undercount
    db_row_count = conn.execute(
        "SELECT COUNT(*) FROM inverter_history WHERE timestamp_epoch BETWEEN ? AND ?",
        (min_epoch, max_epoch),
    ).fetchone()[0]

    first_row_db = conn.execute(
        "SELECT meter_e_total_exp FROM inverter_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
        (min_row['timestamp'],),
    ).fetchone()
    last_row_db = conn.execute(
        "SELECT meter_e_total_exp FROM inverter_history WHERE timestamp = ? ORDER BY id DESC LIMIT 1",
        (max_row['timestamp'],),
    ).fetchone()
    verified = (
        db_row_count == row_count
        and first_row_db is not None
        and str(first_row_db[0]) == str(float(min_row['meter_e_total_exp']))
        and last_row_db is not None
        and str(last_row_db[0]) == str(float(max_row['meter_e_total_exp']))
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


def _run_recovery(conn: sqlite3.Connection, csv_dir: str) -> None:
    """--recover mode: per-row reconcile()s every file currently marked
    'error' in the manifest, instead of a normal whole-directory migration
    pass. A recovered file is marked 'done' (not a separate status) so
    already_migrated() correctly skips it on any future normal run -
    otherwise a plain migrate_file() re-run would immediately re-reject it
    with the same whole-file verification mismatch and undo the recovery.
    """
    error_filenames = [
        row[0] for row in conn.execute(
            "SELECT filename FROM csv_migration_log WHERE status = 'error' ORDER BY filename"
        ).fetchall()
    ]
    logger.info(f"Recovering {len(error_filenames)} error-status file(s) from {csv_dir}")

    total_inserted = total_duplicate = total_clashes = 0
    for filename in error_filenames:
        csv_path = Path(csv_dir) / filename
        if not csv_path.exists():
            logger.error(f"{filename}: not found in {csv_dir}, skipping")
            continue
        try:
            result = reconcile_file(conn, csv_path)
        except Exception as e:
            logger.error(f"{filename}: unexpected error during recovery, skipping: {e}")
            continue
        if result['status'] == 'skipped':
            logger.warning(f"{filename}: header invalid, skipped")
            continue
        _record_migration(conn, filename, result['inserted'], 'done')
        total_inserted += result['inserted']
        total_duplicate += result['duplicate']
        total_clashes += len(result['clashes'])
        logger.info(
            f"{filename}: inserted={result['inserted']} duplicate={result['duplicate']} "
            f"clashes={len(result['clashes'])}"
        )

    logger.info(
        f"Recovery summary: inserted={total_inserted} duplicate={total_duplicate} "
        f"clashes={total_clashes} (clashes never overwrite existing data - see per-file logs above)"
    )


def main():
    parser = argparse.ArgumentParser(description="Migrate legacy data-*.csv files into data.db")
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--csv-dir', default='.')
    parser.add_argument('--db-path', default=storage.DATA_DB_PATH)
    parser.add_argument('--recover', action='store_true',
                         help="Per-row-reconcile files currently marked 'error' in the manifest, "
                              "instead of a normal migration pass - see reconcile_file()")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    conn = storage.init_db_sync(args.db_path, sensor_columns())
    ensure_migration_log_table(conn)

    if args.recover:
        _run_recovery(conn, args.csv_dir)
        conn.close()
        return

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
