import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import storage
from sensors import sensor_columns

migrate = importlib.import_module('_migrate_csv_to_sqlite')

HEADER = 'timestamp,ppv,meter_e_total_exp,meter_e_total_imp,e_load_total\n'


class MigrateCsvToSqliteTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, 'data.db')
        self.conn = storage.init_db_sync(self.db_path, sensor_columns())
        migrate.ensure_migration_log_table(self.conn)

    def tearDown(self):
        self.conn.close()

    def _write_csv(self, name: str, content: str) -> Path:
        path = Path(self.tmp_dir) / name
        path.write_text(content)
        return path

    def test_imports_a_normal_csv_and_marks_it_done(self):
        csv_path = self._write_csv('data-2026-08-28_10-00-00.csv',
                                   HEADER +
                                   '2026-08-28 10:00:00,100.0,1.0,0.5,0.1\n'
                                   '2026-08-28 10:00:01,101.0,1.1,0.5,0.2\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'done')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 2)
        log_status = self.conn.execute(
            "SELECT status, row_count FROM csv_migration_log WHERE filename = ?",
            (csv_path.name,),
        ).fetchone()
        self.assertEqual(log_status, ('done', 2))

    def test_skips_a_single_malformed_trailing_row_instead_of_the_whole_file(self):
        # simulates a process killed mid-write: a truncated final row,
        # zero-padded by the filesystem - null bytes with no delimiters at
        # all, so csv.DictReader maps the whole blob to the 'timestamp'
        # field and leaves every other field None
        csv_path = self._write_csv('data-2026-08-28_09-00-00.csv',
                                   HEADER +
                                   '2026-08-28 09:00:00,100.0,1.0,0.5,0.1\n'
                                   '2026-08-28 09:00:01,101.0,1.1,0.5,0.2\n' +
                                   '\x00' * 200)

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'done')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 2)
        log_status = self.conn.execute(
            "SELECT status, row_count FROM csv_migration_log WHERE filename = ?",
            (csv_path.name,),
        ).fetchone()
        self.assertEqual(log_status, ('done', 2))

    def test_skips_a_row_with_a_missing_required_value(self):
        # a truncated row can also land mid-line rather than as pure nulls -
        # fewer fields than the header, so DictReader fills the rest with
        # None (its restval default) rather than raising
        csv_path = self._write_csv('data-2026-08-28_09-30-00.csv',
                                   HEADER +
                                   '2026-08-28 09:30:00,100.0,1.0,0.5,0.1\n'
                                   '2026-08-28 09:30:01,101.0\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'done')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 1)

    def test_all_rows_malformed_is_treated_as_skipped(self):
        csv_path = self._write_csv('data-2026-08-28_09-45-00.csv', HEADER + '\x00' * 200)

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'skipped')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 0)

    def test_verification_uses_true_min_max_not_first_last_row_order(self):
        # rows out of chronological order (e.g. a mid-file clock jump) -
        # the *last* row written is not the latest timestamp
        csv_path = self._write_csv('data-2026-08-28_09-50-00.csv',
                                   HEADER +
                                   '2026-08-28 09:52:00,100.0,3.0,0.5,0.1\n'
                                   '2026-08-28 09:50:00,101.0,1.0,0.5,0.2\n'
                                   '2026-08-28 09:51:00,102.0,2.0,0.5,0.3\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'done')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 3)

    def test_skips_an_empty_file(self):
        csv_path = self._write_csv('data-2026-08-28_11-00-00.csv', '')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'skipped')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 0)

    def test_skips_a_file_missing_required_columns(self):
        csv_path = self._write_csv('data-2026-08-28_12-00-00.csv', 'timestamp,ppv\n2026-08-28 12:00:00,100.0\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'skipped')

    def test_skips_a_file_already_marked_done(self):
        csv_path = self._write_csv('data-2026-08-28_13-00-00.csv',
                                   HEADER + '2026-08-28 13:00:00,100.0,1.0,0.5,0.1\n')
        migrate.migrate_file(self.conn, csv_path, dry_run=False)

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'skipped')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 1)  # not duplicated

    def test_dry_run_writes_nothing(self):
        csv_path = self._write_csv('data-2026-08-28_14-00-00.csv',
                                   HEADER + '2026-08-28 14:00:00,100.0,1.0,0.5,0.1\n')

        status = migrate.migrate_file(self.conn, csv_path, dry_run=True)

        self.assertEqual(status, 'dry-run')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 0)
        log_count = self.conn.execute("SELECT COUNT(*) FROM csv_migration_log").fetchone()[0]
        self.assertEqual(log_count, 0)

    def test_verification_fails_when_the_db_row_count_for_the_range_does_not_match_the_csv(self):
        csv_path = self._write_csv('data-2026-08-28_16-00-00.csv',
                                   HEADER + '2026-08-28 16:00:00,100.0,1.0,0.5,0.1\n')
        # simulate a pre-existing extra row inside the same timestamp range
        # (e.g. left over from a previous partial/duplicated run) so the
        # real COUNT(*) verification query disagrees with len(rows), even
        # though the insert loop itself completes without raising
        extra_row = {name: ('0' if sql_type == 'REAL' else '') for name, sql_type in sensor_columns()}
        extra_row['timestamp'] = '2026-08-28 16:00:00'
        storage.insert_sample_sync(self.conn, extra_row, commit=True)

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'error')
        log_status = self.conn.execute(
            "SELECT status FROM csv_migration_log WHERE filename = ?",
            (csv_path.name,),
        ).fetchone()
        self.assertEqual(log_status, ('error',))
        # the CSV's own row was rolled back; only the pre-existing extra row remains
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 1)

    def test_verification_spot_checks_the_last_row_too(self):
        csv_path = self._write_csv('data-2026-08-28_17-00-00.csv',
                                   HEADER +
                                   '2026-08-28 17:00:00,100.0,1.0,0.5,0.1\n'
                                   '2026-08-28 17:00:01,101.0,999.0,0.5,0.2\n')
        # simulate the last row's value silently diverging between what was
        # read from the CSV (what verification compares against) and what
        # actually landed in the DB, by monkeypatching storage.insert_samples_batch
        # to corrupt only the last row on its way into the DB
        original_insert = migrate.storage.insert_samples_batch

        def _patched(conn, rows, commit=True):
            rows = list(rows)
            for row in rows:
                if row.get('timestamp') == '2026-08-28 17:00:01':
                    row['meter_e_total_exp'] = '54321.0'
            return original_insert(conn, rows, commit=commit)

        migrate.storage.insert_samples_batch = _patched
        try:
            status = migrate.migrate_file(self.conn, csv_path, dry_run=False)
        finally:
            migrate.storage.insert_samples_batch = original_insert

        self.assertEqual(status, 'error')
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 0)

    def test_recovers_from_a_crashed_previous_attempt_at_the_same_file(self):
        csv_path = self._write_csv('data-2026-08-28_20-00-00.csv',
                                   HEADER +
                                   '2026-08-28 20:00:00,100.0,1.0,0.5,0.1\n'
                                   '2026-08-28 20:00:01,101.0,1.1,0.5,0.2\n')
        # simulate a crash partway through a previous attempt: a 'pending'
        # manifest entry was written before the insert loop started, and
        # some rows got durably committed (chunk commits) before the
        # process died - orphaned, with no 'done'/'error' entry to show for it
        starting_max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM inverter_history").fetchone()[0]
        orphan_row = {name: ('0' if sql_type == 'REAL' else '') for name, sql_type in sensor_columns()}
        orphan_row['timestamp'] = '2026-08-28 20:00:00'
        storage.insert_sample_sync(self.conn, orphan_row, commit=True)
        self.conn.execute(
            "INSERT INTO csv_migration_log (filename, row_count, migrated_at, status, starting_max_id) "
            "VALUES (?, NULL, 0, 'pending', ?)",
            (csv_path.name, starting_max_id),
        )
        self.conn.commit()

        status = migrate.migrate_file(self.conn, csv_path, dry_run=False)

        self.assertEqual(status, 'done')
        # the orphaned row from the crashed attempt was cleaned up first,
        # so only this attempt's 2 rows remain - not 3
        count = self.conn.execute("SELECT COUNT(*) FROM inverter_history").fetchone()[0]
        self.assertEqual(count, 2)
        log_status = self.conn.execute(
            "SELECT status FROM csv_migration_log WHERE filename = ?", (csv_path.name,),
        ).fetchone()
        self.assertEqual(log_status, ('done',))

    def test_ensure_migration_log_table_adds_starting_max_id_to_an_existing_table(self):
        small_db_path = os.path.join(self.tmp_dir, 'small.db')
        conn = sqlite3.connect(small_db_path)
        conn.execute("""
            CREATE TABLE csv_migration_log (
                filename TEXT PRIMARY KEY,
                row_count INTEGER,
                migrated_at INTEGER,
                status TEXT
            )
        """)
        conn.execute(
            "INSERT INTO csv_migration_log (filename, row_count, migrated_at, status) VALUES (?, ?, ?, ?)",
            ('old-file.csv', 5, 12345, 'done'),
        )
        conn.commit()

        migrate.ensure_migration_log_table(conn)

        column_names = {row[1] for row in conn.execute("PRAGMA table_info(csv_migration_log)")}
        self.assertIn('starting_max_id', column_names)
        row = conn.execute(
            "SELECT filename, row_count, status FROM csv_migration_log WHERE filename = 'old-file.csv'"
        ).fetchone()
        self.assertEqual(row, ('old-file.csv', 5, 'done'))
        conn.close()

    def test_a_locked_database_is_reported_as_an_error_not_a_crash(self):
        csv_path = self._write_csv('data-2026-08-28_15-00-00.csv',
                                   HEADER + '2026-08-28 15:00:00,100.0,1.0,0.5,0.1\n')
        blocker = sqlite3.connect(self.db_path, timeout=0)
        blocker.execute("BEGIN EXCLUSIVE")

        try:
            # the lock is held for the entire call, so even the manifest
            # write inside migrate_file's own error handling will fail -
            # the only guaranteed-reliable signal under lock is the return
            # value itself, which must still be 'error', never an
            # unhandled exception
            status = migrate.migrate_file(self.conn, csv_path, dry_run=False)
            self.assertEqual(status, 'error')
        finally:
            blocker.rollback()
            blocker.close()

        # once the lock clears, the file was never marked 'done' (the
        # manifest write itself failed while locked), so a later re-run
        # retries it from scratch rather than silently skipping it
        self.assertFalse(migrate.already_migrated(self.conn, csv_path.name))


class MigrateMainTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, 'data.db')

    def _write_csv(self, name: str, content: str) -> Path:
        path = Path(self.tmp_dir) / name
        path.write_text(content)
        return path

    def test_a_file_that_raises_a_non_sqlite_error_does_not_abort_the_rest_of_the_run(self):
        # a garbage (non-numeric) value in a required numeric column raises
        # ValueError during verification's float(...) spot-check, which is
        # not a sqlite3.Error - main()'s per-file loop must still continue
        # on to the next file rather than letting it propagate and abort
        # the whole run
        self._write_csv('data-2026-08-28_18-00-00.csv',
                         HEADER + '2026-08-28 18:00:00,100.0,NOT_A_NUMBER,0.5,0.1\n')
        self._write_csv('data-2026-08-28_19-00-00.csv',
                         HEADER + '2026-08-28 19:00:00,100.0,1.0,0.5,0.1\n')

        with mock.patch('sys.argv', ['_migrate_csv_to_sqlite.py', '--csv-dir', self.tmp_dir,
                                      '--db-path', self.db_path]):
            migrate.main()  # must not raise

        conn = sqlite3.connect(self.db_path)
        try:
            migrate.ensure_migration_log_table(conn)
            # the second, valid file was still processed despite the first's failure
            log_status = conn.execute(
                "SELECT status FROM csv_migration_log WHERE filename = ?",
                ('data-2026-08-28_19-00-00.csv',),
            ).fetchone()
            self.assertEqual(log_status, ('done',))
        finally:
            conn.close()


if __name__ == '__main__':
    unittest.main()
