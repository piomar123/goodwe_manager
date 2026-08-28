import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

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


if __name__ == '__main__':
    unittest.main()
