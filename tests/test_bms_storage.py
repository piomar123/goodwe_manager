import asyncio
import json
import os
import sqlite3
import tempfile
import unittest

import bms_poller
import bms_storage
from tests.test_bms_poller import CELLS, SUMMARY, WHEN


class BmsStorageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'bms.db')

    def tearDown(self):
        self.tmp.cleanup()

    def test_creates_table_index_and_wal(self):
        async def go():
            conn = await bms_storage.init_db_async(self.path)
            await conn.close()

        asyncio.run(go())
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
            indexes = [r[1] for r in db.execute("PRAGMA index_list('bms_history')")]
            self.assertIn('idx_bms_history_timestamp_epoch', indexes)

    def test_row_round_trips(self):
        sample = bms_poller.decode(SUMMARY, CELLS, WHEN)

        async def go():
            conn = await bms_storage.init_db_async(self.path)
            await bms_storage.insert_sample(conn, sample)
            await conn.close()

        asyncio.run(go())
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT * FROM bms_history').fetchone()
        self.assertEqual(row['timestamp'], '2026-09-29 08:45:30')
        self.assertEqual(row['timestamp_epoch'], int(WHEN.timestamp()))
        self.assertAlmostEqual(row['cell_temp_max'], 27.2)
        self.assertEqual(row['soh'], 97)
        self.assertEqual(json.loads(row['module_voltages']), [98.25, 98.24])
        self.assertEqual(len(json.loads(row['cell_mv'])), 60)
        self.assertEqual(json.loads(row['raw_1100']), SUMMARY)

    def test_default_path_is_looked_up_at_call_time(self):
        orig = bms_storage.BMS_DB_PATH
        bms_storage.BMS_DB_PATH = self.path
        try:
            async def go():
                conn = await bms_storage.init_db_async()
                await conn.close()
            asyncio.run(go())
        finally:
            bms_storage.BMS_DB_PATH = orig
        self.assertTrue(os.path.exists(self.path))


if __name__ == '__main__':
    unittest.main()
