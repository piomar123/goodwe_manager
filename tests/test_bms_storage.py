import asyncio
import json
import os
import sqlite3
import tempfile
import unittest

import bms_poller
import bms_storage
from tests.test_bms_poller import CELLS, SUMMARY, WHEN


V1_SCHEMA = """
    CREATE TABLE bms_history (
        id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, timestamp_epoch INTEGER NOT NULL,
        pack_voltage REAL, bms_temperature REAL, soc REAL, soh REAL,
        cell_voltage_max REAL, cell_voltage_min REAL, cell_voltage_max_id INTEGER, cell_voltage_min_id INTEGER,
        cell_temp_max REAL, cell_temp_min REAL, module_voltages TEXT, cell_mv TEXT, raw_1100 TEXT)
"""


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
        self.assertAlmostEqual(row['module_temp_max'], 27.2)
        self.assertAlmostEqual(row['cell_temp_max'], 28.0)
        self.assertEqual(row['soh'], 97)
        self.assertEqual(row['state'], 'idle')
        self.assertEqual(row['cycle_count'], 679)
        self.assertEqual(row['fully_charged'], 0)
        self.assertEqual(json.loads(row['module_voltages']), [98.273, 98.271])
        self.assertEqual(len(json.loads(row['cell_mv'])), 60)
        self.assertEqual(json.loads(row['raw_1100']), SUMMARY)

    def test_every_sample_field_has_a_column(self):
        async def go():
            conn = await bms_storage.init_db_async(self.path)
            await conn.close()

        asyncio.run(go())
        with sqlite3.connect(self.path) as db:
            columns = {r[1] for r in db.execute("PRAGMA table_info('bms_history')")}
        self.assertEqual(columns - {'id'}, set(bms_poller.BmsSample.__dataclass_fields__))

    def test_first_schema_rows_are_re_decoded_from_the_raw_registers(self):
        # bms.db as written by the first deployed version (2026-10-01): no
        # current/cycle columns, cell_temp_* holding 0x111C/0x111D and
        # module_voltages holding 0x1118/0x1119.
        with sqlite3.connect(self.path) as db:
            db.execute(V1_SCHEMA)
            db.execute(
                "INSERT INTO bms_history (timestamp, timestamp_epoch, pack_voltage, cell_temp_max, cell_temp_min,"
                " module_voltages, cell_mv, raw_1100) VALUES (?, ?, 196.5, 27.2, 25.7, '[98.25, 98.24]', ?, ?)",
                ('2026-09-29 08:45:30', int(WHEN.timestamp()), json.dumps(CELLS[:60]), json.dumps(SUMMARY)))
            db.execute(
                "INSERT INTO bms_history (timestamp, timestamp_epoch, cell_mv, raw_1100) VALUES (?, ?, '[]', ?)",
                ('2026-09-29 08:46:30', int(WHEN.timestamp()) + 60, json.dumps(SUMMARY)))

        async def go():
            conn = await bms_storage.init_db_async(self.path)
            await conn.close()
            conn = await bms_storage.init_db_async(self.path)  # second start: nothing left to do
            await conn.close()

        with self.assertLogs('bms_storage', 'INFO') as logs:
            asyncio.run(go())
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            good, bad = db.execute('SELECT * FROM bms_history ORDER BY id').fetchall()
            version = db.execute('PRAGMA user_version').fetchone()[0]
        self.assertEqual(version, bms_storage.SCHEMA_VERSION)
        self.assertAlmostEqual(good['cell_temp_max'], 28.0)
        self.assertAlmostEqual(good['module_temp_max'], 27.2)
        self.assertAlmostEqual(good['module_voltage_max'], 98.25)
        self.assertEqual(json.loads(good['module_voltages']), [98.273, 98.271])
        self.assertEqual(good['cycle_count'], 679)
        self.assertEqual(good['current'], 0.0)
        self.assertEqual(good['state'], 'idle')
        self.assertIsNone(bad['cycle_count'])  # undecodable row left as it was
        self.assertEqual(len([m for m in logs.output if 're-decoded' in m]), 1)
        self.assertIn('1 of 2', [m for m in logs.output if 're-decoded' in m][0])

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
