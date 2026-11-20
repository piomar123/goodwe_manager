import asyncio
import os
import sqlite3
import tempfile
import unittest

import runtime_raw


class _Response:
    def __init__(self, data: bytes):
        self._data = data

    def response_data(self) -> bytes:
        return self._data


class _FakeInverter:
    """Just the two attributes RawBlockCapture relies on."""

    def __init__(self, block: bytes):
        self._READ_RUNNING_DATA = object()
        self._other = object()
        self._block = block
        self.calls = []

    async def _read_from_socket(self, command):
        self.calls.append(command)
        if command is self._READ_RUNNING_DATA:
            return _Response(self._block)
        return _Response(b'\x00\x01')


def _block(**registers) -> bytes:
    """A 250-byte running-data block, all zero except the given registers
    (keyword r35212=0x10 sets register 35212)."""
    words = [0] * runtime_raw.REGISTER_COUNT
    for name, value in registers.items():
        words[int(name[1:]) - runtime_raw.FIRST_REGISTER] = value
    return b''.join(w.to_bytes(2, 'big') for w in words)


class RawBlockCaptureTest(unittest.TestCase):
    def test_keeps_the_running_data_block_without_extra_reads(self):
        inverter = _FakeInverter(_block(r35212=0x10))
        capture = runtime_raw.RawBlockCapture()
        self.assertTrue(capture.attach(inverter))

        async def go():
            await inverter._read_from_socket(inverter._READ_RUNNING_DATA)
            await inverter._read_from_socket(inverter._other)

        asyncio.run(go())
        self.assertEqual(len(inverter.calls), 2)
        block = capture.take()
        self.assertEqual(runtime_raw.register(block, 35212), 0x10)
        # taken once: a failed next read must not leave a stale block behind
        self.assertIsNone(capture.take())

    def test_other_commands_are_not_kept(self):
        inverter = _FakeInverter(_block())
        capture = runtime_raw.RawBlockCapture()
        capture.attach(inverter)
        asyncio.run(inverter._read_from_socket(inverter._other))
        self.assertIsNone(capture.take())

    def test_wrong_length_block_is_dropped(self):
        inverter = _FakeInverter(b'\x00' * 10)
        capture = runtime_raw.RawBlockCapture()
        capture.attach(inverter)
        asyncio.run(inverter._read_from_socket(inverter._READ_RUNNING_DATA))
        self.assertIsNone(capture.take())

    def test_attach_refuses_an_inverter_without_the_command(self):
        class Other:
            async def _read_from_socket(self, command):
                return _Response(b'')

        self.assertFalse(runtime_raw.RawBlockCapture().attach(Other()))


class RawStorageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'runtime_raw.db')

    def tearDown(self):
        self.tmp.cleanup()

    def test_insert_and_read_back(self):
        async def go():
            conn = await runtime_raw.init_db_async(self.path)
            await runtime_raw.insert_async(conn, 1763650000, _block(r35100=7, r35224=0xFFFF))
            await conn.close()

        asyncio.run(go())
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
            epoch, block = db.execute('SELECT timestamp_epoch, block FROM runtime_raw').fetchone()
        self.assertEqual(epoch, 1763650000)
        self.assertEqual(runtime_raw.register(block, 35100), 7)
        self.assertEqual(runtime_raw.register(block, 35224), 0xFFFF)

    def test_changes_lists_only_registers_that_moved(self):
        rows = [
            (1, _block(r35212=0, r35139=5)),
            (2, _block(r35212=0, r35139=5)),
            (3, _block(r35212=0x10, r35139=5)),
            (4, _block(r35212=0x10, r35139=6)),
        ]
        changes = list(runtime_raw.changes(rows, [35212, 35139, 35222]))
        self.assertEqual(changes, [(3, 35212, 0, 0x10), (4, 35139, 5, 6)])


class EnabledTest(unittest.TestCase):
    def test_default_on_and_off_values(self):
        old = os.environ.pop('RAW_RUNTIME_CAPTURE', None)
        try:
            self.assertTrue(runtime_raw.enabled())
            for off in ('0', 'false', 'no', 'off'):
                os.environ['RAW_RUNTIME_CAPTURE'] = off
                self.assertFalse(runtime_raw.enabled())
        finally:
            os.environ.pop('RAW_RUNTIME_CAPTURE', None)
            if old is not None:
                os.environ['RAW_RUNTIME_CAPTURE'] = old


if __name__ == '__main__':
    unittest.main()
