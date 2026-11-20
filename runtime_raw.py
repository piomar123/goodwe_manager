"""
runtime_raw.py
Keeps the raw ET running-data block - registers 35100-35224, the one request
read_runtime_data() already makes every poll - including the registers the
goodwe library doesn't decode, to look for undocumented status flags (e.g. a
second backup overload flag, see home-assistant-raspberry4's
docs/backup-overload.md). No extra inverter traffic: the inverter's
_read_from_socket is wrapped and the answer to that one command is kept.

runtime_raw.db: one row per poll (timestamp_epoch, the 250-byte block),
separate from data.db like bms.db. ~11 MB/day at 1 Hz. RAW_RUNTIME_CAPTURE=0
in .env turns it off. `_raw_registers.py` lists register changes over a time
window.
"""
import logging
import os
from typing import Iterable, Iterator, Optional

import aiosqlite

logger = logging.getLogger(__name__)

RAW_DB_PATH = 'runtime_raw.db'
FIRST_REGISTER = 35100
REGISTER_COUNT = 125

# Registers in the block that the goodwe library (v0.4.10) decodes nothing
# from. Most single ones follow a 16-bit power/frequency value and are
# probably its other half; 35212-35219 and 35222-35224 are wholly unknown.
UNDECODED = (35124, 35129, 35134, 35137, 35139, 35141, 35143, 35149, 35155,
             35161, 35163, 35165, 35167, 35169, 35171, *range(35212, 35220),
             35222, 35223, 35224)


def enabled() -> bool:
    return os.environ.get('RAW_RUNTIME_CAPTURE', '1').strip().lower() not in ('0', 'false', 'no', 'off')


class RawBlockCapture:
    """Wraps an inverter's _read_from_socket to keep the last running-data
    block. Relies on goodwe's private ET attributes (_READ_RUNNING_DATA,
    _read_from_socket); attach() returns False if they're missing."""

    def __init__(self):
        self._last: Optional[bytes] = None

    def attach(self, inverter) -> bool:
        command = getattr(inverter, '_READ_RUNNING_DATA', None)
        original = getattr(inverter, '_read_from_socket', None)
        if command is None or original is None:
            logger.warning("Raw runtime capture: inverter has no running-data command, not capturing")
            return False

        async def read_and_keep(cmd):
            response = await original(cmd)
            if cmd is command:
                data = bytes(response.response_data())
                self._last = data if len(data) == 2 * REGISTER_COUNT else None
            return response

        inverter._read_from_socket = read_and_keep
        return True

    def take(self) -> Optional[bytes]:
        """The block of the last poll, once: a failed next read can't leave
        a stale block to be stored under a new timestamp."""
        block, self._last = self._last, None
        return block


def register(block: bytes, address: int) -> int:
    i = 2 * (address - FIRST_REGISTER)
    return int.from_bytes(block[i:i + 2], 'big')


async def init_db_async(path: Optional[str] = None) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(path or RAW_DB_PATH)
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS runtime_raw (timestamp_epoch INTEGER NOT NULL, block BLOB NOT NULL)")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_runtime_raw_timestamp_epoch ON runtime_raw (timestamp_epoch)")
    await conn.commit()
    return conn


async def insert_async(conn: aiosqlite.Connection, timestamp_epoch: int, block: bytes) -> None:
    await conn.execute("INSERT INTO runtime_raw (timestamp_epoch, block) VALUES (?, ?)",
                       (timestamp_epoch, block))
    await conn.commit()


def changes(rows: Iterable[tuple], addresses: Iterable[int]) -> Iterator[tuple]:
    """(timestamp_epoch, address, old, new) for every change of the given
    registers across consecutive (timestamp_epoch, block) rows."""
    addresses = list(addresses)
    previous = None
    for epoch, block in rows:
        values = {a: register(block, a) for a in addresses}
        if previous is not None:
            for a in addresses:
                if values[a] != previous[a]:
                    yield epoch, a, previous[a], values[a]
        previous = values
