"""
tests/test_bms_poller.py
Decoding is tested against real register dumps read from the Pylontech
Force H2 BMS through the SolarMan logger on 2026-09-29 (SolarMan app showed
cell temperatures 27.2 / 25.7 °C and SOH 97 % at the time).
"""
import asyncio
import subprocess
import sys
import time
import unittest
from datetime import datetime

import bms_poller

SUMMARY = (
    [1027, 0, 0, 1965, 0, 0, 360, 33, 679, 2160, 0, 1850, 1740, 65535, 63686, 3]
    + [3276, 3273, 3, 24, 280, 250, 30, 22, 9825, 9824, 0, 1, 272, 257, 1, 0]
    + [97, 0, 2347, 0, 2652, 0, 4021, 0, 2652, 0, 4046, 0, 4942, 0, 4902, 0]
    + [0, 0, 0, 0, 0, 0, 2, 60, 0, 0, 0, 0, 0, 0, 0, 0]
)
CELLS = (
    [3276, 3275, 3276, 3277, 3275, 3276, 3276, 3276, 3275, 3276, 3276, 3276, 3275, 3276, 3276, 3276]
    + [3277, 3277, 3275, 3276, 3275, 3276, 3276, 3275, 3273, 3276, 3276, 3276, 3277, 3275, 3276, 3275]
    + [3277, 3276, 3275, 3276, 3277, 3274, 3275, 3274, 3275, 3275, 3275, 3276, 3277, 3277, 3275, 3277]
    + [3274, 3275, 3277, 3276, 3275, 3277, 3276, 3276, 3277, 3277, 3274, 3275, 0, 0, 0, 0]
)
WHEN = datetime(2026, 9, 29, 8, 45, 30)


def with_reg(block, addr, value, start=bms_poller.SUMMARY_START):
    changed = list(block)
    changed[addr - start] = value
    return changed


class DecodeTest(unittest.TestCase):
    def test_decodes_the_real_dump(self):
        s = bms_poller.decode(SUMMARY, CELLS, WHEN)

        self.assertEqual(s.timestamp, '2026-09-29 08:45:30')
        self.assertEqual(s.timestamp_epoch, int(WHEN.timestamp()))
        self.assertAlmostEqual(s.pack_voltage, 196.5)
        self.assertAlmostEqual(s.bms_temperature, 36.0)
        self.assertEqual(s.soc, 33)
        self.assertEqual(s.soh, 97)
        self.assertAlmostEqual(s.cell_voltage_max, 3.276)
        self.assertAlmostEqual(s.cell_voltage_min, 3.273)
        self.assertEqual(s.cell_voltage_max_id, 3)
        self.assertEqual(s.cell_voltage_min_id, 24)
        self.assertAlmostEqual(s.cell_temp_max, 27.2)
        self.assertAlmostEqual(s.cell_temp_min, 25.7)
        self.assertEqual(s.module_voltages, [98.25, 98.24])
        self.assertEqual(len(s.cell_mv), 60)
        self.assertEqual(s.cell_mv[:3], [3276, 3275, 3276])
        self.assertEqual(s.raw_1100, SUMMARY)

    def test_negative_temperatures_are_signed(self):
        summary = with_reg(with_reg(SUMMARY, 0x111C, 65536 - 25), 0x111D, 65536 - 52)

        s = bms_poller.decode(summary, CELLS, WHEN)

        self.assertAlmostEqual(s.cell_temp_max, -2.5)
        self.assertAlmostEqual(s.cell_temp_min, -5.2)

    def test_trailing_cells_after_the_first_zero_are_ignored(self):
        s = bms_poller.decode(SUMMARY, CELLS[:60] + [0, 3300, 3300], WHEN)
        self.assertEqual(len(s.cell_mv), 60)

    def test_module_count_follows_cell_count_not_the_first_zero(self):
        # Seen live on 2026-10-01: 0x111A (0 in the first dump) read 1, which
        # "stop at the first zero" took for a third 0.01 V module.
        summary = with_reg(with_reg(SUMMARY, 0x111A, 1), 0x111B, 1)

        s = bms_poller.decode(summary, CELLS, WHEN)

        self.assertEqual(s.module_voltages, [98.25, 98.24])

    def test_four_modules_for_120_cells(self):
        cells = CELLS[:60] * 2
        summary = with_reg(with_reg(with_reg(SUMMARY, 0x1103, 3930), 0x111A, 9825), 0x111B, 9826)

        s = bms_poller.decode(summary, cells, WHEN)

        self.assertEqual(s.module_voltages, [98.25, 98.24, 98.25, 98.26])

    def test_wrong_summary_length_is_rejected(self):
        with self.assertRaisesRegex(bms_poller.BmsDecodeError, 'summary'):
            bms_poller.decode(SUMMARY[:32], CELLS, WHEN)


class PlausibilityTest(unittest.TestCase):
    def assert_rejected(self, summary=SUMMARY, cells=CELLS, match=''):
        with self.assertRaisesRegex(bms_poller.BmsDecodeError, match):
            bms_poller.decode(summary, cells, WHEN)

    def test_soc_above_100(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1107, 120), match='soc')

    def test_soh_above_100(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1120, 101), match='soh')

    def test_pack_voltage_out_of_range(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1103, 50), match='pack_voltage')

    def test_no_cells(self):
        self.assert_rejected(cells=[0] * 32, match='cell')

    def test_cell_out_of_range(self):
        self.assert_rejected(cells=[1500] + CELLS[1:], match='cell')

    def test_temperature_out_of_range(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1106, 900), match='temperature')

    def test_module_sum_off_by_more_than_two_percent(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1119, 9000), match='module')

    def test_no_modules(self):
        self.assert_rejected(summary=with_reg(SUMMARY, 0x1118, 0), match='module')


class LoadConfigTest(unittest.TestCase):
    def test_disabled_without_host_or_serial(self):
        for env in ({}, {'BMS_LOGGER_HOST': '192.168.1.221'}, {'BMS_LOGGER_SERIAL': '4060493924'},
                    {'BMS_LOGGER_HOST': ' ', 'BMS_LOGGER_SERIAL': '4060493924'}):
            with self.assertLogs('bms_poller', 'INFO') as logs:
                self.assertIsNone(bms_poller.load_bms_config(env))
            self.assertIn('BMS poller disabled', logs.output[0])

    def test_defaults(self):
        cfg = bms_poller.load_bms_config({'BMS_LOGGER_HOST': '192.168.1.221', 'BMS_LOGGER_SERIAL': '4060493924'})
        self.assertEqual(cfg, bms_poller.BmsConfig('192.168.1.221', 4060493924, 8899, 1, 60))

    def test_overrides(self):
        cfg = bms_poller.load_bms_config({'BMS_LOGGER_HOST': 'h', 'BMS_LOGGER_SERIAL': '7',
                                          'BMS_LOGGER_PORT': '9000', 'BMS_SLAVE_ID': '2', 'BMS_POLL_SECONDS': '30'})
        self.assertEqual(cfg, bms_poller.BmsConfig('h', 7, 9000, 2, 30))

    def test_invalid_number_disables_with_error(self):
        for key in ('BMS_LOGGER_SERIAL', 'BMS_LOGGER_PORT', 'BMS_SLAVE_ID', 'BMS_POLL_SECONDS'):
            env = {'BMS_LOGGER_HOST': 'h', 'BMS_LOGGER_SERIAL': '7', key: 'abc'}
            with self.assertLogs('bms_poller', 'ERROR'):
                self.assertIsNone(bms_poller.load_bms_config(env))

    def test_poll_interval_is_clamped_to_ten_seconds(self):
        with self.assertLogs('bms_poller', 'WARNING'):
            cfg = bms_poller.load_bms_config({'BMS_LOGGER_HOST': 'h', 'BMS_LOGGER_SERIAL': '7', 'BMS_POLL_SECONDS': '5'})
        self.assertEqual(cfg.poll_seconds, 10)

    def test_disabled_config_never_imports_pysolarmanv5(self):
        # Subprocess: other tests may import pysolarmanv5 into this process.
        code = ("import sys, bms_poller; bms_poller.load_bms_config({}); "
                "sys.exit(1 if 'pysolarmanv5' in sys.modules else 0)")
        result = subprocess.run([sys.executable, '-c', code], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)


class IllegalDataAddressError(Exception):
    """Same class name as umodbus.exceptions.IllegalDataAddressError."""


class FakeClient:
    """Serves registers from a dict {addr: value}. `hang` makes calls never
    return; `fail_at` raises on reads starting at that address."""

    def __init__(self, registers, hang=False, fail_at=None, hang_connect=False, short=False):
        self.registers = registers
        self.hang = hang
        self.hang_connect = hang_connect
        self.fail_at = fail_at
        self.short = short
        self.connected = False
        self.disconnected = False
        self.reads = []

    async def connect(self):
        if self.hang_connect:
            await asyncio.Event().wait()
        self.connected = True

    async def disconnect(self):
        self.disconnected = True

    async def read_holding_registers(self, register_addr, quantity):
        self.reads.append((register_addr, quantity))
        if self.hang:
            await asyncio.Event().wait()
        if self.fail_at == register_addr:
            raise OSError('connection reset')
        if register_addr not in self.registers:
            raise IllegalDataAddressError()
        values = [self.registers.get(register_addr + i, 0) for i in range(quantity)]
        return values[:-1] if self.short else values


def bms_registers(cells=CELLS):
    regs = {bms_poller.SUMMARY_START + i: v for i, v in enumerate(SUMMARY)}
    regs.update({bms_poller.CELLS_START + i: v for i, v in enumerate(cells)})
    return regs


CONFIG = bms_poller.BmsConfig('h', 7)


class PollerHarness:
    def __init__(self, clients, timeout_s=0.05):
        self.clients = list(clients)
        self.created = []
        self.samples = []

        def factory(config):
            client = self.clients.pop(0)
            self.created.append(client)
            return client

        async def on_sample(sample):
            self.samples.append(sample)

        self.poller = bms_poller.BmsPoller(CONFIG, on_sample, client_factory=factory,
                                           now_fn=lambda: WHEN, timeout_s=timeout_s)


class PollOnceTest(unittest.TestCase):
    def test_success_calls_on_sample_once(self):
        h = PollerHarness([FakeClient(bms_registers())])
        sample = asyncio.run(h.poller.poll_once())
        self.assertEqual(h.samples, [sample])
        self.assertEqual(sample.soh, 97)
        # summary in 2 chunks, cells stop after the chunk containing a zero
        self.assertEqual(h.created[0].reads, [(0x1100, 32), (0x1120, 32), (0x1500, 32), (0x1520, 32)])

    def test_failed_second_read_gives_no_sample(self):
        h = PollerHarness([FakeClient(bms_registers(), fail_at=0x1120)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))
        self.assertEqual(h.samples, [])

    def test_short_response_fails_the_poll(self):
        h = PollerHarness([FakeClient(bms_registers(), short=True)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))
        self.assertEqual(h.samples, [])

    def test_hanging_read_times_out(self):
        h = PollerHarness([FakeClient(bms_registers(), hang=True)])
        started = time.monotonic()
        self.assertIsNone(asyncio.run(h.poller.poll_once()))
        self.assertLess(time.monotonic() - started, 1.0)

    def test_hanging_connect_times_out(self):
        h = PollerHarness([FakeClient(bms_registers(), hang_connect=True)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))

    def test_invalid_data_fails_the_poll(self):
        regs = bms_registers()
        regs[0x1107] = 120  # SoC 120
        h = PollerHarness([FakeClient(regs)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))

    def test_cells_filling_a_chunk_continue_until_illegal_address(self):
        cells = [3300] * 64  # no zero in 0x1500-0x153F; 0x1540 doesn't exist
        h = PollerHarness([FakeClient(bms_registers(cells))])
        sample = asyncio.run(h.poller.poll_once())
        self.assertEqual(len(sample.cell_mv), 64)

    def test_illegal_address_on_first_cell_chunk_fails(self):
        regs = {bms_poller.SUMMARY_START + i: v for i, v in enumerate(SUMMARY)}
        h = PollerHarness([FakeClient(regs)])
        self.assertIsNone(asyncio.run(h.poller.poll_once()))

    def test_new_client_after_a_failure(self):
        bad = FakeClient(bms_registers(), fail_at=0x1100)
        good = FakeClient(bms_registers())
        h = PollerHarness([bad, good])

        async def two_polls():
            await h.poller.poll_once()
            return await h.poller.poll_once()

        self.assertIsNotNone(asyncio.run(two_polls()))
        self.assertTrue(bad.disconnected)
        self.assertEqual(h.created, [bad, good])

    def test_failing_on_sample_does_not_count_as_failure(self):
        h = PollerHarness([FakeClient(bms_registers())])

        async def broken(sample):
            raise RuntimeError('db locked')

        h.poller._on_sample = broken
        with self.assertLogs('bms_poller', 'WARNING'):
            self.assertIsNotNone(asyncio.run(h.poller.poll_once()))
        self.assertEqual(h.poller.interval, 60)


class BackoffAndLoggingTest(unittest.TestCase):
    def test_backoff_sequence_and_reset(self):
        h = PollerHarness([FakeClient({}, fail_at=0x1100) for _ in range(7)] + [FakeClient(bms_registers())])
        intervals = []

        async def polls():
            for _ in range(7):
                await h.poller.poll_once()
                intervals.append(h.poller.interval)
            await h.poller.poll_once()

        with self.assertLogs('bms_poller', 'DEBUG'):
            asyncio.run(polls())
        self.assertEqual(intervals, [60, 60, 120, 240, 480, 600, 600])
        self.assertEqual(h.poller.interval, 60)

    def test_log_levels(self):
        h = PollerHarness([FakeClient({}, fail_at=0x1100) for _ in range(4)] + [FakeClient(bms_registers())])

        async def polls():
            for _ in range(5):
                await h.poller.poll_once()

        with self.assertLogs('bms_poller', 'DEBUG') as logs:
            asyncio.run(polls())
        levels = [r.levelname for r in logs.records]
        self.assertEqual(levels, ['INFO', 'INFO', 'WARNING', 'DEBUG', 'INFO'])
        self.assertIn('recovered after 4', logs.records[-1].getMessage())


class RunLoopTest(unittest.TestCase):
    def test_does_not_block_the_event_loop(self):
        # The inverter poll shares this loop: a never-answering logger must
        # not delay other tasks.
        clients = [FakeClient(bms_registers(), hang=True) for _ in range(50)]
        h = PollerHarness(clients, timeout_s=0.1)
        h.poller._sleep = lambda seconds: asyncio.sleep(0.01)
        gaps = []

        async def ticker():
            last = time.monotonic()
            for _ in range(40):
                await asyncio.sleep(0.01)
                now = time.monotonic()
                gaps.append(now - last)
                last = now

        async def main():
            task = asyncio.create_task(h.poller.run())
            await ticker()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with self.assertLogs('bms_poller', 'DEBUG'):  # the failed polls log; keep test output clean
            asyncio.run(main())
        self.assertLess(max(gaps), 0.05)

    def test_cancel_during_read_ends_promptly_and_closes_client(self):
        client = FakeClient(bms_registers(), hang=True)
        h = PollerHarness([client], timeout_s=10)

        async def main():
            task = asyncio.create_task(h.poller.run())
            await asyncio.sleep(0.05)
            task.cancel()
            started = time.monotonic()
            with self.assertRaises(asyncio.CancelledError):
                await task
            return time.monotonic() - started

        self.assertLess(asyncio.run(main()), 0.5)
        self.assertTrue(client.disconnected)
        self.assertEqual(h.poller._failures, 0)


class PayloadTest(unittest.TestCase):
    def test_payload_has_numbers_and_no_raw_block(self):
        payload = bms_poller.sample_to_payload(bms_poller.decode(SUMMARY, CELLS, WHEN))
        self.assertNotIn('raw_1100', payload)
        self.assertEqual(payload['timestamp'], '2026-09-29 08:45:30')
        self.assertEqual(payload['soh'], 97)
        self.assertIsInstance(payload['cell_temp_max'], float)
        self.assertEqual(len(payload['cell_mv']), 60)


if __name__ == '__main__':
    unittest.main()
