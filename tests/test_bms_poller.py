"""
tests/test_bms_poller.py
Decoding is tested against real register dumps read from the Pylontech
Force H2 BMS through the SolarMan logger on 2026-09-29 (SolarMan app showed
cell temperatures 27.2 / 25.7 °C and SOH 97 % at the time).
"""
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

    def test_module_voltages_stop_at_four_slots(self):
        # A 4-module pack: 0x1118-0x111B all set. Reading must stop there,
        # not run on into 0x111C (cell temperature 27.2 -> "0.272 V").
        summary = with_reg(with_reg(with_reg(SUMMARY, 0x1103, 3930), 0x111A, 9825), 0x111B, 9826)

        s = bms_poller.decode(summary, CELLS, WHEN)

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


if __name__ == '__main__':
    unittest.main()
