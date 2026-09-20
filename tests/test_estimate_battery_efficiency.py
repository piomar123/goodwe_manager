import sqlite3
import sys
import unittest
from unittest import mock

import _estimate_battery_efficiency as estimator

THRESHOLDS = estimator.Thresholds(battery_w=200.0, pv_w=50.0, grid_idle_w=60.0)
CONNECTED = estimator.GRID_MODE_CONNECTED


class ClassifySampleTest(unittest.TestCase):
    def test_battery_ac_charge(self):
        # pbattery1 negative = charging (verified sign, see PR #33) -
        # PV idle, so the charge must be coming via the AC/inverter path.
        state = estimator.classify_sample(pbattery1=-500.0, pgrid=400.0, pgrid2=400.0, pgrid3=400.0,
                                           ppv=0.0, grid_mode=CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.BATTERY_AC_CHARGE)

    def test_battery_ac_discharge(self):
        # pbattery1 positive = discharging.
        state = estimator.classify_sample(pbattery1=500.0, pgrid=400.0, pgrid2=400.0, pgrid3=400.0,
                                           ppv=0.0, grid_mode=CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.BATTERY_AC_DISCHARGE)

    def test_pv_ac(self):
        # PV producing, battery idle - regardless of where the AC output
        # goes (load or export), this is pv_ac.
        state = estimator.classify_sample(pbattery1=0.0, pgrid=400.0, pgrid2=400.0, pgrid3=400.0,
                                           ppv=2000.0, grid_mode=CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.PV_AC)

    def test_pv_charge(self):
        # PV producing, battery charging (negative), inverter's own AC
        # output near zero - the DC-bus bypass path.
        state = estimator.classify_sample(pbattery1=-1500.0, pgrid=0.0, pgrid2=0.0, pgrid3=0.0,
                                           ppv=2000.0, grid_mode=CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.PV_CHARGE)

    def test_pv_producing_and_battery_charging_with_ac_output_present_is_battery_ac_charge_not_pv_charge(self):
        # If the inverter's own AC output isn't near zero while PV
        # produces and battery charges, some of the charge is crossing
        # the AC path (grid contribution) - not a clean DC-bus bypass.
        # Falls through to unclassified since neither pv_ac's "battery
        # idle" nor pv_charge's "AC idle" condition holds.
        state = estimator.classify_sample(pbattery1=-1500.0, pgrid=300.0, pgrid2=300.0, pgrid3=300.0,
                                           ppv=2000.0, grid_mode=CONNECTED, thresholds=THRESHOLDS)
        self.assertIsNone(state)

    def test_everything_idle_is_unclassified(self):
        state = estimator.classify_sample(pbattery1=10.0, pgrid=10.0, pgrid2=10.0, pgrid3=10.0,
                                           ppv=5.0, grid_mode=CONNECTED, thresholds=THRESHOLDS)
        self.assertIsNone(state)

    def test_islanded_sample_is_unclassified_even_if_it_otherwise_looks_clean(self):
        # Regression: a real islanded/off-grid sample was found where
        # pgrid/pgrid2/pgrid3/load_ptotal all read exactly 0.0 (meter-
        # derived fields stale/zeroed during an outage) while ppv and
        # pbattery1 kept reporting normally.
        state = estimator.classify_sample(pbattery1=-700.0, pgrid=0.0, pgrid2=0.0, pgrid3=0.0,
                                           ppv=200.0, grid_mode=0, thresholds=THRESHOLDS)
        self.assertIsNone(state)


class FindLabeledSessionsTest(unittest.TestCase):
    def test_splits_into_contiguous_same_state_runs(self):
        rows = [
            (0, -500.0, 400.0, 400.0, 400.0, 0.0, CONNECTED),   # battery_ac_charge
            (60, -550.0, 420.0, 420.0, 420.0, 0.0, CONNECTED),  # battery_ac_charge
            (120, 10.0, 10.0, 10.0, 10.0, 5.0, CONNECTED),      # idle - a gap
            (180, 500.0, 400.0, 400.0, 400.0, 0.0, CONNECTED),  # battery_ac_discharge
        ]
        sessions = estimator.find_labeled_sessions(rows, THRESHOLDS)

        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0].state, estimator.BATTERY_AC_CHARGE)
        self.assertEqual(sessions[0].start_epoch, 0)
        self.assertEqual(sessions[0].end_epoch, 60)
        self.assertEqual(sessions[1].state, estimator.BATTERY_AC_DISCHARGE)

    def test_empty_input_yields_no_sessions(self):
        self.assertEqual(estimator.find_labeled_sessions([], THRESHOLDS), [])

    def test_islanded_stretch_ends_a_session_and_yields_none(self):
        rows = [
            (0, -1500.0, 0.0, 0.0, 0.0, 2000.0, CONNECTED),   # pv_charge
            (60, -1500.0, 0.0, 0.0, 0.0, 2000.0, 0),          # grid_mode drops to Not Connected mid-run
            (120, -1500.0, 0.0, 0.0, 0.0, 2000.0, CONNECTED),  # pv_charge resumes
        ]
        sessions = estimator.find_labeled_sessions(rows, THRESHOLDS)

        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0].end_epoch, 0)
        self.assertEqual(sessions[1].start_epoch, 120)


class FilterMinDurationTest(unittest.TestCase):
    def test_drops_sessions_shorter_than_threshold(self):
        sessions = [
            estimator.Session(estimator.BATTERY_AC_CHARGE, 0, 3),        # 3s - too short
            estimator.Session(estimator.BATTERY_AC_DISCHARGE, 100, 200),  # 100s - kept
        ]

        filtered = estimator.filter_min_duration(sessions, min_seconds=5)

        self.assertEqual(filtered, [sessions[1]])

    def test_boundary_duration_equal_to_threshold_is_kept(self):
        sessions = [estimator.Session(estimator.BATTERY_AC_CHARGE, 0, 5)]

        filtered = estimator.filter_min_duration(sessions, min_seconds=5)

        self.assertEqual(filtered, sessions)


class MeasureSessionsTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.execute("""
            CREATE TABLE inverter_history (
                timestamp_epoch INTEGER, timestamp TEXT, pbattery1 REAL,
                pgrid REAL, pgrid2 REAL, pgrid3 REAL, ppv REAL,
                e_bat_charge_total REAL, e_bat_discharge_total REAL, e_day REAL
            )
        """)

    def _insert(self, rows):
        self.conn.executemany(
            "INSERT INTO inverter_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        self.conn.commit()

    def test_edge_trim_drops_samples_from_both_ends_before_measuring(self):
        rows = [
            (0, "2026-07-15 10:00:00", -1000.0, 99999.0, 99999.0, 99999.0, 0.0, 200.0, 80.0, 5.0),
            (60, "2026-07-15 10:01:00", -1000.0, 400.0, 400.0, 400.0, 0.0, 200.017, 80.0, 5.0),
            (120, "2026-07-15 10:02:00", -1000.0, 400.0, 400.0, 400.0, 0.0, 200.033, 80.0, 5.0),
            (180, "2026-07-15 10:03:00", -1000.0, -99999.0, -99999.0, -99999.0, 0.0, 200.05, 80.0, 5.0),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.BATTERY_AC_CHARGE, 0, 180)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=1)

        t = totals[estimator.BATTERY_AC_CHARGE]
        self.assertEqual(t.session_count, 1)
        # only the middle two (clean, 1200W sum) samples should contribute
        self.assertAlmostEqual(t.input_integral_wh, 20.0, places=2)

    def test_edge_trim_leaving_fewer_than_two_samples_is_skipped(self):
        rows = [
            (0, "2026-07-15 10:00:00", -1000.0, 400.0, 400.0, 400.0, 0.0, 200.0, 80.0, 5.0),
            (60, "2026-07-15 10:01:00", -1000.0, 400.0, 400.0, 400.0, 0.0, 200.017, 80.0, 5.0),
            (120, "2026-07-15 10:02:00", -1000.0, 400.0, 400.0, 400.0, 0.0, 200.033, 80.0, 5.0),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.BATTERY_AC_CHARGE, 0, 120)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=2)

        self.assertEqual(totals[estimator.BATTERY_AC_CHARGE].session_count, 0)

    def test_battery_ac_charge_session_measures_both_delta_and_integral(self):
        # AC-side (inverter's own on-grid output, all 3 phases) exceeds
        # DC-side (battery charge) -> a positive, computable loss.
        rows = [
            (0, "2026-07-15 10:00:00", -1000.0, 400.0, 400.0, 400.0, 0.0, 200.0, 80.0, 5.0),
            (60, "2026-07-15 10:01:00", -1000.0, 400.0, 400.0, 400.0, 0.0, 200.017, 80.0, 5.0),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.BATTERY_AC_CHARGE, 0, 60)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=0)

        t = totals[estimator.BATTERY_AC_CHARGE]
        self.assertEqual(t.session_count, 1)
        # integral: 1200W AC (3x400) for 60s = 20Wh in, 1000W DC for 60s = 16.67Wh out
        self.assertAlmostEqual(t.input_integral_wh, 20.0, places=2)
        self.assertAlmostEqual(t.output_integral_wh, 16.667, places=2)
        self.assertGreater(t.loss_integral(), 0)
        # no e_total_imp-style counter exists for the inverter's own AC
        # output, so this is never delta-eligible
        self.assertEqual(t.delta_sessions, 0)
        self.assertIsNone(t.loss_delta())

    def test_pv_charge_session_measures_both_delta_and_integral(self):
        rows = [
            (0, "2026-07-15 10:00:00", -1000.0, 0.0, 0.0, 0.0, 1200.0, 200.0, 80.0, 5.0),
            (60, "2026-07-15 10:01:00", -1000.0, 0.0, 0.0, 0.0, 1200.0, 200.017, 80.0, 5.02),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.PV_CHARGE, 0, 60)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=0)

        t = totals[estimator.PV_CHARGE]
        self.assertEqual(t.session_count, 1)
        self.assertEqual(t.delta_sessions, 1)
        self.assertGreater(t.loss_integral(), 0)

    def test_pv_session_skips_delta_when_crossing_midnight(self):
        # e_day resets at midnight - a session straddling it must not
        # measure a delta, but the integral method is unaffected.
        rows = [
            (0, "2026-07-15 23:59:30", -1500.0, 0.0, 0.0, 0.0, 2000.0, 200.0, 80.0, 15.0),
            (60, "2026-07-16 00:00:30", -1500.0, 0.0, 0.0, 0.0, 2000.0, 200.5, 80.0, 0.5),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.PV_CHARGE, 0, 60)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=0)

        t = totals[estimator.PV_CHARGE]
        self.assertEqual(t.session_count, 1)
        self.assertEqual(t.delta_sessions, 0)
        self.assertGreater(t.input_integral_wh, 0)

    def test_energy_weighted_aggregation_not_averaged_per_session_ratio(self):
        rows = [
            # tiny session: 1 second, noisy-looking ratio
            (0, "2026-07-15 10:00:00", -1000.0, 1000.0, 1000.0, 1000.0, 0.0, 200.0, 80.0, 5.0),
            (1, "2026-07-15 10:00:01", -1000.0, 1000.0, 1000.0, 1000.0, 0.0, 200.0008, 80.0, 5.0),
            # large, clean session far later
            (1000, "2026-07-15 10:16:40", -1000.0, 400.0, 400.0, 400.0, 0.0, 209.0, 80.0, 5.0),
            (4600, "2026-07-15 11:16:40", -1000.0, 400.0, 400.0, 400.0, 0.0, 209.833, 80.0, 5.0),
        ]
        self._insert(rows)
        sessions = [
            estimator.Session(estimator.BATTERY_AC_CHARGE, 0, 1),
            estimator.Session(estimator.BATTERY_AC_CHARGE, 1000, 4600),
        ]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=0)
        t = totals[estimator.BATTERY_AC_CHARGE]

        self.assertEqual(t.session_count, 2)
        # the large session's ~1000Wh should dominate the tiny session's ~3.3Wh
        self.assertGreater(t.input_integral_wh, 900.0)


class DeriveBatteryLossTest(unittest.TestCase):
    def test_backs_out_battery_only_loss_from_combined_and_inverter_loss(self):
        # combined efficiency 0.90, inverter efficiency 0.95 ->
        # battery efficiency = 0.90 / 0.95 ~= 0.9474 -> loss ~= 0.0526
        result = estimator._derive_battery_loss(combined_loss=0.10, inverter_loss=0.05)

        self.assertAlmostEqual(result, 0.05263, places=4)

    def test_none_when_either_input_is_missing(self):
        self.assertIsNone(estimator._derive_battery_loss(None, 0.05))
        self.assertIsNone(estimator._derive_battery_loss(0.10, None))


class EstimateEfficiencyTest(unittest.TestCase):
    def test_inverter_loss_comes_from_pv_ac_alone(self):
        totals = {state: estimator.SessionTypeTotals() for state in estimator.ALL_STATES}
        totals[estimator.PV_AC].input_delta_wh = 100.0
        totals[estimator.PV_AC].output_delta_wh = 96.0  # loss 0.04

        estimate = estimator.estimate_efficiency(totals, method='delta')

        self.assertAlmostEqual(estimate.inverter_loss, 0.04, places=6)

    def test_no_data_yields_none_not_a_crash(self):
        totals = {state: estimator.SessionTypeTotals() for state in estimator.ALL_STATES}

        estimate = estimator.estimate_efficiency(totals, method='delta')

        self.assertIsNone(estimate.inverter_loss)
        self.assertIsNone(estimate.battery_loss)
        self.assertIsNone(estimate.battery_loss_discharge)


class MainOnEmptyDatabaseTest(unittest.TestCase):
    def test_does_not_crash_when_inverter_history_table_is_missing(self):
        with mock.patch.object(sys, 'argv', ['_estimate_battery_efficiency.py', '--db-path', ':memory:']):
            try:
                estimator.main()
            except sqlite3.OperationalError as exc:
                self.fail(f"main() raised {exc!r} on a database with no inverter_history table")


if __name__ == '__main__':
    unittest.main()
