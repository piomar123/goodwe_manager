import sqlite3
import sys
import unittest
from unittest import mock

import _estimate_battery_efficiency as estimator

THRESHOLDS = estimator.Thresholds(battery_w=200.0, grid_phase_w=200.0, pv_w=50.0)


class ClassifySampleTest(unittest.TestCase):
    def test_grid_charge(self):
        # importing on all 3 phases, battery charging, no PV
        state = estimator.classify_sample(pbattery1=500.0, pgrid=-400.0, pgrid2=-400.0, pgrid3=-400.0,
                                           ppv=0.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.GRID_CHARGE)

    def test_grid_discharge(self):
        state = estimator.classify_sample(pbattery1=-500.0, pgrid=400.0, pgrid2=400.0, pgrid3=400.0,
                                           ppv=0.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.GRID_DISCHARGE)

    def test_pv_export(self):
        state = estimator.classify_sample(pbattery1=0.0, pgrid=400.0, pgrid2=400.0, pgrid3=400.0,
                                           ppv=2000.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.PV_EXPORT)

    def test_pv_charge(self):
        state = estimator.classify_sample(pbattery1=1500.0, pgrid=0.0, pgrid2=0.0, pgrid3=0.0,
                                           ppv=2000.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.PV_CHARGE)

    def test_battery_to_load(self):
        state = estimator.classify_sample(pbattery1=-500.0, pgrid=0.0, pgrid2=0.0, pgrid3=0.0,
                                           ppv=0.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertEqual(state, estimator.BATTERY_TO_LOAD)

    def test_pv_charge_rejects_large_phase_imbalanced_grid_flow_as_not_idle(self):
        # Regression: _grid_direction() returning None (phases disagree
        # in sign) used to be treated as "grid idle" here, letting a
        # real but phase-imbalanced flow (one phase importing 3kW while
        # another exports 3kW, netting near zero) contaminate a
        # supposedly PV-only charging measurement.
        state = estimator.classify_sample(pbattery1=1500.0, pgrid=3000.0, pgrid2=-3000.0, pgrid3=0.0,
                                           ppv=2000.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertIsNone(state)

    def test_phase_imbalanced_grid_is_unclassified_not_idle(self):
        # netting near zero, but individual phases disagree in sign -
        # a real but non-clean flow state, not "idle".
        state = estimator.classify_sample(pbattery1=0.0, pgrid=400.0, pgrid2=-400.0, pgrid3=0.0,
                                           ppv=0.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertIsNone(state)

    def test_everything_idle_is_unclassified(self):
        state = estimator.classify_sample(pbattery1=10.0, pgrid=10.0, pgrid2=10.0, pgrid3=10.0,
                                           ppv=5.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertIsNone(state)

    def test_multiple_signals_active_at_once_is_unclassified(self):
        # PV producing AND grid importing AND battery charging all at
        # once doesn't match any of the five clean paths.
        state = estimator.classify_sample(pbattery1=500.0, pgrid=-400.0, pgrid2=-400.0, pgrid3=-400.0,
                                           ppv=2000.0, grid_mode=estimator.GRID_MODE_CONNECTED, thresholds=THRESHOLDS)
        self.assertIsNone(state)

    def test_islanded_sample_is_unclassified_even_if_it_otherwise_looks_like_pv_charge(self):
        # Regression: a real islanded/off-grid sample was found where
        # pgrid/pgrid2/pgrid3/load_ptotal all read exactly 0.0 (meter-
        # derived fields stale/zeroed during an outage) while ppv and
        # pbattery1 kept reporting normally - this would otherwise have
        # looked exactly like a clean pv_charge sample.
        state = estimator.classify_sample(pbattery1=700.0, pgrid=0.0, pgrid2=0.0, pgrid3=0.0,
                                           ppv=200.0, grid_mode=0, thresholds=THRESHOLDS)
        self.assertIsNone(state)


class FindLabeledSessionsTest(unittest.TestCase):
    def test_splits_into_contiguous_same_state_runs(self):
        rows = [
            (0, 500.0, -400.0, -400.0, -400.0, 0.0, estimator.GRID_MODE_CONNECTED),   # grid_charge
            (60, 550.0, -420.0, -420.0, -420.0, 0.0, estimator.GRID_MODE_CONNECTED),  # grid_charge
            (120, 10.0, 10.0, 10.0, 10.0, 5.0, estimator.GRID_MODE_CONNECTED),         # idle - a gap
            (180, -500.0, 400.0, 400.0, 400.0, 0.0, estimator.GRID_MODE_CONNECTED),   # grid_discharge
        ]
        sessions = estimator.find_labeled_sessions(rows, THRESHOLDS)

        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0].state, estimator.GRID_CHARGE)
        self.assertEqual(sessions[0].start_epoch, 0)
        self.assertEqual(sessions[0].end_epoch, 60)
        self.assertEqual(sessions[1].state, estimator.GRID_DISCHARGE)

    def test_empty_input_yields_no_sessions(self):
        self.assertEqual(estimator.find_labeled_sessions([], THRESHOLDS), [])

    def test_islanded_stretch_ends_a_session_and_yields_none(self):
        rows = [
            (0, 700.0, 0.0, 0.0, 0.0, 200.0, estimator.GRID_MODE_CONNECTED),   # pv_charge
            (60, 700.0, 0.0, 0.0, 0.0, 200.0, 0),  # grid_mode drops to Not Connected mid-run
            (120, 700.0, 0.0, 0.0, 0.0, 200.0, estimator.GRID_MODE_CONNECTED),  # pv_charge resumes
        ]
        sessions = estimator.find_labeled_sessions(rows, THRESHOLDS)

        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0].end_epoch, 0)
        self.assertEqual(sessions[1].start_epoch, 120)


class FilterMinDurationTest(unittest.TestCase):
    def test_drops_sessions_shorter_than_threshold(self):
        sessions = [
            estimator.Session(estimator.GRID_CHARGE, 0, 3),      # 3s - too short
            estimator.Session(estimator.GRID_DISCHARGE, 100, 200),  # 100s - kept
        ]

        filtered = estimator.filter_min_duration(sessions, min_seconds=5)

        self.assertEqual(filtered, [sessions[1]])

    def test_boundary_duration_equal_to_threshold_is_kept(self):
        sessions = [estimator.Session(estimator.GRID_CHARGE, 0, 5)]

        filtered = estimator.filter_min_duration(sessions, min_seconds=5)

        self.assertEqual(filtered, sessions)


class MeasureSessionsTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.execute("""
            CREATE TABLE inverter_history (
                timestamp_epoch INTEGER, timestamp TEXT, pbattery1 REAL,
                pgrid REAL, pgrid2 REAL, pgrid3 REAL, ppv REAL, load_ptotal REAL,
                e_total_imp REAL, e_total_exp REAL, e_bat_charge_total REAL,
                e_bat_discharge_total REAL, e_day REAL, e_load_total REAL
            )
        """)

    def _insert(self, rows):
        self.conn.executemany(
            "INSERT INTO inverter_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        self.conn.commit()

    def test_edge_trim_drops_samples_from_both_ends_before_measuring(self):
        # A garbage first/last sample (as if cross-register skew at the
        # transition made pgrid read something implausible) - trimming 1
        # sample off each end should exclude both from the integral.
        rows = [
            (0, "2026-07-15 10:00:00", 1000.0, -99999.0, -99999.0, -99999.0, 0.0, 0.0,
             100.0, 50.0, 200.0, 80.0, 5.0, 30.0),  # garbage edge sample
            (60, "2026-07-15 10:01:00", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             100.02, 50.0, 200.017, 80.0, 5.0, 30.0),
            (120, "2026-07-15 10:02:00", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             100.04, 50.0, 200.033, 80.0, 5.0, 30.0),
            (180, "2026-07-15 10:03:00", 1000.0, 99999.0, 99999.0, 99999.0, 0.0, 0.0,
             100.06, 50.0, 200.05, 80.0, 5.0, 30.0),  # garbage edge sample
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.GRID_CHARGE, 0, 180)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=1)

        t = totals[estimator.GRID_CHARGE]
        self.assertEqual(t.session_count, 1)
        # only the middle two (clean, 400W) samples should contribute -
        # 1200W AC for 60s = 20Wh, nowhere near the 99999W garbage values
        self.assertAlmostEqual(t.input_integral_wh, 20.0, places=2)

    def test_edge_trim_leaving_fewer_than_two_samples_is_skipped(self):
        rows = [
            (0, "2026-07-15 10:00:00", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             100.0, 50.0, 200.0, 80.0, 5.0, 30.0),
            (60, "2026-07-15 10:01:00", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             100.02, 50.0, 200.017, 80.0, 5.0, 30.0),
            (120, "2026-07-15 10:02:00", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             100.04, 50.0, 200.033, 80.0, 5.0, 30.0),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.GRID_CHARGE, 0, 120)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=2)

        self.assertEqual(totals[estimator.GRID_CHARGE].session_count, 0)

    def test_grid_charge_session_measures_both_delta_and_integral(self):
        # AC-side (grid import, all 3 phases) consistently exceeds DC-side
        # (battery charge) -> a positive, computable loss both ways.
        rows = [
            (0, "2026-07-15 10:00:00", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             100.0, 50.0, 200.0, 80.0, 5.0, 30.0),
            (60, "2026-07-15 10:01:00", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             100.02, 50.0, 200.017, 80.0, 5.0, 30.0),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.GRID_CHARGE, 0, 60)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=0)

        t = totals[estimator.GRID_CHARGE]
        self.assertEqual(t.session_count, 1)
        self.assertEqual(t.delta_sessions, 1)
        # integral: 1200W AC (3x400) for 60s = 20Wh in, 1000W DC for 60s = 16.67Wh out
        self.assertAlmostEqual(t.input_integral_wh, 20.0, places=2)
        self.assertAlmostEqual(t.output_integral_wh, 16.667, places=2)
        loss_integral = t.loss_integral()
        self.assertGreater(loss_integral, 0)
        # delta: (100.02-100.0)*1000 = 20Wh in, (200.017-200.0)*1000 = 17Wh out
        self.assertAlmostEqual(t.input_delta_wh, 20.0, places=2)
        self.assertAlmostEqual(t.output_delta_wh, 17.0, places=2)

    def test_pv_session_skips_delta_when_crossing_midnight(self):
        # e_day resets at midnight - a session straddling it must not
        # measure a delta (would read as a nonsensical negative/garbage
        # value), but the integral method is unaffected.
        rows = [
            (0, "2026-07-15 23:59:30", 1500.0, 0.0, 0.0, 0.0, 2000.0, 0.0,
             0.0, 0.0, 0.0, 0.0, 15.0, 0.0),
            (60, "2026-07-16 00:00:30", 1500.0, 0.0, 0.0, 0.0, 2000.0, 0.0,
             0.0, 0.0, 0.0, 0.0, 0.5, 0.0),
        ]
        self._insert(rows)
        sessions = [estimator.Session(estimator.PV_CHARGE, 0, 60)]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=0)

        t = totals[estimator.PV_CHARGE]
        self.assertEqual(t.session_count, 1)
        self.assertEqual(t.delta_sessions, 0)
        self.assertGreater(t.input_integral_wh, 0)

    def test_energy_weighted_aggregation_not_averaged_per_session_ratio(self):
        # Two grid_charge sessions: one tiny (noisy ratio if averaged
        # naively), one large and clean. Energy-weighted totals should be
        # dominated by the large session, not skewed 50/50 by the tiny one.
        rows = [
            # tiny session: 1 second, noisy-looking ratio
            (0, "2026-07-15 10:00:00", 1000.0, -1000.0, -1000.0, -1000.0, 0.0, 0.0,
             100.0, 50.0, 200.0, 80.0, 5.0, 30.0),
            (1, "2026-07-15 10:00:01", 1000.0, -1000.0, -1000.0, -1000.0, 0.0, 0.0,
             100.0008, 50.0, 200.0002, 80.0, 5.0, 30.0),
            # large, clean session far later
            (1000, "2026-07-15 10:16:40", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             110.0, 50.0, 209.0, 80.0, 5.0, 30.0),
            (4600, "2026-07-15 11:16:40", 1000.0, -400.0, -400.0, -400.0, 0.0, 0.0,
             111.0, 50.0, 209.833, 80.0, 5.0, 30.0),
        ]
        self._insert(rows)
        sessions = [
            estimator.Session(estimator.GRID_CHARGE, 0, 1),
            estimator.Session(estimator.GRID_CHARGE, 1000, 4600),
        ]

        totals = estimator.measure_sessions(self.conn, sessions, edge_trim_samples=0)
        t = totals[estimator.GRID_CHARGE]

        self.assertEqual(t.session_count, 2)
        # the large session's ~1000Wh should dominate the tiny session's ~3.3Wh
        self.assertGreater(t.input_delta_wh, 900.0)


class DeriveBatteryLossDischargeTest(unittest.TestCase):
    def test_backs_out_battery_only_loss_from_combined_and_inverter_loss(self):
        # combined efficiency 0.90, inverter efficiency 0.95 ->
        # battery efficiency = 0.90 / 0.95 ~= 0.9474 -> loss ~= 0.0526
        result = estimator._derive_battery_loss_discharge(combined_loss=0.10, inverter_loss=0.05)

        self.assertAlmostEqual(result, 0.05263, places=4)

    def test_none_when_either_input_is_missing(self):
        self.assertIsNone(estimator._derive_battery_loss_discharge(None, 0.05))
        self.assertIsNone(estimator._derive_battery_loss_discharge(0.10, None))


class EstimateEfficiencyTest(unittest.TestCase):
    def test_inverter_loss_averages_the_three_paths(self):
        totals = {state: estimator.SessionTypeTotals() for state in estimator.ALL_STATES}
        totals[estimator.GRID_CHARGE].input_delta_wh = 100.0
        totals[estimator.GRID_CHARGE].output_delta_wh = 96.0  # loss 0.04
        totals[estimator.GRID_DISCHARGE].input_delta_wh = 100.0
        totals[estimator.GRID_DISCHARGE].output_delta_wh = 95.0  # loss 0.05
        totals[estimator.PV_EXPORT].input_delta_wh = 100.0
        totals[estimator.PV_EXPORT].output_delta_wh = 97.0  # loss 0.03

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
