import sqlite3
import sys
import time
import unittest
from unittest import mock

import _estimate_battery_efficiency as estimator


class FindBatterySessionsTest(unittest.TestCase):
    def test_splits_into_contiguous_same_sign_runs_above_noise_threshold(self):
        # (timestamp_epoch, pbattery1) - positive = charging, negative = discharging,
        # by this repo's existing sign convention (see sensors.py's pbattery1 comment context)
        samples = [
            (0, 500.0), (60, 600.0), (120, 550.0),   # charge session
            (180, 5.0),                               # below noise threshold - a gap
            (240, -400.0), (300, -450.0),              # discharge session
        ]
        sessions = estimator.find_battery_sessions(samples, noise_threshold_w=20.0)

        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0].sign, 'charge')
        self.assertEqual(sessions[0].start_epoch, 0)
        self.assertEqual(sessions[0].end_epoch, 120)
        self.assertEqual(sessions[1].sign, 'discharge')

    def test_empty_input_yields_no_sessions(self):
        self.assertEqual(estimator.find_battery_sessions([], noise_threshold_w=20.0), [])


class FindMatchedCyclePairsTest(unittest.TestCase):
    def test_pairs_a_charge_and_a_later_discharge_returning_to_similar_soc(self):
        # (timestamp_epoch, battery_soc)
        soc_samples = [(0, 40.0), (3600, 70.0), (7200, 68.0), (10800, 41.0)]
        # The SOC swing here (40 -> 70, a 30-point excursion) comfortably
        # clears a 10.0-point min_excursion, so this preserves the test's
        # original intent (a real charge/discharge round trip) while also
        # exercising the new guard.
        pairs = estimator.find_matched_cycle_pairs(soc_samples, max_gap_seconds=24 * 3600, soc_tolerance=5.0,
                                                     min_excursion=10.0)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0], (0, 10800))

    def test_no_pair_when_soc_never_returns_within_tolerance(self):
        soc_samples = [(0, 40.0), (3600, 70.0), (7200, 90.0)]
        pairs = estimator.find_matched_cycle_pairs(soc_samples, max_gap_seconds=24 * 3600, soc_tolerance=5.0,
                                                     min_excursion=10.0)

        self.assertEqual(pairs, [])

    def test_no_pair_when_soc_barely_moves_even_within_tolerance(self):
        # Consecutive 1Hz-style samples that never really depart from the
        # starting SOC - before the min_excursion guard, every one of
        # these adjacent pairs would spuriously qualify as its own
        # "cycle" since they're trivially within soc_tolerance of each
        # other.
        soc_samples = [(0, 50.0), (1, 50.01), (2, 50.02), (3, 50.0), (4, 49.99)]
        pairs = estimator.find_matched_cycle_pairs(soc_samples, max_gap_seconds=24 * 3600, soc_tolerance=3.0,
                                                     min_excursion=10.0)

        self.assertEqual(pairs, [])

    def test_one_pair_for_a_realistic_1hz_slow_charge_then_discharge_cycle(self):
        # Realistic 1Hz sampling: SOC creeps up 0.01%/second for an hour
        # (a ~36-point excursion), then back down over another hour. The
        # old implementation (no min_excursion guard) would have produced
        # thousands of spurious one-second "cycles" here instead of the
        # one real charge/discharge round trip.
        soc_samples = []
        soc = 20.0
        epoch = 0
        for _ in range(3600):
            soc_samples.append((epoch, round(soc, 4)))
            soc += 0.01
            epoch += 1
        for _ in range(3600):
            soc_samples.append((epoch, round(soc, 4)))
            soc -= 0.01
            epoch += 1
        soc_samples.append((epoch, round(soc, 4)))  # back near the start

        pairs = estimator.find_matched_cycle_pairs(soc_samples, max_gap_seconds=24 * 3600, soc_tolerance=1.0,
                                                     min_excursion=10.0)

        self.assertEqual(len(pairs), 1)

    def test_completes_quickly_on_50000_samples(self):
        # Regression guard against the old O(n^2) list-slicing behavior
        # (confirmed by profiling: 20k samples took 0.065s, 80k took 1.0s,
        # 160k took 4.1s - clean quadratic growth). This should stay well
        # under a second with the index-based implementation.
        soc_samples = []
        soc = 20.0
        epoch = 0
        for _ in range(25000):
            soc_samples.append((epoch, round(soc, 4)))
            soc += 0.01
            epoch += 1
        for _ in range(25000):
            soc_samples.append((epoch, round(soc, 4)))
            soc -= 0.01
            epoch += 1

        start = time.monotonic()
        estimator.find_matched_cycle_pairs(soc_samples, max_gap_seconds=24 * 3600, soc_tolerance=1.0,
                                            min_excursion=10.0)
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 5.0)

    def test_completes_quickly_on_flat_1hz_data_with_no_qualifying_cycle_anywhere(self):
        # This is the actual worst case the min_excursion guard introduced:
        # test_completes_quickly_on_50000_samples above is all genuine
        # charge/discharge movement, so every start index quickly finds a
        # qualifying end and the inner loop breaks early - it never
        # exercises the slow path. A stretch with NO qualifying cycle at
        # all (e.g. the battery sat idle for days, flat SOC) instead scans
        # every start index all the way out to max_gap_seconds worth of
        # samples before giving up, which is roughly quadratic in sample
        # count - measured at ~90x slower than the old (buggy) code on
        # flat data (61s at 40k samples vs 0.67s). Downsampling to
        # 1-per-minute (as main() now does via downsample_to_interval
        # before calling find_matched_cycle_pairs) shrinks the input by
        # ~60x and keeps this fast even on a long flat stretch.
        soc_samples = [(epoch, 50.0) for epoch in range(40000)]
        downsampled = estimator.downsample_to_interval(soc_samples, 60)

        start = time.monotonic()
        pairs = estimator.find_matched_cycle_pairs(downsampled, max_gap_seconds=3 * 24 * 3600, soc_tolerance=3.0,
                                                     min_excursion=10.0)
        elapsed = time.monotonic() - start

        self.assertEqual(pairs, [])
        self.assertLess(elapsed, 5.0)


class DownsampleToIntervalTest(unittest.TestCase):
    def test_keeps_only_the_first_sample_in_each_bucket(self):
        samples = [(0, 1.0), (10, 2.0), (30, 3.0), (60, 4.0), (65, 5.0), (119, 6.0), (120, 7.0)]

        downsampled = estimator.downsample_to_interval(samples, 60)

        self.assertEqual(downsampled, [(0, 1.0), (60, 4.0), (120, 7.0)])

    def test_empty_input_returns_empty_list(self):
        self.assertEqual(estimator.downsample_to_interval([], 60), [])

    def test_interval_larger_than_sample_gaps_still_yields_one_sample_per_bucket(self):
        # Samples every 5s, but a 60s bucket - each bucket should still
        # contribute exactly its first sample, not every sample within it.
        samples = [(t, float(t)) for t in range(0, 180, 5)]

        downsampled = estimator.downsample_to_interval(samples, 60)

        self.assertEqual(downsampled, [(0, 0.0), (60, 60.0), (120, 120.0)])


class EstimateInverterLossTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.execute("CREATE TABLE inverter_history (timestamp_epoch INTEGER, pbattery1 REAL, pgrid REAL, battery_soc REAL)")

    def test_charge_session_with_known_ac_dc_ratio(self):
        # AC-side draws consistently more than DC-side receives -> a
        # positive, computable charge loss.
        rows = [(0, 1000.0, 1100.0, 40.0), (60, 1000.0, 1100.0, 41.0), (120, 1000.0, 1100.0, 42.0)]
        self.conn.executemany("INSERT INTO inverter_history VALUES (?, ?, ?, ?)", rows)
        self.conn.commit()

        sessions = estimator.find_battery_sessions([(r[0], r[1]) for r in rows], noise_threshold_w=20.0)
        result = estimator.estimate_inverter_loss(self.conn, sessions)

        charge_loss, charge_n = result['charge']
        self.assertGreater(charge_loss, 0)
        self.assertLess(charge_loss, 0.2)
        self.assertEqual(charge_n, 1)


class MainOnEmptyDatabaseTest(unittest.TestCase):
    def test_does_not_crash_when_inverter_history_table_is_missing(self):
        # An empty/never-initialized data.db (e.g. a fresh checkout that
        # hasn't run main.py yet) shouldn't crash the script - it should
        # print a "no data" style message instead.
        with mock.patch.object(sys, 'argv', ['_estimate_battery_efficiency.py', '--db-path', ':memory:']):
            try:
                estimator.main()
            except sqlite3.OperationalError as exc:
                self.fail(f"main() raised {exc!r} on a database with no inverter_history table")


if __name__ == '__main__':
    unittest.main()
