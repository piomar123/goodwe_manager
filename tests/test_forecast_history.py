import os
import tempfile
import unittest

import forecast_history


class ForecastHistoryTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)  # sqlite3.connect creates it fresh
        self.conn = forecast_history.init_db(self.db_path)

    def tearDown(self):
        self.conn.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def test_init_db_is_idempotent(self):
        forecast_history.init_db(self.db_path).close()

    def test_get_snapshot_missing_returns_none(self):
        self.assertIsNone(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 100))

    def test_write_then_get_snapshot_round_trips(self):
        payload = {'07:00': 0.5, '08:00': 0.9}
        fetched_at = forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', payload, now=1000)
        self.assertEqual(fetched_at, 1000)
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 1000), payload)

    def test_write_snapshot_with_different_payload_inserts_new_row(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.6}, now=2000)
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 1000), {'07:00': 0.5})
        self.assertEqual(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 2000), {'07:00': 0.6})

    def test_write_snapshot_with_identical_payload_dedupes_by_bumping_valid_until(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        returned_fetched_at = forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=2000)
        # deduped into the original snapshot, not a new row at fetched_at=2000
        self.assertEqual(returned_fetched_at, 1000)
        self.assertIsNone(forecast_history.get_snapshot(self.conn, 'meteosource', '2026-01-01', 2000))
        row = self.conn.execute(
            "SELECT valid_until FROM forecast_snapshots WHERE source = 'meteosource' AND date = '2026-01-01' AND fetched_at = 1000"
        ).fetchone()
        self.assertEqual(row[0], 2000)

    def test_get_fetch_times_returns_distinct_times_across_both_sources_newest_first(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-01', {'07:00': {'c10': 0.1, 'c50': 0.2, 'c90': 0.3}}, now=2000)
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-01'), [2000, 1000])

    def test_get_fetch_times_only_for_requested_date(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-02', {'07:00': 0.5}, now=2000)
        self.assertEqual(forecast_history.get_fetch_times(self.conn, '2026-01-02'), [2000])

    def test_get_latest_merged_with_single_snapshot_returns_it(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5, '08:00': 0.9}, now=1000)
        self.assertEqual(
            forecast_history.get_latest_merged(self.conn, 'meteosource', '2026-01-01'),
            {'07:00': 0.5, '08:00': 0.9},
        )

    def test_get_latest_merged_prefers_newer_period_but_keeps_older_periods_the_newer_snapshot_lacks(self):
        # Simulates Solcast: an early snapshot covers periods a later,
        # forward-looking-only snapshot no longer includes.
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-01', {'07:00': 1.0, '08:00': 2.0}, now=1000)
        forecast_history.write_snapshot(self.conn, 'solcast', '2026-01-01', {'08:00': 2.5, '09:00': 3.0}, now=2000)
        self.assertEqual(
            forecast_history.get_latest_merged(self.conn, 'solcast', '2026-01-01'),
            {'07:00': 1.0, '08:00': 2.5, '09:00': 3.0},
        )

    def test_get_latest_merged_with_no_snapshots_returns_empty_dict(self):
        self.assertEqual(forecast_history.get_latest_merged(self.conn, 'solcast', '2026-01-01'), {})

    def test_get_merged_since_excludes_snapshots_before_the_threshold(self):
        forecast_history.write_snapshot(self.conn, 'solcast_actuals', '2026-01-01', {'07:00': 1.0}, now=1000)
        forecast_history.write_snapshot(self.conn, 'solcast_actuals', '2026-01-01', {'07:00': 2.0}, now=2000)
        self.assertEqual(
            forecast_history.get_merged_since(self.conn, 'solcast_actuals', '2026-01-01', 2000),
            {'07:00': 2.0},
        )

    def test_get_merged_since_merges_periods_like_get_latest_merged(self):
        forecast_history.write_snapshot(self.conn, 'solcast_actuals', '2026-01-01', {'07:00': 1.0, '08:00': 2.0}, now=1000)
        forecast_history.write_snapshot(self.conn, 'solcast_actuals', '2026-01-01', {'08:00': 2.5}, now=2000)
        self.assertEqual(
            forecast_history.get_merged_since(self.conn, 'solcast_actuals', '2026-01-01', 1000),
            {'07:00': 1.0, '08:00': 2.5},
        )

    def test_get_merged_since_with_nothing_at_or_after_threshold_returns_empty_dict(self):
        forecast_history.write_snapshot(self.conn, 'solcast_actuals', '2026-01-01', {'07:00': 1.0}, now=1000)
        self.assertEqual(
            forecast_history.get_merged_since(self.conn, 'solcast_actuals', '2026-01-01', 2000),
            {},
        )

    def test_has_fetched_since_true_when_a_snapshot_is_at_or_after_the_threshold(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=2000)
        self.assertTrue(forecast_history.has_fetched_since(self.conn, 'meteosource', 2000))
        self.assertTrue(forecast_history.has_fetched_since(self.conn, 'meteosource', 1000))

    def test_has_fetched_since_false_when_latest_snapshot_is_older_than_the_threshold(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        self.assertFalse(forecast_history.has_fetched_since(self.conn, 'meteosource', 2000))

    def test_has_fetched_since_false_when_source_has_no_snapshots_at_all(self):
        self.assertFalse(forecast_history.has_fetched_since(self.conn, 'meteosource', 0))

    def test_has_fetched_since_checks_across_all_dates_for_the_source(self):
        # A single fetch call writes snapshots for several dates at once
        # (e.g. Solcast actuals' 7-day trailing window) - a snapshot for
        # any date should count, not just "today".
        forecast_history.write_snapshot(self.conn, 'solcast_actuals', '2026-01-03', {'07:00': 0.5}, now=3000)
        self.assertTrue(forecast_history.has_fetched_since(self.conn, 'solcast_actuals', 3000))

    def test_has_fetched_since_is_source_specific(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=2000)
        self.assertFalse(forecast_history.has_fetched_since(self.conn, 'solcast', 1000))

    def test_has_fetched_date_since_true_when_that_date_has_a_snapshot_at_or_after_the_threshold(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=2000)
        self.assertTrue(forecast_history.has_fetched_date_since(self.conn, 'meteosource', '2026-01-01', 2000))
        self.assertTrue(forecast_history.has_fetched_date_since(self.conn, 'meteosource', '2026-01-01', 1000))

    def test_has_fetched_date_since_false_when_that_date_has_no_snapshot(self):
        # Unlike has_fetched_since, a fresh snapshot for a *different* date
        # doesn't count - this is what has_fetched_since can't tell apart,
        # the gap that let Meteosource's "tomorrow" catch-up get silently
        # skipped once "today" alone looked fresh.
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=2000)
        self.assertFalse(forecast_history.has_fetched_date_since(self.conn, 'meteosource', '2026-01-02', 2000))

    def test_has_fetched_date_since_false_when_that_dates_snapshot_is_older_than_the_threshold(self):
        forecast_history.write_snapshot(self.conn, 'meteosource', '2026-01-01', {'07:00': 0.5}, now=1000)
        self.assertFalse(forecast_history.has_fetched_date_since(self.conn, 'meteosource', '2026-01-01', 2000))


if __name__ == '__main__':
    unittest.main()
