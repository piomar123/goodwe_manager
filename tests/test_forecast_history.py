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


if __name__ == '__main__':
    unittest.main()
