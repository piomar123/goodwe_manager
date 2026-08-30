import unittest
from datetime import date, datetime, time as dtime, timedelta

import rce_prefetch


class SecondsUntilTest(unittest.TestCase):
    def test_target_later_today(self):
        now = datetime(2026, 1, 1, 10, 0, 0)
        result = rce_prefetch.seconds_until(now, dtime(14, 15))
        self.assertEqual(result, 4 * 3600 + 15 * 60)

    def test_target_already_passed_today_rolls_to_tomorrow(self):
        now = datetime(2026, 1, 1, 15, 0, 0)
        result = rce_prefetch.seconds_until(now, dtime(14, 15))
        expected = (datetime(2026, 1, 2, 14, 15, 0) - now).total_seconds()
        self.assertEqual(result, expected)

    def test_target_equal_to_now_rolls_to_tomorrow(self):
        now = datetime(2026, 1, 1, 14, 15, 0)
        result = rce_prefetch.seconds_until(now, dtime(14, 15))
        self.assertEqual(result, 24 * 3600)


class PastCutoffTest(unittest.TestCase):
    def test_before_cutoff(self):
        self.assertFalse(rce_prefetch.past_cutoff(datetime(2026, 1, 1, 19, 59), dtime(20, 0)))

    def test_at_cutoff(self):
        self.assertTrue(rce_prefetch.past_cutoff(datetime(2026, 1, 1, 20, 0), dtime(20, 0)))

    def test_after_cutoff(self):
        self.assertTrue(rce_prefetch.past_cutoff(datetime(2026, 1, 1, 20, 1), dtime(20, 0)))


class RunPrefetchCycleTest(unittest.TestCase):
    def test_succeeds_on_first_try(self):
        calls = []
        sleeps = []
        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=calls.append,
            target_date=date(2026, 1, 2),
            sleep_fn=sleeps.append,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
        )
        self.assertTrue(result)
        self.assertEqual(calls, [date(2026, 1, 2)])
        self.assertEqual(sleeps, [])

    def test_retries_on_not_yet_published_then_succeeds(self):
        attempts = {'n': 0}

        def fetch_fn(d):
            attempts['n'] += 1
            if attempts['n'] < 3:
                raise RuntimeError("No data found for 2026-01-02")

        sleeps = []
        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=fetch_fn,
            target_date=date(2026, 1, 2),
            sleep_fn=sleeps.append,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
        )
        self.assertTrue(result)
        self.assertEqual(attempts['n'], 3)
        self.assertEqual(sleeps, [rce_prefetch.RETRY_INTERVAL_SECONDS, rce_prefetch.RETRY_INTERVAL_SECONDS])

    def test_retries_on_unexpected_error_then_succeeds(self):
        attempts = {'n': 0}

        def fetch_fn(d):
            attempts['n'] += 1
            if attempts['n'] < 2:
                raise ConnectionError("network down")

        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=fetch_fn,
            target_date=date(2026, 1, 2),
            sleep_fn=lambda s: None,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
        )
        self.assertTrue(result)
        self.assertEqual(attempts['n'], 2)

    def test_gives_up_at_cutoff(self):
        clock = {'now': datetime(2026, 1, 1, 19, 59, 50)}

        def fetch_fn(d):
            raise RuntimeError("No data found")

        def sleep_fn(seconds):
            clock['now'] += timedelta(seconds=seconds)

        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=fetch_fn,
            target_date=date(2026, 1, 2),
            sleep_fn=sleep_fn,
            now_fn=lambda: clock['now'],
        )
        self.assertFalse(result)

    def test_should_stop_halts_the_loop_without_calling_fetch_fn(self):
        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=lambda d: (_ for _ in ()).throw(AssertionError("fetch_fn should not be called")),
            target_date=date(2026, 1, 2),
            sleep_fn=lambda s: None,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
            should_stop=lambda: True,
        )
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
