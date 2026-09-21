import tempfile
import unittest
from datetime import date, datetime, time as dtime
from zoneinfo import ZoneInfo

import tariff_engine

WARSAW = ZoneInfo('Europe/Warsaw')


class DaySpecMatchesTest(unittest.TestCase):
    def test_single_abbreviation(self):
        self.assertTrue(tariff_engine.day_spec_matches('Mo', date(2026, 9, 21)))  # a Monday
        self.assertFalse(tariff_engine.day_spec_matches('Tu', date(2026, 9, 21)))

    def test_range(self):
        monday, saturday = date(2026, 9, 21), date(2026, 9, 26)
        self.assertTrue(tariff_engine.day_spec_matches('Mo-Fr', monday))
        self.assertFalse(tariff_engine.day_spec_matches('Mo-Fr', saturday))

    def test_comma_list(self):
        saturday, sunday, monday = date(2026, 9, 26), date(2026, 9, 27), date(2026, 9, 28)
        self.assertTrue(tariff_engine.day_spec_matches('Sa,Su', saturday))
        self.assertTrue(tariff_engine.day_spec_matches('Sa,Su', sunday))
        self.assertFalse(tariff_engine.day_spec_matches('Sa,Su', monday))

    def test_work_keyword_excludes_public_holidays(self):
        # 2026-11-11 is Polish Independence Day (a Wednesday) - a public
        # holiday, so "Work" must exclude it even though it's a weekday.
        independence_day = date(2026, 11, 11)
        self.assertFalse(tariff_engine.day_spec_matches('Work', independence_day))
        self.assertTrue(tariff_engine.day_spec_matches('Work', date(2026, 11, 12)))  # the Thursday after

    def test_holiday_keyword(self):
        self.assertTrue(tariff_engine.day_spec_matches('Holiday', date(2026, 11, 11)))
        self.assertFalse(tariff_engine.day_spec_matches('Holiday', date(2026, 11, 12)))

    def test_holiday_does_not_match_an_ordinary_weekend(self):
        # A Saturday that isn't also a public holiday should match "Sa" but
        # not "Holiday" - the two keywords are not synonyms.
        self.assertFalse(tariff_engine.day_spec_matches('Holiday', date(2026, 9, 26)))

    def test_mixed_list_of_keyword_and_abbreviations(self):
        saturday, wednesday_holiday = date(2026, 9, 26), date(2026, 11, 11)
        self.assertTrue(tariff_engine.day_spec_matches('Sa,Su,Holiday', saturday))
        self.assertTrue(tariff_engine.day_spec_matches('Sa,Su,Holiday', wednesday_holiday))
        self.assertFalse(tariff_engine.day_spec_matches('Sa,Su,Holiday', date(2026, 9, 24)))  # an ordinary Thursday

    def test_range_wraps_the_week(self):
        # Fr-Mo should cover Friday through Monday, wrapping past Sunday -
        # start_idx (4) > end_idx (0) must not just always fail to match.
        friday, saturday, sunday, monday = (date(2026, 9, 25), date(2026, 9, 26),
                                             date(2026, 9, 27), date(2026, 9, 28))
        tuesday = date(2026, 9, 29)
        self.assertTrue(tariff_engine.day_spec_matches('Fr-Mo', friday))
        self.assertTrue(tariff_engine.day_spec_matches('Fr-Mo', saturday))
        self.assertTrue(tariff_engine.day_spec_matches('Fr-Mo', sunday))
        self.assertTrue(tariff_engine.day_spec_matches('Fr-Mo', monday))
        self.assertFalse(tariff_engine.day_spec_matches('Fr-Mo', tuesday))

    def test_country_selects_a_different_public_holiday_calendar(self):
        # 2026-11-11 is a public holiday in Poland (Independence Day) but
        # an ordinary Wednesday in Germany.
        wednesday = date(2026, 11, 11)
        self.assertTrue(tariff_engine.day_spec_matches('Holiday', wednesday, country='PL'))
        self.assertFalse(tariff_engine.day_spec_matches('Holiday', wednesday, country='DE'))


class TimeSpecMatchesTest(unittest.TestCase):
    def test_within_range(self):
        self.assertTrue(tariff_engine.time_spec_matches('13:00', '15:00', dtime(14, 0)))
        self.assertFalse(tariff_engine.time_spec_matches('13:00', '15:00', dtime(15, 0)))  # end is exclusive

    def test_seconds_optional(self):
        self.assertTrue(tariff_engine.time_spec_matches('13:00:00', '15:00:00', dtime(14, 0, 30)))

    def test_overnight_wraps_past_midnight(self):
        self.assertTrue(tariff_engine.time_spec_matches('22:00', '06:00', dtime(23, 30)))
        self.assertTrue(tariff_engine.time_spec_matches('22:00', '06:00', dtime(2, 0)))
        self.assertFalse(tariff_engine.time_spec_matches('22:00', '06:00', dtime(10, 0)))

    def test_missing_start_and_end_means_whole_day(self):
        self.assertTrue(tariff_engine.time_spec_matches(None, None, dtime(0, 0)))
        self.assertTrue(tariff_engine.time_spec_matches(None, None, dtime(23, 59, 59)))


class SeasonForDateTest(unittest.TestCase):
    SEASON_BOUNDARIES = {
        'summer': {'start': '01.04', 'end': '30.09'},
        'winter': {'start': '01.10', 'end': '31.03'},
    }

    def test_summer_date(self):
        self.assertEqual(tariff_engine.season_for_date(self.SEASON_BOUNDARIES, date(2026, 7, 15)), 'summer')

    def test_winter_date_before_year_end(self):
        self.assertEqual(tariff_engine.season_for_date(self.SEASON_BOUNDARIES, date(2026, 11, 1)), 'winter')

    def test_winter_date_after_year_start_wraps_correctly(self):
        # winter's range (01.10-31.03) wraps the year boundary - a date in
        # January must still resolve to winter, not fall through to "no match"
        self.assertEqual(tariff_engine.season_for_date(self.SEASON_BOUNDARIES, date(2026, 1, 15)), 'winter')

    def test_boundary_dates_are_inclusive(self):
        self.assertEqual(tariff_engine.season_for_date(self.SEASON_BOUNDARIES, date(2026, 4, 1)), 'summer')
        self.assertEqual(tariff_engine.season_for_date(self.SEASON_BOUNDARIES, date(2026, 9, 30)), 'summer')
        self.assertEqual(tariff_engine.season_for_date(self.SEASON_BOUNDARIES, date(2026, 10, 1)), 'winter')

    def test_leap_day_boundary_falls_back_to_the_28th_in_a_non_leap_year(self):
        # 2026 is not a leap year - date(2026, 2, 29) would raise
        # ValueError if taken literally; a season boundary shouldn't
        # depend on whether the current year happens to be a leap year.
        boundaries = {
            'a': {'start': '01.01', 'end': '29.02'},
            'b': {'start': '01.03', 'end': '31.12'},
        }
        self.assertEqual(tariff_engine.season_for_date(boundaries, date(2026, 2, 28)), 'a')
        self.assertEqual(tariff_engine.season_for_date(boundaries, date(2026, 3, 1)), 'b')


COMPONENT = {
    'prices': {'cheap': 0.50, 'expensive': 1.00},
    'season_boundaries': {
        'summer': {'start': '01.04', 'end': '30.09'},
        'winter': {'start': '01.10', 'end': '31.03'},
    },
    'bands': {
        'default': [
            {'start': '22:00', 'end': '06:00', 'days': 'Work', 'price': 'cheap'},
            {'days': 'Sa,Su,Holiday', 'price': 'cheap'},
        ],
        'summer': [{'start': '15:00', 'end': '17:00', 'days': 'Work', 'price': 'cheap'}],
        'winter': [{'start': '13:00', 'end': '15:00', 'days': 'Work', 'price': 'cheap'}],
    },
    'default_price': 'expensive',
}


class ComponentPriceAtTest(unittest.TestCase):
    def test_weekday_default_band_night(self):
        dt = datetime(2026, 7, 15, 23, 0)  # summer, Wednesday night
        self.assertEqual(tariff_engine.component_price_at(COMPONENT, dt), 0.50)

    def test_weekday_season_specific_midday_summer(self):
        dt = datetime(2026, 7, 15, 16, 0)  # summer midday window is 15:00-17:00
        self.assertEqual(tariff_engine.component_price_at(COMPONENT, dt), 0.50)

    def test_weekday_season_specific_midday_winter(self):
        dt = datetime(2026, 12, 15, 14, 0)  # winter midday window is 13:00-15:00
        self.assertEqual(tariff_engine.component_price_at(COMPONENT, dt), 0.50)

    def test_weekday_midday_window_does_not_apply_in_the_other_season(self):
        dt = datetime(2026, 12, 15, 16, 0)  # 15:00-17:00 is a summer-only band
        self.assertEqual(tariff_engine.component_price_at(COMPONENT, dt), 1.00)

    def test_weekend_all_day_default_band(self):
        dt = datetime(2026, 7, 18, 12, 0)  # a Saturday
        self.assertEqual(tariff_engine.component_price_at(COMPONENT, dt), 0.50)

    def test_falls_back_to_default_price_outside_all_bands(self):
        dt = datetime(2026, 7, 15, 10, 0)  # summer Wednesday mid-morning: no band matches
        self.assertEqual(tariff_engine.component_price_at(COMPONENT, dt), 1.00)

    def test_season_specific_band_takes_precedence_over_default(self):
        # An overlapping-bands config where a season band and a default
        # band both cover the same instant with different prices - the
        # season-specific one must win (spec: "season-specific bands
        # checked first, then default").
        overlapping = {
            'prices': {'cheap': 0.1, 'expensive': 1.0},
            'season_boundaries': COMPONENT['season_boundaries'],
            'bands': {
                'default': [{'start': '10:00', 'end': '12:00', 'price': 'expensive'}],
                'summer': [{'start': '10:00', 'end': '12:00', 'price': 'cheap'}],
            },
            'default_price': 'expensive',
        }
        dt = datetime(2026, 7, 15, 11, 0)
        self.assertEqual(tariff_engine.component_price_at(overlapping, dt), 0.1)


class PriceAtTest(unittest.TestCase):
    def test_single_component_config(self):
        config = {'components': {'total': COMPONENT}}
        dt = datetime(2026, 7, 15, 10, 0)
        self.assertEqual(tariff_engine.price_at(config, dt), 1.00)

    def test_two_components_are_summed(self):
        distribution = {
            'prices': {'cheap': 0.1, 'expensive': 0.2},
            'bands': {'default': [{'days': 'Sa,Su,Holiday', 'price': 'cheap'}]},
            'default_price': 'expensive',
        }
        sales = {
            'prices': {'flat': 0.5},
            'bands': {'default': [{'price': 'flat'}]},
            'default_price': 'flat',
        }
        config = {'components': {'distribution': distribution, 'sales': sales}}
        weekday = datetime(2026, 7, 15, 10, 0)  # Wednesday: distribution=expensive(0.2) + sales=flat(0.5)
        self.assertAlmostEqual(tariff_engine.price_at(config, weekday), 0.7)
        saturday = datetime(2026, 7, 18, 10, 0)  # distribution=cheap(0.1) + sales=flat(0.5)
        self.assertAlmostEqual(tariff_engine.price_at(config, saturday), 0.6)

    def test_top_level_country_key_controls_the_holiday_calendar(self):
        component = {
            'prices': {'cheap': 0.1, 'expensive': 0.2},
            'bands': {'default': [{'days': 'Holiday', 'price': 'cheap'}]},
            'default_price': 'expensive',
        }
        wednesday = datetime(2026, 11, 11, 10, 0)  # PL Independence Day, ordinary day in Germany
        self.assertAlmostEqual(tariff_engine.price_at({'components': {'c': component}, 'country': 'PL'}, wednesday), 0.1)
        self.assertAlmostEqual(tariff_engine.price_at({'components': {'c': component}, 'country': 'DE'}, wednesday), 0.2)
        # No 'country' key at all defaults to PL, unchanged from before this key existed.
        self.assertAlmostEqual(tariff_engine.price_at({'components': {'c': component}}, wednesday), 0.1)


class ValidateConfigTest(unittest.TestCase):
    def test_valid_config_raises_nothing(self):
        tariff_engine.validate_config({'components': {'total': COMPONENT}})

    def test_no_components_key(self):
        with self.assertRaisesRegex(ValueError, "no 'components'"):
            tariff_engine.validate_config({})

    def test_missing_default_price(self):
        with self.assertRaisesRegex(ValueError, "missing required 'default_price'"):
            tariff_engine.validate_config({'components': {'c': {'prices': {'a': 0.1}, 'bands': {}}}})

    def test_default_price_not_in_prices(self):
        with self.assertRaisesRegex(ValueError, "is not in 'prices'"):
            tariff_engine.validate_config(
                {'components': {'c': {'prices': {'a': 0.1}, 'bands': {}, 'default_price': 'missing'}}})

    def test_band_price_not_in_prices(self):
        config = {'components': {'c': {
            'prices': {'a': 0.1}, 'default_price': 'a',
            'bands': {'default': [{'price': 'nonexistent'}]},
        }}}
        with self.assertRaisesRegex(ValueError, "band price 'nonexistent'"):
            tariff_engine.validate_config(config)

    def test_season_band_with_no_matching_season_boundary(self):
        config = {'components': {'c': {
            'prices': {'a': 0.1}, 'default_price': 'a',
            'bands': {'summer': [{'price': 'a'}]},
        }}}
        with self.assertRaisesRegex(ValueError, "no matching entry in season_boundaries"):
            tariff_engine.validate_config(config)

    def test_start_without_end(self):
        config = {'components': {'c': {
            'prices': {'a': 0.1}, 'default_price': 'a',
            'bands': {'default': [{'start': '10:00', 'price': 'a'}]},
        }}}
        with self.assertRaisesRegex(ValueError, "'start' without 'end'"):
            tariff_engine.validate_config(config)

    def test_unrecognized_day_token(self):
        config = {'components': {'c': {
            'prices': {'a': 0.1}, 'default_price': 'a',
            'bands': {'default': [{'days': 'Xx', 'price': 'a'}]},
        }}}
        with self.assertRaisesRegex(ValueError, "unrecognized day token"):
            tariff_engine.validate_config(config)

    def test_season_boundary_missing_end(self):
        config = {'components': {'c': {
            'prices': {'a': 0.1}, 'default_price': 'a',
            'bands': {}, 'season_boundaries': {'summer': {'start': '01.04'}},
        }}}
        with self.assertRaisesRegex(ValueError, "needs both 'start' and 'end'"):
            tariff_engine.validate_config(config)

    def test_reports_every_problem_not_just_the_first(self):
        config = {'components': {'c': {
            'prices': {'a': 0.1}, 'default_price': 'missing',
            'bands': {'default': [{'price': 'also_missing'}]},
        }}}
        with self.assertRaises(ValueError) as ctx:
            tariff_engine.validate_config(config)
        self.assertIn('default_price', str(ctx.exception))
        self.assertIn('also_missing', str(ctx.exception))


class LoadConfigTest(unittest.TestCase):
    def test_loads_a_single_component_file(self):
        yaml_text = """
components:
  total:
    prices:
      cheap: 0.5
      expensive: 1.0
    bands:
      default:
        - days: "Sa,Su,Holiday"
          price: cheap
    default_price: expensive
"""
        with tempfile.NamedTemporaryFile('w', suffix='.yaml', delete=False) as f:
            f.write(yaml_text)
            path = f.name
        try:
            config = tariff_engine.load_config(path)
            dt = datetime(2026, 7, 18, 10, 0)  # Saturday
            self.assertEqual(tariff_engine.price_at(config, dt), 0.5)
        finally:
            import os
            os.remove(path)


class BandsForDayTest(unittest.TestCase):
    def test_no_forced_15_minute_slicing_one_interval_per_contiguous_band(self):
        config = {'components': {'total': COMPONENT}}
        # A summer Wednesday: 00:00-13:00 expensive minus the 06:00-cutoff
        # of the default night band, etc. - the key assertion is that the
        # 15:00-17:00 cheap window becomes ONE two-hour entry, not eight
        # 15-minute ones.
        bands = tariff_engine.bands_for_day(config, date(2026, 7, 15), WARSAW)
        cheap_afternoon = [b for b in bands if b['start'].endswith('T15:00:00+02:00')]
        self.assertEqual(len(cheap_afternoon), 1)
        self.assertEqual(cheap_afternoon[0]['end'], '2026-07-15T17:00:00+02:00')
        self.assertEqual(cheap_afternoon[0]['value'], 0.50)

    def test_timestamps_are_timezone_aware_with_explicit_offset(self):
        config = {'components': {'total': COMPONENT}}
        bands = tariff_engine.bands_for_day(config, date(2026, 7, 15), WARSAW)
        for band in bands:
            self.assertRegex(band['start'], r'\+\d{2}:\d{2}$')
            self.assertRegex(band['end'], r'\+\d{2}:\d{2}$')

    def test_dst_spring_forward_day_has_23_hours_not_24(self):
        # 2026-03-29 is Poland's DST spring-forward day (clocks jump
        # 02:00->03:00) - the day's bands must cover a 23-hour span, not
        # silently produce a 24-hour one with a wrong offset.
        config = {'components': {'total': COMPONENT}}
        bands = tariff_engine.bands_for_day(config, date(2026, 3, 29), WARSAW)
        self.assertEqual(bands[0]['start'], '2026-03-29T00:00:00+01:00')
        self.assertEqual(bands[-1]['end'], '2026-03-30T00:00:00+02:00')

    def test_covers_the_whole_day_with_no_gaps(self):
        config = {'components': {'total': COMPONENT}}
        bands = tariff_engine.bands_for_day(config, date(2026, 7, 15), WARSAW)
        for earlier, later in zip(bands, bands[1:]):
            self.assertEqual(earlier['end'], later['start'])


if __name__ == '__main__':
    unittest.main()
