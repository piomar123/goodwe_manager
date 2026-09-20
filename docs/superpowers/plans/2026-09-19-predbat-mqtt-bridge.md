# Predbat MQTT Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `goodwe_manager` into an MQTT bridge that publishes everything Home Assistant/Predbat needs for read-only PV/battery plan evaluation (live telemetry, daily energy counters, export price, import tariff price, PV forecast), plus a reusable tariff engine and an offline battery-efficiency estimator, without a second process ever polling the inverter.

**Architecture:** All new logic is pure/testable modules (`tariff_engine.py`, `export_price.py`, `mqtt_bridge.py`) wired into the existing `AsyncioThread`/`RcePrefetchThread`/`ForecastPrefetchThread` singletons in `main.py`. The MQTT publish path reuses data those threads already fetch — no new inverter reads, no new pollers. Everything is optional/env-gated so a fork with no MQTT/tariff config behaves exactly as today.

**Tech Stack:** Python 3, `aiomqtt` (async MQTT client, fits the existing `asyncio` event loop in `AsyncioThread`), `PyYAML` (tariff config), `holidays` (Polish public holidays), `unittest` (matches existing test suite).

**Spec:** `docs/superpowers/specs/2026-09-19-predbat-mqtt-bridge-design.md`

## Global Constraints

- All prices stay denominated in **zł** everywhere in this codebase — no currency conversion, no implicit ×100 (per spec's "Units" section). Predbat-side threshold tuning is a setup note, not code.
- Every new feature is **optional and off by default**: unset `MQTT_HOST` disables the whole MQTT bridge; unset `TARIFF_IMPORT_CONFIG` disables the tariff engine's use in `_calculate_income.py` (flat `IMPORT_PRICE_KWH` stays the fallback). A fresh checkout/fork with no new env vars set must behave exactly as it does on `origin/main` today.
- No MQTT command/write topics — this phase is read-only against the inverter (per spec's Non-goals).
- No real tariff rates or distributor-specific numbers are hardcoded in code, or committed anywhere except a placeholder-rate example under `tariff_examples/`.
- All new MQTT-published timestamps are timezone-aware (`Europe/Warsaw` via `zoneinfo`), never naive local time — this is what makes DST transition days resolve correctly without special-casing.
- Season boundary dates use `dd.mm` string format; time-of-day uses `HH:MM` with optional `:SS`.
- New Python dependencies (`aiomqtt`, `PyYAML`, `holidays`) get added to `requirements.txt` with pinned versions matching whatever `pip install` resolves at implementation time (check `pip show <package>` after installing, same convention as the existing pinned list).

---

### Task 1: `tariff_engine.py` — day-spec grammar, band resolution, multi-component pricing

**Files:**
- Create: `tariff_engine.py`
- Test: `tests/test_tariff_engine.py`

**Interfaces:**
- Consumes: nothing from other tasks (this is the foundational module).
- Produces (used by Tasks 3, 4, 7):
  - `load_config(path: str) -> dict` — parses and validates a tariff YAML file (the `components` map shape from the spec) into a plain `dict` (no custom classes — the rest of this module works directly on the parsed YAML structure, keeping the config format and the code that reads it in one place).
  - `price_at(config: dict, dt: datetime) -> float` — the zł price at a specific instant, summed across every component in `config['components']`.
  - `bands_for_day(config: dict, day: date, tz: ZoneInfo) -> list[dict]` — one `{"from": iso8601_str, "to": iso8601_str, "value": float}` entry per contiguous same-price interval across the whole day (in `tz`), value being the component-summed price. Used by Task 4 (import price MQTT publish) to build `raw_today`/`raw_tomorrow`.

- [ ] **Step 1: Write failing tests for day-spec parsing**

```python
import unittest
from datetime import date, datetime
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


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `python -m pytest tests/test_tariff_engine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tariff_engine'` (or `AttributeError` once the module exists but the function doesn't).

- [ ] **Step 3: Implement the day-spec grammar**

```python
"""
tariff_engine.py
Evaluates a multi-component, seasonal, day-of-week-aware electricity
tariff schedule (YAML config) for a given instant or day. See
docs/superpowers/specs/2026-09-19-predbat-mqtt-bridge-design.md,
Component 4, for the full schema and rationale.
"""
from datetime import date, datetime, time as dtime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import holidays
import yaml

_WEEKDAY_ABBREVIATIONS = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su']
_PL_HOLIDAYS = holidays.PL()


def _weekday_abbreviation(d: date) -> str:
    return _WEEKDAY_ABBREVIATIONS[d.weekday()]


def _is_public_holiday(d: date) -> bool:
    return d in _PL_HOLIDAYS


def _token_matches(token: str, d: date) -> bool:
    token = token.strip()
    if token == 'Work':
        return d.weekday() < 5 and not _is_public_holiday(d)
    if token == 'Holiday':
        return _is_public_holiday(d)
    if '-' in token:
        start_abbr, end_abbr = token.split('-', 1)
        start_idx = _WEEKDAY_ABBREVIATIONS.index(start_abbr)
        end_idx = _WEEKDAY_ABBREVIATIONS.index(end_abbr)
        return start_idx <= d.weekday() <= end_idx
    return _WEEKDAY_ABBREVIATIONS[d.weekday()] == token


def day_spec_matches(day_spec: str, d: date) -> bool:
    """day_spec is a comma-separated list of tokens: weekday abbreviations
    (Mo,Tu,We,Th,Fr,Sa,Su), ranges (Mo-Fr), or the keywords Work/Holiday.
    Matches if any token matches (OR semantics) - see spec Component 4.
    """
    return any(_token_matches(token, d) for token in day_spec.split(','))
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `python -m pytest tests/test_tariff_engine.py -v`
Expected: PASS (all `DaySpecMatchesTest` cases)

- [ ] **Step 5: Commit**

```bash
git add tariff_engine.py tests/test_tariff_engine.py
git commit -m "feat: add tariff_engine day-spec grammar (weekday/range/Work/Holiday)"
```

- [ ] **Step 6: Write failing tests for time-of-day and band matching within one component**

```python
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
```

- [ ] **Step 7: Run to confirm failure, then implement time-spec and season resolution**

```python
def time_spec_matches(start: Optional[str], end: Optional[str], t: dtime) -> bool:
    """start/end are 'HH:MM' or 'HH:MM:SS' strings, or both None (whole
    day). end is exclusive. start > end (e.g. 22:00-06:00) wraps past
    midnight."""
    if start is None and end is None:
        return True
    start_t = _parse_time(start)
    end_t = _parse_time(end)
    if start_t <= end_t:
        return start_t <= t < end_t
    return t >= start_t or t < end_t


def _parse_time(s: str) -> dtime:
    parts = [int(p) for p in s.split(':')]
    while len(parts) < 3:
        parts.append(0)
    return dtime(*parts)


def _parse_season_boundary_date(s: str, year: int) -> date:
    day, month = (int(p) for p in s.split('.'))
    return date(year, month, day)


def season_for_date(season_boundaries: dict, d: date) -> Optional[str]:
    """Returns the season name whose [start, end] range (inclusive, dd.mm
    format) contains d, wrapping the year boundary when start > end (e.g.
    winter's "01.10"-"31.03"). None if no season's range contains d - a
    config gap the caller should treat as a validation error, not silently
    ignore."""
    for season_name, bounds in season_boundaries.items():
        start = _parse_season_boundary_date(bounds['start'], d.year)
        end = _parse_season_boundary_date(bounds['end'], d.year)
        if start <= end:
            if start <= d <= end:
                return season_name
        else:
            # wraps the year boundary (e.g. 01.10 - 31.03)
            if d >= start or d <= end:
                return season_name
    return None
```

- [ ] **Step 8: Run tests, confirm pass, commit**

Run: `python -m pytest tests/test_tariff_engine.py -v`
Expected: PASS

```bash
git add tariff_engine.py tests/test_tariff_engine.py
git commit -m "feat: add tariff_engine time-spec and season-boundary resolution"
```

- [ ] **Step 9: Write failing tests for single-component band resolution (default_price fallback, band precedence)**

```python
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
```

- [ ] **Step 10: Run to confirm failure, then implement `component_price_at`**

```python
def _band_matches(band: dict, dt: datetime) -> bool:
    days = band.get('days')
    if days is not None and not day_spec_matches(days, dt.date()):
        return False
    return time_spec_matches(band.get('start'), band.get('end'), dt.time())


def component_price_at(component: dict, dt: datetime) -> float:
    """Resolves one component's price at `dt`: that component's own season
    bands first (in file order), then its `bands.default` (in file
    order), then `default_price` - see spec Component 4's "Resolution
    order"."""
    season = season_for_date(component['season_boundaries'], dt.date()) if component.get('season_boundaries') else None
    bands = component.get('bands', {})
    season_bands = bands.get(season, []) if season else []
    default_bands = bands.get('default', [])
    for band in (*season_bands, *default_bands):
        if _band_matches(band, dt):
            return component['prices'][band['price']]
    return component['prices'][component['default_price']]
```

- [ ] **Step 11: Run tests, confirm pass, commit**

Run: `python -m pytest tests/test_tariff_engine.py -v`
Expected: PASS

```bash
git add tariff_engine.py tests/test_tariff_engine.py
git commit -m "feat: add tariff_engine single-component band resolution"
```

- [ ] **Step 12: Write failing tests for multi-component summation and `price_at`**

```python
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
```

- [ ] **Step 13: Run to confirm failure, then implement `price_at`**

```python
def price_at(config: dict, dt: datetime) -> float:
    return sum(component_price_at(component, dt) for component in config['components'].values())
```

- [ ] **Step 14: Run tests, confirm pass, commit**

```bash
git add tariff_engine.py tests/test_tariff_engine.py
git commit -m "feat: add tariff_engine multi-component price summation"
```

- [ ] **Step 15: Write failing tests for `load_config` (YAML parsing)**

```python
import tempfile


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
```

- [ ] **Step 16: Run to confirm failure, then implement `load_config`**

```python
def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)
```

- [ ] **Step 17: Run tests, confirm pass, commit**

```bash
git add tariff_engine.py tests/test_tariff_engine.py
git commit -m "feat: add tariff_engine YAML config loading"
```

- [ ] **Step 18: Write failing tests for `bands_for_day` (the raw_today/raw_tomorrow band-list builder)**

```python
class BandsForDayTest(unittest.TestCase):
    def test_no_forced_15_minute_slicing_one_interval_per_contiguous_band(self):
        config = {'components': {'total': COMPONENT}}
        # A summer Wednesday: 00:00-13:00 expensive minus the 06:00-cutoff
        # of the default night band, etc. - the key assertion is that the
        # 15:00-17:00 cheap window becomes ONE two-hour entry, not eight
        # 15-minute ones.
        bands = tariff_engine.bands_for_day(config, date(2026, 7, 15), WARSAW)
        cheap_afternoon = [b for b in bands if b['from'].endswith('T15:00:00+02:00')]
        self.assertEqual(len(cheap_afternoon), 1)
        self.assertEqual(cheap_afternoon[0]['to'], '2026-07-15T17:00:00+02:00')
        self.assertEqual(cheap_afternoon[0]['value'], 0.50)

    def test_timestamps_are_timezone_aware_with_explicit_offset(self):
        config = {'components': {'total': COMPONENT}}
        bands = tariff_engine.bands_for_day(config, date(2026, 7, 15), WARSAW)
        for band in bands:
            self.assertRegex(band['from'], r'\+\d{2}:\d{2}$')
            self.assertRegex(band['to'], r'\+\d{2}:\d{2}$')

    def test_dst_spring_forward_day_has_23_hours_not_24(self):
        # 2026-03-29 is Poland's DST spring-forward day (clocks jump
        # 02:00->03:00) - the day's bands must cover a 23-hour span, not
        # silently produce a 24-hour one with a wrong offset.
        config = {'components': {'total': COMPONENT}}
        bands = tariff_engine.bands_for_day(config, date(2026, 3, 29), WARSAW)
        self.assertEqual(bands[0]['from'], '2026-03-29T00:00:00+01:00')
        self.assertEqual(bands[-1]['to'], '2026-03-30T00:00:00+02:00')

    def test_covers_the_whole_day_with_no_gaps(self):
        config = {'components': {'total': COMPONENT}}
        bands = tariff_engine.bands_for_day(config, date(2026, 7, 15), WARSAW)
        for earlier, later in zip(bands, bands[1:]):
            self.assertEqual(earlier['to'], later['from'])
```

- [ ] **Step 19: Run to confirm failure, then implement `bands_for_day`**

```python
def bands_for_day(config: dict, day: date, tz: ZoneInfo) -> list:
    """Builds one {"from", "to", "value"} entry per contiguous interval of
    constant price across `day` (local `tz`), evaluated at 1-minute
    resolution and merged - fine-grained enough that no real tariff's
    band boundaries fall between samples, coarse enough to be cheap for a
    single day. Adjacent minutes with the same price merge into one
    entry (see spec Component 4's "No forced 15-minute slicing").
    Timestamps carry `tz`'s real UTC offset for that instant, so a DST
    transition day naturally produces a 23- or 25-hour span instead of a
    fixed 24."""
    start = datetime.combine(day, dtime(0, 0), tzinfo=tz)
    end = datetime.combine(day + timedelta(days=1), dtime(0, 0), tzinfo=tz)
    bands = []
    cursor = start
    current_value = None
    current_start = None
    step = timedelta(minutes=1)
    while cursor < end:
        value = price_at(config, cursor.replace(tzinfo=None))
        if current_value is None:
            current_value, current_start = value, cursor
        elif value != current_value:
            bands.append(_band_entry(current_start, cursor, current_value))
            current_value, current_start = value, cursor
        cursor += step
    bands.append(_band_entry(current_start, end, current_value))
    return bands


def _band_entry(start: datetime, end: datetime, value: float) -> dict:
    return {'from': start.isoformat(), 'to': end.isoformat(), 'value': value}
```

- [ ] **Step 20: Run tests, confirm pass**

Run: `python -m pytest tests/test_tariff_engine.py -v`
Expected: PASS (all tests in the file)

- [ ] **Step 21: Commit**

```bash
git add tariff_engine.py tests/test_tariff_engine.py
git commit -m "feat: add tariff_engine bands_for_day for Predbat raw_today/raw_tomorrow"
```

- [ ] **Step 22: Add dependencies to requirements.txt**

Run: `pip install PyYAML holidays` in the project's venv, then `pip show PyYAML holidays` to get the resolved versions, and add both lines (alphabetically sorted, matching the existing list's style) to `requirements.txt`, e.g. `PyYAML==<resolved version>` and `holidays==<resolved version>`.

```bash
git add requirements.txt
git commit -m "chore: add PyYAML and holidays dependencies for tariff_engine"
```

---

### Task 2: `tariff_examples/` directory and `.gitignore` entry

**Files:**
- Create: `tariff_examples/g12w_pge.yaml`
- Modify: `.gitignore`
- Modify: `README.MD`

**Interfaces:**
- Consumes: `tariff_engine.load_config`'s expected YAML shape (Task 1).
- Produces: a checked-in template other tasks/users copy into their own gitignored `tariff_import.yaml`.

- [ ] **Step 1: Create the example file with real PGE G12w hours and placeholder rates**

```yaml
# tariff_examples/g12w_pge.yaml
#
# PGE Dystrybucja's G12w schedule (hours are published tariff-card
# information - see docs/superpowers/specs/2026-09-19-predbat-mqtt-bridge-design.md
# Component 4 for sourcing). Rates below are PLACEHOLDERS - replace
# "cheap"/"expensive" with your own actual gross zł/kWh from your latest
# invoice before using this for real. Copy this file to a location of
# your choice (e.g. tariff_import.yaml, already gitignored) and point
# TARIFF_IMPORT_CONFIG at it.

components:
  total:
    prices:
      cheap: 0.01      # PLACEHOLDER - off-peak (strefa tańsza), zł/kWh gross
      expensive: 0.01  # PLACEHOLDER - peak (strefa droższa), zł/kWh gross

    season_boundaries:
      summer:
        start: "01.04"
        end: "30.09"
      winter:
        start: "01.10"
        end: "31.03"

    bands:
      default:
        - start: "22:00"
          end: "06:00"
          days: "Work"
          price: cheap
        - days: "Sa,Su,Holiday"
          price: cheap
      summer:
        - start: "15:00"
          end: "17:00"
          days: "Work"
          price: cheap
      winter:
        - start: "13:00"
          end: "15:00"
          days: "Work"
          price: cheap

    default_price: expensive
```

- [ ] **Step 2: Add the real config file to .gitignore**

In `.gitignore`, add near the other data-file entries (after `/rce_prices.db-shm`):

```
/tariff_import.yaml
```

- [ ] **Step 3: Document the new file in README.MD**

Add a new section to `README.MD` (after the "Upgrading" section):

```markdown
## Tariff-aware import pricing (optional)

`_calculate_income.py` and the MQTT bridge (see the Predbat bridge design
doc) can price imported energy using a real time-of-use tariff schedule
instead of a flat rate. Copy an example from `tariff_examples/` (e.g.
`g12w_pge.yaml`) to `tariff_import.yaml` (already gitignored), fill in
your actual rates, and set `TARIFF_IMPORT_CONFIG=tariff_import.yaml` in
`.env`. Without this set, both keep using the flat `IMPORT_PRICE_KWH`
constant, unchanged from today's behavior.
```

- [ ] **Step 4: Verify the example loads and evaluates without error**

Run: `python -c "import tariff_engine; from datetime import datetime; c = tariff_engine.load_config('tariff_examples/g12w_pge.yaml'); print(tariff_engine.price_at(c, datetime(2026, 7, 15, 10, 0)))"`
Expected: prints `0.01` (the placeholder "expensive" rate) with no exception.

- [ ] **Step 5: Commit**

```bash
git add tariff_examples/g12w_pge.yaml .gitignore README.MD
git commit -m "feat: add checked-in G12w tariff example and gitignore the real config"
```

---

### Task 3: `_calculate_income.py` — optional tariff-aware import pricing

**Files:**
- Modify: `_calculate_income.py`
- Modify: `tests/test_calculate_income.py`

**Interfaces:**
- Consumes: `tariff_engine.load_config(path) -> dict`, `tariff_engine.price_at(config, dt) -> float` (Task 1).
- Produces: `compute_hour_income(hourly_export, hourly_import, load_kwh, rce_price_pln_per_mwh, import_price_kwh=IMPORT_PRICE_KWH)` — same signature as today plus one new optional parameter, so existing callers/tests keep working unchanged when it's omitted.

- [ ] **Step 1: Write failing tests for the new optional parameter**

Add to `tests/test_calculate_income.py`, inside `ComputeHourIncomeTest`:

```python
    def test_custom_import_price_is_used_instead_of_the_flat_constant(self):
        result = income.compute_hour_income(hourly_export=1.0, hourly_import=5.0, load_kwh=3.0,
                                            rce_price_pln_per_mwh=400.0, import_price_kwh=0.35)

        self.assertAlmostEqual(result['balance_kwh'], -4.0)
        self.assertAlmostEqual(result['meter_pln'], -4.0 * 0.35)
        self.assertAlmostEqual(result['no_buy_pln'], 3.0 * 0.35)

    def test_default_import_price_is_still_the_flat_constant(self):
        # unchanged behavior when the new parameter is omitted entirely
        result = income.compute_hour_income(hourly_export=1.0, hourly_import=5.0, load_kwh=3.0,
                                            rce_price_pln_per_mwh=400.0)

        self.assertAlmostEqual(result['meter_pln'], -4.0 * income.IMPORT_PRICE_KWH)
```

- [ ] **Step 2: Run to confirm failure**

Run: `python -m pytest tests/test_calculate_income.py -v`
Expected: FAIL with `TypeError: compute_hour_income() got an unexpected keyword argument 'import_price_kwh'`

- [ ] **Step 3: Add the parameter to `compute_hour_income`**

```python
def compute_hour_income(hourly_export: float, hourly_import: float, load_kwh: float,
                        rce_price_pln_per_mwh: float, import_price_kwh: float = IMPORT_PRICE_KWH) -> dict:
    """Pure per-hour income calculation - same formula as before: a positive
    meter balance (net export) is valued at the RCE market price, a negative
    balance (net import) at `import_price_kwh`, and the load itself is
    separately valued at `import_price_kwh` to represent the cost avoided
    by self-consumption. `import_price_kwh` defaults to the flat
    IMPORT_PRICE_KWH constant; callers with a tariff config pass the real
    per-hour zone-aware rate instead (see main()'s --tariff-config).
    """
    balance_kwh = hourly_export - hourly_import
    rce_price_kwh = rce_price_pln_per_mwh / 1000.
    no_buy_pln = load_kwh * import_price_kwh
    if balance_kwh > 0:
        meter_pln = balance_kwh * rce_price_kwh
    else:
        meter_pln = balance_kwh * import_price_kwh
    return {
        'balance_kwh': balance_kwh,
        'rce_price_kwh': rce_price_kwh,
        'no_buy_pln': no_buy_pln,
        'meter_pln': meter_pln,
        'gain_pln': meter_pln + no_buy_pln,
    }
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `python -m pytest tests/test_calculate_income.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add _calculate_income.py tests/test_calculate_income.py
git commit -m "feat: let compute_hour_income accept a per-call import price"
```

- [ ] **Step 6: Wire `main()` to use a tariff config when available**

```python
import os
import tariff_engine
```

Add near the top of `_calculate_income.py` (after `IMPORT_PRICE_KWH = 1.1`):

```python
TARIFF_IMPORT_CONFIG = os.environ.get('TARIFF_IMPORT_CONFIG')
```

In `main()`, add a `--tariff-config` CLI argument (falling back to the env var) and pass the resolved per-hour rate into `compute_hour_income`:

```python
    parser.add_argument("--tariff-config", help="Path to a tariff_engine YAML config (defaults to TARIFF_IMPORT_CONFIG env var; omit both to use the flat IMPORT_PRICE_KWH)",
                        type=str, default=TARIFF_IMPORT_CONFIG)
```

```python
    tariff_config = tariff_engine.load_config(args.tariff_config) if args.tariff_config else None
```

Inside the `for hour in range(24):` loop, before calling `compute_hour_income`:

```python
        if tariff_config is not None:
            hour_dt = datetime(parsed_date.year, parsed_date.month, parsed_date.day, hour)
            import_price_kwh = tariff_engine.price_at(tariff_config, hour_dt)
        else:
            import_price_kwh = IMPORT_PRICE_KWH

        result = compute_hour_income(hourly_export, hourly_import, load_kwh, rce_hour_price[1], import_price_kwh)
```

- [ ] **Step 7: Manually verify both paths**

Run: `python _calculate_income.py --date t` (no tariff config) — must print identical output to before this change (flat-rate behavior unchanged).
Run: `python _calculate_income.py --date t --tariff-config tariff_examples/g12w_pge.yaml` — must run without error and print per-hour rates that vary by time of day (visible in the printed `RCE:` line... actually the printed line shows the RCE export price, not the import price - add a quick manual sanity check instead: temporarily add `print(import_price_kwh)` inside the loop, confirm it prints `0.01` for peak hours and `0.01` for off-peak in the placeholder example - since both placeholders happen to be equal, verify instead against a copy of the example with `cheap: 0.3`/`expensive: 0.9` temporarily edited in, confirming the printed values actually differ by hour before reverting the temporary edit).

- [ ] **Step 8: Commit**

```bash
git add _calculate_income.py
git commit -m "feat: use tariff_engine for import pricing in _calculate_income.py when configured"
```

---

### Task 4: `export_price.py` — RCE export price bands for Predbat

**Files:**
- Create: `export_price.py`
- Test: `tests/test_export_price.py`

**Interfaces:**
- Consumes: `rce_storage.init_db()`, `rce_storage.is_cached(conn, business_date)`, `rce_storage.get_cached_prices(conn, business_date) -> list[(period, rce_pln)]` (existing, from `rce_storage.py`).
- Produces (used by Task 6): `build_export_price_payload(today: date, tz: ZoneInfo) -> dict` returning `{"raw_today": [...], "raw_tomorrow": [...]}` (the second list empty if tomorrow isn't cached yet), each entry shaped `{"from": iso8601_str, "to": iso8601_str, "value": float}` in zł/kWh.

- [ ] **Step 1: Write failing tests**

```python
import os
import tempfile
import unittest
from datetime import date
from zoneinfo import ZoneInfo

import rce_storage
import export_price

WARSAW = ZoneInfo('Europe/Warsaw')


class BuildExportPricePayloadTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.remove(self.db_path)
        rce_storage.RCE_DB_PATH = self.db_path

    def tearDown(self):
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

    def _store(self, business_date, periods_and_prices):
        conn = rce_storage.init_db()
        rce_storage.store_prices(conn, business_date, periods_and_prices)
        conn.close()

    def test_applies_vat_bonus_and_converts_to_kwh(self):
        self._store('2026-07-15', [('00:00', 400.0), ('00:15', 400.0), ('24:00', 400.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        # 400 PLN/MWh -> 0.4 zl/kWh, x1.23 VAT bonus = 0.492
        self.assertAlmostEqual(payload['raw_today'][0]['value'], 0.492, places=6)

    def test_tomorrow_omitted_when_not_yet_cached(self):
        self._store('2026-07-15', [('00:00', 400.0), ('24:00', 400.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertEqual(payload['raw_tomorrow'], [])

    def test_tomorrow_included_once_cached(self):
        self._store('2026-07-15', [('00:00', 400.0), ('24:00', 400.0)])
        self._store('2026-07-16', [('00:00', 500.0), ('24:00', 500.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertGreater(len(payload['raw_tomorrow']), 0)

    def test_timestamps_are_timezone_aware(self):
        self._store('2026-07-15', [('00:00', 400.0), ('24:00', 400.0)])
        payload = export_price.build_export_price_payload(date(2026, 7, 15), WARSAW)

        self.assertRegex(payload['raw_today'][0]['from'], r'\+\d{2}:\d{2}$')


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run to confirm failure**

Run: `python -m pytest tests/test_export_price.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'export_price'`

- [ ] **Step 3: Implement `export_price.py`**

```python
"""
export_price.py
Converts cached RCE 15-minute prices (rce_storage.py) into the
{"raw_today", "raw_tomorrow"} band-list shape Predbat's generic
metric_octopus_export mechanism expects, applying the prosument export
VAT bonus. See docs/superpowers/specs/2026-09-19-predbat-mqtt-bridge-design.md,
Component 3.
"""
from datetime import date, datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

import rce_storage

EXPORT_VAT_BONUS_MULTIPLIER = 1.23


def _parse_period_start(period: str, day: date, tz: ZoneInfo) -> datetime:
    """period is 'HH:MM' (or the DST fall-back day's disambiguated
    'HHa:MM' form, per rce.py's convert_to_series_15min) - the 'a' suffix
    is stripped since it only exists to disambiguate PSE's own display,
    not to change the wall-clock hour."""
    hh_mm = period.replace('a', '')
    hour, minute = (int(p) for p in hh_mm.split(':'))
    if hour == 24:
        day, hour = day + timedelta(days=1), 0
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=tz)


def _bands_for_business_date(business_date_str: str, tz: ZoneInfo) -> List[dict]:
    conn = rce_storage.init_db()
    try:
        if not rce_storage.is_cached(conn, business_date_str):
            return []
        periods = rce_storage.get_cached_prices(conn, business_date_str)
    finally:
        conn.close()

    day = datetime.strptime(business_date_str, '%Y-%m-%d').date()
    bands = []
    for (period, rce_pln), (next_period, _) in zip(periods, periods[1:]):
        start = _parse_period_start(period, day, tz)
        end = _parse_period_start(next_period, day, tz)
        value = rce_pln / 1000.0 * EXPORT_VAT_BONUS_MULTIPLIER
        bands.append({'from': start.isoformat(), 'to': end.isoformat(), 'value': value})
    return bands


def build_export_price_payload(today: date, tz: ZoneInfo) -> dict:
    """Returns {"raw_today": [...], "raw_tomorrow": [...]}. raw_tomorrow
    is an empty list (not an error) when tomorrow's RCE prices aren't
    cached yet - see spec Component 3's "Tomorrow not cached yet"."""
    tomorrow = today + timedelta(days=1)
    return {
        'raw_today': _bands_for_business_date(today.strftime('%Y-%m-%d'), tz),
        'raw_tomorrow': _bands_for_business_date(tomorrow.strftime('%Y-%m-%d'), tz),
    }
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `python -m pytest tests/test_export_price.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add export_price.py tests/test_export_price.py
git commit -m "feat: add export_price.py for Predbat raw_today/raw_tomorrow RCE bands"
```

---

### Task 5: Daily (midnight-anchored) energy counters in `sensors.py`/`storage.py`

**Files:**
- Modify: `sensors.py`
- Modify: `storage.py`
- Modify: `tests/test_sensors.py`
- Test: `tests/test_storage.py` (add one case)

**Interfaces:**
- Consumes: nothing new.
- Produces (used by Task 7): `CalculatedValuesEvaluator.seed_day_start(sensors_data)`, four new keys in `calculate_values()`'s return dict (`_day_start_timestamp`, `_daily_meter_export`, `_daily_meter_import`, `_daily_load`), `storage.current_day_bounds(now: datetime) -> (start_epoch, end_epoch)`.

- [ ] **Step 1: Write failing tests for the new calculated values**

Add to `tests/test_sensors.py` (new test class, same file):

```python
class DailyCalculatedValuesTest(unittest.TestCase):
    def test_first_sample_becomes_its_own_day_start_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        sample = {
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        }

        calculated = evaluator.calculate_values(sample)

        self.assertEqual(calculated['_day_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_daily_meter_export'], '0.00')
        self.assertEqual(calculated['_daily_meter_import'], '0.00')
        self.assertEqual(calculated['_daily_load'], '0.0')

    def test_running_totals_accumulate_within_the_same_day_across_hour_boundaries(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 14:00:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        # crosses an hour boundary (14 -> 15) but stays the same day
        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 15:30:05',
            'meter_e_total_exp': '108.0',
            'meter_e_total_imp': '53.0',
            'e_load_total': '18.0',
        })

        self.assertEqual(calculated['_day_start_timestamp'], '2026-08-28 14:00:05')
        self.assertEqual(calculated['_daily_meter_export'], '8.00')
        self.assertEqual(calculated['_daily_meter_import'], '3.00')
        self.assertEqual(calculated['_daily_load'], '8.0')

    def test_new_day_resets_the_daily_baseline_but_not_the_hourly_one_independently(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.calculate_values({
            'timestamp': '2026-08-28 23:30:05',
            'meter_e_total_exp': '100.0',
            'meter_e_total_imp': '50.0',
            'e_load_total': '10.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-29 00:05:00',
            'meter_e_total_exp': '101.0',
            'meter_e_total_imp': '50.5',
            'e_load_total': '10.5',
        })

        self.assertEqual(calculated['_day_start_timestamp'], '2026-08-29 00:05:00')
        self.assertEqual(calculated['_daily_meter_export'], '0.00')
        # the hour also rolled over here, so the hourly baseline resets too
        self.assertEqual(calculated['_hour_start_timestamp'], '2026-08-29 00:05:00')

    def test_seed_day_start_restores_a_prior_baseline(self):
        evaluator = CalculatedValuesEvaluator()
        evaluator.seed_day_start({
            'timestamp': '2026-08-28 00:00:03',
            'meter_e_total_exp': '90.0',
            'meter_e_total_imp': '40.0',
            'e_load_total': '5.0',
        })

        calculated = evaluator.calculate_values({
            'timestamp': '2026-08-28 10:00:00',
            'meter_e_total_exp': '95.0',
            'meter_e_total_imp': '42.0',
            'e_load_total': '9.0',
        })

        self.assertEqual(calculated['_daily_meter_export'], '5.00')
        self.assertEqual(calculated['_daily_meter_import'], '2.00')
        self.assertEqual(calculated['_daily_load'], '4.0')
```

- [ ] **Step 2: Run to confirm failure**

Run: `python -m pytest tests/test_sensors.py -v`
Expected: FAIL with `KeyError: '_day_start_timestamp'`

- [ ] **Step 3: Extend `CALCULATED_VALUE_HEADERS` and `CalculatedValuesEvaluator`**

In `sensors.py`, change:

```python
CALCULATED_VALUE_HEADERS = [
    '_hour_start_timestamp',
    '_hourly_meter_export',
    '_hourly_meter_import',
    '_hourly_load',
]
TEXT_CALCULATED_COLUMNS = {'_hour_start_timestamp'}
```

to:

```python
CALCULATED_VALUE_HEADERS = [
    '_hour_start_timestamp',
    '_hourly_meter_export',
    '_hourly_meter_import',
    '_hourly_load',
    '_day_start_timestamp',
    '_daily_meter_export',
    '_daily_meter_import',
    '_daily_load',
]
TEXT_CALCULATED_COLUMNS = {'_hour_start_timestamp', '_day_start_timestamp'}
```

Change the `CalculatedValuesEvaluator` class:

```python
class CalculatedValuesEvaluator:
    def __init__(self):
        self._hour_start_sensors = None
        self._day_start_sensors = None

    def calculate_values(self, sensors_data: Mapping[str, Any]) -> dict:
        if self._hour_start_sensors is None or sensors_data['timestamp'][:13] != self._hour_start_sensors['timestamp'][
                                                                                 :13]:
            self._hour_start_sensors = sensors_data
        if self._day_start_sensors is None or sensors_data['timestamp'][:10] != self._day_start_sensors['timestamp'][:10]:
            self._day_start_sensors = sensors_data
        calculated_values = {
            '_hour_start_timestamp': self._hour_start_sensors['timestamp'],
            '_hourly_meter_export': f"{float(sensors_data['meter_e_total_exp']) - float(self._hour_start_sensors['meter_e_total_exp']):.2f}",
            '_hourly_meter_import': f"{float(sensors_data['meter_e_total_imp']) - float(self._hour_start_sensors['meter_e_total_imp']):.2f}",
            '_hourly_load': f"{float(sensors_data['e_load_total']) - float(self._hour_start_sensors['e_load_total']):.1f}",
            '_day_start_timestamp': self._day_start_sensors['timestamp'],
            '_daily_meter_export': f"{float(sensors_data['meter_e_total_exp']) - float(self._day_start_sensors['meter_e_total_exp']):.2f}",
            '_daily_meter_import': f"{float(sensors_data['meter_e_total_imp']) - float(self._day_start_sensors['meter_e_total_imp']):.2f}",
            '_daily_load': f"{float(sensors_data['e_load_total']) - float(self._day_start_sensors['e_load_total']):.1f}",
        }
        self._verify_header(calculated_values)
        return calculated_values

    def seed_hour_start(self, sensors_data: Optional[Mapping[str, Any]]) -> None:
        self._hour_start_sensors = dict(sensors_data) if sensors_data is not None else None

    def seed_day_start(self, sensors_data: Optional[Mapping[str, Any]]) -> None:
        """Same restore-from-a-prior-sample role as seed_hour_start, but
        for the midnight-anchored daily baseline - see that method's
        docstring."""
        self._day_start_sensors = dict(sensors_data) if sensors_data is not None else None

    @staticmethod
    def headers():
        return CALCULATED_VALUE_HEADERS

    def _verify_header(self, calculated_values):
        for header, key in zip(self.headers(), calculated_values.keys()):
            if header != key:
                raise AssertionError(f"Implementation error: headers do not correspond to set keys: {key} != {header}, "
                                     f"{self.headers()} != {calculated_values.keys()}")
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `python -m pytest tests/test_sensors.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sensors.py tests/test_sensors.py
git commit -m "feat: add midnight-anchored daily energy deltas to CalculatedValuesEvaluator"
```

- [ ] **Step 6: Write a failing test for `storage.current_day_bounds`**

Add to `tests/test_storage.py`:

```python
class CurrentDayBoundsTest(unittest.TestCase):
    def test_returns_local_midnight_to_midnight(self):
        now = datetime(2026, 8, 28, 14, 30, 0)
        start_epoch, end_epoch = storage.current_day_bounds(now)

        self.assertEqual(datetime.fromtimestamp(start_epoch), datetime(2026, 8, 28, 0, 0, 0))
        self.assertEqual(end_epoch - start_epoch, 24 * 3600)
```

- [ ] **Step 7: Run to confirm failure, then implement**

In `storage.py`, add right after `current_hour_bounds`:

```python
def current_day_bounds(now: datetime) -> Tuple[int, int]:
    """Returns (start_epoch, end_epoch) for the local calendar day
    containing `now` - the day-anchored counterpart to
    current_hour_bounds, used to seed CalculatedValuesEvaluator's daily
    baseline the same way current_hour_bounds seeds its hourly one."""
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_epoch = int(day_start.timestamp())
    return start_epoch, start_epoch + 24 * 3600
```

- [ ] **Step 8: Run tests, confirm pass**

Run: `python -m pytest tests/test_storage.py -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add storage.py tests/test_storage.py
git commit -m "feat: add storage.current_day_bounds for daily baseline seeding"
```

---

### Task 6: `mqtt_bridge.py` — MQTT client lifecycle and publish methods

**Files:**
- Create: `mqtt_bridge.py`
- Test: `tests/test_mqtt_bridge.py`

**Interfaces:**
- Consumes: `export_price.build_export_price_payload` (Task 4), `tariff_engine.bands_for_day`/`load_config` (Task 1) — called by Task 7's wiring code, not by this module directly (this module only knows how to publish already-built payloads).
- Produces (used by Task 7): `MqttBridge` class with `enabled: bool` property, `async connect()`, `async publish_offline_and_disconnect()`, `async publish_telemetry(payload: dict)`, `async publish_export_prices(payload: dict)`, `async publish_import_prices(payload: dict)`, `async publish_pv_forecast(series: dict)`. Also the pure `build_telemetry_topic`/`build_status_topic`-style helpers used internally, exposed for testing.

- [ ] **Step 1: Write failing tests using a fake client (no real broker needed)**

```python
"""
tests/test_mqtt_bridge.py
MqttBridge is tested against a FakeMqttClient rather than a real broker -
it records every publish call (topic, payload, retain) so tests can
assert on exact topic names and retain flags without any network I/O.
"""
import asyncio
import json
import unittest

import mqtt_bridge


class FakeMqttClient:
    def __init__(self):
        self.published = []
        self.connected = False
        self.disconnected = False

    async def __aenter__(self):
        self.connected = True
        return self

    async def __aexit__(self, *exc_info):
        self.disconnected = True

    async def publish(self, topic, payload, retain=False):
        self.published.append((topic, payload, retain))


class MqttBridgeDisabledTest(unittest.TestCase):
    def test_disabled_when_no_host_given(self):
        bridge = mqtt_bridge.MqttBridge(host=None)
        self.assertFalse(bridge.enabled)

    def test_publish_is_a_no_op_when_disabled(self):
        bridge = mqtt_bridge.MqttBridge(host=None)
        asyncio.run(bridge.connect())
        asyncio.run(bridge.publish_telemetry({'ppv': '100'}))  # must not raise


class MqttBridgeEnabledTest(unittest.TestCase):
    def setUp(self):
        self.fake_client = FakeMqttClient()
        self.bridge = mqtt_bridge.MqttBridge(host='localhost', topic_prefix='goodwe',
                                             client_factory=lambda **kwargs: self.fake_client)

    def test_connect_publishes_online_status_retained(self):
        asyncio.run(self.bridge.connect())

        self.assertTrue(self.fake_client.connected)
        self.assertIn(('goodwe/bridge/status', 'online', True), self.fake_client.published)

    def test_publish_offline_and_disconnect_publishes_offline_status_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_offline_and_disconnect())

        self.assertIn(('goodwe/bridge/status', 'offline', True), self.fake_client.published)
        self.assertTrue(self.fake_client.disconnected)

    def test_publish_telemetry_is_not_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/telemetry')
        self.assertEqual(json.loads(payload), {'ppv': '100'})
        self.assertFalse(retain)

    def test_publish_export_prices_is_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_export_prices({'raw_today': [], 'raw_tomorrow': []}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/prices/export')
        self.assertTrue(retain)

    def test_publish_import_prices_is_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_import_prices({'raw_today': [], 'raw_tomorrow': []}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/prices/import')
        self.assertTrue(retain)

    def test_publish_pv_forecast_is_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_pv_forecast({'12:00': 1.5}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/forecast/pv')
        self.assertTrue(retain)

    def test_a_publish_failure_is_swallowed_not_raised(self):
        async def raising_publish(*args, **kwargs):
            raise ConnectionError("broker unreachable")
        self.fake_client.publish = raising_publish
        asyncio.run(self.bridge.connect())

        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))  # must not raise


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run to confirm failure**

Run: `python -m pytest tests/test_mqtt_bridge.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mqtt_bridge'`

- [ ] **Step 3: Implement `mqtt_bridge.py`**

```python
"""
mqtt_bridge.py
Optional MQTT publish path for goodwe_manager - turns the existing
inverter polling loop's already-fetched data into the topics Home
Assistant/Predbat need, without a second connection to the inverter. See
docs/superpowers/specs/2026-09-19-predbat-mqtt-bridge-design.md.

Disabled entirely (every publish_* method becomes a no-op) when no
MQTT_HOST is configured - a fresh checkout or another user's fork with no
new env vars set behaves exactly as before this feature existed.
"""
import json
import logging
from typing import Any, Callable, Optional

import aiomqtt

logger = logging.getLogger(__name__)


class MqttBridge:
    def __init__(self, host: Optional[str], port: int = 1883, username: Optional[str] = None,
                 password: Optional[str] = None, topic_prefix: str = 'goodwe',
                 client_factory: Optional[Callable[..., Any]] = None):
        """client_factory, if given, is called with the same kwargs
        aiomqtt.Client would take, and must return an object supporting
        `async with` and an `async def publish(topic, payload, retain)`
        method - used by tests to inject a fake client instead of a real
        aiomqtt.Client. Defaults to aiomqtt.Client itself."""
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._prefix = topic_prefix
        self._client_factory = client_factory or aiomqtt.Client
        self._client = None

    @property
    def enabled(self) -> bool:
        return self._host is not None

    def _topic(self, suffix: str) -> str:
        return f'{self._prefix}/{suffix}'

    async def connect(self) -> None:
        if not self.enabled:
            return
        will = aiomqtt.Will(topic=self._topic('bridge/status'), payload='offline', retain=True)
        self._client = self._client_factory(
            hostname=self._host, port=self._port,
            username=self._username, password=self._password, will=will,
        )
        await self._client.__aenter__()
        await self._publish('bridge/status', 'online', retain=True)

    async def publish_offline_and_disconnect(self) -> None:
        """Explicit offline publish before a clean disconnect - the MQTT
        Will above only fires on an *unclean* disconnect (spec's Units/
        Component 1 note), so a deliberate shutdown needs this to avoid
        leaving the retained status topic stuck on 'online'."""
        if not self.enabled or self._client is None:
            return
        await self._publish('bridge/status', 'offline', retain=True)
        try:
            await self._client.__aexit__(None, None, None)
        except Exception as e:
            logger.warning(f"Error disconnecting MQTT client: {e}")

    async def publish_telemetry(self, payload: dict) -> None:
        await self._publish('telemetry', json.dumps(payload), retain=False)

    async def publish_export_prices(self, payload: dict) -> None:
        await self._publish('prices/export', json.dumps(payload), retain=True)

    async def publish_import_prices(self, payload: dict) -> None:
        await self._publish('prices/import', json.dumps(payload), retain=True)

    async def publish_pv_forecast(self, series: dict) -> None:
        await self._publish('forecast/pv', json.dumps(series), retain=True)

    async def _publish(self, topic_suffix: str, payload, retain: bool) -> None:
        if not self.enabled or self._client is None:
            return
        try:
            await self._client.publish(self._topic(topic_suffix), payload, retain=retain)
        except Exception as e:
            # A broker hiccup must never take down inverter polling - see
            # spec Component 1: "MQTT being down must never stop inverter
            # polling/storage/SSE from working."
            logger.warning(f"MQTT publish to {topic_suffix} failed: {e}")
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `python -m pytest tests/test_mqtt_bridge.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add mqtt_bridge.py tests/test_mqtt_bridge.py
git commit -m "feat: add mqtt_bridge.py with LWT-backed status and publish methods"
```

- [ ] **Step 6: Add aiomqtt to requirements.txt**

Run: `pip install aiomqtt`, then `pip show aiomqtt` to get the resolved version, and add `aiomqtt==<resolved version>` to `requirements.txt`.

```bash
git add requirements.txt
git commit -m "chore: add aiomqtt dependency for mqtt_bridge"
```

---

### Task 7: Wire the bridge into `main.py`

**Files:**
- Modify: `main.py`
- Modify: `rce_prefetch.py`
- Modify: `tests/test_rce_prefetch.py`
- Modify: `forecast_prefetch.py`
- Modify: `tests/test_forecast_prefetch.py`
- Modify: `README.MD`
- Modify: `.env.example`

**Interfaces:**
- Consumes: `mqtt_bridge.MqttBridge` (Task 6), `export_price.build_export_price_payload` (Task 4), `tariff_engine.load_config`/`bands_for_day` (Task 1), `CalculatedValuesEvaluator.seed_day_start`/daily calculated fields (Task 5), `solcast.sum_sites`'s existing combined-forecast shape (already in the codebase, unchanged).
- Produces: nothing further downstream — this is the final integration task.

- [ ] **Step 1: Write a failing test for the RCE prefetch success callback**

Add to `tests/test_rce_prefetch.py`, inside `RunPrefetchCycleTest`:

```python
    def test_calls_on_success_callback_after_a_successful_fetch(self):
        calls = []
        result = rce_prefetch.run_prefetch_cycle(
            fetch_fn=lambda d: None,
            target_date=date(2026, 1, 2),
            sleep_fn=lambda s: None,
            now_fn=lambda: datetime(2026, 1, 1, 14, 15),
            on_success=lambda d: calls.append(d),
        )
        self.assertTrue(result)
        self.assertEqual(calls, [date(2026, 1, 2)])

    def test_on_success_callback_is_not_called_when_fetch_never_succeeds(self):
        calls = []
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
            on_success=lambda d: calls.append(d),
        )
        self.assertFalse(result)
        self.assertEqual(calls, [])
```

- [ ] **Step 2: Run to confirm failure, then add the callback parameter**

In `rce_prefetch.py`, change `run_prefetch_cycle`'s signature and body:

```python
def run_prefetch_cycle(fetch_fn, target_date, sleep_fn, now_fn=datetime.now,
                       cutoff_time=CUTOFF_TIME, retry_interval_seconds=RETRY_INTERVAL_SECONDS,
                       should_stop=lambda: False, on_success=lambda target_date: None) -> bool:
    """... (existing docstring unchanged) ... on_success, if given, is
    called with target_date immediately after a successful fetch - used
    by main.py to trigger an MQTT export-price republish without this
    module needing to know anything about MQTT."""
    while not should_stop():
        if past_cutoff(now_fn(), cutoff_time):
            logger.error(f"Giving up prefetching RCE prices for {target_date} - not published by cutoff")
            return False
        try:
            fetch_fn(target_date)
            logger.info(f"Prefetched RCE prices for {target_date}")
            on_success(target_date)
            return True
        except RuntimeError as e:
            logger.info(f"RCE prices for {target_date} not published yet: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error prefetching RCE prices for {target_date}: {e}")
        sleep_fn(retry_interval_seconds)
    return False
```

And thread it through `RcePrefetchThread`:

```python
class RcePrefetchThread(threading.Thread):
    def __init__(self, on_success=lambda target_date: None):
        super().__init__(name='RcePrefetchThread', daemon=True)
        self._should_stop = threading.Event()
        self._on_success = on_success

    def run(self):
        while not self._should_stop.is_set():
            wait_seconds = seconds_until(datetime.now(), WAKE_TIME)
            if self._should_stop.wait(wait_seconds):
                return
            tomorrow = (datetime.now() + timedelta(days=1)).date()
            run_prefetch_cycle(
                fetch_fn=rce.get_rce_15min,
                target_date=tomorrow,
                sleep_fn=lambda seconds: self._should_stop.wait(seconds),
                should_stop=self._should_stop.is_set,
                on_success=self._on_success,
            )
```

- [ ] **Step 3: Run tests, confirm pass, commit**

Run: `python -m pytest tests/test_rce_prefetch.py -v`
Expected: PASS

```bash
git add rce_prefetch.py tests/test_rce_prefetch.py
git commit -m "feat: add on_success callback to RcePrefetchThread"
```

- [ ] **Step 4: Write a failing test for the forecast prefetch success callback**

Add to `tests/test_forecast_prefetch.py` (check the existing file first for its exact `fetch_and_store_solcast` test setup/mocking pattern, and mirror it) a test asserting that a `on_solcast_updated` callback (passed into `ForecastPrefetchThread.__init__`) is invoked after `fetch_and_store_solcast` succeeds inside `run_catch_up`, using the same style of fake/monkeypatched `solcast.fetch_solcast_forecast_30min` the existing tests already use.

- [ ] **Step 5: Add the callback to `ForecastPrefetchThread`**

In `forecast_prefetch.py`:

```python
class ForecastPrefetchThread(threading.Thread):
    def __init__(self, db_path=None, on_solcast_updated=lambda: None):
        super().__init__(name='ForecastPrefetchThread', daemon=True)
        self._should_stop = threading.Event()
        self._db_path = db_path
        self._on_solcast_updated = on_solcast_updated
```

Call `self._on_solcast_updated()` immediately after each successful `fetch_and_store_solcast(...)` call — both inside `run_catch_up` (module-level function; pass the callback in as a parameter, e.g. `run_catch_up(conn, now, on_solcast_updated=lambda: None)`, called from `self.run()` as `run_catch_up(conn, datetime.now(), on_solcast_updated=self._on_solcast_updated)`) and inside the main loop's `if next_wake == next_forecast:` branch.

- [ ] **Step 6: Run tests, confirm pass, commit**

```bash
git add forecast_prefetch.py tests/test_forecast_prefetch.py
git commit -m "feat: add on_solcast_updated callback to ForecastPrefetchThread"
```

- [ ] **Step 7: Wire everything into `main.py`**

Add imports near the top of `main.py`:

```python
from zoneinfo import ZoneInfo

import export_price
import mqtt_bridge
import tariff_engine
```

Add near the other env var reads (after `BACKUP_ACTIVE_THRESHOLD_W`):

```python
MQTT_HOST = os.environ.get('MQTT_HOST')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
MQTT_TOPIC_PREFIX = os.environ.get('MQTT_TOPIC_PREFIX', 'goodwe')
TARIFF_IMPORT_CONFIG = os.environ.get('TARIFF_IMPORT_CONFIG')
WARSAW_TZ = ZoneInfo('Europe/Warsaw')
```

Add the module-level bridge instance next to `asyncio_thread`/`rce_prefetch_thread`:

```python
mqtt = mqtt_bridge.MqttBridge(host=MQTT_HOST, port=MQTT_PORT, username=MQTT_USERNAME,
                              password=MQTT_PASSWORD, topic_prefix=MQTT_TOPIC_PREFIX)


def _publish_export_prices():
    payload = export_price.build_export_price_payload(datetime.now().date(), WARSAW_TZ)
    asyncio_thread.run_coroutine_threadsafe(mqtt.publish_export_prices(payload))


def _publish_import_prices():
    if not TARIFF_IMPORT_CONFIG:
        return
    config = tariff_engine.load_config(TARIFF_IMPORT_CONFIG)
    today = datetime.now().date()
    payload = {
        'raw_today': tariff_engine.bands_for_day(config, today, WARSAW_TZ),
        'raw_tomorrow': tariff_engine.bands_for_day(config, today + timedelta(days=1), WARSAW_TZ),
    }
    asyncio_thread.run_coroutine_threadsafe(mqtt.publish_import_prices(payload))


def _publish_pv_forecast():
    # Reuses whatever ForecastPrefetchThread already just fetched and
    # stored (forecast_history.db) rather than calling Solcast again -
    # see spec Component 5.
    with _forecast_history_connection() as conn:
        today = datetime.now().strftime('%Y-%m-%d')
        series = forecast_history.get_latest_merged(conn, 'solcast', today)
    asyncio_thread.run_coroutine_threadsafe(mqtt.publish_pv_forecast(series))
```

`timedelta` needs importing too (`from datetime import datetime, timedelta`, replacing the existing bare `datetime` import).

Update the thread constructions to pass the new callbacks:

```python
rce_prefetch_thread = RcePrefetchThread(on_success=lambda target_date: _publish_export_prices())
forecast_prefetch_thread = ForecastPrefetchThread(on_solcast_updated=_publish_pv_forecast)
```

Inside `AsyncioThread._get_inverter_data`, replace:

```python
        self._db_conn = await storage.init_db_async(storage.DATA_DB_PATH, sensor_columns())
        try:
            await self._seed_hour_start_baseline()
            await self._backfill_hourly_summary()
```

with:

```python
        self._db_conn = await storage.init_db_async(storage.DATA_DB_PATH, sensor_columns())
        await mqtt.connect()
        await mqtt.publish_export_prices(export_price.build_export_price_payload(datetime.now().date(), WARSAW_TZ))
        if TARIFF_IMPORT_CONFIG:
            _publish_import_prices()
        _publish_pv_forecast()
        try:
            await self._seed_hour_start_baseline()
            await self._seed_day_start_baseline()
            await self._backfill_hourly_summary()
```

(the bridge connects and publishes its initial snapshot of everything — export/import prices, PV forecast — right after the DB connection is ready, so a restart doesn't wait for the next natural trigger; see spec's "Publish cadence... at bridge startup")

Add the new `_seed_day_start_baseline` method right after `_seed_hour_start_baseline`:

```python
    async def _seed_day_start_baseline(self):
        day_start_epoch, day_end_epoch = storage.current_day_bounds(datetime.now())
        baseline = await storage.get_current_hour_start_sample_async(self._db_conn, day_start_epoch, day_end_epoch)
        self._calculated_values_evaluator.seed_day_start(baseline)
```

Inside the polling `while True:` loop, after the existing hour-rollover block (`if new_hour_start != current_hour_start: ...`), add day-rollover detection that triggers a fresh price publish (today/tomorrow shift at midnight — spec Component 3/4's "midnight rollover" cadence) and publish telemetry on every tick:

```python
                await mqtt.publish_telemetry(sensors_data_with_calculated)
                new_day_start, _ = storage.current_day_bounds(datetime.now())
                if new_day_start != current_day_start:
                    current_day_start = new_day_start
                    await mqtt.publish_export_prices(export_price.build_export_price_payload(datetime.now().date(), WARSAW_TZ))
                    if TARIFF_IMPORT_CONFIG:
                        _publish_import_prices()
```

Initialize `current_day_start` alongside the existing `current_hour_start` initialization:

```python
            current_hour_start, _ = storage.current_hour_bounds(datetime.now())
            current_day_start, _ = storage.current_day_bounds(datetime.now())
```

In `AsyncioThread.finish()`, publish offline before stopping the loop:

```python
    def finish(self):
        """Called from another thread to finish and stop the asyncio loop"""
        logger.info("Finishing asyncio loop...")
        self._should_stop.set()
        loop = self._asyncio_loop
        if loop is None:
            return
        try:
            offline_future = asyncio.run_coroutine_threadsafe(mqtt.publish_offline_and_disconnect(), loop)
            offline_future.result(timeout=5)
        except Exception as e:
            logger.warning(f"Could not publish MQTT offline status cleanly: {e}")
        try:
            stop_future = asyncio.run_coroutine_threadsafe(self._stop_event_loop(), loop)
            stop_future.result(timeout=5)
        except concurrent.futures.TimeoutError:
            logger.warning("Timed out while requesting asyncio loop stop")
        except RuntimeError:
            return
        logger.info("Waiting for the asyncio loop finish result...")
        self.join(timeout=30)
        if self.is_alive():
            logger.warning("Asyncio thread did not stop within timeout")
```

- [ ] **Step 8: Document the new env vars**

Add to `.env.example`:

```
MQTT_HOST=
MQTT_PORT=1883
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_TOPIC_PREFIX=goodwe
TARIFF_IMPORT_CONFIG=
```

Add a new section to `README.MD` (after the "Tariff-aware import pricing" section added in Task 2):

```markdown
## MQTT bridge for Home Assistant / Predbat (optional)

Set `MQTT_HOST` (and `MQTT_USERNAME`/`MQTT_PASSWORD` if your broker
requires auth) in `.env` to publish live telemetry, daily energy
counters, RCE export prices, tariff-based import prices (if
`TARIFF_IMPORT_CONFIG` is also set), and the combined PV forecast to your
MQTT broker. Unset `MQTT_HOST` (the default) disables this entirely - no
behavior change from before this feature existed. See
`docs/superpowers/specs/2026-09-19-predbat-mqtt-bridge-design.md` for the
full topic list and Home Assistant/Predbat setup.
```

- [ ] **Step 9: Run the full test suite**

Run: `python -m pytest tests/ -v`
Expected: PASS (every test, old and new)

- [ ] **Step 10: Manual verification against the real Mosquitto broker**

On `raspberry4.local` (or locally with `--dry-run` against a local Mosquitto): set `MQTT_HOST`/credentials in `.env`, start `goodwe_manager`, and run `mosquitto_sub -h <host> -u <user> -P <pass> -t 'goodwe/#' -v` to confirm `goodwe/bridge/status` = `online` (retained), `goodwe/telemetry` updating every ~1s, `goodwe/prices/export` populated, and (if `TARIFF_IMPORT_CONFIG` is set) `goodwe/prices/import` populated. Stop the service with Ctrl-C and confirm `goodwe/bridge/status` flips to `offline`.

- [ ] **Step 11: Commit**

```bash
git add main.py .env.example README.MD
git commit -m "feat: wire mqtt_bridge/export_price/tariff_engine into main.py"
```

---

### Task 8: `_estimate_battery_efficiency.py` — offline efficiency analysis

**Files:**
- Create: `_estimate_battery_efficiency.py`
- Test: `tests/test_estimate_battery_efficiency.py`

**Interfaces:**
- Consumes: `storage.DATA_DB_PATH`, the `inverter_history` table's `pbattery1`, `battery_soc`, `pgrid`/`house_consumption`-family columns (all already stored today, no schema change needed).
- Produces: a standalone script; also exposes pure functions (`find_battery_sessions`, `find_matched_cycle_pairs`, `estimate_inverter_loss`, `estimate_battery_round_trip_loss`) for unit testing, per the spec's "verified by manual inspection... plausible ranges" note plus a real unit-tested core.

- [ ] **Step 1: Write failing tests for session detection**

```python
import unittest

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
        pairs = estimator.find_matched_cycle_pairs(soc_samples, max_gap_seconds=24 * 3600, soc_tolerance=5.0)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0], (0, 10800))

    def test_no_pair_when_soc_never_returns_within_tolerance(self):
        soc_samples = [(0, 40.0), (3600, 70.0), (7200, 90.0)]
        pairs = estimator.find_matched_cycle_pairs(soc_samples, max_gap_seconds=24 * 3600, soc_tolerance=5.0)

        self.assertEqual(pairs, [])
```

- [ ] **Step 2: Run to confirm failure**

Run: `python -m pytest tests/test_estimate_battery_efficiency.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement session/cycle detection**

```python
"""
_estimate_battery_efficiency.py
One-off offline analysis: estimates Predbat's battery_loss,
inverter_loss_charge, inverter_loss_discharge config values from this
household's ~2 years of inverter_history. Run manually; prints a report
with recommended values and sample-size caveats - nothing here writes to
Predbat's config automatically. See
docs/superpowers/specs/2026-09-19-predbat-mqtt-bridge-design.md,
Component 6.
"""
import argparse
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple

import storage

DEFAULT_NOISE_THRESHOLD_W = 20.0
DEFAULT_MAX_CYCLE_GAP_SECONDS = 3 * 24 * 3600
DEFAULT_SOC_TOLERANCE = 3.0


@dataclass
class BatterySession:
    sign: str  # 'charge' or 'discharge'
    start_epoch: int
    end_epoch: int


def _sign(pbattery1: float, noise_threshold_w: float) -> Optional[str]:
    if pbattery1 > noise_threshold_w:
        return 'charge'
    if pbattery1 < -noise_threshold_w:
        return 'discharge'
    return None


def find_battery_sessions(samples: List[Tuple[int, float]], noise_threshold_w: float) -> List[BatterySession]:
    """samples: [(timestamp_epoch, pbattery1_watts), ...], time-ordered.
    Splits into contiguous runs sharing the same sign (above
    noise_threshold_w in magnitude) - a sample within the noise band ends
    the current run without starting a new one (see spec Component 6's
    "contiguous run where pbattery1 holds one sign above a noise
    threshold")."""
    sessions = []
    current_sign = None
    current_start = None
    current_end = None
    for epoch, pbattery1 in samples:
        sign = _sign(pbattery1, noise_threshold_w)
        if sign is None:
            if current_sign is not None:
                sessions.append(BatterySession(current_sign, current_start, current_end))
                current_sign = None
            continue
        if sign != current_sign:
            if current_sign is not None:
                sessions.append(BatterySession(current_sign, current_start, current_end))
            current_sign, current_start = sign, epoch
        current_end = epoch
    if current_sign is not None:
        sessions.append(BatterySession(current_sign, current_start, current_end))
    return sessions


def find_matched_cycle_pairs(soc_samples: List[Tuple[int, float]], max_gap_seconds: int,
                             soc_tolerance: float) -> List[Tuple[int, int]]:
    """soc_samples: [(timestamp_epoch, battery_soc), ...], time-ordered.
    Returns (start_epoch, end_epoch) pairs where SOC at end_epoch is
    within soc_tolerance of SOC at start_epoch, and end_epoch - start_epoch
    <= max_gap_seconds - see spec Component 6's "matched cycle pairs
    where SOC returns to roughly its starting level within a short
    window". Greedy: each start_epoch matches at most one (the first
    qualifying) end_epoch, to avoid double-counting overlapping cycles."""
    pairs = []
    used_starts = set()
    for i, (start_epoch, start_soc) in enumerate(soc_samples):
        if start_epoch in used_starts:
            continue
        for end_epoch, end_soc in soc_samples[i + 1:]:
            if end_epoch - start_epoch > max_gap_seconds:
                break
            if abs(end_soc - start_soc) <= soc_tolerance:
                pairs.append((start_epoch, end_epoch))
                used_starts.add(start_epoch)
                break
    return pairs
```

- [ ] **Step 4: Run tests, confirm pass, commit**

Run: `python -m pytest tests/test_estimate_battery_efficiency.py -v`
Expected: PASS

```bash
git add _estimate_battery_efficiency.py tests/test_estimate_battery_efficiency.py
git commit -m "feat: add battery session/cycle detection for efficiency estimation"
```

- [ ] **Step 5: Add the inverter-loss and round-trip-loss estimators plus the report-printing `main()`**

```python
def estimate_inverter_loss(conn: sqlite3.Connection, sessions: List[BatterySession]) -> dict:
    """For each charge session, compares AC-side energy drawn (pgrid
    positive = importing, attributable to charging when the battery is
    the only active load driving that import) against pbattery1 energy
    integrated over the same window; symmetric for discharge sessions
    against AC-side energy delivered. Returns
    {'charge': (loss_fraction, sample_count), 'discharge': (loss_fraction, sample_count)}.
    Sessions with fewer than 2 samples are skipped (no meaningful energy
    integral). This is deliberately approximate - see spec Component 6 -
    real-world PV/load noise means a session's AC-side energy isn't
    purely attributable to battery charging/discharging.
    """
    results = {'charge': [], 'discharge': []}
    for session in sessions:
        rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1, pgrid FROM inverter_history "
            "WHERE timestamp_epoch >= ? AND timestamp_epoch <= ? ORDER BY timestamp_epoch",
            (session.start_epoch, session.end_epoch),
        ).fetchall()
        if len(rows) < 2:
            continue
        dc_energy_wh = _trapezoidal_energy_wh([(r[0], abs(r[1])) for r in rows if r[1] is not None])
        ac_energy_wh = _trapezoidal_energy_wh([(r[0], abs(r[2])) for r in rows if r[2] is not None])
        if dc_energy_wh <= 0 or ac_energy_wh <= 0:
            continue
        if session.sign == 'charge':
            loss = 1 - (dc_energy_wh / ac_energy_wh)
        else:
            loss = 1 - (ac_energy_wh / dc_energy_wh)
        results[session.sign].append(loss)

    def _summarize(losses):
        if not losses:
            return None, 0
        return sum(losses) / len(losses), len(losses)

    charge_loss, charge_n = _summarize(results['charge'])
    discharge_loss, discharge_n = _summarize(results['discharge'])
    return {'charge': (charge_loss, charge_n), 'discharge': (discharge_loss, discharge_n)}


def _trapezoidal_energy_wh(power_samples: List[Tuple[int, float]]) -> float:
    """power_samples: [(timestamp_epoch, watts), ...], time-ordered.
    Trapezoidal integration to watt-hours."""
    if len(power_samples) < 2:
        return 0.0
    energy_ws = 0.0
    for (t0, p0), (t1, p1) in zip(power_samples, power_samples[1:]):
        energy_ws += (p0 + p1) / 2.0 * (t1 - t0)
    return energy_ws / 3600.0


def estimate_battery_round_trip_loss(conn: sqlite3.Connection, pairs: List[Tuple[int, int]]) -> Tuple[Optional[float], int]:
    """For each matched cycle pair, integrates pbattery1 energy in
    (positive) vs out (negative, absolute value) across the whole
    window - a round-trip cycle returning to similar SOC should have
    total-in > total-out, the gap being the round-trip loss fraction.
    Returns (average_loss_fraction, sample_count)."""
    losses = []
    for start_epoch, end_epoch in pairs:
        rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1 FROM inverter_history "
            "WHERE timestamp_epoch >= ? AND timestamp_epoch <= ? ORDER BY timestamp_epoch",
            (start_epoch, end_epoch),
        ).fetchall()
        charge_samples = [(t, p) for t, p in rows if p is not None and p > 0]
        discharge_samples = [(t, abs(p)) for t, p in rows if p is not None and p < 0]
        energy_in = _trapezoidal_energy_wh(charge_samples)
        energy_out = _trapezoidal_energy_wh(discharge_samples)
        if energy_in <= 0 or energy_out <= 0 or energy_out > energy_in:
            continue
        losses.append(1 - (energy_out / energy_in))
    if not losses:
        return None, 0
    return sum(losses) / len(losses), len(losses)


def main():
    parser = argparse.ArgumentParser(description="Estimate Predbat battery_loss/inverter_loss_charge/inverter_loss_discharge from history")
    parser.add_argument("--db-path", type=str, default=storage.DATA_DB_PATH)
    parser.add_argument("--noise-threshold-w", type=float, default=DEFAULT_NOISE_THRESHOLD_W)
    parser.add_argument("--max-cycle-gap-seconds", type=int, default=DEFAULT_MAX_CYCLE_GAP_SECONDS)
    parser.add_argument("--soc-tolerance", type=float, default=DEFAULT_SOC_TOLERANCE)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    try:
        battery_rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1 FROM inverter_history WHERE pbattery1 IS NOT NULL ORDER BY timestamp_epoch"
        ).fetchall()
        soc_rows = conn.execute(
            "SELECT timestamp_epoch, battery_soc FROM inverter_history WHERE battery_soc IS NOT NULL ORDER BY timestamp_epoch"
        ).fetchall()

        sessions = find_battery_sessions(battery_rows, args.noise_threshold_w)
        inverter_loss = estimate_inverter_loss(conn, sessions)
        pairs = find_matched_cycle_pairs(soc_rows, args.max_cycle_gap_seconds, args.soc_tolerance)
        battery_loss, battery_n = estimate_battery_round_trip_loss(conn, pairs)
    finally:
        conn.close()

    print("Predbat efficiency estimate (approximate - verify plausibility before use):")
    charge_loss, charge_n = inverter_loss['charge']
    discharge_loss, discharge_n = inverter_loss['discharge']
    print(f"  inverter_loss_charge:    {charge_loss:.3f}" if charge_loss is not None else "  inverter_loss_charge:    no data")
    print(f"    (based on {charge_n} charge sessions)")
    print(f"  inverter_loss_discharge: {discharge_loss:.3f}" if discharge_loss is not None else "  inverter_loss_discharge: no data")
    print(f"    (based on {discharge_n} discharge sessions)")
    print(f"  battery_loss:            {battery_loss:.3f}" if battery_loss is not None else "  battery_loss:            no data")
    print(f"    (based on {battery_n} matched charge/discharge cycle pairs)")
    print()
    print("Plausibility check: battery_loss and inverter_loss_* are each")
    print("normally somewhere in 0.02-0.15 for a modern li-ion system.")
    print("A value far outside that range likely means noisy/insufficient")
    print("data rather than a real result - inspect session/cycle counts above.")


if __name__ == '__main__':
    main()
```

- [ ] **Step 6: Write failing tests for the estimator functions using a small synthetic DB**

```python
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
```

- [ ] **Step 7: Run tests, confirm pass**

Run: `python -m pytest tests/test_estimate_battery_efficiency.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add _estimate_battery_efficiency.py tests/test_estimate_battery_efficiency.py
git commit -m "feat: add inverter/battery loss estimation and report output"
```

- [ ] **Step 9: Run it manually against real data and sanity-check the output**

Run: `python _estimate_battery_efficiency.py`
Expected: prints the report; sanity-check the three values fall in a plausible range (see the script's own printed guidance) and that session/cycle counts aren't suspiciously low for ~2 years of data (a near-zero count signals a detection-logic bug, not a real result — investigate before trusting the numbers).

---

## Final verification

- [ ] Run the entire test suite once more: `python -m pytest tests/ -v` — every test passes.
- [ ] Confirm a checkout with no new env vars set (`MQTT_HOST`, `TARIFF_IMPORT_CONFIG` both unset) starts and behaves identically to `origin/main` — run `python main.py --dry-run` and confirm no MQTT-related errors/warnings appear and `_calculate_income.py --date t` output is byte-identical to a pre-change run.
- [ ] Manual MQTT verification per Task 7 Step 10, against the real Pi and Mosquitto broker.
