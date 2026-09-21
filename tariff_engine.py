"""
tariff_engine.py
Evaluates a multi-component, seasonal, day-of-week-aware electricity
tariff schedule (YAML config) for a given instant or day. See PR #35
for the full schema and rationale.
"""
from datetime import date, datetime, time as dtime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import holidays
import yaml

_WEEKDAY_ABBREVIATIONS = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su']
DEFAULT_HOLIDAYS_COUNTRY = 'PL'
# Cached per country code (holidays.country_holidays() builds a lookup
# table for the whole calendar span it's queried over - not free to
# rebuild on every call) rather than hardcoding a single country's
# object, so a non-Polish tariff config (set via the optional top-level
# `country` YAML key) gets correct public holidays too.
_HOLIDAYS_BY_COUNTRY: dict = {}


def _holidays_for(country: str):
    if country not in _HOLIDAYS_BY_COUNTRY:
        _HOLIDAYS_BY_COUNTRY[country] = holidays.country_holidays(country)
    return _HOLIDAYS_BY_COUNTRY[country]


def _weekday_abbreviation(d: date) -> str:
    return _WEEKDAY_ABBREVIATIONS[d.weekday()]


def _is_public_holiday(d: date, country: str = DEFAULT_HOLIDAYS_COUNTRY) -> bool:
    return d in _holidays_for(country)


def _token_matches(token: str, d: date, country: str = DEFAULT_HOLIDAYS_COUNTRY) -> bool:
    token = token.strip()
    if token == 'Work':
        return d.weekday() < 5 and not _is_public_holiday(d, country)
    if token == 'Holiday':
        return _is_public_holiday(d, country)
    if '-' in token:
        start_abbr, end_abbr = token.split('-', 1)
        start_idx = _WEEKDAY_ABBREVIATIONS.index(start_abbr)
        end_idx = _WEEKDAY_ABBREVIATIONS.index(end_abbr)
        if start_idx <= end_idx:
            return start_idx <= d.weekday() <= end_idx
        # Wraps the week (e.g. Fr-Mo covering the weekend plus Friday).
        return d.weekday() >= start_idx or d.weekday() <= end_idx
    return _WEEKDAY_ABBREVIATIONS[d.weekday()] == token


def day_spec_matches(day_spec: str, d: date, country: str = DEFAULT_HOLIDAYS_COUNTRY) -> bool:
    """day_spec is a comma-separated list of tokens: weekday abbreviations
    (Mo,Tu,We,Th,Fr,Sa,Su), ranges (Mo-Fr, wrapping the week e.g. Fr-Mo),
    or the keywords Work/Holiday. Matches if any token matches (OR
    semantics) - see PR #35. `country` (an ISO code the `holidays`
    package recognizes) controls Work/Holiday's public holiday
    calendar - see the top-level `country` config key.
    """
    return any(_token_matches(token, d, country) for token in day_spec.split(','))


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
    try:
        return date(year, month, day)
    except ValueError:
        if month == 2 and day == 29:
            # 29.02 in a non-leap year: fall back to the 28th rather than
            # raising - a season boundary shouldn't depend on whether the
            # current year happens to be a leap year.
            return date(year, 2, 28)
        raise


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


def _band_matches(band: dict, dt: datetime, country: str = DEFAULT_HOLIDAYS_COUNTRY) -> bool:
    days = band.get('days')
    if days is not None and not day_spec_matches(days, dt.date(), country):
        return False
    return time_spec_matches(band.get('start'), band.get('end'), dt.time())


def component_price_at(component: dict, dt: datetime, country: str = DEFAULT_HOLIDAYS_COUNTRY) -> float:
    """Resolves one component's price at `dt`: that component's own season
    bands first (in file order), then its `bands.default` (in file
    order), then `default_price` - see PR #35's "Resolution order"
    discussion."""
    season = season_for_date(component['season_boundaries'], dt.date()) if component.get('season_boundaries') else None
    bands = component.get('bands', {})
    season_bands = bands.get(season, []) if season else []
    default_bands = bands.get('default', [])
    for band in (*season_bands, *default_bands):
        if _band_matches(band, dt, country):
            return component['prices'][band['price']]
    return component['prices'][component['default_price']]


def price_at(config: dict, dt: datetime) -> float:
    country = config.get('country', DEFAULT_HOLIDAYS_COUNTRY)
    return sum(component_price_at(component, dt, country) for component in config['components'].values())


def load_config(path: str) -> dict:
    with open(path) as f:
        config = yaml.safe_load(f)
    validate_config(config)
    return config


def validate_config(config: dict) -> None:
    """Raises ValueError with every problem found (not just the first),
    at load time rather than letting a typo surface as a bare
    KeyError/AttributeError deep inside component_price_at at publish
    time - see MQTT_TOPICS.md's "Failure behavior" section on how
    unrecoverable that is for a retained price topic."""
    errors = []
    components = config.get('components')
    if not components:
        raise ValueError("tariff config has no 'components' map")
    for name, component in components.items():
        prices = component.get('prices', {})
        default_price = component.get('default_price')
        if default_price is None:
            errors.append(f"component '{name}': missing required 'default_price'")
        elif default_price not in prices:
            errors.append(f"component '{name}': default_price '{default_price}' is not in 'prices'")
        season_boundaries = component.get('season_boundaries')
        bands = component.get('bands', {})
        for season_name, season_bands in bands.items():
            if season_name != 'default' and (not season_boundaries or season_name not in season_boundaries):
                errors.append(f"component '{name}': bands has a '{season_name}' season with no matching "
                              f"entry in season_boundaries")
            for band in season_bands:
                price_name = band.get('price')
                if price_name is None:
                    errors.append(f"component '{name}': a band in '{season_name}' is missing 'price'")
                elif price_name not in prices:
                    errors.append(f"component '{name}': band price '{price_name}' (in '{season_name}') "
                                  f"is not in 'prices'")
                start, end = band.get('start'), band.get('end')
                if (start is None) != (end is None):
                    errors.append(f"component '{name}': a band in '{season_name}' has 'start' without "
                                  f"'end' (or vice versa) - specify both or neither")
                days = band.get('days')
                if days is not None:
                    for token in days.split(','):
                        token = token.strip()
                        if token in ('Work', 'Holiday'):
                            continue
                        if '-' in token:
                            start_abbr, end_abbr = token.split('-', 1)
                            bad = [a for a in (start_abbr, end_abbr) if a not in _WEEKDAY_ABBREVIATIONS]
                        else:
                            bad = [token] if token not in _WEEKDAY_ABBREVIATIONS else []
                        if bad:
                            errors.append(f"component '{name}': band in '{season_name}' has an unrecognized "
                                          f"day token {bad!r} in 'days: {days}'")
        if season_boundaries:
            for season_name, boundary in season_boundaries.items():
                if 'start' not in boundary or 'end' not in boundary:
                    errors.append(f"component '{name}': season '{season_name}' needs both 'start' and 'end'")
    if errors:
        raise ValueError("Invalid tariff config:\n" + "\n".join(f"  - {e}" for e in errors))


def bands_for_day(config: dict, day: date, tz: ZoneInfo) -> list:
    """Builds one {"from", "to", "value"} entry per contiguous interval of
    constant price across `day` (local `tz`), evaluated at 1-minute
    resolution and merged - fine-grained enough that no real tariff's
    band boundaries fall between samples, coarse enough to be cheap for a
    single day. Adjacent minutes with the same price merge into one
    entry (see PR #35's "No forced 15-minute slicing" discussion).
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
