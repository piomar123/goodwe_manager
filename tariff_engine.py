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
    Matches if any token matches (OR semantics) - see PR #35.
    """
    return any(_token_matches(token, d) for token in day_spec.split(','))


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


def _band_matches(band: dict, dt: datetime) -> bool:
    days = band.get('days')
    if days is not None and not day_spec_matches(days, dt.date()):
        return False
    return time_spec_matches(band.get('start'), band.get('end'), dt.time())


def component_price_at(component: dict, dt: datetime) -> float:
    """Resolves one component's price at `dt`: that component's own season
    bands first (in file order), then its `bands.default` (in file
    order), then `default_price` - see PR #35's "Resolution order"
    discussion."""
    season = season_for_date(component['season_boundaries'], dt.date()) if component.get('season_boundaries') else None
    bands = component.get('bands', {})
    season_bands = bands.get(season, []) if season else []
    default_bands = bands.get('default', [])
    for band in (*season_bands, *default_bands):
        if _band_matches(band, dt):
            return component['prices'][band['price']]
    return component['prices'][component['default_price']]


def price_at(config: dict, dt: datetime) -> float:
    return sum(component_price_at(component, dt) for component in config['components'].values())


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


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
