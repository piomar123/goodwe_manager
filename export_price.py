"""
export_price.py
Converts cached RCE 15-minute prices (rce_storage.py) into the
{"raw_today", "raw_tomorrow"} band-list shape Predbat's generic
metric_octopus_export mechanism expects, applying the prosument export
VAT bonus. See PR #35.
"""
from datetime import date, datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

import rce_storage

EXPORT_VAT_BONUS_MULTIPLIER = 1.23


def _parse_period_start(period: str, day: date, tz: ZoneInfo) -> datetime:
    """period is 'HH:MM' (or the DST fall-back day's disambiguated
    'HHa:MM' form, per rce.py's convert_to_series_15min) - the 'a' suffix
    marks the second (post-fall-back, CET/+01:00) occurrence of an
    ambiguous local hour and is stripped for hour/minute parsing since it
    doesn't change the wall-clock hour, but its presence must still be
    recorded via `fold=1` so zoneinfo picks the correct UTC offset for
    that second occurrence instead of colliding with the first (fold=0,
    CEST/+02:00) occurrence's identical wall-clock time."""
    is_ambiguous_second_occurrence = 'a' in period
    hh_mm = period.replace('a', '')
    hour, minute = (int(p) for p in hh_mm.split(':'))
    if hour == 24:
        day, hour = day + timedelta(days=1), 0
    return datetime(
        day.year, day.month, day.day, hour, minute,
        tzinfo=tz, fold=1 if is_ambiguous_second_occurrence else 0,
    )


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
