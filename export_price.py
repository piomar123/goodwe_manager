"""
export_price.py
Converts cached RCE 15-minute prices (rce_storage.py) into the
{"raw_today", "raw_tomorrow"} band-list shape Predbat's generic
metric_octopus_export mechanism expects, applying the prosument export
VAT bonus. See PR #35.
"""
from datetime import date, datetime, timedelta
from typing import List, Tuple
from zoneinfo import ZoneInfo

import rce_storage

EXPORT_VAT_BONUS_MULTIPLIER = 1.23


def _export_value(rce_pln: float, negative_prices: str) -> float:
    """Converts a PLN/MWh RCE price into the zl/kWh export value Predbat
    publishes. `negative_prices` is 'zero' (default - net-billing pays
    nothing for a negative-price period, so publish 0.0) or 'raw'
    (publish the true negative value). The VAT bonus multiplier only
    applies to non-negative prices: the law doesn't yet define a bonus on
    a value that isn't income, so a negative price is published as
    rce_pln/1000 with no multiplier rather than making the loss look
    23% larger."""
    if rce_pln < 0:
        return rce_pln / 1000.0 if negative_prices == 'raw' else 0.0
    return rce_pln / 1000.0 * EXPORT_VAT_BONUS_MULTIPLIER


def _hour_key(period: str) -> str:
    """Groups a 'HH:MM' (or DST fall-back 'HHa:MM') period string by its
    hour, keeping the 'a' suffix so the disambiguated fall-back hour
    still forms its own group rather than merging with the first
    (fold=0) occurrence of the same wall-clock hour."""
    hh, _, _ = period.partition(':')
    return hh


def _average_by_hour(periods: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
    """Collapses 15-minute (period, rce_pln) rows into one row per hour,
    keyed by the hour's first period string (e.g. '13:00') so downstream
    band-building can treat it exactly like a single period - PGE settles
    export credit on the hourly average of the four quarters, not each
    quarter individually. The trailing '24:00' sentinel (rce.py's
    convert_to_series_15min appends it as a same-value boundary marker,
    not a real price) is passed through unaveraged so it still closes out
    the final hourly band."""
    real_periods = [p for p in periods if _hour_key(p[0]) != '24']
    sentinel = [p for p in periods if _hour_key(p[0]) == '24']

    sums: dict = {}
    counts: dict = {}
    first_period: dict = {}
    for period, rce_pln in real_periods:
        key = _hour_key(period)
        sums[key] = sums.get(key, 0.0) + rce_pln
        counts[key] = counts.get(key, 0) + 1
        first_period.setdefault(key, period)
    hourly = [
        (first_period[key], sums[key] / counts[key])
        for key in sorted(sums, key=lambda k: first_period[k])
    ]
    return hourly + sentinel


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


def _bands_for_business_date(
    business_date_str: str, tz: ZoneInfo, granularity: str = '15min', negative_prices: str = 'zero',
) -> List[dict]:
    conn = rce_storage.init_db()
    try:
        if not rce_storage.is_cached(conn, business_date_str):
            return []
        periods = rce_storage.get_cached_prices(conn, business_date_str)
    finally:
        conn.close()

    if granularity == 'hourly':
        periods = _average_by_hour(periods)

    day = datetime.strptime(business_date_str, '%Y-%m-%d').date()
    bands = []
    for (period, rce_pln), (next_period, _) in zip(periods, periods[1:]):
        start = _parse_period_start(period, day, tz)
        end = _parse_period_start(next_period, day, tz)
        value = _export_value(rce_pln, negative_prices)
        bands.append({'from': start.isoformat(), 'to': end.isoformat(), 'value': value})
    return bands


def build_export_price_payload(
    today: date, tz: ZoneInfo, granularity: str = '15min', negative_prices: str = 'zero',
) -> dict:
    """Returns {"raw_today": [...], "raw_tomorrow": [...]}. raw_tomorrow
    is an empty list (not an error) when tomorrow's RCE prices aren't
    cached yet - see spec Component 3's "Tomorrow not cached yet".

    `granularity` is '15min' (default, matches the RCE market's own
    settlement period) or 'hourly' (averages each hour's four quarters
    before pricing, matching how PGE settles export credit for
    prosumers on an hourly basis).

    `negative_prices` is 'zero' (default - a negative RCE price
    publishes as 0.0, matching net-billing paying nothing for it) or
    'raw' (publishes the true negative value, with no VAT bonus applied
    since the law doesn't define a bonus on a loss)."""
    tomorrow = today + timedelta(days=1)
    return {
        'raw_today': _bands_for_business_date(today.strftime('%Y-%m-%d'), tz, granularity, negative_prices),
        'raw_tomorrow': _bands_for_business_date(tomorrow.strftime('%Y-%m-%d'), tz, granularity, negative_prices),
    }
