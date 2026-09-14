"""
solcast.py
Fetches PV production forecasts from Solcast's free Hobbyist API
(https://docs.solcast.com.au/). Free tier: 10 calls/day, max 2 rooftop
sites, no historical data - see
docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md.
"""
import os
from datetime import datetime, timedelta
from typing import Dict

import requests

SOLCAST_API_BASE = 'https://api.solcast.com.au'

# Solcast's rooftop_sites/forecasts pv_estimate*/pv_estimate10/pv_estimate90
# fields are documented as average power (kW) over the period, not energy
# (kWh) - this converts a period's kW estimate to that period's kWh
# (kW * period-hours). NOT yet verified against a real account's actual
# response (this repo has no Solcast key checked in). If a real response
# turns out to already be in kWh, set this to 1.0 - same "one constant to
# fix" precedent as forecast.py's HOURLY_VALUE_TO_KWH for the analogous
# Meteosource uncertainty.
PERIOD_KW_TO_KWH = 0.5  # 30-minute period = 0.5 hours


def fetch_solcast_forecast_30min(resource_id: str) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Calls GET /rooftop_sites/{resource_id}/forecasts at Solcast's native
    30-minute period. Returns {"YYYY-MM-DD": {"HH:MM": {"c10": kwh, "c50":
    kwh, "c90": kwh}}} covering whatever calendar dates Solcast's rolling,
    forward-looking-from-call-time forecast returned - there is no
    date/count guarantee here, callers just use whatever comes back.
    HH:MM is each period's *start* time, in local time (period_end from the
    API, a real UTC ISO8601 timestamp - unlike Meteosource's quirky epoch
    field - minus 30 minutes, converted via datetime.astimezone()).
    """
    api_key = os.environ.get('SOLCAST_API_KEY')
    assert api_key, "SOLCAST_API_KEY environment variable not set"
    response = requests.get(
        f"{SOLCAST_API_BASE}/rooftop_sites/{resource_id}/forecasts",
        params={'format': 'json', 'period': 'PT30M', 'api_key': api_key},
    )
    response.raise_for_status()
    data = response.json()

    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    for period in data.get('forecasts', []):
        # Solcast's period_end carries 7 fractional-second digits
        # ("2026-09-14T12:00:00.0000000Z"), which Python 3.9's
        # datetime.fromisoformat (limited to 0, 3, or 6 digits) rejects -
        # strip the fractional part entirely since sub-second precision
        # is irrelevant at 30-minute resolution.
        period_end_str = period['period_end'].split('.')[0] + '+00:00'
        period_end_utc = datetime.fromisoformat(period_end_str)
        period_start_local = (period_end_utc - timedelta(minutes=30)).astimezone()
        date_str = period_start_local.strftime('%Y-%m-%d')
        hhmm = period_start_local.strftime('%H:%M')
        result.setdefault(date_str, {})[hhmm] = {
            'c10': round(period['pv_estimate10'] * PERIOD_KW_TO_KWH, 2),
            'c50': round(period['pv_estimate'] * PERIOD_KW_TO_KWH, 2),
            'c90': round(period['pv_estimate90'] * PERIOD_KW_TO_KWH, 2),
        }
    return result


def sum_sites(*site_forecasts: Dict[str, Dict[str, Dict[str, float]]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Sums 2+ fetch_solcast_forecast_30min() results (east+west) into one
    combined forecast, same nested shape. A date/period present in only
    some sites sums whichever sites have it, treating a missing site's
    period as 0kWh - mirrors forecast.py's own missing-orientation handling."""
    combined: Dict[str, Dict[str, Dict[str, float]]] = {}
    for site in site_forecasts:
        for date_str, periods in site.items():
            date_bucket = combined.setdefault(date_str, {})
            for hhmm, values in periods.items():
                period_bucket = date_bucket.setdefault(hhmm, {'c10': 0.0, 'c50': 0.0, 'c90': 0.0})
                for key in ('c10', 'c50', 'c90'):
                    period_bucket[key] = round(period_bucket[key] + values[key], 2)
    return combined
