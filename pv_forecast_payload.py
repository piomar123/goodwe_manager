"""
pv_forecast_payload.py
Converts goodwe_manager's stored {"HH:MM": {"c10", "c50", "c90"}} solar
forecast shape (forecast_history.get_latest_merged's return value) into
Predbat's expected detailedForecast list shape - see MQTT_TOPICS.md's
`forecast/pv` section, and Predbat's own templates/huawei.yaml
(pv_forecast_today/pv_forecast_tomorrow + *_attribute: detailedForecast).
"""
from datetime import date, datetime
from typing import Dict, List
from zoneinfo import ZoneInfo


def build_detailed_forecast(periods: Dict[str, Dict[str, float]], day: date, tz: ZoneInfo) -> List[dict]:
    """periods: {"HH:MM": {"c10": kwh, "c50": kwh, "c90": kwh}}, as returned
    by forecast_history.get_latest_merged for a single date. Returns a
    list of {"period_start", "pv_estimate", "pv_estimate10", "pv_estimate90"}
    entries, one per period, sorted by time, with tz-aware ISO8601
    period_start timestamps - the shape Predbat's detailedForecast
    attribute expects. Returns [] for an empty/missing periods dict
    (e.g. tomorrow's forecast not fetched yet), not an error.
    """
    entries = []
    for hhmm in sorted(periods.keys()):
        hour, minute = (int(p) for p in hhmm.split(':'))
        period_start = datetime(day.year, day.month, day.day, hour, minute, tzinfo=tz)
        values = periods[hhmm]
        entries.append({
            'period_start': period_start.isoformat(),
            'pv_estimate': values['c50'],
            'pv_estimate10': values['c10'],
            'pv_estimate90': values['c90'],
        })
    return entries
