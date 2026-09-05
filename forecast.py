"""
fetch https://solar.meteosource.com/
parse it using bs4
extract a <script> tag with a data JSON variable
"""
import asyncio
import json
import os
import sys
from datetime import datetime

import dotenv
import requests
from bs4 import BeautifulSoup

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from rce import parse_date


# The scraped "value" field per hourly entry is passed through unchanged and
# labelled "kWh" everywhere in this app (see forecast.html / main.py), matching
# the pre-existing behavior of fetch_pv_production_forecast_kwh's daily sum.
# ASSUMPTION: nobody has actually confirmed the real unit meteosource.com
# returns (it may well be Wh, not kWh). If that turns out wrong, fix it in one
# place: this constant.
HOURLY_VALUE_TO_KWH = 1


def _fetch_pv_production_forecast_raw(date, orientation):
    """Scrape meteosource.com and return its raw per-hour entries: list of
    {"date": epoch_millis, "value": <see HOURLY_VALUE_TO_KWH>}. Despite being
    millisecond epoch, "date" is *not* true UTC - see
    _fetch_pv_production_forecast_local_day_raw for why, and how it must be
    read."""
    assert os.environ.get('PV_LAT'), "PV_LAT environment variable not set"
    assert os.environ.get('PV_LON'), "PV_LON environment variable not set"
    assert os.environ.get('PV_POWER'), "PV_POWER environment variable not set"
    assert os.environ.get('PV_TILT'), "PV_TILT environment variable not set"
    url = (f"https://solar.meteosource.com/?date={date}"
           f"&lat={os.environ.get('PV_LAT')}"
           f"&lon={os.environ.get('PV_LON')}"
           f"&modulePower={os.environ.get('PV_POWER')}"
           f"&orientation={orientation}"
           f"&tilt={os.environ.get('PV_TILT')}"
           f"&geolocate=0")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    found = soup.find_all('script')
    potential_scripts = [s for s in found if s.string and 'var data = ' in s.string]
    if len(potential_scripts) != 1:
        raise ValueError(f"Couldn't not find a single script with 'var data = ' in it. Found {len(potential_scripts)}")
    script = potential_scripts[0]
    return json.loads(script.string.split('var data = ')[1])


def _fetch_pv_production_forecast_local_day_raw(date, orientation):
    """meteosource.com's per-hour `date` field is millisecond epoch, which
    would normally mean true UTC - but it isn't: reading it with UTC methods
    (datetime.utcfromtimestamp, *not* datetime.fromtimestamp) gives the
    correct *local* wall-clock hour for the requested lat/lon directly, no
    further offset needed.

    This was verified (twice - see git history for the first, wrong
    diagnosis) against real measured production: applying the system's real
    UTC offset on top made every day's forecast curve turn up ~2h later than
    real production's onset/peak/offset; reading the epoch's UTC calendar
    fields as already-local matched real production almost exactly. A
    single date=YYYY-MM-DD fetch already covers exactly that local calendar
    day (hours 00-23, same date) under this reading - so no day-boundary
    juggling is needed here, just filtering out whatever stray hours (if
    any) don't land on the requested date.
    """
    day = datetime.strptime(date, '%Y-%m-%d').date()
    raw = _fetch_pv_production_forecast_raw(date, orientation)
    return sorted(
        (entry for entry in raw if datetime.utcfromtimestamp(entry['date'] / 1000).date() == day),
        key=lambda entry: entry['date'],
    )


def fetch_pv_production_forecast_kwh(date, orientation):
    data = _fetch_pv_production_forecast_local_day_raw(date, orientation)
    return sum(entry['value'] for entry in data) * HOURLY_VALUE_TO_KWH


def fetch_pv_production_forecast_hourly_kwh(date, orientation):
    """Same source as fetch_pv_production_forecast_kwh, but broken down per
    hour instead of summed. Returns a list of (epoch_millis, kwh) tuples,
    ascending by time, covering the local calendar day of `date`. Callers
    MUST turn epoch_millis into an hour label via datetime.utcfromtimestamp,
    not datetime.fromtimestamp - see _fetch_pv_production_forecast_local_day_raw."""
    data = _fetch_pv_production_forecast_local_day_raw(date, orientation)
    return [(entry['date'], entry['value'] * HOURLY_VALUE_TO_KWH) for entry in data]


async def main():
    date_in = await asyncio.to_thread(input, "Date (or [t]oday, [y]esterday, [n]tomorrow): ")
    date = parse_date(date_in)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    print(f"Fetching forecast for {date_yyyymmdd}")

    total_kwh = 0
    orientations = (90, 270)
    forecasts = await asyncio.gather(*[asyncio.to_thread(fetch_pv_production_forecast_kwh, date_yyyymmdd, orientation) for orientation in orientations])
    for orientation, forecast in zip(orientations, forecasts):
        total_kwh += forecast
        print(f"{orientation}°: {forecast:.1f} kWh")
    print(f"Total: {total_kwh:.1f} kWh")


if __name__ == '__main__':
    dotenv.load_dotenv()
    asyncio.run(main())
