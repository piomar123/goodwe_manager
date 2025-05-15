"""
fetch https://solar.meteosource.com/
parse it using bs4
extract a <script> tag with a data JSON variable
"""
import asyncio
import json
import os
import sys

import dotenv
import requests
from bs4 import BeautifulSoup

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from rce import parse_date


def fetch_pv_production_forecast_kwh(date, orientation):
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
    data = json.loads(script.string.split('var data = ')[1])
    return sum(entry['value'] for entry in data)


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
