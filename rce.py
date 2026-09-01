import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pprint import pprint
from typing import Pattern

import matplotlib.pyplot as plt
import numpy as np
import requests
from matplotlib import rcParams
from matplotlib.figure import Figure
from matplotlib.ticker import FixedLocator

import rce_storage

ACCEPT_JSON_HEADER = {'Accept': 'application/json'}


@dataclass
class DatePattern:
    pattern: Pattern
    date_fmt: str


DATE_PATTERNS = [
    DatePattern(re.compile(r'\d{4}-\d?\d-\d?\d'), '%Y-%m-%d'),
    DatePattern(re.compile(r'\d?\d\.\d?\d\.\d{4}'), '%d.%m.%Y'),
]


def parse_date(date_str: str):
    if date_str == 't':
        return datetime.today()
    if date_str == 'n':
        return datetime.today() + timedelta(days=1)
    if date_str == 'y':
        return datetime.today() - timedelta(days=1)
    for pattern in DATE_PATTERNS:
        if pattern.pattern.match(date_str):
            return datetime.strptime(date_str, pattern.date_fmt).date()
    raise RuntimeError(f"Unknown date format: {date_str}")


def query_pse_rce_15min(query_date: datetime.date) -> list[tuple[str, float]]:
    """
    Returns e.g. [('00:00', 511.42), ('01:00', 500.12), ..., ('23:00', 432.12), ('24:00', 432.12)]
    Example response from API query:
        {"value": [
            {
                "dtime": "2024-06-14 00:15:00",
                "period": "00:00 - 00:15",
                "rce_pln": 876.1,
                "dtime_utc": "2024-06-13 22:15:00",
                "period_utc": "22:00 - 22:15",
                "business_date": "2024-06-14",
                "publication_ts": "2024-06-13 17:05:05",
                "publication_ts_utc": "2024-06-13 15:05:05.000000"
            },
    """
    date_yyyymmdd = query_date.strftime('%Y-%m-%d')
    response = requests.get(f'https://api.raporty.pse.pl/api/rce-pln?$select=period,rce_pln&$filter=business_date%20eq%20%27{date_yyyymmdd}%27',
                            headers=ACCEPT_JSON_HEADER)
    response.raise_for_status()
    data = response.json()
    if not data['value']:
        raise RuntimeError(f"No data found for {date_yyyymmdd}")
    return convert_to_series_15min(data)


def get_rce_15min(query_date: datetime.date) -> list[tuple[str, float]]:
    """Single read/write-through entry point for 15-minute RCE prices -
    every other direct call to query_pse_rce_15min in this codebase has
    been replaced with this function, so there's exactly one path into the
    cache.

    Cache hit (a marker row exists in rce_prices_fetched for this date):
    reads rce_prices.db, no network call. Cache miss: calls the live
    query_pse_rce_15min(), then INSERT OR REPLACEs the result into
    rce_prices.db (both the price rows and the completeness marker)
    before returning - so any live fetch, from whichever caller triggered
    it, gets cached.
    """
    business_date = query_date.strftime('%Y-%m-%d')
    conn = rce_storage.init_db()
    try:
        if rce_storage.is_cached(conn, business_date):
            return rce_storage.get_cached_prices(conn, business_date)
        series = query_pse_rce_15min(query_date)
        rce_storage.store_prices(conn, business_date, series)
        return series
    finally:
        conn.close()


def query_pse_rce(query_date: datetime.date) -> list[tuple[str, float]]:
    """
    Returns hourly RCE prices. To get 15-minute intervals use get_rce_15min().
    """
    rce_15min = get_rce_15min(query_date)
    # noinspection PyTypeChecker
    s = np.array_split(rce_15min, range(4, len(rce_15min), 4))
    return [(chunk[0][0].item(), np.mean([float(price) for _, price in chunk]).item()) for chunk in s]


def setup_plot_style():
    rcParams['font.family'] = 'sans-serif'
    rcParams['font.sans-serif'] = ['DejaVu Sans', 'Liberation Sans', 'Arial']
    rcParams['font.size'] = 11
    plt.ioff()
    # plt.style.use('default')
    plt.style.use('dark_background')


def main():
    setup_plot_style()
    parser = argparse.ArgumentParser(description="Find RCE electricity prices")
    parser.add_argument("--date", help="Date for which to find the prices", type=str, default=None)
    args = parser.parse_args()
    print(vars(args))

    date_in = args.date or input("Date (or [t]oday, [y]esterday, [n]tomorrow): ")
    date = parse_date(date_in)
    rce = get_rce_15min(date)
    print(rce)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    fig = plot_rce(rce, date_yyyymmdd)
    plt.show()


def convert_to_series_15min(response_data):
    # Split on the ' - ' delimiter rather than slicing entry['period'][0:5]:
    # on a DST fall-back day PSE disambiguates the repeated hour with an
    # 'a' suffix (e.g. period "02a:15 - 02a:30"), which is 6 characters
    # long, not the usual 5 ("00:00 - 00:15") - a fixed [0:5] slice would
    # truncate "02a:15" down to "02a:1".
    series = [(entry['period'].split(' - ')[0], entry['rce_pln']) for entry in response_data['value']]
    series.append(('24:00', series[-1][1]))
    return series


def convert_to_series(response_data):
    """
    Use convert_to_series_15min instead
    """
    return convert_to_series_15min(response_data)[::4]


def plot_rce(series, date) -> Figure:
    fig = plt.figure(f"RCE {date}", figsize=(14, 8))
    axes = fig.gca()
    axes.plot([row[0] for row in series], [row[1] for row in series], '-', drawstyle='steps-post', label=f"RCE")
    # series has one point per quarter-hour plus the appended '24:00' -
    # normally 24*4+1 = 97, but a DST fall-back day has 100 periods (101
    # points) and a spring-forward day has 92 (93 points). Matplotlib
    # plots the string x-values on a categorical axis (each label gets
    # the next sequential integer position), so hardcoding xlim/ticks to
    # 24*4 would clip the last ~hour of a fall-back day's chart off the
    # visible axes and leave a dead stretch on a spring-forward day's.
    last_index = len(series) - 1
    axes.set_xlim(0, last_index)
    min_price = min(row[1] for row in series)
    axes.set_ylim(min(min_price, 0), None)
    # plt.legend()
    if date == datetime.today().strftime('%Y-%m-%d'):
        now = datetime.now().time()
        hour_float = now.hour + now.minute / 60.
        # Note: this still assumes position == hour*4, which is only true
        # up to the DST transition point on a fall-back/spring-forward
        # day - the 'now' marker can be off by about an hour for the
        # remainder of those two days per year.
        axes.axvline(x=hour_float*4, color='red')
    axes.axhline(y=0, color='gray', linestyle=':')
    axes.grid(color='gray', linestyle=':')
    axes.xaxis.set_major_locator(FixedLocator(list(range(0, last_index+1, 4))))
    fig.subplots_adjust(top=0.95, bottom=0.1, left=0.1, right=0.95)
    return fig


# currently unused
def query_meteo():
    """
    GET https://devmgramapi.meteo.pl/meteorograms/available
    find newest from response['um4_60']

    POST https://devmgramapi.meteo.pl/meteorograms/um4_60
    Accept: application/json

    {date: 1721952000, point: {lat: "51.60000", lon: "19.50000"}}
    """
    response = requests.get(f'https://devmgramapi.meteo.pl/meteorograms/available', headers=ACCEPT_JSON_HEADER)
    response.raise_for_status()
    meteorograms_available = response.json()
    newest_um4_60 = max(meteorograms_available['um4_60'])

    response = requests.post(f'https://devmgramapi.meteo.pl/meteorograms/um4_60',
                             headers=ACCEPT_JSON_HEADER,
                             json={'date': newest_um4_60, 'point': {'lat': '51.60000', 'lon': '19.50000'}})
    response.raise_for_status()
    meteorogram = response.json()
    pprint(meteorogram['data']['cldtot_aver'])


if __name__ == '__main__':
    # query_meteo()
    main()

# TODO:
# [ ] integrate with goodwe API
# [ ] set export limit to 0 for negative prices
# [ ] try to set battery discharging for highest prices
