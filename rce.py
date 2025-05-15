import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pprint import pprint
from typing import Pattern

import matplotlib.pyplot as plt
import requests
from matplotlib import rcParams
from matplotlib.figure import Figure

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


def query_pse_rce(query_date: datetime.date) -> list[tuple[str, float]]:
    """
    Returns e.g. [('00:00', 511.42), ('01:00', 500.12), ..., ('23:00', 432.12), ('24:00', 432.12)]
    """
    date_yyyymmdd = query_date.strftime('%Y-%m-%d')
    response = requests.get(f'https://api.raporty.pse.pl/api/rce-pln?$filter=doba%20eq%20%27{date_yyyymmdd}%27', headers=ACCEPT_JSON_HEADER)
    response.raise_for_status()
    data = response.json()
    if not data['value']:
        raise RuntimeError(f"No data found for {date_yyyymmdd}")
    return convert_to_series(data)
    # {'value': [{'doba': '2024-07-25', 'rce_pln': 511.42, 'udtczas': '2024-07-25 00:15',
    # 'udtczas_oreb': '00:00 - 00:15', 'business_date': '2024-07-25', 'source_datetime': '2024-07-24 14:57:15.417'}, {...


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
    rce = query_pse_rce(date)
    print(rce)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    fig = plot_rce(rce, date_yyyymmdd)
    plt.show()


def convert_to_series(response_data):
    series = [(entry['udtczas_oreb'][0:5], entry['rce_pln']) for entry in response_data['value']]
    series.append(('24:00', series[-1][1]))
    return series[::4]


def plot_rce(series, date) -> Figure:
    fig = plt.figure(f"RCE {date}", figsize=(14, 8))
    axes = fig.gca()
    axes.plot([row[0] for row in series], [row[1] for row in series], '-', drawstyle='steps-post', label=f"RCE")
    axes.set_xlim(0, 24)
    min_price = min(row[1] for row in series)
    axes.set_ylim(min(min_price, 0), None)
    # plt.legend()
    if date == datetime.today().strftime('%Y-%m-%d'):
        now = datetime.now().time()
        hour_float = now.hour + now.minute / 60.
        axes.axvline(x=hour_float, color='red')
    axes.axhline(y=0, color='gray', linestyle=':')
    axes.grid(color='gray', linestyle=':')
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
