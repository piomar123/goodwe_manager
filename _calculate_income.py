"""
Calculate profit from PV production for a given date using CSV data
"""
from pprint import pprint
from typing import Optional

from rce import query_pse_rce, parse_date
import argparse
import csv
import bisect
from pathlib import Path

IMPORT_PRICE_KWH = 1.1


def main():
    parser = argparse.ArgumentParser(description="Calculate income from PV production for a given date")
    parser.add_argument("--date", help="Date for which to calculate the income (YYYY-MM-DD or DD.MM.YYYY)", type=str, required=True)
    parser.add_argument("--csv-dir", help="Directory with CSVs with inverter runtime data", type=str, required=True)
    args = parser.parse_args()
    print(vars(args))
    # walk through all csv files in dir
    parsed_date = parse_date(args.date)
    print("Querying PSE...")
    rce = query_pse_rce(parsed_date)

    csv_files = Path(args.csv_dir).glob('data-*.csv')
    lookup_date = parsed_date.strftime('%Y-%m-%d')
    found_hour_rows = []
    hour_to_lookup = 0
    sorted_csv_paths = list(sorted(csv_files))
    sorted_csv_file_names = [p.name for p in sorted_csv_paths]
    start_index = max(0, bisect.bisect_left(sorted_csv_file_names, f'data-{lookup_date}_00-00-00.csv') - 1)
    print(f"Starting from file {sorted_csv_file_names[start_index]}")
    for csv_file in sorted_csv_paths[start_index:]:
        with open(csv_file) as f:
            csv_reader = csv.DictReader(f)
            if csv_reader.fieldnames is None:
                print(f"Skipping {csv_file} - empty file")
                continue
            header = set(csv_reader.fieldnames)
            if not all(col in header for col in ('timestamp', 'meter_e_total_exp', 'meter_e_total_imp', 'e_load_total')):
                print(f"Skipping {csv_file} - missing required columns")
                continue
            print(f"Processing {csv_file}")
            rows, hour_to_lookup = find_hours(csv_reader, lookup_date, start=hour_to_lookup)
            found_hour_rows.extend(rows)
            if hour_to_lookup is None:
                break
    else:
        print(f"Couldn't find all 24 hours of data for {lookup_date}. Calculating partial income.")

    # print("\n".join(str(r.items()) for r in found_hour_rows))

    total_meter_only_pln = 0
    total_gain_pln = 0
    total_export_kwh = 0
    total_import_khw = 0
    total_load_kwh = 0
    for hour in range(len(found_hour_rows) - 1):
        rce_lookup_time = f'{hour:02}:00'
        rce_hour_price = rce[hour]
        if rce_hour_price[0] != rce_lookup_time:
            raise ValueError(f"RCE time mismatch for {rce_lookup_time}, found: '{rce_hour_price[0]}' instead")
        # print(f"{found_hour_rows[hour]['meter_e_total_exp']}, "
        #       f"{found_hour_rows[hour + 1]['meter_e_total_exp']}, "
        #       f"{found_hour_rows[hour]['meter_e_total_imp']}, "
        #       f"{found_hour_rows[hour + 1]['meter_e_total_imp']}")
        hourly_export = calc_diff(found_hour_rows, hour, 'meter_e_total_exp')
        total_export_kwh += hourly_export
        hourly_import = calc_diff(found_hour_rows, hour, 'meter_e_total_imp')
        total_import_khw += hourly_import
        load_kwh = calc_diff(found_hour_rows, hour, 'e_load_total')
        total_load_kwh += load_kwh
        balance_kwh = hourly_export - hourly_import
        rce_price_kwh = rce_hour_price[1] / 1000.
        no_buy_pln = load_kwh * IMPORT_PRICE_KWH
        if balance_kwh > 0:
            meter_pln = balance_kwh * rce_price_kwh
        else:
            meter_pln = balance_kwh * IMPORT_PRICE_KWH
        gain_pln = meter_pln + no_buy_pln
        print(f"{rce_lookup_time}: gain: {gain_pln:.2f} zł ({meter_pln:.2f} + {no_buy_pln:.2f}), "
              f"meter: +{hourly_export:.2f} -{hourly_import:.2f} = {balance_kwh:.2f} kWh, "
              f"load: {load_kwh:.1f} kWh, "
              f"RCE: {rce_price_kwh :.4f} zł/kWh")
        total_meter_only_pln += meter_pln
        total_gain_pln += gain_pln
    if len(found_hour_rows) < 25:
        print(f"[!] Couldn't find all 24 hours of data for {lookup_date}. Calculating partial income.")
    print(f"Total gain with self-consumption: {total_gain_pln:.2f} zł (meter balance only: {total_meter_only_pln:.2f} zł)")
    print(f"Total meter: +{total_export_kwh:.2f} -{total_import_khw:.2f} = {total_export_kwh - total_import_khw:.2f} kWh")
    print(f"Total load: {total_load_kwh:.1f} kWh")


def find_hours(csv_reader, lookup_date, start: int = 0) -> tuple[list[dict], Optional[int]]:
    found_hour_rows = []
    date_found = False
    hour = start
    for hour in range(start, 24):
        lookup_time = f' {hour:02}:0'
        while True:
            try:
                row = next(csv_reader)
                timestamp = row['timestamp']  # "2024-08-12 09:02:40"
                already_past_lookup_date = date_found and lookup_date not in timestamp
                if already_past_lookup_date:
                    # we assume if the date changed, further timestamps won't be found anywhere
                    print(f"[!] Couldn't find the time '{lookup_time}' in the CSV file, date changed.")
                    return found_hour_rows, None
                elif lookup_date in timestamp:
                    date_found = True
                    if lookup_time in timestamp:
                        found_hour_rows.append(row)
                        break  # next hour
            except StopIteration:
                print(f"CSV file finished without lookup time '{lookup_time}'")
                return found_hour_rows, hour

    # FIXME: szpachla alert, adding last entry from current file for the given date
    assert hour == 23
    try:
        while lookup_date in (row := next(csv_reader))['timestamp']:
            continue
        found_hour_rows.append(row)
    except StopIteration:
        pass
    return found_hour_rows, None


def calc_diff(found_hour_rows, hour, entry):
    return float(found_hour_rows[hour + 1][entry]) - float(found_hour_rows[hour][entry])


if __name__ == '__main__':
    main()
