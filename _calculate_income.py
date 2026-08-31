"""
Calculate profit from PV production for a given date, using hourly_summary
data in SQLite (data.db) instead of the legacy CSV files.
"""
import argparse
import sqlite3
from datetime import datetime

import storage
from rce import query_pse_rce, parse_date

IMPORT_PRICE_KWH = 1.1


def fetch_hourly_summary(conn: sqlite3.Connection, date: datetime) -> dict:
    """Returns {hour_of_day: (meter_export_kwh, meter_import_kwh, load_kwh)}
    for every hourly_summary row within the given local calendar date.
    `date` may be a datetime.date or datetime.datetime (rce.parse_date
    returns either depending on the input format).
    """
    day_start = datetime(date.year, date.month, date.day)
    day_start_epoch = int(day_start.timestamp())
    day_end_epoch = day_start_epoch + 24 * 3600
    rows = conn.execute(
        "SELECT hour_start, meter_export_kwh, meter_import_kwh, load_kwh FROM hourly_summary "
        "WHERE hour_start >= ? AND hour_start < ? ORDER BY hour_start",
        (day_start_epoch, day_end_epoch),
    ).fetchall()
    result = {}
    for hour_start, export_kwh, import_kwh, load_kwh in rows:
        hour_of_day = datetime.fromtimestamp(hour_start).hour
        result[hour_of_day] = (export_kwh, import_kwh, load_kwh)
    return result


def compute_hour_income(hourly_export: float, hourly_import: float, load_kwh: float,
                        rce_price_pln_per_mwh: float) -> dict:
    """Pure per-hour income calculation - same formula as before: a positive
    meter balance (net export) is valued at the RCE market price, a negative
    balance (net import) at the flat IMPORT_PRICE_KWH, and the load itself is
    separately valued at the flat import price to represent the cost avoided
    by self-consumption.
    """
    balance_kwh = hourly_export - hourly_import
    rce_price_kwh = rce_price_pln_per_mwh / 1000.
    no_buy_pln = load_kwh * IMPORT_PRICE_KWH
    if balance_kwh > 0:
        meter_pln = balance_kwh * rce_price_kwh
    else:
        meter_pln = balance_kwh * IMPORT_PRICE_KWH
    return {
        'balance_kwh': balance_kwh,
        'rce_price_kwh': rce_price_kwh,
        'no_buy_pln': no_buy_pln,
        'meter_pln': meter_pln,
        'gain_pln': meter_pln + no_buy_pln,
    }


def main():
    parser = argparse.ArgumentParser(description="Calculate income from PV production for a given date")
    parser.add_argument("--date", help="Date for which to calculate the income (YYYY-MM-DD or DD.MM.YYYY)", type=str, required=True)
    parser.add_argument("--db-path", help="Path to the SQLite database", type=str, default=storage.DATA_DB_PATH)
    args = parser.parse_args()
    print(vars(args))

    parsed_date = parse_date(args.date)
    lookup_date_str = parsed_date.strftime('%Y-%m-%d')
    print("Querying PSE...")
    rce = query_pse_rce(parsed_date)

    conn = sqlite3.connect(args.db_path)
    try:
        hourly_rows = fetch_hourly_summary(conn, parsed_date)
    finally:
        conn.close()

    total_meter_only_pln = 0.0
    total_gain_pln = 0.0
    total_export_kwh = 0.0
    total_import_kwh = 0.0
    total_load_kwh = 0.0
    calculated_hours = 0

    for hour in range(24):
        row = hourly_rows.get(hour)
        if row is None:
            print(f"{hour:02}:00: [!] no hourly_summary data, skipping")
            continue
        hourly_export, hourly_import, load_kwh = row
        if hourly_export is None or hourly_import is None or load_kwh is None:
            print(f"{hour:02}:00: [!] incomplete data (no prior baseline), skipping")
            continue

        rce_lookup_time = f'{hour:02}:00'
        rce_hour_price = rce[hour]
        if rce_hour_price[0] != rce_lookup_time:
            raise ValueError(f"RCE time mismatch for {rce_lookup_time}, found: '{rce_hour_price[0]}' instead")

        result = compute_hour_income(hourly_export, hourly_import, load_kwh, rce_hour_price[1])
        print(f"{rce_lookup_time}: gain: {result['gain_pln']:.2f} zł ({result['meter_pln']:.2f} + {result['no_buy_pln']:.2f}), "
              f"meter: +{hourly_export:.2f} -{hourly_import:.2f} = {result['balance_kwh']:.2f} kWh, "
              f"load: {load_kwh:.1f} kWh, "
              f"RCE: {result['rce_price_kwh']:.4f} zł/kWh")

        total_export_kwh += hourly_export
        total_import_kwh += hourly_import
        total_load_kwh += load_kwh
        total_meter_only_pln += result['meter_pln']
        total_gain_pln += result['gain_pln']
        calculated_hours += 1

    if calculated_hours < 24:
        print(f"[!] Only {calculated_hours}/24 hours had usable data for {lookup_date_str}. Calculating partial income.")
    print(f"Total gain with self-consumption: {total_gain_pln:.2f} zł (meter balance only: {total_meter_only_pln:.2f} zł)")
    print(f"Total meter: +{total_export_kwh:.2f} -{total_import_kwh:.2f} = {total_export_kwh - total_import_kwh:.2f} kWh")
    print(f"Total load: {total_load_kwh:.1f} kWh")


if __name__ == '__main__':
    main()
