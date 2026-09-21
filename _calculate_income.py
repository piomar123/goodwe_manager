"""
Calculate profit from PV production for a given date, using hourly_summary
data in SQLite (data.db) instead of the legacy CSV files.
"""
import argparse
import os
import sqlite3
from datetime import datetime

import export_price
import storage
import tariff_engine
from rce import query_pse_rce, parse_date

IMPORT_PRICE_KWH = 1.1
TARIFF_IMPORT_CONFIG = os.environ.get('TARIFF_IMPORT_CONFIG')
DEFAULT_NEGATIVE_PRICES = os.environ.get('RCE_EXPORT_NEGATIVE_PRICES', 'zero')


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
                        rce_price_pln_per_mwh: float, import_price_kwh: float = IMPORT_PRICE_KWH,
                        negative_prices: str = DEFAULT_NEGATIVE_PRICES) -> dict:
    """Pure per-hour income calculation - a positive meter balance (net
    export) is valued at the RCE market export price, a negative balance
    (net import) at `import_price_kwh`, and the load itself is separately
    valued at `import_price_kwh` to represent the cost avoided by
    self-consumption. `import_price_kwh` defaults to the flat
    IMPORT_PRICE_KWH constant; callers with a tariff config pass the real
    per-hour zone-aware rate instead (see main()'s --tariff-config).

    The export leg is priced via export_price.export_value(), the same
    function the live MQTT bridge uses for Predbat's export price feed -
    this applies the prosument VAT bonus (x1.23) to a non-negative RCE
    price, and handles a negative RCE price per `negative_prices` ('zero',
    the default matching RCE_EXPORT_NEGATIVE_PRICES's own default - net
    billing pays nothing for it; or 'raw' - publish/value the true
    negative price, still with no VAT bonus). Without this, a flat
    rce_pln/1000 would understate the real export credit by ~23%.
    """
    balance_kwh = hourly_export - hourly_import
    export_price_kwh = export_price.export_value(rce_price_pln_per_mwh, negative_prices)
    no_buy_pln = load_kwh * import_price_kwh
    if balance_kwh > 0:
        meter_pln = balance_kwh * export_price_kwh
    else:
        meter_pln = balance_kwh * import_price_kwh
    return {
        'balance_kwh': balance_kwh,
        'rce_price_kwh': export_price_kwh,
        'no_buy_pln': no_buy_pln,
        'meter_pln': meter_pln,
        'gain_pln': meter_pln + no_buy_pln,
    }


def main():
    parser = argparse.ArgumentParser(description="Calculate income from PV production for a given date")
    parser.add_argument("--date", help="Date for which to calculate the income (YYYY-MM-DD or DD.MM.YYYY)", type=str, required=True)
    parser.add_argument("--db-path", help="Path to the SQLite database", type=str, default=storage.DATA_DB_PATH)
    parser.add_argument("--tariff-config", help="Path to a tariff_engine YAML config (defaults to TARIFF_IMPORT_CONFIG env var; omit both to use the flat IMPORT_PRICE_KWH)",
                        type=str, default=TARIFF_IMPORT_CONFIG)
    parser.add_argument("--negative-prices", help="How to value an hour with a negative RCE export price: "
                        "'zero' (default, matches RCE_EXPORT_NEGATIVE_PRICES's own default - net billing pays "
                        "nothing for it) or 'raw' (value the true negative price, still with no VAT bonus)",
                        type=str, choices=('zero', 'raw'), default=DEFAULT_NEGATIVE_PRICES)
    args = parser.parse_args()
    print(vars(args))

    parsed_date = parse_date(args.date)
    lookup_date_str = parsed_date.strftime('%Y-%m-%d')
    tariff_config = tariff_engine.load_config(args.tariff_config) if args.tariff_config else None
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

        if tariff_config is not None:
            hour_dt = datetime(parsed_date.year, parsed_date.month, parsed_date.day, hour)
            import_price_kwh = tariff_engine.price_at(tariff_config, hour_dt)
        else:
            import_price_kwh = IMPORT_PRICE_KWH

        result = compute_hour_income(hourly_export, hourly_import, load_kwh, rce_hour_price[1], import_price_kwh,
                                     args.negative_prices)
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
