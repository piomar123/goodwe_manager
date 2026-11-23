"""
Fills daily_settlement (see settlement.py) for every day with hourly_summary
data, or a --from/--to range. Fetches missing RCE days from PSE into
rce_prices.db (cached, so a rerun makes no network calls).

    python _backfill_daily_settlement.py [--from 2024-08-12] [--to 2026-11-23]
"""
import argparse
import sqlite3
import time
from datetime import date, datetime, timedelta

import dotenv

import rce
import rce_storage
import settlement
import storage


def main():
    dotenv.load_dotenv()
    parser = argparse.ArgumentParser(description="Back-fill daily_settlement")
    parser.add_argument('--db-path', default=storage.DATA_DB_PATH)
    parser.add_argument('--from', dest='start', type=date.fromisoformat)
    parser.add_argument('--to', dest='end', type=date.fromisoformat)
    args = parser.parse_args()

    tariffs = settlement.tariff_history_from_env()
    if tariffs is None:
        raise SystemExit("Set TARIFF_HISTORY_CONFIG or TARIFF_IMPORT_CONFIG")
    factor = settlement.pv_extra_load_factor_from_env()

    conn = sqlite3.connect(args.db_path)
    settlement.init_table(conn)
    first, last = conn.execute("SELECT MIN(hour_start), MAX(hour_start) FROM hourly_summary").fetchone()
    start = args.start or datetime.fromtimestamp(first, settlement.LOCAL_TZ).date()
    end = args.end or datetime.fromtimestamp(last, settlement.LOCAL_TZ).date()

    rce_conn = rce_storage.init_db()

    def fetch(day):
        cached = rce_storage.is_cached(rce_conn, day.isoformat())
        prices = rce.get_rce_15min(day)
        if not cached:
            time.sleep(0.3)  # be gentle with PSE's API
        return prices

    day = start
    while day <= end:
        totals = settlement.update_day(conn, day, tariffs, fetch, factor)
        if totals:
            print(f"{day} hours={totals['hours']:2d} net_import={totals['net_import_kwh']:6.2f} kWh "
                  f"{totals['net_import_pln']:7.2f} zl  net_export={totals['net_export_kwh']:6.2f} kWh "
                  f"{totals['export_credit_pln']:6.2f} zl  savings={totals['savings_pln']:7.2f} zl"
                  + (f"  [no RCE for {totals['export_price_missing_hours']} h]" if totals['export_price_missing_hours'] else ''))
        day += timedelta(days=1)
    rce_conn.close()
    conn.close()


if __name__ == '__main__':
    main()
