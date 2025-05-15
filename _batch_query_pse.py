"""
Query RCE prices for a given range of dates
"""
import argparse
from datetime import timedelta

from rce import query_pse_rce, parse_date


def main():
    parser = argparse.ArgumentParser(description="Batch PSE RCE query")
    parser.add_argument("--start-date", help="Start date for batch query (YYYY-MM-DD or DD.MM.YYYY)", type=str, required=True)
    parser.add_argument("--end-date", help="End date for batch query (YYYY-MM-DD or DD.MM.YYYY)", type=str, required=True)
    args = parser.parse_args()
    print(vars(args))
    parsed_start_date = parse_date(args.start_date)
    parsed_end_date = parse_date(args.end_date)
    prices = {}
    for day in range((parsed_end_date - parsed_start_date).days + 1):
        query_date = parsed_start_date + timedelta(days=day)
        print(f"Querying PSE for {query_date}...")
        rce = query_pse_rce(query_date)
        prices[query_date] = rce

    for date, day_rce in prices.items():
        row_prices = '\t'.join(str(hour[1]) for hour in day_rce[:-1])
        print(f"{date}\t{row_prices}")


if __name__ == '__main__':
    main()
