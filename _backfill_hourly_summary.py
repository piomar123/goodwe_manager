"""
One-off/manual runner for storage.backfill_hourly_summary(). The live
main.py process already calls this automatically on every hour rollover
(see AsyncioThread._backfill_hourly_summary), so this script only exists for
cases the live watermark-bounded scan can't reach on its own:
- backdated data imported after the live watermark already advanced past it
  (e.g. restoring an archival CSV backup, or importing a corrected batch)
- a hourly_summary row manually deleted to force a recompute (e.g. after an
  aggregation-logic change) without also clearing every row after it

Usage: python _backfill_hourly_summary.py [--full-rescan] [--db-path PATH]
"""
import argparse
import logging
import sqlite3

import storage

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill hourly_summary from inverter_history")
    parser.add_argument("--full-rescan", action="store_true",
                         help="Ignore the watermark and scan the whole table - use this for a gap "
                              "that sits behind an hour already in hourly_summary (see module docstring)")
    parser.add_argument("--db-path", default=storage.DATA_DB_PATH, help="Path to the SQLite database")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    conn = sqlite3.connect(args.db_path)
    try:
        backfilled = storage.backfill_hourly_summary(conn, full_rescan=args.full_rescan)
    finally:
        conn.close()
    print(f"Backfilled {backfilled} hour(s){' (full rescan)' if args.full_rescan else ''}")


if __name__ == '__main__':
    main()
