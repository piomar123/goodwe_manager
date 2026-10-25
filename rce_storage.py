"""
SQLite cache for RCE 15-minute electricity prices (rce_prices.db).
"""
import sqlite3
import time
from typing import List, Optional, Tuple

RCE_DB_PATH = 'rce_prices.db'

# PRAGMA user_version: 1 = period labels derived from PSE's UTC fields
# (rce._period_start_label) instead of the start of PSE's `period` text.
SCHEMA_VERSION = 1


def init_db(path: Optional[str] = None) -> sqlite3.Connection:
    """Opens (creating if needed) the RCE price cache DB and ensures both
    tables exist. `path` defaults to the module-level RCE_DB_PATH, looked
    up here inside the function body (not as a bound default parameter
    value) so tests can monkeypatch rce_storage.RCE_DB_PATH and have
    default-path callers pick up the patched value.
    """
    conn = sqlite3.connect(path or RCE_DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rce_prices (
            business_date TEXT NOT NULL,
            period TEXT NOT NULL,
            rce_pln REAL NOT NULL,
            PRIMARY KEY (business_date, period)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rce_prices_fetched (
            business_date TEXT PRIMARY KEY,
            period_count INTEGER NOT NULL,
            fetched_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    if _schema_version(conn) < SCHEMA_VERSION:
        # Re-checked under the write lock: init_db runs on every read, from
        # several threads, and relabelling twice would shift labels again.
        conn.execute("BEGIN IMMEDIATE")
        try:
            if _schema_version(conn) < 1:
                _relabel_dst_days(conn)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    return conn


def _schema_version(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA user_version").fetchone()[0]


def _relabel_dst_days(conn: sqlite3.Connection) -> None:
    """Rewrites DST-day rows cached before labels came from the UTC
    fields. Done in place rather than by dropping the days for a
    re-fetch, because export_price only reads the cache: dropping today
    would leave Predbat without export prices until the next day.

    Polish DST always changes at 01:00 UTC, so the old labels differ in
    known places only. Fall-back day (has an 'a' label): '03:00' was the
    first quarter of the repeated hour (now '02a:00') and '03a:00' the
    03:00 CET quarter (now '03:00'). Spring-forward day (has '02:00'
    but no '03:00'): '02:00' was the 03:00 CEST quarter."""
    fall_back_days = [row[0] for row in conn.execute(
        "SELECT DISTINCT business_date FROM rce_prices WHERE period LIKE '%a%'")]
    for day in fall_back_days:
        conn.execute("UPDATE rce_prices SET period = '02a:00' WHERE business_date = ? AND period = '03:00'", (day,))
        conn.execute("UPDATE rce_prices SET period = '03:00' WHERE business_date = ? AND period = '03a:00'", (day,))
    conn.execute("""
        UPDATE rce_prices SET period = '03:00'
        WHERE period = '02:00' AND business_date NOT IN (
            SELECT business_date FROM rce_prices WHERE period = '03:00')
    """)


def is_cached(conn: sqlite3.Connection, business_date: str) -> bool:
    """A cache hit is "a marker row exists in rce_prices_fetched for this
    business_date" - deliberately NOT a row-count check against
    rce_prices, since Poland's DST transitions mean a valid day has 92 or
    100 periods instead of always 96.
    """
    row = conn.execute(
        "SELECT 1 FROM rce_prices_fetched WHERE business_date = ?",
        (business_date,),
    ).fetchone()
    return row is not None


def get_cached_prices(conn: sqlite3.Connection, business_date: str) -> List[Tuple[str, float]]:
    rows = conn.execute(
        "SELECT period, rce_pln FROM rce_prices WHERE business_date = ? ORDER BY period",
        (business_date,),
    ).fetchall()
    return [(period, rce_pln) for period, rce_pln in rows]


def store_prices(conn: sqlite3.Connection, business_date: str, series: List[Tuple[str, float]]) -> None:
    """Write-through cache store: INSERT OR REPLACE every (period, rce_pln)
    row, plus the rce_prices_fetched completeness marker, committed
    together. period_count just records how many periods were fetched
    (92/96/100 depending on DST) - it's never asserted against later, only
    used for observability.
    """
    conn.executemany(
        "INSERT OR REPLACE INTO rce_prices (business_date, period, rce_pln) VALUES (?, ?, ?)",
        [(business_date, period, rce_pln) for period, rce_pln in series],
    )
    conn.execute(
        "INSERT OR REPLACE INTO rce_prices_fetched (business_date, period_count, fetched_at) VALUES (?, ?, ?)",
        (business_date, len(series), int(time.time())),
    )
    conn.commit()
