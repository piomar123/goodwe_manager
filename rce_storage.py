"""
SQLite cache for RCE 15-minute electricity prices (rce_prices.db).
See docs/superpowers/specs/2026-08-27-sqlite-storage-design.md,
"RCE price cache" section.
"""
import sqlite3
import time
from typing import List, Optional, Tuple

RCE_DB_PATH = 'rce_prices.db'


def init_db(path: Optional[str] = None) -> sqlite3.Connection:
    """Opens (creating if needed) the RCE price cache DB and ensures both
    tables exist. `path` defaults to the module-level RCE_DB_PATH, looked
    up here inside the function body (not as a bound default parameter
    value) so tests can monkeypatch rce_storage.RCE_DB_PATH and have
    default-path callers pick up the patched value.
    """
    conn = sqlite3.connect(path or RCE_DB_PATH)
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
    return conn


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
