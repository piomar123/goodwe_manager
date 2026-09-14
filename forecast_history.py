"""
forecast_history.py
SQLite-backed fetch history for PV production forecasts (forecast_history.db)
- one row per distinct forecast snapshot per (source, date), instead of a
single overwritten cache entry. See
docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md section 4
for the design rationale (why history, not a TTL cache; why the merge in
get_latest_merged is needed).
"""
import json
import sqlite3
import time
from typing import Dict, List, Optional

FORECAST_HISTORY_DB_PATH = 'forecast_history.db'


def init_db(path: Optional[str] = None) -> sqlite3.Connection:
    """Opens (creating if needed) the forecast history DB. `path` defaults
    to FORECAST_HISTORY_DB_PATH, looked up inside the function body (not as
    a bound default parameter) so tests can monkeypatch the module-level
    path and have default-path callers pick it up - same convention as
    rce_storage.init_db.
    """
    conn = sqlite3.connect(path or FORECAST_HISTORY_DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecast_snapshots (
            source TEXT NOT NULL,
            date TEXT NOT NULL,
            fetched_at INTEGER NOT NULL,
            valid_until INTEGER NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (source, date, fetched_at)
        )
    """)
    conn.commit()
    return conn


def _latest_snapshot_row(conn: sqlite3.Connection, source: str, date: str):
    return conn.execute(
        "SELECT fetched_at, payload FROM forecast_snapshots "
        "WHERE source = ? AND date = ? ORDER BY fetched_at DESC LIMIT 1",
        (source, date),
    ).fetchone()


def write_snapshot(conn: sqlite3.Connection, source: str, date: str, payload: Dict, now: Optional[int] = None) -> int:
    """Writes a forecast snapshot for (source, date). Dedupes against the
    immediately-prior snapshot for that (source, date): if `payload` is
    identical (compared as sorted-key JSON, so key order never matters) to
    the latest existing snapshot, this just bumps that snapshot's
    valid_until to `now` instead of inserting a duplicate row. Returns the
    fetched_at that now represents `payload` - either the new one, or the
    deduped-into existing one.
    """
    now = now if now is not None else int(time.time())
    payload_json = json.dumps(payload, sort_keys=True)
    existing = _latest_snapshot_row(conn, source, date)
    if existing is not None and existing[1] == payload_json:
        fetched_at = existing[0]
        conn.execute(
            "UPDATE forecast_snapshots SET valid_until = ? WHERE source = ? AND date = ? AND fetched_at = ?",
            (now, source, date, fetched_at),
        )
        conn.commit()
        return fetched_at
    conn.execute(
        "INSERT INTO forecast_snapshots (source, date, fetched_at, valid_until, payload) VALUES (?, ?, ?, ?, ?)",
        (source, date, now, now, payload_json),
    )
    conn.commit()
    return now


def get_snapshot(conn: sqlite3.Connection, source: str, date: str, fetched_at: int) -> Optional[Dict]:
    """Returns exactly what was fetched at `fetched_at` - gaps included.
    Backs the fetch-time dropdown's "view a specific past fetch" mode."""
    row = conn.execute(
        "SELECT payload FROM forecast_snapshots WHERE source = ? AND date = ? AND fetched_at = ?",
        (source, date, fetched_at),
    ).fetchone()
    return json.loads(row[0]) if row else None


def get_fetch_times(conn: sqlite3.Connection, date: str) -> List[int]:
    """Distinct fetched_at values across BOTH sources for `date`, newest
    first - backs the single shared fetch-time dropdown (both sources fetch
    on the same schedule, so in practice they share timestamps)."""
    rows = conn.execute(
        "SELECT DISTINCT fetched_at FROM forecast_snapshots WHERE date = ? ORDER BY fetched_at DESC",
        (date,),
    ).fetchall()
    return [r[0] for r in rows]


def get_latest_merged(conn: sqlite3.Connection, source: str, date: str) -> Dict:
    """The default ("latest") read: merges every snapshot for (source,
    date) period-by-period, oldest to newest, so a newer snapshot's periods
    overwrite an older snapshot's - but a period only the older snapshot
    ever covered survives. This is what makes "today" work for Solcast:
    each call is forward-looking from its own call time, so no single
    snapshot alone covers a full day once part of it is in the past
    relative to that snapshot's fetch time. Returns {} if there are no
    snapshots at all for (source, date).
    """
    rows = conn.execute(
        "SELECT payload FROM forecast_snapshots WHERE source = ? AND date = ? ORDER BY fetched_at ASC",
        (source, date),
    ).fetchall()
    merged: Dict = {}
    for (payload_json,) in rows:
        merged.update(json.loads(payload_json))
    return merged
