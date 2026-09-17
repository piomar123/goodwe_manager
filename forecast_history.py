"""
forecast_history.py
SQLite-backed fetch history for PV production forecasts (forecast_history.db)
- one row per distinct forecast snapshot per (source, date), instead of a
single overwritten cache entry. See
https://github.com/piomar123/goodwe_manager/pull/26 for the design rationale
(why history, not a TTL cache; why the merge in get_latest_merged is
needed) - the design spec doc itself was removed from the tree, but is
still visible in that PR's history.
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
    """Distinct fetched_at values across every source for `date`, newest
    first - backs the single shared fetch-time dropdown. Sources are
    fetched independently (see forecast_prefetch.py's wake schedule) and do
    not actually share timestamps in practice, despite feeding one shared
    dropdown - selecting a timestamp only one source has data for is a
    known, pre-existing UX gap (not addressed here)."""
    rows = conn.execute(
        "SELECT DISTINCT fetched_at FROM forecast_snapshots WHERE date = ? ORDER BY fetched_at DESC",
        (date,),
    ).fetchall()
    return [r[0] for r in rows]


def get_merged_since(conn: sqlite3.Connection, source: str, date: str, since_epoch: int) -> Dict:
    """Like get_latest_merged, but only merges snapshots with fetched_at >=
    since_epoch. Backs the fetch-time dropdown's read of solcast_actuals:
    actuals are fetched once/day and don't change once measured, so pinning
    them to the exact selected fetched_at (like forecasts) only produces
    spurious "unavailable" gaps for a viewing time earlier in the day than
    that day's once-daily actuals fetch. Showing the freshest actuals
    fetched on or after the selected fetch time's calendar day is what the
    dropdown should mean for actuals, even though it isn't literally what
    was known at that exact instant.
    """
    rows = conn.execute(
        "SELECT payload FROM forecast_snapshots WHERE source = ? AND date = ? AND fetched_at >= ? ORDER BY fetched_at ASC",
        (source, date, since_epoch),
    ).fetchall()
    merged: Dict = {}
    for (payload_json,) in rows:
        merged.update(json.loads(payload_json))
    return merged


def has_fetched_since(conn: sqlite3.Connection, source: str, since_epoch: int) -> bool:
    """True if `source` has any snapshot (any date) with fetched_at >=
    since_epoch. Checked across all dates rather than one - a single fetch
    call can write snapshots for several dates at once (e.g. Solcast
    estimated_actuals' 7-day trailing window), so "was this source touched
    recently at all" is the meaningful question, not "does today have a
    row". Backs forecast_prefetch.py's startup catch-up: a quick restart
    minutes after a real fetch finds this True and skips the extra call; a
    genuine cold start finds it False and fetches immediately."""
    row = conn.execute(
        "SELECT 1 FROM forecast_snapshots WHERE source = ? AND fetched_at >= ? LIMIT 1",
        (source, since_epoch),
    ).fetchone()
    return row is not None


def get_latest_merged(conn: sqlite3.Connection, source: str, date: str) -> Dict:
    """The default ("latest") read: merges every snapshot for (source,
    date) period-by-period, oldest to newest, so a newer snapshot's periods
    overwrite an older snapshot's - but a period only the older snapshot
    ever covered survives. This is what makes "today" work for Solcast:
    each call is forward-looking from its own call time, so no single
    snapshot alone covers a full day once part of it is in the past
    relative to that snapshot's fetch time. Returns {} if there are no
    snapshots at all for (source, date). fetched_at is NOT NULL and never
    negative (see write_snapshot), so since_epoch=0 merges every snapshot -
    this is just get_merged_since with no lower bound.
    """
    return get_merged_since(conn, source, date, 0)
