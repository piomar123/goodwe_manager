"""
settlement.py
Daily net-billing settlement and PV/battery savings, stored in data.db's
daily_settlement table so they can be compared with the grid operator's
bill and summed per month without recomputing.

Per hour (the operator's meter keeps 1 h windows):
- the meter's import and export are netted within the hour. A net import
  is paid at that hour's import price; a net export is credited to the
  prosumer deposit at that hour's export price (hourly average of the
  15-minute RCE, x1.23 from EXPORT_BONUS_START, 0 for a negative hour).
- "without PV and battery" is the house load bought from the grid at that
  hour's import price, minus PV_EXTRA_LOAD_FACTOR of the PV used in the
  house, which stands for load that exists only because PV made it cheap
  (resistive DHW boost, AC).
- savings = without-PV cost - net import cost + export credit. Grid energy
  bought cheap, stored and used at an expensive hour is included, since
  the expensive hour's load is in the baseline but not in the net import.

Import prices come from a tariff history (TARIFF_HISTORY_CONFIG): a list
of periods, each a flat price or a tariff_engine config, see
load_tariff_history(). Days are local calendar days.
"""
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import yaml

import tariff_engine

logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo('Europe/Warsaw')
EXPORT_BONUS_MULTIPLIER = 1.23
EXPORT_BONUS_START = date(2025, 2, 1)
DEFAULT_PV_EXTRA_LOAD_FACTOR = 0.10

DAILY_COLUMNS = [
    ('hours', 'INTEGER'),             # hours with complete data (24 on a normal day)
    ('import_kwh', 'REAL'),           # meter import before hourly netting
    ('export_kwh', 'REAL'),           # meter export before hourly netting
    ('net_import_kwh', 'REAL'),       # sum of hourly net imports (what the bill charges)
    ('net_export_kwh', 'REAL'),       # sum of hourly net exports (what goes to the deposit)
    ('net_import_pln', 'REAL'),
    ('export_credit_pln', 'REAL'),
    ('load_kwh', 'REAL'),
    ('pv_kwh', 'REAL'),
    ('pv_self_kwh', 'REAL'),          # PV not exported in the same hour
    ('battery_charge_kwh', 'REAL'),
    ('battery_discharge_kwh', 'REAL'),
    ('baseline_kwh', 'REAL'),         # load without PV and battery
    ('baseline_pln', 'REAL'),
    ('savings_pln', 'REAL'),
    ('export_price_missing_hours', 'INTEGER'),
    ('computed_at', 'INTEGER'),
]
_COLUMN_NAMES = [name for name, _ in DAILY_COLUMNS]
_SUM_COLUMNS = [name for name, _ in DAILY_COLUMNS if name not in ('computed_at',)]


# --- tariff history -------------------------------------------------------

@dataclass
class TariffPeriod:
    start: date
    flat_price: Optional[float] = None
    config: Optional[dict] = None

    def price_at(self, dt: datetime) -> float:
        if self.flat_price is not None:
            return self.flat_price
        return tariff_engine.price_at(self.config, dt.replace(tzinfo=None))


@dataclass
class TariffHistory:
    periods: List[TariffPeriod] = field(default_factory=list)

    def price_at(self, dt: datetime) -> Optional[float]:
        """Import price (zl/kWh gross) at local datetime `dt`, or None
        before the first period."""
        chosen = None
        for period in self.periods:
            if period.start <= dt.date():
                chosen = period
        return chosen.price_at(dt) if chosen else None


def load_tariff_history(path: str) -> TariffHistory:
    """YAML:

        periods:
          - from: 2024-01-01
            price: 1.10                 # flat gross zl/kWh (e.g. G11)
          - from: 2025-09-18
            tariff: tariff_import.yaml  # tariff_engine config, relative to this file

    Periods are sorted by `from`; each one lasts until the next starts.
    """
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    periods = []
    errors = []
    for i, entry in enumerate(raw.get('periods') or []):
        start = entry.get('from')
        if isinstance(start, str):
            start = date.fromisoformat(start)
        if not isinstance(start, date):
            errors.append(f"periods[{i}]: 'from' must be a date (YYYY-MM-DD)")
            continue
        if ('price' in entry) == ('tariff' in entry):
            errors.append(f"periods[{i}]: give exactly one of 'price' or 'tariff'")
            continue
        if 'price' in entry:
            periods.append(TariffPeriod(start, flat_price=float(entry['price'])))
        else:
            tariff_path = entry['tariff']
            if not os.path.isabs(tariff_path):
                tariff_path = os.path.join(os.path.dirname(os.path.abspath(path)), tariff_path)
            periods.append(TariffPeriod(start, config=tariff_engine.load_config(tariff_path)))
    if not periods and not errors:
        errors.append("no periods")
    if errors:
        raise ValueError(f"{path}: " + '; '.join(errors))
    periods.sort(key=lambda p: p.start)
    return TariffHistory(periods)


def tariff_history_from_env(environ=os.environ) -> Optional[TariffHistory]:
    """TARIFF_HISTORY_CONFIG if set, otherwise TARIFF_IMPORT_CONFIG for
    all dates, otherwise None (no import prices: settlement is skipped)."""
    history_path = environ.get('TARIFF_HISTORY_CONFIG')
    if history_path:
        return load_tariff_history(history_path)
    import_path = environ.get('TARIFF_IMPORT_CONFIG')
    if import_path:
        return TariffHistory([TariffPeriod(date.min, config=tariff_engine.load_config(import_path))])
    return None


# --- export prices --------------------------------------------------------

def _hour_key(period: str) -> str:
    """'13:45' -> '13', DST fall-back repeat '02a:15' -> '02a'."""
    return period.partition(':')[0]


def hourly_export_prices(rce_15min: Iterable[Tuple[str, float]], day: date) -> Dict[str, float]:
    """{hour key: zl/kWh deposit value} from one day's 15-minute RCE
    (PLN/MWh). The operator credits the hourly average of the quarters;
    a negative hour is worth nothing; the x1.23 bonus applies from
    EXPORT_BONUS_START."""
    sums: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    for period, rce_pln in rce_15min:
        key = _hour_key(period)
        if key == '24':  # rce.py's end-of-day sentinel
            continue
        sums[key] = sums.get(key, 0.0) + rce_pln
        counts[key] = counts.get(key, 0) + 1
    multiplier = EXPORT_BONUS_MULTIPLIER if day >= EXPORT_BONUS_START else 1.0
    return {key: max(0.0, sums[key] / counts[key]) / 1000.0 * multiplier for key in sums}


def hour_key_of(dt: datetime) -> str:
    return f"{dt.hour:02d}{'a' if dt.fold else ''}"


# --- daily computation ----------------------------------------------------

def day_bounds(day: date) -> Tuple[int, int]:
    start = datetime(day.year, day.month, day.day, tzinfo=LOCAL_TZ)
    end = start + timedelta(days=1)  # wall-clock arithmetic: next local midnight, 23/25 h on DST days
    return int(start.timestamp()), int(end.timestamp())


def fetch_day_hours(conn: sqlite3.Connection, day: date) -> list:
    start, end = day_bounds(day)
    return conn.execute(
        "SELECT hour_start, meter_import_kwh, meter_export_kwh, load_kwh, pv_kwh, "
        "battery_charge_kwh, battery_discharge_kwh FROM hourly_summary "
        "WHERE hour_start >= ? AND hour_start < ? ORDER BY hour_start",
        (start, end),
    ).fetchall()


def compute_day(hours: list, import_price_at: Callable[[datetime], Optional[float]],
                export_prices: Dict[str, float],
                pv_extra_load_factor: float = DEFAULT_PV_EXTRA_LOAD_FACTOR) -> dict:
    """Pure: one day's totals from its hourly_summary rows
    (hour_start, import, export, load, pv, battery_charge, battery_discharge).
    Hours with any missing meter/load/PV value are left out (counted in
    `hours`); missing battery values count as 0."""
    totals = {name: 0.0 for name in _SUM_COLUMNS}
    totals['hours'] = 0
    totals['export_price_missing_hours'] = 0
    for hour_start, imp, exp, load, pv, b_chg, b_dis in hours:
        if None in (imp, exp, load, pv):
            continue
        dt = datetime.fromtimestamp(hour_start, LOCAL_TZ)
        import_price = import_price_at(dt)
        if import_price is None:
            continue
        export_price = export_prices.get(hour_key_of(dt))
        net = exp - imp
        totals['hours'] += 1
        totals['import_kwh'] += imp
        totals['export_kwh'] += exp
        if net < 0:
            totals['net_import_kwh'] += -net
            totals['net_import_pln'] += -net * import_price
        else:
            totals['net_export_kwh'] += net
            if export_price is None:
                if net > 0:
                    totals['export_price_missing_hours'] += 1
            else:
                totals['export_credit_pln'] += net * export_price
        pv_self = max(0.0, pv - exp)
        baseline = max(0.0, load - pv_extra_load_factor * pv_self)
        totals['load_kwh'] += load
        totals['pv_kwh'] += pv
        totals['pv_self_kwh'] += pv_self
        totals['battery_charge_kwh'] += b_chg or 0.0
        totals['battery_discharge_kwh'] += b_dis or 0.0
        totals['baseline_kwh'] += baseline
        totals['baseline_pln'] += baseline * import_price
    totals['savings_pln'] = totals['baseline_pln'] - totals['net_import_pln'] + totals['export_credit_pln']
    return {k: (round(v, 4) if isinstance(v, float) else v) for k, v in totals.items()}


# --- storage --------------------------------------------------------------

def init_table(conn: sqlite3.Connection) -> None:
    cols = ', '.join(f'{name} {sql_type}' for name, sql_type in DAILY_COLUMNS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS daily_settlement (day TEXT PRIMARY KEY, {cols})")
    existing = {row[1] for row in conn.execute("PRAGMA table_info(daily_settlement)")}
    for name, sql_type in DAILY_COLUMNS:
        if name not in existing:
            conn.execute(f"ALTER TABLE daily_settlement ADD COLUMN {name} {sql_type}")
    conn.commit()


def store_day(conn: sqlite3.Connection, day: date, totals: dict) -> None:
    row = dict(totals, computed_at=int(time.time()))
    placeholders = ', '.join('?' for _ in range(len(_COLUMN_NAMES) + 1))
    conn.execute(
        f"INSERT OR REPLACE INTO daily_settlement (day, {', '.join(_COLUMN_NAMES)}) VALUES ({placeholders})",
        [day.isoformat()] + [row.get(name) for name in _COLUMN_NAMES],
    )
    conn.commit()


def update_day(conn: sqlite3.Connection, day: date, tariffs: TariffHistory,
               rce_fetch: Callable[[date], list],
               pv_extra_load_factor: float = DEFAULT_PV_EXTRA_LOAD_FACTOR) -> Optional[dict]:
    """Recomputes and stores one day. Returns the totals, or None if the
    day has no hourly data yet. A failed RCE fetch leaves the export
    credit for those hours out (export_price_missing_hours) rather than
    failing the day; the next run fills it in."""
    hours = fetch_day_hours(conn, day)
    if not hours:
        return None
    try:
        export_prices = hourly_export_prices(rce_fetch(day), day)
    except Exception as exc:  # network, PSE not published, ...
        logger.warning(f"Settlement {day}: no RCE prices ({exc}), export credit left out")
        export_prices = {}
    totals = compute_day(hours, tariffs.price_at, export_prices, pv_extra_load_factor)
    store_day(conn, day, totals)
    return totals


def days_needing_update(conn: sqlite3.Connection, today: date) -> List[date]:
    """Today, yesterday, and any earlier day whose row is incomplete
    (missing export prices) or older than its hourly data."""
    days = {today, today - timedelta(days=1)}
    for (day_str,) in conn.execute(
            "SELECT day FROM daily_settlement WHERE export_price_missing_hours > 0 AND day < ?",
            ((today - timedelta(days=1)).isoformat(),)):
        days.add(date.fromisoformat(day_str))
    return sorted(days)


def pv_extra_load_factor_from_env(environ=os.environ) -> float:
    return float(environ.get('PV_EXTRA_LOAD_FACTOR', DEFAULT_PV_EXTRA_LOAD_FACTOR))


# --- reading --------------------------------------------------------------

def _sum_select(group_expr: str) -> str:
    sums = ', '.join(f'SUM({name}) AS {name}' for name in _SUM_COLUMNS)
    return f"SELECT {group_expr} AS period, COUNT(*) AS days, {sums} FROM daily_settlement"


def _rows(cursor) -> List[dict]:
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def monthly(conn: sqlite3.Connection) -> List[dict]:
    return _rows(conn.execute(_sum_select('substr(day, 1, 7)') + " GROUP BY period ORDER BY period"))


def daily(conn: sqlite3.Connection, month: str) -> List[dict]:
    cols = ', '.join(_SUM_COLUMNS)
    return _rows(conn.execute(
        f"SELECT day AS period, 1 AS days, {cols} FROM daily_settlement WHERE day LIKE ? ORDER BY day",
        (month + '-%',)))


def payback(rows_by_day: List[Tuple[str, float]], investment_pln: float, today: date) -> dict:
    """Cumulative savings against the net investment, with a payback date
    projected from the last 365 days' savings rate."""
    total = sum(s for _, s in rows_by_day)
    last_year_start = (today - timedelta(days=365)).isoformat()
    last_year = [s for d, s in rows_by_day if d >= last_year_start]
    result = {'total_pln': round(total, 2), 'investment_pln': investment_pln,
              'paid_back_fraction': (total / investment_pln) if investment_pln else None,
              'per_year_pln': None, 'payback_date': None}
    if last_year:
        days_covered = min(365, len(last_year))
        per_day = sum(last_year) / days_covered
        result['per_year_pln'] = round(per_day * 365, 2)
        if investment_pln and per_day > 0:
            remaining = max(0.0, investment_pln - total)
            result['payback_date'] = (today + timedelta(days=round(remaining / per_day))).isoformat()
    return result


def cumulative(conn: sqlite3.Connection) -> List[Tuple[str, float]]:
    return conn.execute("SELECT day, savings_pln FROM daily_settlement ORDER BY day").fetchall()
