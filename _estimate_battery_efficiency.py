"""
_estimate_battery_efficiency.py
One-off offline analysis: estimates Predbat's real config settings
`battery_loss` (charge direction), `battery_loss_discharge`, and
`inverter_loss` (one symmetric value, used both directions) from this
household's inverter_history. Run manually; prints a report with
recommended values and sample-size caveats - nothing here writes to
Predbat's config automatically. See PR #35.

Methodology: classifies each 1Hz sample into one of five mutually
exclusive flow states based on the signs of PV production, battery
power, and grid power (all three phases required to individually agree
in sign, above a noise threshold - a phase-imbalanced reading nets
close to zero but isn't a clean "idle", so it's left unclassified
rather than folded into one), and requires grid_mode == Connected -
an islanded/off-grid period (grid outage, backup mode) has its own,
different loss characteristics and its meter-derived fields (pgrid,
load_ptotal) can read zero/stale while DC-side power keeps flowing
normally, which would otherwise look like (and get misattributed as)
clean PV-only or battery-only flow. Contiguous same-state runs are one
"session". Each session type measures energy on both sides of one
conversion boundary, via two independent methods that get compared:
the inverter's own cumulative energy counters (a value delta across
the session) and trapezoidal integration of the raw power samples.
Energy is summed globally per session type (across all its sessions)
before taking one ratio, rather than averaging a ratio per session -
a single short/noisy session's ratio would otherwise dominate a plain
average (see PR #35's fix for exactly this on the old methodology).

- grid_charge / grid_discharge: battery <-> grid, PV ~0. Two
  independent inverter_loss estimates (one per direction).
- pv_export: PV production -> grid export, battery idle. A third,
  physically independent inverter_loss estimate (PV's own DC/AC path,
  not the battery's).
- pv_charge: PV production -> battery charge, grid ~0. On a hybrid
  inverter, PV and battery share the DC bus ahead of the single AC/DC
  stage, so this path never crosses the inverter at all - the measured
  gap is battery_loss (charge direction) alone.
- battery_to_load: battery discharge -> house load, PV and grid ~0.
  This path *does* cross the inverter (DC battery -> AC load), so it
  measures a *combined* battery+inverter discharge loss; the
  battery-only battery_loss_discharge is backed out by dividing by the
  inverter_loss average from the three paths above, in efficiency
  terms: combined_efficiency = battery_efficiency * inverter_efficiency.
"""
import argparse
import sqlite3
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import storage

# battery-grid-direction-from-sign (PR #33, merged to main) measured
# pbattery1/grid-meter sign disagreement concentrated within a +/-200W
# band on this hardware - a much wider noise floor than a "clean" sensor
# would need, so both battery and grid-phase classification use it here
# too rather than a smaller, seemingly-safer default that would actually
# just be noise-dominated (see PR #35's finding of 5-second median
# session length at a 20W threshold).
DEFAULT_BATTERY_NOISE_W = 200.0
DEFAULT_GRID_PHASE_NOISE_W = 200.0
# PV can't read negative, so this only needs to separate "producing" from
# sensor noise around zero, not disambiguate a sign - a much smaller
# threshold is fine.
DEFAULT_PV_NOISE_W = 50.0
# Goodwe's protocol reads registers sequentially, not atomically - at a
# genuine state transition, different sensors (pbattery1, pgrid*, ppv)
# can reflect slightly different real instants within the same poll.
# This isn't higher noise (a magnitude threshold doesn't help); it's a
# few seconds right at every transition where the sample set is
# internally inconsistent. Guarded two ways: reject sessions too short
# to be a real event rather than a transition artifact, and drop a few
# samples off each session's start/end before measuring energy on it.
DEFAULT_MIN_SESSION_SECONDS = 5
DEFAULT_EDGE_TRIM_SAMPLES = 2
# "grid idle" (required by pv_export/pv_charge/battery_to_load) is a
# magnitude check on the phase SUM, deliberately distinct from
# DEFAULT_GRID_PHASE_NOISE_W's per-phase agreement test used by
# grid_charge/grid_discharge. _grid_direction() returning None means
# "phases disagree in sign", not "near zero" - a real but phase-
# imbalanced flow (one phase importing 3kW while another exports 3kW)
# nets close to zero yet is definitely not "the grid is idle", so
# treating "unclassified direction" as "idle" would let real grid
# contribution contaminate a PV-only or battery-only measurement.
#
# Verified against real data this needs to be small: a genuine
# pv_charge session was found with pgrid/pgrid2/pgrid3 steady at
# ~90-94W each (a ~270-280W sum, comfortably "idle" under a naive 300W
# default) that was actually real, continuous grid import for baseline
# house load running concurrently with PV charging the battery -
# nowhere near idle relative to this household's PV (50-250W) and
# battery (200-250W) levels in the same session.
DEFAULT_GRID_IDLE_SUM_W = 60.0

GRID_CHARGE = 'grid_charge'
GRID_DISCHARGE = 'grid_discharge'
PV_EXPORT = 'pv_export'
PV_CHARGE = 'pv_charge'
BATTERY_TO_LOAD = 'battery_to_load'
ALL_STATES = (GRID_CHARGE, GRID_DISCHARGE, PV_EXPORT, PV_CHARGE, BATTERY_TO_LOAD)

# Matches static/js/diagram-calc.js's GRID_MODE.CONNECTED - same raw
# grid_mode register, same convention, kept in sync deliberately rather
# than re-derived.
GRID_MODE_CONNECTED = 1


@dataclass
class Thresholds:
    battery_w: float = DEFAULT_BATTERY_NOISE_W
    grid_phase_w: float = DEFAULT_GRID_PHASE_NOISE_W
    pv_w: float = DEFAULT_PV_NOISE_W
    grid_idle_w: float = DEFAULT_GRID_IDLE_SUM_W


@dataclass
class Session:
    state: str
    start_epoch: int
    end_epoch: int


def _sign(value: Optional[float], threshold: float) -> Optional[str]:
    """'pos'/'neg' above `threshold` in magnitude, else None - shared by
    battery and per-phase grid classification."""
    if value is None:
        return None
    if value > threshold:
        return 'pos'
    if value < -threshold:
        return 'neg'
    return None


def _grid_direction(pgrid: Optional[float], pgrid2: Optional[float], pgrid3: Optional[float],
                     threshold: float) -> Optional[str]:
    """'export' or 'import' only when all three phases individually
    agree in sign above `threshold`. A phase-imbalanced reading (one
    phase importing while another exports, netting near zero) is left
    unclassified rather than folded into "idle" - it's a real but
    non-clean flow state that shouldn't be attributed to either
    direction."""
    signs = (_sign(pgrid, threshold), _sign(pgrid2, threshold), _sign(pgrid3, threshold))
    if signs[0] is None or signs[0] != signs[1] or signs[1] != signs[2]:
        return None
    return 'export' if signs[0] == 'pos' else 'import'


def _grid_power_sum(pgrid: Optional[float], pgrid2: Optional[float], pgrid3: Optional[float]) -> Optional[float]:
    if pgrid is None or pgrid2 is None or pgrid3 is None:
        return None
    return abs(pgrid) + abs(pgrid2) + abs(pgrid3)


def _grid_is_idle(pgrid: Optional[float], pgrid2: Optional[float], pgrid3: Optional[float],
                   threshold: float) -> bool:
    """True only when the phase-sum magnitude is small - NOT the same as
    _grid_direction() returning None, which just means the phases
    disagree in sign and says nothing about how much power is actually
    flowing (see DEFAULT_GRID_IDLE_SUM_W)."""
    total = _grid_power_sum(pgrid, pgrid2, pgrid3)
    return total is not None and total < threshold


def classify_sample(pbattery1: Optional[float], pgrid: Optional[float], pgrid2: Optional[float],
                     pgrid3: Optional[float], ppv: Optional[float], grid_mode: Optional[float],
                     thresholds: Thresholds) -> Optional[str]:
    """Classifies one sample into one of the five flow states (see module
    docstring), or None if it matches none of them cleanly (multiple
    things active at once, everything idle, or grid_mode isn't
    Connected - an islanded/off-grid period has different loss
    characteristics and shouldn't be attributed to any of these five
    normal-operation paths)."""
    if grid_mode != GRID_MODE_CONNECTED:
        return None
    battery_sign = _sign(pbattery1, thresholds.battery_w)
    grid_dir = _grid_direction(pgrid, pgrid2, pgrid3, thresholds.grid_phase_w)
    grid_idle = _grid_is_idle(pgrid, pgrid2, pgrid3, thresholds.grid_idle_w)
    pv_producing = ppv is not None and ppv > thresholds.pv_w

    if grid_dir == 'import' and battery_sign == 'pos' and not pv_producing:
        return GRID_CHARGE
    if grid_dir == 'export' and battery_sign == 'neg' and not pv_producing:
        return GRID_DISCHARGE
    if pv_producing and grid_dir == 'export' and battery_sign is None:
        return PV_EXPORT
    if pv_producing and battery_sign == 'pos' and grid_idle:
        return PV_CHARGE
    if battery_sign == 'neg' and grid_idle and not pv_producing:
        return BATTERY_TO_LOAD
    return None


def find_labeled_sessions(rows: List[Tuple[int, Optional[float], Optional[float], Optional[float],
                                            Optional[float], Optional[float], Optional[float]]],
                           thresholds: Thresholds) -> List[Session]:
    """rows: [(epoch, pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode), ...],
    time-ordered. Splits into contiguous runs sharing the same
    classify_sample() label - a sample matching no state, or a
    different one, ends the current run."""
    sessions = []
    current_state = None
    current_start = None
    current_end = None
    for epoch, pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode in rows:
        state = classify_sample(pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode, thresholds)
        if state != current_state:
            if current_state is not None:
                sessions.append(Session(current_state, current_start, current_end))
            current_state, current_start = state, epoch
        if state is not None:
            current_end = epoch
    if current_state is not None:
        sessions.append(Session(current_state, current_start, current_end))
    return sessions


def filter_min_duration(sessions: List[Session], min_seconds: int) -> List[Session]:
    """Drops sessions shorter than min_seconds - at Goodwe's per-register
    (non-atomic) poll cadence, a session lasting only a couple of
    seconds is more likely a transition artifact (see module docstring)
    than a real sustained flow."""
    return [s for s in sessions if (s.end_epoch - s.start_epoch) >= min_seconds]


def _trapezoidal_energy_wh(power_samples: List[Tuple[int, float]]) -> float:
    """power_samples: [(timestamp_epoch, watts), ...], time-ordered.
    Trapezoidal integration to watt-hours."""
    if len(power_samples) < 2:
        return 0.0
    energy_ws = 0.0
    for (t0, p0), (t1, p1) in zip(power_samples, power_samples[1:]):
        energy_ws += (p0 + p1) / 2.0 * (t1 - t0)
    return energy_ws / 3600.0




def _abs_or_none(value: Optional[float]) -> Optional[float]:
    return abs(value) if value is not None else None


# Per session-type spec: which raw column(s) represent the "input"
# (pre-conversion-loss) and "output" (post-loss) power/energy-counter
# pair. `same_day` marks e_day (the only column here that resets daily,
# unlike the lifetime e_total_*/e_bat_*_total/e_load_total counters) -
# a session whose e_day delta would otherwise span a midnight rollover
# has that delta measurement skipped (the integral method is unaffected).
SESSION_SPECS: Dict[str, dict] = {
    GRID_CHARGE: {
        'input_power': lambda r: _grid_power_sum(r['pgrid'], r['pgrid2'], r['pgrid3']),
        'input_counter': 'e_total_imp',
        'input_same_day': False,
        'output_power': lambda r: _abs_or_none(r['pbattery1']),
        'output_counter': 'e_bat_charge_total',
        'output_same_day': False,
    },
    GRID_DISCHARGE: {
        'input_power': lambda r: _abs_or_none(r['pbattery1']),
        'input_counter': 'e_bat_discharge_total',
        'input_same_day': False,
        'output_power': lambda r: _grid_power_sum(r['pgrid'], r['pgrid2'], r['pgrid3']),
        'output_counter': 'e_total_exp',
        'output_same_day': False,
    },
    PV_EXPORT: {
        'input_power': lambda r: r['ppv'],
        'input_counter': 'e_day',
        'input_same_day': True,
        'output_power': lambda r: _grid_power_sum(r['pgrid'], r['pgrid2'], r['pgrid3']),
        'output_counter': 'e_total_exp',
        'output_same_day': False,
    },
    PV_CHARGE: {
        'input_power': lambda r: r['ppv'],
        'input_counter': 'e_day',
        'input_same_day': True,
        'output_power': lambda r: _abs_or_none(r['pbattery1']),
        'output_counter': 'e_bat_charge_total',
        'output_same_day': False,
    },
    BATTERY_TO_LOAD: {
        'input_power': lambda r: _abs_or_none(r['pbattery1']),
        'input_counter': 'e_bat_discharge_total',
        'input_same_day': False,
        'output_power': lambda r: r['load_ptotal'],
        'output_counter': 'e_load_total',
        'output_same_day': False,
    },
}

SESSION_ROW_COLUMNS = ('timestamp_epoch', 'timestamp', 'pbattery1', 'pgrid', 'pgrid2', 'pgrid3', 'ppv',
                        'load_ptotal', 'e_total_imp', 'e_total_exp', 'e_bat_charge_total',
                        'e_bat_discharge_total', 'e_day', 'e_load_total')


@dataclass
class SessionTypeTotals:
    session_count: int = 0
    input_delta_wh: float = 0.0
    input_integral_wh: float = 0.0
    output_delta_wh: float = 0.0
    output_integral_wh: float = 0.0
    delta_sessions: int = 0  # sessions that contributed a delta measurement (same_day guard may skip some)

    def loss_delta(self) -> Optional[float]:
        if self.input_delta_wh <= 0:
            return None
        return 1 - (self.output_delta_wh / self.input_delta_wh)

    def loss_integral(self) -> Optional[float]:
        if self.input_integral_wh <= 0:
            return None
        return 1 - (self.output_integral_wh / self.input_integral_wh)


def _measure_session(conn: sqlite3.Connection, session: Session, spec: dict,
                      edge_trim_samples: int = DEFAULT_EDGE_TRIM_SAMPLES) -> Optional[dict]:
    rows = conn.execute(
        f"SELECT {', '.join(SESSION_ROW_COLUMNS)} FROM inverter_history "
        "WHERE timestamp_epoch >= ? AND timestamp_epoch <= ? ORDER BY timestamp_epoch",
        (session.start_epoch, session.end_epoch),
    ).fetchall()

    def as_dict(row):
        return dict(zip(SESSION_ROW_COLUMNS, row))

    dict_rows = [as_dict(r) for r in rows]
    # Drop samples right at the session's start/end: Goodwe reads
    # registers sequentially, not atomically, so the instant a flow
    # state actually changes, different sensors can momentarily disagree
    # about it - trimming the edges keeps that transition skew out of
    # the energy measurement rather than trying to filter it by magnitude.
    if edge_trim_samples > 0:
        dict_rows = dict_rows[edge_trim_samples:len(dict_rows) - edge_trim_samples]
    if len(dict_rows) < 2:
        return None

    input_series = [(r['timestamp_epoch'], spec['input_power'](r)) for r in dict_rows]
    output_series = [(r['timestamp_epoch'], spec['output_power'](r)) for r in dict_rows]
    input_integral_wh = _trapezoidal_energy_wh([(t, v) for t, v in input_series if v is not None])
    output_integral_wh = _trapezoidal_energy_wh([(t, v) for t, v in output_series if v is not None])

    first, last = dict_rows[0], dict_rows[-1]

    def counter_delta(counter_name: str, same_day: bool) -> Optional[float]:
        if same_day and first['timestamp'][:10] != last['timestamp'][:10]:
            return None
        start_val, end_val = first[counter_name], last[counter_name]
        if start_val is None or end_val is None:
            return None
        return (float(end_val) - float(start_val)) * 1000.0  # kWh -> Wh

    input_delta_wh = counter_delta(spec['input_counter'], spec['input_same_day'])
    output_delta_wh = counter_delta(spec['output_counter'], spec['output_same_day'])

    return {
        'input_integral_wh': input_integral_wh,
        'output_integral_wh': output_integral_wh,
        'input_delta_wh': input_delta_wh,
        'output_delta_wh': output_delta_wh,
    }


def measure_sessions(conn: sqlite3.Connection, sessions: List[Session],
                      edge_trim_samples: int = DEFAULT_EDGE_TRIM_SAMPLES) -> Dict[str, SessionTypeTotals]:
    """Sums input/output energy (both by counter-delta and by power
    integration) across every session of each type, then exposes one
    loss ratio per type per method via SessionTypeTotals - energy-
    weighted, not a plain average of per-session ratios (see module
    docstring)."""
    totals = {state: SessionTypeTotals() for state in ALL_STATES}
    for session in sessions:
        spec = SESSION_SPECS[session.state]
        measurement = _measure_session(conn, session, spec, edge_trim_samples)
        if measurement is None:
            continue
        t = totals[session.state]
        t.session_count += 1
        t.input_integral_wh += measurement['input_integral_wh']
        t.output_integral_wh += measurement['output_integral_wh']
        if measurement['input_delta_wh'] is not None and measurement['output_delta_wh'] is not None \
                and measurement['input_delta_wh'] > 0:
            t.input_delta_wh += measurement['input_delta_wh']
            t.output_delta_wh += measurement['output_delta_wh']
            t.delta_sessions += 1
    return totals


@dataclass
class EfficiencyEstimate:
    inverter_loss: Optional[float]
    inverter_loss_paths: Dict[str, Tuple[Optional[float], Optional[float]]]  # state -> (delta_loss, integral_loss)
    battery_loss: Optional[float]  # charge direction (pv_charge)
    battery_loss_discharge: Optional[float]


def _average(values: List[float]) -> Optional[float]:
    values = [v for v in values if v is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _derive_battery_loss_discharge(combined_loss: Optional[float], inverter_loss: Optional[float]) -> Optional[float]:
    """battery_to_load measures the *combined* battery+inverter discharge
    path (DC battery -> AC load), since discharging always crosses the
    inverter (unlike pv_charge's DC-bus bypass for the charge
    direction). Backs out the battery-only factor: combined_efficiency
    = battery_efficiency * inverter_efficiency."""
    if combined_loss is None or inverter_loss is None:
        return None
    inverter_efficiency = 1 - inverter_loss
    if inverter_efficiency <= 0:
        return None
    combined_efficiency = 1 - combined_loss
    battery_efficiency = combined_efficiency / inverter_efficiency
    return 1 - battery_efficiency


def estimate_efficiency(totals: Dict[str, SessionTypeTotals], method: str = 'delta') -> EfficiencyEstimate:
    """method is 'delta' (energy-counter deltas) or 'integral' (power
    trapezoidal integration) - pick which measurement the headline
    numbers use; both are still available per-path for comparison."""
    loss_fn = (lambda t: t.loss_delta()) if method == 'delta' else (lambda t: t.loss_integral())

    inverter_paths = (GRID_CHARGE, GRID_DISCHARGE, PV_EXPORT)
    inverter_loss = _average([loss_fn(totals[s]) for s in inverter_paths])
    inverter_loss_paths = {
        s: (totals[s].loss_delta(), totals[s].loss_integral()) for s in inverter_paths
    }

    battery_loss = loss_fn(totals[PV_CHARGE])
    battery_loss_discharge = _derive_battery_loss_discharge(loss_fn(totals[BATTERY_TO_LOAD]), inverter_loss)

    return EfficiencyEstimate(
        inverter_loss=inverter_loss,
        inverter_loss_paths=inverter_loss_paths,
        battery_loss=battery_loss,
        battery_loss_discharge=battery_loss_discharge,
    )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)
    ).fetchone()
    return row is not None


def _fmt_loss(loss: Optional[float]) -> str:
    return f"{loss:.3f}" if loss is not None else "no data"


def main():
    parser = argparse.ArgumentParser(
        description="Estimate Predbat's real battery_loss/battery_loss_discharge/inverter_loss settings from history")
    parser.add_argument("--db-path", type=str, default=storage.DATA_DB_PATH)
    parser.add_argument("--battery-noise-w", type=float, default=DEFAULT_BATTERY_NOISE_W)
    parser.add_argument("--grid-phase-noise-w", type=float, default=DEFAULT_GRID_PHASE_NOISE_W)
    parser.add_argument("--pv-noise-w", type=float, default=DEFAULT_PV_NOISE_W)
    parser.add_argument("--min-session-seconds", type=int, default=DEFAULT_MIN_SESSION_SECONDS,
                         help="Discard sessions shorter than this - at Goodwe's non-atomic per-register "
                              "poll cadence, a session this short is more likely a transition artifact "
                              "(different sensors momentarily disagreeing about a state change) than a "
                              "real sustained flow.")
    parser.add_argument("--edge-trim-samples", type=int, default=DEFAULT_EDGE_TRIM_SAMPLES,
                         help="Drop this many samples off each session's start/end before measuring "
                              "energy on it, for the same cross-register skew reason.")
    args = parser.parse_args()

    thresholds = Thresholds(battery_w=args.battery_noise_w, grid_phase_w=args.grid_phase_noise_w,
                             pv_w=args.pv_noise_w)

    conn = sqlite3.connect(args.db_path)
    try:
        if not _table_exists(conn, "inverter_history"):
            print("No inverter_history table found in the database - nothing to analyze yet.")
            print(f"(db path: {args.db_path})")
            return

        rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode FROM inverter_history "
            "ORDER BY timestamp_epoch"
        ).fetchall()
        sessions = find_labeled_sessions(rows, thresholds)
        sessions = filter_min_duration(sessions, args.min_session_seconds)
        totals = measure_sessions(conn, sessions, args.edge_trim_samples)
    finally:
        conn.close()

    delta_estimate = estimate_efficiency(totals, method='delta')
    integral_estimate = estimate_efficiency(totals, method='integral')

    print("Predbat efficiency estimate (approximate - verify plausibility before use):")
    print()
    for label, state in (("grid_charge", GRID_CHARGE), ("grid_discharge", GRID_DISCHARGE),
                         ("pv_export", PV_EXPORT), ("pv_charge", PV_CHARGE),
                         ("battery_to_load", BATTERY_TO_LOAD)):
        t = totals[state]
        print(f"  [{label}] sessions: {t.session_count} (delta-eligible: {t.delta_sessions})")
        print(f"    loss via energy-counter delta: {_fmt_loss(t.loss_delta())}")
        print(f"    loss via power integration:    {_fmt_loss(t.loss_integral())}")
    print()
    print("Recommended apps.yaml values:")
    print(f"  inverter_loss           (delta-based):    {_fmt_loss(delta_estimate.inverter_loss)}")
    print(f"  inverter_loss           (integral-based):  {_fmt_loss(integral_estimate.inverter_loss)}")
    print(f"  battery_loss            (delta-based):    {_fmt_loss(delta_estimate.battery_loss)}")
    print(f"  battery_loss            (integral-based):  {_fmt_loss(integral_estimate.battery_loss)}")
    print(f"  battery_loss_discharge  (delta-based):    {_fmt_loss(delta_estimate.battery_loss_discharge)}")
    print(f"  battery_loss_discharge  (integral-based):  {_fmt_loss(integral_estimate.battery_loss_discharge)}")
    print()
    print("If the delta-based and integral-based numbers disagree substantially, the")
    print("energy counters (0.1 kWh resolution) are likely too coarse relative to typical")
    print("session size on this system - prefer the integral-based numbers in that case.")
    print("Plausibility check: all three settings are normally somewhere in 0.02-0.15 for")
    print("a modern li-ion system. A value far outside that range likely means noisy or")
    print("insufficient data rather than a real result - inspect the session counts above.")


if __name__ == '__main__':
    main()
