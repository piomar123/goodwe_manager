"""
_estimate_battery_efficiency.py
One-off offline analysis: estimates Predbat's battery_loss,
inverter_loss_charge, inverter_loss_discharge config values from this
household's ~2 years of inverter_history. Run manually; prints a report
with recommended values and sample-size caveats - nothing here writes to
Predbat's config automatically. See PR #35.
"""
import argparse
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple

import storage

DEFAULT_NOISE_THRESHOLD_W = 20.0
DEFAULT_MAX_CYCLE_GAP_SECONDS = 3 * 24 * 3600
DEFAULT_SOC_TOLERANCE = 3.0
DEFAULT_MIN_EXCURSION_SOC = 10.0


@dataclass
class BatterySession:
    sign: str  # 'charge' or 'discharge'
    start_epoch: int
    end_epoch: int


def _sign(pbattery1: float, noise_threshold_w: float) -> Optional[str]:
    if pbattery1 > noise_threshold_w:
        return 'charge'
    if pbattery1 < -noise_threshold_w:
        return 'discharge'
    return None


def find_battery_sessions(samples: List[Tuple[int, float]], noise_threshold_w: float) -> List[BatterySession]:
    """samples: [(timestamp_epoch, pbattery1_watts), ...], time-ordered.
    Splits into contiguous runs sharing the same sign (above
    noise_threshold_w in magnitude) - a sample within the noise band ends
    the current run without starting a new one (see spec Component 6's
    "contiguous run where pbattery1 holds one sign above a noise
    threshold")."""
    sessions = []
    current_sign = None
    current_start = None
    current_end = None
    for epoch, pbattery1 in samples:
        sign = _sign(pbattery1, noise_threshold_w)
        if sign is None:
            if current_sign is not None:
                sessions.append(BatterySession(current_sign, current_start, current_end))
                current_sign = None
            continue
        if sign != current_sign:
            if current_sign is not None:
                sessions.append(BatterySession(current_sign, current_start, current_end))
            current_sign, current_start = sign, epoch
        current_end = epoch
    if current_sign is not None:
        sessions.append(BatterySession(current_sign, current_start, current_end))
    return sessions


def find_matched_cycle_pairs(soc_samples: List[Tuple[int, float]], max_gap_seconds: int,
                             soc_tolerance: float, min_excursion: float = DEFAULT_MIN_EXCURSION_SOC) -> List[Tuple[int, int]]:
    """soc_samples: [(timestamp_epoch, battery_soc), ...], time-ordered.
    Returns (start_epoch, end_epoch) pairs where SOC at end_epoch is
    within soc_tolerance of SOC at start_epoch, and end_epoch - start_epoch
    <= max_gap_seconds - see spec Component 6's "matched cycle pairs
    where SOC returns to roughly its starting level within a short
    window". Greedy: each start_epoch matches at most one (the first
    qualifying) end_epoch, to avoid double-counting overlapping cycles.
    Once a pair is matched, every sample epoch inside its window is
    skipped as a candidate start, so a cycle's own SOC wobble in the
    middle doesn't get double-counted as a nested cycle.

    min_excursion guards against real-world ~1Hz sampling: without it,
    any two adjacent samples are trivially "within soc_tolerance" of each
    other (SOC barely moves in one second), so every single second would
    spuriously qualify as its own "cycle". A candidate end_epoch only
    counts if SOC departed from start_soc by at least min_excursion at
    some point between start_epoch and end_epoch before returning within
    soc_tolerance - i.e. a real charge/discharge round trip, not a flat
    plateau.

    Iterates by index rather than slicing soc_samples[i + 1:] for each
    outer iteration - the slice used to copy O(n) elements on every one
    of the O(n) outer iterations, making this quadratic in wall time on
    real ~1Hz history (60M+ rows over ~2 years)."""
    pairs = []
    consumed_until = None  # end_epoch of the last matched pair, if any
    n = len(soc_samples)
    for i in range(n):
        start_epoch, start_soc = soc_samples[i]
        if consumed_until is not None and start_epoch <= consumed_until:
            continue
        max_excursion_seen = 0.0
        for j in range(i + 1, n):
            end_epoch, end_soc = soc_samples[j]
            if end_epoch - start_epoch > max_gap_seconds:
                break
            excursion = abs(end_soc - start_soc)
            if excursion > max_excursion_seen:
                max_excursion_seen = excursion
            if max_excursion_seen >= min_excursion and abs(end_soc - start_soc) <= soc_tolerance:
                pairs.append((start_epoch, end_epoch))
                consumed_until = end_epoch
                break
    return pairs


def estimate_inverter_loss(conn: sqlite3.Connection, sessions: List[BatterySession]) -> dict:
    """For each charge session, compares AC-side energy drawn (pgrid
    positive = importing, attributable to charging when the battery is
    the only active load driving that import) against pbattery1 energy
    integrated over the same window; symmetric for discharge sessions
    against AC-side energy delivered. Returns
    {'charge': (loss_fraction, sample_count), 'discharge': (loss_fraction, sample_count)}.
    Sessions with fewer than 2 samples are skipped (no meaningful energy
    integral). This is deliberately approximate - see spec Component 6 -
    real-world PV/load noise means a session's AC-side energy isn't
    purely attributable to battery charging/discharging.
    """
    results = {'charge': [], 'discharge': []}
    for session in sessions:
        rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1, pgrid FROM inverter_history "
            "WHERE timestamp_epoch >= ? AND timestamp_epoch <= ? ORDER BY timestamp_epoch",
            (session.start_epoch, session.end_epoch),
        ).fetchall()
        if len(rows) < 2:
            continue
        dc_energy_wh = _trapezoidal_energy_wh([(r[0], abs(r[1])) for r in rows if r[1] is not None])
        ac_energy_wh = _trapezoidal_energy_wh([(r[0], abs(r[2])) for r in rows if r[2] is not None])
        if dc_energy_wh <= 0 or ac_energy_wh <= 0:
            continue
        if session.sign == 'charge':
            loss = 1 - (dc_energy_wh / ac_energy_wh)
        else:
            loss = 1 - (ac_energy_wh / dc_energy_wh)
        results[session.sign].append(loss)

    def _summarize(losses):
        if not losses:
            return None, 0
        return sum(losses) / len(losses), len(losses)

    charge_loss, charge_n = _summarize(results['charge'])
    discharge_loss, discharge_n = _summarize(results['discharge'])
    return {'charge': (charge_loss, charge_n), 'discharge': (discharge_loss, discharge_n)}


def _trapezoidal_energy_wh(power_samples: List[Tuple[int, float]]) -> float:
    """power_samples: [(timestamp_epoch, watts), ...], time-ordered.
    Trapezoidal integration to watt-hours."""
    if len(power_samples) < 2:
        return 0.0
    energy_ws = 0.0
    for (t0, p0), (t1, p1) in zip(power_samples, power_samples[1:]):
        energy_ws += (p0 + p1) / 2.0 * (t1 - t0)
    return energy_ws / 3600.0


def estimate_battery_round_trip_loss(conn: sqlite3.Connection, pairs: List[Tuple[int, int]]) -> Tuple[Optional[float], int]:
    """For each matched cycle pair, integrates pbattery1 energy in
    (positive) vs out (negative, absolute value) across the whole
    window - a round-trip cycle returning to similar SOC should have
    total-in > total-out, the gap being the round-trip loss fraction.
    Returns (average_loss_fraction, sample_count)."""
    losses = []
    for start_epoch, end_epoch in pairs:
        rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1 FROM inverter_history "
            "WHERE timestamp_epoch >= ? AND timestamp_epoch <= ? ORDER BY timestamp_epoch",
            (start_epoch, end_epoch),
        ).fetchall()
        charge_samples = [(t, p) for t, p in rows if p is not None and p > 0]
        discharge_samples = [(t, abs(p)) for t, p in rows if p is not None and p < 0]
        energy_in = _trapezoidal_energy_wh(charge_samples)
        energy_out = _trapezoidal_energy_wh(discharge_samples)
        if energy_in <= 0 or energy_out <= 0 or energy_out > energy_in:
            continue
        losses.append(1 - (energy_out / energy_in))
    if not losses:
        return None, 0
    return sum(losses) / len(losses), len(losses)


def downsample_to_interval(samples: List[Tuple[int, float]], interval_seconds: int) -> List[Tuple[int, float]]:
    """samples: [(timestamp_epoch, value), ...], time-ordered. Returns one
    sample per interval_seconds-wide bucket (the first sample seen in each
    bucket), reducing resolution for expensive downstream algorithms where
    finer-than-interval_seconds precision isn't meaningful - e.g.
    find_matched_cycle_pairs, where real charge/discharge cycles operate
    on the scale of tens of minutes, not seconds. Bucketing by
    `epoch // interval_seconds` (rather than a blind every-Nth-row index
    stride) stays correct even if the real sampling interval isn't exactly
    1 second, e.g. across gaps in the data.
    """
    downsampled = []
    last_bucket = None
    for epoch, value in samples:
        bucket = epoch // interval_seconds
        if bucket != last_bucket:
            downsampled.append((epoch, value))
            last_bucket = bucket
    return downsampled


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)
    ).fetchone()
    return row is not None


def main():
    parser = argparse.ArgumentParser(description="Estimate Predbat battery_loss/inverter_loss_charge/inverter_loss_discharge from history")
    parser.add_argument("--db-path", type=str, default=storage.DATA_DB_PATH)
    parser.add_argument("--noise-threshold-w", type=float, default=DEFAULT_NOISE_THRESHOLD_W)
    parser.add_argument("--max-cycle-gap-seconds", type=int, default=DEFAULT_MAX_CYCLE_GAP_SECONDS)
    parser.add_argument("--soc-tolerance", type=float, default=DEFAULT_SOC_TOLERANCE)
    parser.add_argument("--min-excursion-soc", type=float, default=DEFAULT_MIN_EXCURSION_SOC)
    parser.add_argument("--soc-downsample-seconds", type=int, default=60,
                         help="Downsample SOC samples to one per this many seconds before matching cycle "
                              "pairs - real charge/discharge cycles operate on the scale of tens of minutes "
                              "to hours, so second-level precision buys nothing here and this directly "
                              "shrinks find_matched_cycle_pairs's worst-case window-scan cost.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    try:
        if not _table_exists(conn, "inverter_history"):
            # An empty/never-initialized data.db (e.g. a fresh checkout
            # that hasn't run main.py yet) - report "no data" rather than
            # crashing on a missing table.
            print("No inverter_history table found in the database - nothing to analyze yet.")
            print(f"(db path: {args.db_path})")
            return

        battery_rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1 FROM inverter_history WHERE pbattery1 IS NOT NULL ORDER BY timestamp_epoch"
        ).fetchall()
        soc_rows = conn.execute(
            "SELECT timestamp_epoch, battery_soc FROM inverter_history WHERE battery_soc IS NOT NULL ORDER BY timestamp_epoch"
        ).fetchall()

        sessions = find_battery_sessions(battery_rows, args.noise_threshold_w)
        inverter_loss = estimate_inverter_loss(conn, sessions)
        downsampled_soc_rows = downsample_to_interval(soc_rows, args.soc_downsample_seconds)
        pairs = find_matched_cycle_pairs(downsampled_soc_rows, args.max_cycle_gap_seconds, args.soc_tolerance,
                                          args.min_excursion_soc)
        battery_loss, battery_n = estimate_battery_round_trip_loss(conn, pairs)
    finally:
        conn.close()

    print("Predbat efficiency estimate (approximate - verify plausibility before use):")
    charge_loss, charge_n = inverter_loss['charge']
    discharge_loss, discharge_n = inverter_loss['discharge']
    print(f"  inverter_loss_charge:    {charge_loss:.3f}" if charge_loss is not None else "  inverter_loss_charge:    no data")
    print(f"    (based on {charge_n} charge sessions)")
    print(f"  inverter_loss_discharge: {discharge_loss:.3f}" if discharge_loss is not None else "  inverter_loss_discharge: no data")
    print(f"    (based on {discharge_n} discharge sessions)")
    print(f"  battery_loss:            {battery_loss:.3f}" if battery_loss is not None else "  battery_loss:            no data")
    print(f"    (based on {battery_n} matched charge/discharge cycle pairs)")
    print()
    print("Plausibility check: battery_loss and inverter_loss_* are each")
    print("normally somewhere in 0.02-0.15 for a modern li-ion system.")
    print("A value far outside that range likely means noisy/insufficient")
    print("data rather than a real result - inspect session/cycle counts above.")


if __name__ == '__main__':
    main()
