"""
_estimate_battery_efficiency.py
One-off offline analysis: estimates Predbat's real config settings
`battery_loss` (charge direction), `battery_loss_discharge`, and
`inverter_loss` (one symmetric value, used both directions) from this
household's inverter_history. Run manually; prints a report with
recommended values and sample-size caveats - nothing here writes to
Predbat's config automatically. See PR #35 and GOODWE_SENSOR_NOTES.md
for the sensor-level findings this methodology depends on.

Methodology: classifies each 1Hz sample into one of four mutually
exclusive flow states, requiring grid_mode == Connected throughout (an
islanded/off-grid period has different loss characteristics, and its
meter-derived fields can read zero/stale while DC-side power keeps
flowing normally - see GOODWE_SENSOR_NOTES.md). Contiguous same-state
runs are one "session", each measuring energy on both sides of a
conversion boundary via two independent methods (the inverter's own
cumulative energy counters, and trapezoidal integration of raw power),
summed globally per session type before taking one ratio - not
averaged per-session, since a single short/noisy session's ratio would
otherwise dominate a plain average.

Only three of these four measurements are *clean* (uncontaminated by
another loss source):

- pv_ac: PV production -> the inverter's on-grid AC output
  (`pgrid`+`pgrid2`+`pgrid3` directly - this already covers energy
  delivered to load AND/OR exported, in one number, since it's the
  inverter's own AC-side reading regardless of where that power ends
  up; see GOODWE_SENSOR_NOTES.md on why `pgrid` is not "grid
  import/export"). Battery idle throughout, so this cleanly isolates
  `inverter_loss` - the only path here that doesn't also involve the
  battery.
- pv_charge: PV production -> battery charge, with the inverter's own
  AC output near zero (no AC crossing at all). On a hybrid inverter,
  PV and battery share the DC bus ahead of the single AC/DC stage, so
  this path never touches inverter_loss - the measured gap is
  `battery_loss` (charge direction) alone.

The other two (battery charging/discharging via the AC bus, PV idle)
each measure a *combined* battery+inverter loss, since both directions
cross the inverter:

- battery_ac_charge: battery charging while PV is idle - all of the
  inverter's AC input goes to charging the battery. Combined loss =
  inverter_loss * battery_loss(charge) - given inverter_loss from
  pv_ac, `battery_loss` can be backed out here too, as a second,
  usually much better-populated source than pv_charge.
- battery_ac_discharge: battery discharging while PV is idle - all of
  the inverter's AC output comes from the battery, whether it ends up
  covering load or being exported (no distinction in loss either way,
  since both go through the identical DC->AC stage - there's no
  separate "battery to grid" vs "battery to load" loss). Combined loss
  = inverter_loss * battery_loss_discharge; this is the *only* source
  for battery_loss_discharge; there's no DC-bus bypass for discharging
  the way pv_charge bypasses it for charging.
"""
import argparse
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import storage

# PR #33's ~19%-of-Discharge-samples disagreement (concentrated within
# +/-200W of zero) is cross-register poll timing skew, not power-value
# noise (see GOODWE_SENSOR_NOTES.md) - a magnitude threshold doesn't fix
# a timing problem, so this alone would suggest a modest value; the real
# transition guards are DEFAULT_MIN_SESSION_SECONDS/DEFAULT_EDGE_TRIM_SAMPLES
# below. But real data showed a second, distinct effect: 92% of
# battery_ac_charge sessions at a 30W threshold clustered tightly at
# 31-33W average power (a real BMS self-consumption trickle - see the
# full-SOC hypothesis below - not noise, but not real charging either),
# with a clean gap and zero sessions in [40,60)W before genuine charge
# events resume at 60W+. 60W sits in that gap, cleanly excluding the
# trickle while keeping every real event (verified: discharge-direction
# sessions are already all >=77W, so this doesn't affect that side).
DEFAULT_BATTERY_NOISE_W = 60.0
# Hypothesis: the BMS draws a small, roughly-constant amount of power
# continuously (it's powered by the inverter at all times, not just
# when charging/discharging), which pbattery1 reports as part of
# whatever else is happening - not a threshold to filter out, but a
# bias to correct on every sample. Best empirical estimate: the
# charge-direction trickle cluster found while tuning
# DEFAULT_BATTERY_NOISE_W averaged 31.9W (see GOODWE_SENSOR_NOTES.md) -
# taken as the offset magnitude, sign chosen so a true-idle sample
# (raw ~-31.9W, charge-direction) corrects back to ~0. Threaded through
# as a Thresholds field / --battery-offset-w rather than a global, so
# main() can override it (e.g. for the sweep in GOODWE_SENSOR_NOTES.md)
# without mutating module state.
DEFAULT_BATTERY_OFFSET_W = 31.9
# _grid_phases_agree's noise floor for "is a phase actively signed" -
# see that function's docstring. Exposed the same way as the offset
# above, for the same reason.
DEFAULT_PHASE_AGREE_NOISE_W = 20.0
# Zero, not a small positive margin: any nonzero PV, even a few watts,
# is real DC power reaching the shared PV/battery bus on a hybrid
# inverter, and battery_ac_charge/battery_ac_discharge require PV
# fully absent to attribute 100% of the battery's DC energy to the AC
# path alone - a nonzero-but-"idle" PV margin was found to leak into
# and bias those two measurements (real data: DC-side battery energy
# consistently exceeded the AC-side pgrid measurement, an otherwise
# impossible result, at a 50W PV-idle margin).
DEFAULT_PV_NOISE_W = 0.0
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
# A gap this large between two consecutive same-state samples is not
# normal ~1Hz polling jitter - it's a service restart/deploy or a
# wifi/inverter outage. Without this guard, find_labeled_sessions()
# would bridge straight across the gap: a single trapezoid would span
# whatever time the logger was down, and a counter delta across that
# gap would silently absorb everything that happened while unobserved -
# both far worse than the normal per-sample skew the guards above
# already handle.
DEFAULT_MAX_SAMPLE_GAP_SECONDS = 30
# "inverter AC output idle" (required only by pv_charge, to confirm
# charging is happening via the DC-bus bypass rather than the AC path)
# is a magnitude check on pgrid+pgrid2+pgrid3, using the *correct*
# grid-facing field this time (see GOODWE_SENSOR_NOTES.md).
DEFAULT_GRID_IDLE_SUM_W = 60.0

PV_AC = 'pv_ac'
PV_CHARGE = 'pv_charge'
BATTERY_AC_CHARGE = 'battery_ac_charge'
BATTERY_AC_DISCHARGE = 'battery_ac_discharge'
ALL_STATES = (PV_AC, PV_CHARGE, BATTERY_AC_CHARGE, BATTERY_AC_DISCHARGE)

# Matches static/js/diagram-calc.js's GRID_MODE.CONNECTED - same raw
# grid_mode register, same convention, kept in sync deliberately rather
# than re-derived.
GRID_MODE_CONNECTED = 1


@dataclass
class Thresholds:
    battery_w: float = DEFAULT_BATTERY_NOISE_W
    pv_w: float = DEFAULT_PV_NOISE_W
    grid_idle_w: float = DEFAULT_GRID_IDLE_SUM_W
    phase_agree_w: float = DEFAULT_PHASE_AGREE_NOISE_W
    battery_offset_w: float = DEFAULT_BATTERY_OFFSET_W


@dataclass
class Session:
    state: str
    start_epoch: int
    end_epoch: int


def _sign(value: Optional[float], threshold: float) -> Optional[str]:
    """'pos'/'neg' above `threshold` in magnitude, else None."""
    if value is None:
        return None
    if value > threshold:
        return 'pos'
    if value < -threshold:
        return 'neg'
    return None


def _corrected_pbattery1(pbattery1: Optional[float], offset: float = DEFAULT_BATTERY_OFFSET_W) -> Optional[float]:
    """Applies `offset` as an additive correction, not just a
    classification threshold: the BMS is powered continuously by the
    inverter (hypothesis - see GOODWE_SENSOR_NOTES.md), so this constant
    draw is present in every pbattery1 reading, not only near-idle ones.
    Left uncorrected, it systematically overstates charge-direction
    magnitude and understates discharge-direction magnitude by the same
    amount everywhere, not just at low power - exactly the shape of the
    negative-combined-loss anomaly this was introduced to test."""
    if pbattery1 is None:
        return None
    return pbattery1 + offset


def _battery_direction(pbattery1: Optional[float], threshold: float,
                        offset: float = DEFAULT_BATTERY_OFFSET_W) -> Optional[str]:
    """'charge'/'discharge' - pbattery1's sign convention is NOT what a
    naive reading suggests: verified against 90 days of production data
    in PR #33, positive = discharging, negative = charging (see
    GOODWE_SENSOR_NOTES.md). Classifies on the offset-corrected value,
    consistent with every other use of pbattery1 in this module."""
    sign = _sign(_corrected_pbattery1(pbattery1, offset), threshold)
    if sign == 'pos':
        return 'discharge'
    if sign == 'neg':
        return 'charge'
    return None


def _grid_power_sum(pgrid: Optional[float], pgrid2: Optional[float], pgrid3: Optional[float]) -> Optional[float]:
    """pgrid/pgrid2/pgrid3 is the inverter's own on-grid AC output per
    phase (not a grid import/export meter - see GOODWE_SENSOR_NOTES.md),
    so this is the AC-side magnitude regardless of whether that power
    ends up covering load or being exported."""
    if pgrid is None or pgrid2 is None or pgrid3 is None:
        return None
    return abs(pgrid) + abs(pgrid2) + abs(pgrid3)


def _grid_phases_agree(pgrid: Optional[float], pgrid2: Optional[float], pgrid3: Optional[float],
                        threshold: float = DEFAULT_PHASE_AGREE_NOISE_W) -> bool:
    """True unless two non-negligible phases actively disagree in sign -
    a real but phase-imbalanced flow (e.g. one phase's load pulling
    import while another exports) that isn't attributable to a single
    coherent AC-side energy transfer. Verified on real data: 23%/12% of
    samples within battery_ac_charge/discharge sessions had disagreeing
    phases (see GOODWE_SENSOR_NOTES.md) - summing their absolute values
    would overstate the AC-side magnitude actually attributable to the
    battery."""
    signs = {_sign(v, threshold) for v in (pgrid, pgrid2, pgrid3)}
    signs.discard(None)
    return len(signs) <= 1


def classify_sample(pbattery1: Optional[float], pgrid: Optional[float], pgrid2: Optional[float],
                     pgrid3: Optional[float], ppv: Optional[float], grid_mode: Optional[float],
                     thresholds: Thresholds) -> Optional[str]:
    """Classifies one sample into one of the four flow states (see module
    docstring), or None if it matches none of them cleanly (multiple
    things active at once, everything idle, phase-imbalanced grid flow,
    or grid_mode isn't Connected - an islanded/off-grid period has
    different loss characteristics and shouldn't be attributed to any
    of these four normal-operation paths)."""
    if grid_mode != GRID_MODE_CONNECTED:
        return None
    if not _grid_phases_agree(pgrid, pgrid2, pgrid3, thresholds.phase_agree_w):
        return None
    battery_dir = _battery_direction(pbattery1, thresholds.battery_w, thresholds.battery_offset_w)
    pv_producing = ppv is not None and ppv > thresholds.pv_w
    grid_ac_idle = (_grid_power_sum(pgrid, pgrid2, pgrid3) or 0) < thresholds.grid_idle_w

    if pv_producing and battery_dir is None:
        return PV_AC
    if pv_producing and battery_dir == 'charge' and grid_ac_idle:
        return PV_CHARGE
    if not pv_producing and battery_dir == 'charge':
        return BATTERY_AC_CHARGE
    if not pv_producing and battery_dir == 'discharge':
        return BATTERY_AC_DISCHARGE
    return None


def find_labeled_sessions(rows: List[Tuple[int, Optional[float], Optional[float], Optional[float],
                                            Optional[float], Optional[float], Optional[float]]],
                           thresholds: Thresholds,
                           max_gap_seconds: int = DEFAULT_MAX_SAMPLE_GAP_SECONDS) -> List[Session]:
    """rows: [(epoch, pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode), ...],
    time-ordered. Splits into contiguous runs sharing the same
    classify_sample() label - a sample matching no state, a different
    one, or too large a time gap since the last sample (a service
    restart/outage, not normal polling jitter - see
    DEFAULT_MAX_SAMPLE_GAP_SECONDS) all end the current run."""
    sessions = []
    current_state = None
    current_start = None
    current_end = None
    for epoch, pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode in rows:
        state = classify_sample(pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode, thresholds)
        gapped = current_end is not None and epoch - current_end > max_gap_seconds
        if state != current_state or gapped:
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


def _pgrid_sum_or_none(r: dict, offset: float = DEFAULT_BATTERY_OFFSET_W) -> Optional[float]:
    """`offset` is unused here (pgrid needs no BMS correction) - accepted
    only so every SESSION_SPECS power callable shares one calling
    convention (see _measure_session)."""
    return _grid_power_sum(r['pgrid'], r['pgrid2'], r['pgrid3'])


# Per session-type spec: which raw column(s) represent the "input"
# (pre-conversion-loss) and "output" (post-loss) power/energy-counter
# pair. `same_day` marks e_day (the only column here that resets daily,
# unlike the lifetime e_total_*/e_bat_*_total counters) - a session
# whose e_day delta would otherwise span a midnight rollover has that
# delta measurement skipped (the integral method is unaffected).
# pgrid/pgrid2/pgrid3 has no single matching lifetime energy counter,
# but e_total_exp + e_total_imp summed IS its equivalent - confirmed
# empirically (7 days of real data: pgrid_sum integral 168,087.6Wh vs
# e_total_exp+e_total_imp delta 168,400Wh, within 0.2%) and by register
# layout (both counters live in the same _READ_RUNNING_DATA block as
# pgrid itself, read atomically together - see GOODWE_SENSOR_NOTES.md).
# This was initially misattributed to active_power (a different,
# separately-read register whose integral came out ~40% smaller over
# the same window) - active_power has no energy-counter equivalent at
# all among the fields collected here.
PGRID_ENERGY_COUNTERS = ('e_total_exp', 'e_total_imp')

# Every input_power/output_power callable takes (row, battery_offset_w) -
# a uniform signature so _measure_session can call either without caring
# which side actually uses the offset (only the pbattery1-derived ones do).
SESSION_SPECS: Dict[str, dict] = {
    PV_AC: {
        'input_power': lambda r, offset: r['ppv'],
        'input_counter': 'e_day',
        'input_same_day': True,
        'output_power': _pgrid_sum_or_none,
        'output_counter': PGRID_ENERGY_COUNTERS,
        'output_same_day': False,
    },
    PV_CHARGE: {
        'input_power': lambda r, offset: r['ppv'],
        'input_counter': 'e_day',
        'input_same_day': True,
        'output_power': lambda r, offset: _abs_or_none(_corrected_pbattery1(r['pbattery1'], offset)),
        'output_counter': 'e_bat_charge_total',
        'output_same_day': False,
    },
    BATTERY_AC_CHARGE: {
        'input_power': _pgrid_sum_or_none,
        'input_counter': PGRID_ENERGY_COUNTERS,
        'input_same_day': False,
        'output_power': lambda r, offset: _abs_or_none(_corrected_pbattery1(r['pbattery1'], offset)),
        'output_counter': 'e_bat_charge_total',
        'output_same_day': False,
    },
    BATTERY_AC_DISCHARGE: {
        'input_power': lambda r, offset: _abs_or_none(_corrected_pbattery1(r['pbattery1'], offset)),
        'input_counter': 'e_bat_discharge_total',
        'input_same_day': False,
        'output_power': _pgrid_sum_or_none,
        'output_counter': PGRID_ENERGY_COUNTERS,
        'output_same_day': False,
    },
}

SESSION_ROW_COLUMNS = ('timestamp_epoch', 'timestamp', 'pbattery1', 'pgrid', 'pgrid2', 'pgrid3', 'ppv',
                        'e_bat_charge_total', 'e_bat_discharge_total', 'e_day', 'e_total_exp', 'e_total_imp')


@dataclass
class SessionTypeTotals:
    session_count: int = 0
    # Paired totals: only accumulated together, from sessions where BOTH
    # sides had a usable delta - loss_delta()'s ratio is only meaningful
    # across the same set of sessions on both sides.
    input_delta_wh: float = 0.0
    output_delta_wh: float = 0.0
    delta_sessions: int = 0
    input_integral_wh: float = 0.0
    output_integral_wh: float = 0.0
    # Diagnostic-only, single-side totals: accumulated independently of
    # the other side, so the counter-backed side (e.g. battery, when
    # pgrid has no matching energy counter at all - see
    # GOODWE_SENSOR_NOTES.md) can still be compared against its own
    # integral even when a full paired loss_delta() is impossible. The
    # integral half is summed only over the SAME sessions that
    # contributed a delta, so the two numbers describe the same
    # population rather than being biased by session-count differences.
    input_delta_wh_any: float = 0.0
    input_integral_wh_when_delta_any: float = 0.0
    input_delta_sessions_any: int = 0
    output_delta_wh_any: float = 0.0
    output_integral_wh_when_delta_any: float = 0.0
    output_delta_sessions_any: int = 0

    def loss_delta(self) -> Optional[float]:
        if self.delta_sessions == 0 or self.input_delta_wh <= 0:
            return None
        return 1 - (self.output_delta_wh / self.input_delta_wh)

    def loss_integral(self) -> Optional[float]:
        if self.input_integral_wh <= 0:
            return None
        return 1 - (self.output_integral_wh / self.input_integral_wh)

    def _side_precision_note(self, side: str) -> Optional[str]:
        """Compares a counter-backed side's delta sum against its own
        integral sum, over the same sessions - a direct precision
        cross-check independent of whether the other side has a counter
        at all."""
        delta_wh = getattr(self, f'{side}_delta_wh_any')
        integral_wh = getattr(self, f'{side}_integral_wh_when_delta_any')
        n = getattr(self, f'{side}_delta_sessions_any')
        if n == 0 or integral_wh <= 0:
            return None
        pct_diff = (delta_wh - integral_wh) / integral_wh * 100.0
        return f"{side}: delta={delta_wh:.1f}Wh vs integral={integral_wh:.1f}Wh ({pct_diff:+.1f}%, n={n})"

    def precision_notes(self) -> List[str]:
        notes = [self._side_precision_note('input'), self._side_precision_note('output')]
        return [n for n in notes if n is not None]


def _measure_session(conn: sqlite3.Connection, session: Session, spec: dict,
                      edge_trim_samples: int = DEFAULT_EDGE_TRIM_SAMPLES,
                      battery_offset_w: float = DEFAULT_BATTERY_OFFSET_W) -> Optional[dict]:
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

    input_series = [(r['timestamp_epoch'], spec['input_power'](r, battery_offset_w)) for r in dict_rows]
    output_series = [(r['timestamp_epoch'], spec['output_power'](r, battery_offset_w)) for r in dict_rows]
    input_integral_wh = _trapezoidal_energy_wh([(t, v) for t, v in input_series if v is not None])
    output_integral_wh = _trapezoidal_energy_wh([(t, v) for t, v in output_series if v is not None])

    first, last = dict_rows[0], dict_rows[-1]

    def counter_delta(counter_names, same_day: bool) -> Optional[float]:
        """counter_names is a single column name, or a tuple of names to
        sum (pgrid has no single matching energy counter - its
        equivalent is e_total_exp + e_total_imp summed; see
        GOODWE_SENSOR_NOTES.md)."""
        if counter_names is None:
            return None
        if isinstance(counter_names, str):
            counter_names = (counter_names,)
        if same_day and first['timestamp'][:10] != last['timestamp'][:10]:
            return None
        total_wh = 0.0
        for counter_name in counter_names:
            start_val, end_val = first[counter_name], last[counter_name]
            if start_val is None or end_val is None:
                return None
            total_wh += (float(end_val) - float(start_val)) * 1000.0  # kWh -> Wh
        return total_wh

    input_delta_wh = counter_delta(spec['input_counter'], spec['input_same_day'])
    output_delta_wh = counter_delta(spec['output_counter'], spec['output_same_day'])

    return {
        'input_integral_wh': input_integral_wh,
        'output_integral_wh': output_integral_wh,
        'input_delta_wh': input_delta_wh,
        'output_delta_wh': output_delta_wh,
    }


def measure_sessions(conn: sqlite3.Connection, sessions: List[Session],
                      edge_trim_samples: int = DEFAULT_EDGE_TRIM_SAMPLES,
                      battery_offset_w: float = DEFAULT_BATTERY_OFFSET_W) -> Dict[str, SessionTypeTotals]:
    """Sums input/output energy (both by counter-delta and by power
    integration) across every session of each type, then exposes one
    loss ratio per type per method via SessionTypeTotals - energy-
    weighted, not a plain average of per-session ratios (see module
    docstring).

    Note `battery_offset_w` only affects the integral method: it
    corrects the raw pbattery1 *power* series (see _corrected_pbattery1),
    but the counter-delta method reads e_bat_charge_total/
    e_bat_discharge_total directly, which cannot be offset-corrected
    after the fact. Sweeping --battery-offset-w therefore only
    meaningfully moves the integral-based loss numbers directly - it
    only affects the delta-based ones indirectly, via which sessions get
    classified into which state in the first place."""
    totals = {state: SessionTypeTotals() for state in ALL_STATES}
    for session in sessions:
        spec = SESSION_SPECS[session.state]
        measurement = _measure_session(conn, session, spec, edge_trim_samples, battery_offset_w)
        if measurement is None:
            continue
        t = totals[session.state]
        t.session_count += 1
        t.input_integral_wh += measurement['input_integral_wh']
        t.output_integral_wh += measurement['output_integral_wh']
        input_delta = measurement['input_delta_wh']
        output_delta = measurement['output_delta_wh']
        if input_delta is not None and input_delta > 0:
            t.input_delta_wh_any += input_delta
            t.input_integral_wh_when_delta_any += measurement['input_integral_wh']
            t.input_delta_sessions_any += 1
        if output_delta is not None and output_delta > 0:
            t.output_delta_wh_any += output_delta
            t.output_integral_wh_when_delta_any += measurement['output_integral_wh']
            t.output_delta_sessions_any += 1
        # Both sides must have actually ticked, not just be non-None:
        # counter resolution is 0.1kWh, so the smaller (post-loss) side
        # ticks less often than the input side. A session where input
        # ticked but output read exactly 0.0 isn't "zero output energy" -
        # it's "too little energy to move a 0.1kWh counter yet" - and
        # counting it here would silently score that session as loss=1.0,
        # biasing loss_delta() upward. Same reasoning as the output_delta
        # > 0 check just above, applied to the paired total.
        if input_delta is not None and output_delta is not None and input_delta > 0 and output_delta > 0:
            t.input_delta_wh += input_delta
            t.output_delta_wh += output_delta
            t.delta_sessions += 1
    return totals


@dataclass
class EfficiencyEstimate:
    inverter_loss: Optional[float]  # from pv_ac alone - the only uncontaminated path
    battery_loss: Optional[float]  # charge direction, preferring battery_ac_charge (usually better populated)
    battery_loss_pv_charge: Optional[float]  # cross-check via the DC-bus-bypass path, if any data
    battery_loss_discharge: Optional[float]


def _derive_battery_loss(combined_loss: Optional[float], inverter_loss: Optional[float]) -> Optional[float]:
    """battery_ac_charge/battery_ac_discharge each measure a *combined*
    battery+inverter loss, since both directions cross the inverter.
    Backs out the battery-only factor: combined_efficiency =
    battery_efficiency * inverter_efficiency."""
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

    inverter_loss = loss_fn(totals[PV_AC])
    battery_loss = _derive_battery_loss(loss_fn(totals[BATTERY_AC_CHARGE]), inverter_loss)
    battery_loss_pv_charge = loss_fn(totals[PV_CHARGE])
    battery_loss_discharge = _derive_battery_loss(loss_fn(totals[BATTERY_AC_DISCHARGE]), inverter_loss)

    return EfficiencyEstimate(
        inverter_loss=inverter_loss,
        battery_loss=battery_loss,
        battery_loss_pv_charge=battery_loss_pv_charge,
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
    parser.add_argument("--battery-offset-w", type=float, default=DEFAULT_BATTERY_OFFSET_W,
                         help="BMS self-consumption trickle added back to every pbattery1 reading before "
                              "classifying/measuring it (see DEFAULT_BATTERY_OFFSET_W). Sweep this to see its "
                              "effect on battery_loss/battery_loss_discharge - note it only moves the "
                              "integral-based numbers directly (see measure_sessions' docstring).")
    parser.add_argument("--battery-noise-w", type=float, default=DEFAULT_BATTERY_NOISE_W)
    parser.add_argument("--grid-idle-w", type=float, default=DEFAULT_GRID_IDLE_SUM_W)
    parser.add_argument("--pv-noise-w", type=float, default=DEFAULT_PV_NOISE_W)
    parser.add_argument("--phase-agree-w", type=float, default=DEFAULT_PHASE_AGREE_NOISE_W,
                         help="Noise floor below which a pgrid/pgrid2/pgrid3 phase doesn't count as actively "
                              "signed for the phase-sign-agreement check (see _grid_phases_agree).")
    parser.add_argument("--min-session-seconds", type=int, default=DEFAULT_MIN_SESSION_SECONDS,
                         help="Discard sessions shorter than this - at Goodwe's non-atomic per-register "
                              "poll cadence, a session this short is more likely a transition artifact "
                              "(different sensors momentarily disagreeing about a state change) than a "
                              "real sustained flow.")
    parser.add_argument("--edge-trim-samples", type=int, default=DEFAULT_EDGE_TRIM_SAMPLES,
                         help="Drop this many samples off each session's start/end before measuring "
                              "energy on it, for the same cross-register skew reason.")
    parser.add_argument("--max-gap-seconds", type=int, default=DEFAULT_MAX_SAMPLE_GAP_SECONDS,
                         help="Split a session wherever consecutive samples are more than this far apart - "
                              "a real outage/restart, not normal polling jitter (see DEFAULT_MAX_SAMPLE_GAP_SECONDS).")
    args = parser.parse_args()

    thresholds = Thresholds(battery_w=args.battery_noise_w, grid_idle_w=args.grid_idle_w,
                             pv_w=args.pv_noise_w, phase_agree_w=args.phase_agree_w,
                             battery_offset_w=args.battery_offset_w)

    conn = sqlite3.connect(args.db_path)
    try:
        if not _table_exists(conn, "inverter_history"):
            print("No inverter_history table found in the database - nothing to analyze yet.")
            print(f"(db path: {args.db_path})")
            return

        # Streamed via the cursor (not .fetchall()) - a full year of history
        # is tens of millions of rows, and find_labeled_sessions() only ever
        # needs one forward pass, so materializing them all into a Python
        # list first just wastes memory (OOM'd a 3.7GB Raspberry Pi on a
        # real 16M-row/365-day sample before this change).
        rows = conn.execute(
            "SELECT timestamp_epoch, pbattery1, pgrid, pgrid2, pgrid3, ppv, grid_mode FROM inverter_history "
            "ORDER BY timestamp_epoch"
        )
        sessions = find_labeled_sessions(rows, thresholds, args.max_gap_seconds)
        sessions = filter_min_duration(sessions, args.min_session_seconds)
        totals = measure_sessions(conn, sessions, args.edge_trim_samples, thresholds.battery_offset_w)
    finally:
        conn.close()

    delta_estimate = estimate_efficiency(totals, method='delta')
    integral_estimate = estimate_efficiency(totals, method='integral')

    print("Predbat efficiency estimate (approximate - verify plausibility before use):")
    print()
    for label, state in (("pv_ac", PV_AC), ("pv_charge", PV_CHARGE),
                         ("battery_ac_charge", BATTERY_AC_CHARGE), ("battery_ac_discharge", BATTERY_AC_DISCHARGE)):
        t = totals[state]
        print(f"  [{label}] sessions: {t.session_count} (delta-eligible: {t.delta_sessions})")
        print(f"    loss via energy-counter delta: {_fmt_loss(t.loss_delta())}")
        print(f"    loss via power integration:    {_fmt_loss(t.loss_integral())}")
        for note in t.precision_notes():
            print(f"    counter-vs-integral precision, {note}")
    print()
    print("Recommended apps.yaml values:")
    print(f"  inverter_loss             (delta-based):   {_fmt_loss(delta_estimate.inverter_loss)}")
    print(f"  inverter_loss             (integral-based): {_fmt_loss(integral_estimate.inverter_loss)}")
    print(f"  battery_loss              (delta-based):   {_fmt_loss(delta_estimate.battery_loss)}")
    print(f"  battery_loss              (integral-based): {_fmt_loss(integral_estimate.battery_loss)}")
    print(f"  battery_loss (pv_charge cross-check, delta):    {_fmt_loss(delta_estimate.battery_loss_pv_charge)}")
    print(f"  battery_loss (pv_charge cross-check, integral): {_fmt_loss(integral_estimate.battery_loss_pv_charge)}")
    print(f"  battery_loss_discharge    (delta-based):   {_fmt_loss(delta_estimate.battery_loss_discharge)}")
    print(f"  battery_loss_discharge    (integral-based): {_fmt_loss(integral_estimate.battery_loss_discharge)}")
    print()
    print("If the delta-based and integral-based numbers disagree substantially, the")
    print("energy counters (0.1 kWh resolution) are likely too coarse relative to typical")
    print("session size on this system - prefer the integral-based numbers in that case.")
    print("Plausibility check: all three settings are normally somewhere in 0.02-0.15 for")
    print("a modern li-ion system. A value far outside that range likely means noisy or")
    print("insufficient data rather than a real result - inspect the session counts above.")


if __name__ == '__main__':
    main()
