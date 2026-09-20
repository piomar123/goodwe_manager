# Goodwe sensor notes: directions, grid meters, noise, and quirks

Reference for anything computing energy/power balances across PV, battery,
grid, and load fields (`_estimate_battery_efficiency.py`, `_calculate_income.py`,
the dashboard diagram). Register offsets and raw type (signed/unsigned,
2/4 bytes) are already documented in the `goodwe` library's source
(`goodwe/et.py`, `goodwe/sensor.py`) - not repeated here except where
needed for context. This file covers what the library does **not**
document: which field is the actual grid meter, sign conventions the
library leaves unstated, and noise/quirks verified against this
household's real data.

## Which field is the grid meter?

There are two, unrelated grid-facing quantities - easy to conflate, and we did:

- **`pgrid`/`pgrid2`/`pgrid3`** ("On-grid L1/L2/L3 Power", register
  35125/35130/35135) is the **inverter's own AC output** onto the
  grid-tied bus per phase - whatever the inverter is producing from
  PV and/or battery combined, regardless of whether that power ends up
  covering house load or actually leaving through the utility meter.
  It is *not* a grid import/export reading.
- **`active_power`** ("Active Power", register 35140, `Kind.GRID`) is
  the true net utility grid power: **negative = importing, positive =
  exporting**. This is confirmed directly from the library's own
  `read_grid_mode()` (`goodwe/sensor.py`), which derives `grid_in_out`/
  `grid_in_out_label` from this exact register with a ±90W deadband
  (`value < -90` → Importing, `value >= 90` → Exporting, else Idle) -
  that deadband is the library's own documented noise threshold for
  grid-idle classification, not something to reinvent.
- **`meter_active_power1/2/3`/`meter_active_power_total`** (register
  36019-36025, a different register block from `active_power`) is a
  second reading of the same net grid quantity. There is only one
  physical external CT/smart-meter accessory on these hybrid inverters
  (it's the only way the inverter could know what crosses the utility
  connection at all), and the library confirms these two fields come
  from genuinely separate Modbus reads of it: `_READ_RUNNING_DATA`
  (0x891C, where `active_power` lives) vs. the dedicated
  `_READ_METER_DATA`/`_READ_METER_DATA_EXTENDED[2]` (0x8CA0, where
  `meter_active_power*` lives) - issued as two sequential requests per
  poll, not one atomic one. `active_power` is almost certainly the
  inverter firmware's own real-time-control copy of the same meter feed,
  cached into its main telemetry block; `meter_active_power*` is that
  meter queried directly. Same sign convention, verified empirically
  against 7 days of real data grouped by `grid_in_out`: idle → 3.9W avg,
  exporting → +1544W avg, importing → -448W avg - the two fields agree
  closely but not exactly, consistent with being two separate reads of
  one sensor rather than one shared register.
- **`house_consumption`** (a `Calculated` sensor in `goodwe/et.py`) is
  documented in the library's own source as `ppv1+ppv2+ppv3+ppv4 +
  pbattery1(signed) - active_power(signed)` - i.e. exactly the KCL
  relationship `load = inverter_output_equivalent - grid_net`, using
  `active_power` (not `pgrid`) as the grid term. Despite being a
  documented formula, `house_consumption` is unreliable in practice:
  summing/differencing several registers that aren't read atomically
  produces real error during battery ramps (occasionally even negative,
  which is physically impossible for a load reading) - prefer
  `load_ptotal` for load tracking instead.
- **`load_ptotal`** (register 35172, "Load", `Kind.AC`) is a directly
  reported register, not a library-side calculation - presumably the
  inverter firmware's own internal equivalent of `pgrid_sum -
  active_power`. Since `pgrid`/`pgrid2`/`pgrid3` are already a direct
  hardware reading of "AC output regardless of destination," there is
  no need to reconstruct that same quantity from `load_ptotal` plus the
  grid meter - use `pgrid` directly instead of round-tripping through
  load and meter to get back to (approximately) the same number.

## `pbattery1` sign

**Not documented by the library** (`Power4S`, "Battery Power" - no
semantic sign notes). Verified against 90 days of production data in
this repo's `battery-grid-direction-from-sign` work (PR #33):
**positive = discharging, negative = charging.**

That same investigation found real, physical BMS self-consumption: a
~-30W idle trickle even in Standby mode is not sensor noise or
calibration bias - it's real, always-flowing power (per PR #33's
commit message and `diagram-calc.js`'s comment above `batteryState()`).

One hypothesis (unconfirmed) for *why* PR #33 found `pbattery1` reading
negative in ~19% of samples labeled Discharge by `battery_mode`: when
the battery is full, the BMS caps max charge current at 0A (nothing
more to charge), but the BMS itself still draws a small amount of power
for its own operation - which could show up as a small negative
(charge-direction) reading even while the mode label still says
Discharge. If true, this is a distinct, real steady-state phenomenon
tied to full-SOC specifically, separate from (and additional to) the
cross-register poll timing skew described below, which is a transient
effect around any state transition regardless of SOC.

## Noise / cross-sensor disagreement, verified against this hardware

- **Cross-register poll skew, not power-value noise**: Goodwe's
  protocol reads registers sequentially, not atomically - different
  sensors (`pbattery1`, `pgrid*`, `ppv`, and `active_power` vs.
  `meter_active_power*` from an entirely separate Modbus command; see
  above) can reflect slightly different real instants within the same
  poll. PR #33's finding that `pbattery1`'s sign disagrees with the
  coarser `battery_mode` label for ~19% of Discharge-mode samples
  (concentrated within ±200W of zero) is this same timing effect, not
  a noisy power reading - it shows up specifically at genuine state
  transitions (a few seconds where the sample set is internally
  inconsistent), not as steady-state noise. A magnitude threshold
  doesn't fix a timing problem: the real guards are rejecting
  too-short sessions and trimming a couple of samples off each
  session's start/end, which is what actually excludes the transition
  window rather than papering over it with an oversized noise floor.
- **Islanding zeroes out meter-derived fields**: during an
  islanded/off-grid period (`grid_mode != Connected` - see
  `GRID_MODES`/`grid_mode_label`), `active_power`/`meter_active_power*`
  and `load_ptotal` can read zero/stale (no functioning grid
  connection to measure), while PV/battery DC-side readings keep
  reporting normally. A sample that otherwise looks like clean
  PV-only or battery-only flow can actually be an outage - always gate
  on `grid_mode == Connected` when isolating a clean energy path.
- **`total_inverter_power`** (register 35138, distinct from `pgrid`
  and `active_power`) has its own confirmed reporting gap around
  1150-1200W regardless of battery mode (see
  `docs/superpowers/mockups/battery-negative-power-vs-sumabs-pgrid.html`)
  - avoid it as an AC-side quantity; use `pgrid`+`pgrid2`+`pgrid3`
  instead, which has continuous coverage through the same range.

## Power sensors and their energy-counter equivalents

| Power sensor | Lifetime energy counter | Today-only counter |
|---|---|---|
| `ppv` (= ppv1+ppv2+ppv3+ppv4) | `e_total` | `e_day` |
| `pbattery1` > 0 (discharging) | `e_bat_discharge_total` | `e_bat_discharge_day` |
| `pbattery1` < 0 (charging) | `e_bat_charge_total` | `e_bat_charge_day` |
| `pgrid`/`pgrid2`/`pgrid3` (sum, either direction) | `e_total_exp` + `e_total_imp` (summed) | `e_day_exp` + `e_day_imp` (summed) |
| `meter_active_power_total`/`1`/`2`/`3` (exporting) | `meter_e_total_exp`/`exp1`/`exp2`/`exp3` | - (no today-only variant) |
| `meter_active_power_total`/`1`/`2`/`3` (importing) | `meter_e_total_imp`/`imp1`/`imp2`/`imp3` | - (no today-only variant) |
| `load_ptotal` | `e_load_total` | `e_load_day` |
| `active_power` (exporting) | `meter_e_total_exp` (via `meter_active_power_total` - see below) | - |
| `active_power` (importing) | `meter_e_total_imp` (via `meter_active_power_total` - see below) | - |

`active_power` has no counter of its own in the `_READ_RUNNING_DATA`
block. It doesn't need one: `active_power` and `meter_active_power_total`
are the same physical net-grid quantity (see above), so `active_power`'s
practical energy equivalent is `meter_active_power_total`'s own counters,
`meter_e_total_exp`/`meter_e_total_imp` - just read from the separate
`_READ_METER_DATA` block rather than being paired 1:1 by register
proximity.

**This was initially misattributed the other way around** (`e_total_exp`/
`e_total_imp` assumed to belong to `active_power`, `pgrid` assumed to have
no counter) - corrected after empirically integrating both over 7 real
days: `pgrid_sum`'s integral (168,087.6 Wh) matches `e_total_exp +
e_total_imp`'s delta (168,400 Wh) within 0.2%, while `active_power`'s
integral (102,084.7 Wh) is ~40% off from both. `active_power` (35140) and
`e_total_exp`/`e_total_imp` (35195/35200) are all read together in the
same `_READ_RUNNING_DATA` block as `pgrid` (35125-35135) - register
proximity alone doesn't tell you which counter belongs to which power
field, which is exactly why this was misattributed the first time; only
the empirical integral comparison settles it.

## Energy counter units

`e_total_imp`/`e_total_exp`/`e_bat_charge_total`/`e_bat_discharge_total`/
`e_load_total`/`e_day` are all **kWh**, via the library's `Energy4`
class - 4-byte register, 0.1 kWh resolution (`value / 10`). That
resolution matters for short-session measurements: a 5-second session
at a few hundred watts moves these counters by much less than one
tick, so a delta-based measurement needs either a long enough session
or many sessions summed together before it means anything -
`meter_e_total_exp`/`meter_e_total_imp` instead use `Energy4W`
(`value / 1000`), a finer resolution from the separate smart-meter
register block.
