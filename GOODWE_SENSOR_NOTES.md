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
  second, independent reading of the same net grid quantity - almost
  certainly from an external CT/smart-meter accessory rather than the
  inverter's own internal sensing. Verified empirically against 7 days
  of real data, grouped by `grid_in_out`: idle → 3.9W avg, exporting →
  +1544W avg, importing → -448W avg. Same sign convention as
  `active_power`, and the two agree closely - either is usable as "the
  grid meter"; `meter_active_power_total` is one physical layer more
  independent from the inverter's own PV/battery sensing, if that
  matters for cross-checking.
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

## Noise / cross-sensor disagreement, verified against this hardware

- **±200W band**: PR #33 found `pbattery1`'s sign disagrees with the
  coarser `battery_mode` label for ~19% of all Discharge-mode samples,
  concentrated (34%) within a ±200W band around zero. Any threshold
  meant to separate "real activity" from noise on this hardware should
  be at least this wide - a naive 20W threshold produces a median
  session length of 5 seconds (pure noise-driven flapping), not real
  events.
- **Cross-register poll skew**: Goodwe's protocol reads registers
  sequentially, not atomically, so different sensors (`pbattery1`,
  `pgrid*`, `ppv`) can reflect slightly different real instants within
  the same poll. This shows up specifically at genuine state
  transitions (a few seconds where the sample set is internally
  inconsistent) - not as steady-state noise, so a magnitude threshold
  doesn't help. Guard by rejecting too-short sessions and trimming a
  couple of samples off each session's start/end.
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
