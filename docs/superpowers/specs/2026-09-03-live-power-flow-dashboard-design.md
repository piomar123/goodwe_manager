# Live power-flow dashboard — design

## Purpose

Replace the plain-text runtime-data block at the top of `templates/index.html`
(everything above the `<hr>`, i.e. PV/Battery/Load/Backup/Output/Grid/Meter/
Diagnose/Hourly rows) with a graphical power-flow diagram: a physically
accurate node/arrow layout showing PV, Inverter, Battery, Backup, Grid and
Load, with arrow thickness/color/direction encoding live power flow. The raw
JSON dump below the `<hr>` (`#event-target`) is untouched.

Almost no backend/data-model changes are needed — nearly every field the
diagram uses was already present in `SELECTED_SENSORS` (`sensors.py`) and
already flows to the browser once per second via the existing `/listen` SSE
endpoint (`main.py`'s `stream_messages` → `EventSource('/listen')` in
`index.html`). One exception, caught mid-investigation: `work_mode` and
`battery_mode` (numeric) were missing from `SELECTED_SENSORS` - see Color
and status rules below for what was added and why it's low-risk. Otherwise
this is a frontend-only change (HTML/CSS/JS in `templates/index.html`, plus
a new small JS module for the diagram if it grows large enough to warrant
splitting out).

## Layout

Single-column layout, physically accurate to the actual AC wiring (grid and
load share one connection point; backup is a separate isolated output), and
usable on both mobile and desktop (capped max-width, centered on wide
screens; the vertical arrangement below is inherently mobile-friendly since
it's already a narrow column):

```
                     [ PV ]
                       |
   [Battery] — [Inverter] — [Junction] — [Grid]
                       |          |
                   [Backup]    [Load]
```

- **PV → Inverter**: straight down, centered above the Inverter node.
  Always one-way (PV can't draw power). Dashed/grey at 0 W.
- **Battery ↔ Inverter**: left branch, bidirectional. Arrowhead direction
  flips with charge/discharge; color redundantly encodes the same thing (see
  Color rules below) so direction is legible even at a glance / in bright
  light on mobile.
- **Inverter → Backup**: straight down from the Inverter (not through the
  Junction — backup is an isolated relay output on GoodWe hybrids, not the
  same physical connection as grid/load). One-way, always rendered, greyed
  and thin at 0 W (no layout shift when backup is unused).
- **Inverter → Junction → Grid**: rightward branch. Bidirectional
  (import/export).
- **Junction → Load**: down from the Junction. One-way (the house only
  consumes). Junction node exists purely to visually express that Grid and
  Load are the same physical AC connection, not two independent inverter
  outputs.

## Data mapping

All values come from the existing SSE payload (same keys as
`SELECTED_SENSORS`). Power values are displayed in whole watts everywhere
(no kW switching, no fractional precision) — this keeps small flows (e.g. a
~30 W battery standby trickle, confirmed real via production `data.db`, see
Idle-battery investigation below) visible and exact, consistent with all
other nodes.

Arrow thickness scales **linearly** with the absolute power value, with a
minimum visible floor thickness so small flows don't disappear (kept
intentionally simple; can move to a log scale later if linear turns out to
compress the visible range too much — noted as a candidate follow-up, not a
requirement now).

| Node/edge | Primary value(s) | Extra details (inline expand) |
|---|---|---|
| PV | `ppv` | `ppv1`/`vpv1`, `ppv2`/`vpv2` (per-string) |
| Battery | `abs(pbattery1)`, `battery_soc` (always visible, not hidden) | `vbattery1`, `battery_temperature` |
| Inverter | `work_mode_label` (status text) | `temperature_air`, `temperature` |
| Backup | `backup_ptotal` | `backup_i1`/`backup_i2`/`backup_i3` (each shown red when ≥ 13.5A) |
| Inverter → Junction | `pgrid`+`pgrid2`+`pgrid3` (inverter's own AC output onto the shared bus — see Verified data mappings below) | — |
| Junction → Grid | `meter_active_power_total` (actual utility import/export) | `meter_active_power1`/`meter_active_power2`/`meter_active_power3`, `vgrid`/`vgrid2`/`vgrid3`, `fgrid`/`fgrid2`/`fgrid3` |
| Load | `load_ptotal` | `load_p1`/`load_p2`/`load_p3` |

Extra details use an **inline expand** interaction: tapping/clicking a node
expands its details directly under/beside that node (not a shared panel, not
an ephemeral tooltip). Multiple nodes can be expanded at once. The layout is
allowed to shift when a node's details open — accepted tradeoff for keeping
details spatially tied to their node.

## Color and status rules

All color/state logic is driven off the **numeric** enum codes
(`work_mode`, `grid_mode`, `battery_mode`, `grid_in_out`), not the
human-readable `_label` strings — robust against label text changes, and
matches how the `goodwe` library (family `ET`, hardcoded in `main.py`)
actually models these.

**`work_mode` and `battery_mode` were missing from `SELECTED_SENSORS`** -
only their `_label` text versions were present (`grid_mode`/`grid_in_out`
already had both). This was caught mid-investigation, not assumed away:
confirmed against the `goodwe` library source (`et.py`) that both exist as
plain `Integer` sensors right next to their `_label` counterparts (register
35187 and 35184 respectively), and added them to `SELECTED_SENSORS` in
`sensors.py`. This is a small, low-risk backend change, not the "no backend
changes" violation it might look like: `storage.py`'s
`_reconcile_table_columns_*` already auto-`ALTER TABLE ADD COLUMN`s for any
sensor present in `SELECTED_SENSORS` but missing from an existing
`data.db` on startup - no manual migration needed, confirmed by running the
full test suite (133 passed) after the change. The two most load-bearing
enums:

```
work_mode (ET):  0 Wait Mode | 1 Normal (On-Grid) | 2 Normal (Off-Grid)
                 | 3 Fault Mode | 4 Flash Mode | 5 Check Mode
grid_mode:       0 Not connected to grid | 1 Connected to grid | 2 Fault
```

- **Battery**: direction/color come from numeric `battery_mode` (per
  `BATTERY_MODES` in the `goodwe` library: 0 No battery, 1 Standby,
  2 Discharge, 3 Charge, 4 To be charged, 5 To be discharged; `battery_mode_label`
  is the matching display text) — **not** from the sign of `pbattery1`,
  which is unreliable (verified against production data: both a confirmed
  `Charge` sample and a confirmed `Discharge` sample had negative
  `pbattery1`). Green = Charge (or "To be charged"), orange = Discharge (or
  "To be discharged"), red = at/below `battery_discharge_limit` SoC floor
  (overrides charge/discharge color), grey = Standby/No battery.

  **Standby draws an undirected line (no arrowhead); No-battery draws no
  line at all.** The battery still shows real, nonzero power at idle (the
  ~30W standby trickle from the idle-battery investigation above) - but 30
  days of production data (`|pbattery1| < 50W`, `ppv = 0`) is 98.6%
  `Standby` (48,285 samples) and only 1.4% `Discharge` (672 samples), i.e.
  this trickle is essentially always reported as `Standby`, not
  `Discharge`. Standby has no defined charge/discharge direction, and
  `pbattery1`'s sign is the value already established as unreliable - so
  there is no trustworthy way to know which way that trickle is actually
  flowing. Rather than assert a direction we don't know, the
  Battery↔Inverter line renders as a plain line (magnitude/thickness still
  reflects the real wattage) with no arrowhead when `battery_mode` is
  Standby; the arrowhead only appears for Charge/Discharge/To-be-charged/
  To-be-discharged, where direction is actually known. No-battery is a
  different case - there's no battery to show a wattage for, so instead
  of a zero-ish plain line, the Battery↔Inverter line is omitted entirely
  and the Battery node itself is faded to half-opacity.
- **Grid**: color/state come from numeric `grid_in_out` (per
  `GRID_IN_OUT_MODES`: 0 Idle, 1 Exporting, 2 Importing; `grid_in_out_label`
  is the matching display text) on the Junction→Grid edge — green =
  Exporting, orange = Importing, grey = Idle. Additionally: `grid_mode == 2`
  (Fault) → grid icon **red** (overrides the above) **and** the
  Inverter–Junction segment (the shared bus) is drawn crossed out, since
  the inverter isn't outputting to that bus at all. `grid_mode == 0` (Not
  connected) → Inverter–Junction segment also crossed out, icon stays grey
  (no fault, just no connection). In both cases the Junction→Grid segment
  itself is left uncrossed and keeps rendering from `grid_in_out`/
  `meter_active_power_total` as normal - the utility meter reads the
  actual grid independently of the inverter's connection state, so that
  segment can still reflect real import/export happening on the grid side
  of the disconnect; only the inverter's own path to the bus is what's
  actually severed.
- **Inverter**: icon color from `work_mode` — 0 Wait=grey, 1 Normal
  (On-Grid)=green, 2 Normal (Off-Grid)=**pink**, 3 Fault=red, 4 Flash=orange,
  5 Check=yellow. Status text shown on the node is `work_mode_label` as-is.
- **Backup**: each of `backup_i1`/`backup_i2`/`backup_i3` (shown in the
  Backup node's extra details) is displayed in red when that phase's
  current is ≥ 13.5A, otherwise normal text color.

## Technical approach

- **LeaderLine** (small, dependency-free JS library, loaded via CDN like
  Chart.js/Bootstrap already are) draws the SVG arrows between plain
  HTML/CSS node boxes. Nodes are ordinary responsive elements (flex/grid
  layout) so mobile reflow is normal CSS; LeaderLine only owns the arrows
  (color, width, direction, position) and is told to reposition on
  resize/orientation-change events.
- Driven from the existing `eventSource.onmessage` handler in
  `templates/index.html` — each incoming SSE message updates node text and
  recomputes arrow color/width/direction, same pattern as the existing
  freshness-badge update logic.
- No new backend endpoint. `work_mode`/`battery_mode` (numeric) are the one
  addition to `SELECTED_SENSORS`/the SSE payload/the DB schema - already
  made (see Color and status rules), auto-migrates via `storage.py`'s
  existing `_reconcile_table_columns_*`, no manual migration needed.

## Idle-battery investigation (background finding)

Checked production `data.db` on `raspberry4.local` (not the small local dev
copy, which had no idle/full-battery samples). At `battery_soc = 100%`,
`ppv = 0`: `pbattery1` reads -31 to -33 W, and `ibattery1` is a genuine
nonzero -0.1 A at ~201 V — a real current draw, not a rounding/display
artifact. This is the battery's own BMS/electronics drawing a small
continuous trickle from the battery itself (separate from the inverter's
AC-side self-consumption). **Decision: render it as-is** (a thin but visible
line via the linear-with-floor thickness rule) — no special-case
suppression, since it's physically real. (This sample happened to be
labeled `Discharge`, but a later, larger check found the trickle is
`Standby` 98.6% of the time — see the Battery direction rule under Color
and status rules below, which is why this renders as an undirected line,
not a "discharge" arrow specifically.)

## Verified data mappings (resolved against production `data.db`)

Two sign/field-meaning questions were resolved by pulling real samples from
`raspberry4.local`'s production `data.db` rather than assumed:

- **`pbattery1`'s sign is reliable at real power levels, but noisy near
  zero — use `battery_mode` for direction anyway, to avoid flicker.**
  Grouping 14 days of production samples by `battery_mode_label`: `Charge`
  is 100% negative with zero exceptions, down to -3683W — sign holds up
  fine once real power is moving. `Discharge` is only 75.5% positive, but
  99.97% of its negative readings are between -1W and -99W (near-zero
  ripple/noise while the BMS sits in a steady `Discharge` state, which
  apparently has hysteresis and doesn't flap its label for tiny dips); only
  ~29 samples out of 340k were more negative than -100W, and those trace to
  register-read skew at fast charge↔discharge transitions (confirmed by
  inspecting one directly: mode flipped to `Discharge` one ~2s sample before
  `pbattery1` caught up from a -2000W charge reading). Net effect: were the
  UI to color the battery arrow from raw sign, it would flicker between
  charge/discharge colors every second or two whenever the battery is near
  idle — exactly the ~30W standby-trickle band this dashboard needs to
  render smoothly. Direction therefore comes from `battery_mode`
  (`battery_mode_label`'s numeric code, stable/hysteresis-backed), magnitude
  from `abs(pbattery1)`.

  **Known accepted artifact:** since direction (`battery_mode`) and
  magnitude (`pbattery1`) are separate registers that can be read a moment
  apart, a fast charge↔discharge transition can pair the new direction with
  the still-stale, large-magnitude old reading for a single ~1-2s frame
  (e.g. briefly showing "Discharging 1940W" right as a ~2000W charge
  session ends) before the next sample self-corrects. Rare (~29 occurrences
  out of 340k samples in the 14-day check above) — left as-is rather than
  adding transition-tick clamping/smoothing, to keep the implementation
  simple.
- **`pgrid`/`pgrid2`/`pgrid3` is the inverter's own AC output onto the
  shared bus, not grid import/export.** In a self-sufficient moment (PV
  1367W, load ~960W, battery charging the surplus), `pgrid` read ~315W
  (inverter actively driving the bus to cover load) while
  `grid_in_out_label = "Idle"` and `meter_active_power_total ≈ 0` — nothing
  was actually crossing the utility meter. So the Inverter→Junction arrow
  uses `pgrid`+`pgrid2`+`pgrid3`, and the Junction→Grid arrow (the actual
  utility flow) uses `meter_active_power_total`, with `grid_in_out_label`
  giving a clean three-state Idle/Exporting/Importing for color — no sign
  interpretation needed.
