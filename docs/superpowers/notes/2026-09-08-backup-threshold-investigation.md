# Backup power threshold — investigation notes

Status: **partially resolved, 2026-09-12**. Ported a flat constant
threshold (not the adaptive formula below) into production:
`diagram-calc.js`'s `backupState()` now flags `active` via
`BACKUP_ACTIVE_THRESHOLD_W` (default 35, matching the "35W @ full load"
endpoint discussed below) instead of `watts > 0`, overridable at runtime
via `setBackupActiveThreshold()`. The real value comes from the
`BACKUP_ACTIVE_THRESHOLD_W` env var (`main.py`), injected into
`templates/index.html` as `window.BACKUP_ACTIVE_THRESHOLD_W`. The
adaptive formula (rises with inverter output) is still not implemented -
picking that back up requires choosing between the two candidate
formulas below (or gathering more data).

The `battery_discharge_limit` units-mismatch bug described below has also
been fixed in production (`batteryState()` now checks
`dischargeLimit === 0`, not `soc <= dischargeLimit`).

## Goal

Reliably classify "is the backup output real load, or just CT-crosstalk
noise" for coloring the Backup node grey vs. active, without hardcoding a
binary on/off assumption (some installs run continuous real load through
backup) and without misclassifying genuine backup-in-use events (grid
faults, manual switch-over) as noise.

## Bug found along the way

`battery_discharge_limit` is **amperes**, not a SoC percentage (confirmed
against `goodwe`'s `et.py:184`, register 37005, `Kind.BAT`). The existing
`soc <= dischargeLimit` check in `diagram-calc.js`'s `batteryState()` is
comparing a % against an A value — a genuine bug. Fix: battery is "low/red"
when `dischargeLimit === 0`, not via that comparison.

**Fixed in production 2026-09-12** (was still unfixed as of 2026-09-10):
`diagram-calc.js`'s `batteryState()` now checks `dischargeLimit === 0`
instead of `soc <= dischargeLimit`.

## Design shape agreed so far

- Backup-active = **disconnected** (fault / off-grid, wattage-independent)
  **OR** (mode == Normal On-Grid **AND** `backup_ptotal` exceeds a
  load-dependent threshold).
- Threshold is **adaptive** (a function of inverter output), not a flat
  constant — the noise ceiling rises with load (CT crosstalk scales with
  the current being measured on the adjacent phase).
- Config should offer 3 modes: Adaptive / Constant / Disabled — some
  installs may want to turn the heuristic off entirely.
- Only `work_mode_label == 'Normal (On-Grid)'` samples count toward
  calibrating/checking the noise ceiling. Check Mode, Fault, Off-Grid are
  not "normal" and must not be used to justify the threshold — backup
  should read active in those modes regardless of wattage.

## Predictor

`x = abs(pgrid) + abs(pgrid2) + abs(pgrid3)` (inverter's own AC bus output,
absolute per-phase sum). Originally used a signed sum, which was a bug —
large-magnitude negative samples were misclassified as "low output".

## Known real backup-usage events (excluded from all noise analysis)

- **2026-08-20, ~04:11–08:10 local (02:11–06:10 UTC)**: genuine grid fault
  (`vgrid`/`vgrid2`/`vgrid3` → 0V, `grid_mode_label` → Fault,
  `work_mode_label` → Off-Grid), with a relay-chatter tail that outlasted
  the fault/mode flags by 25–30+ min.
- **2026-06-13, ~07:15–07:50 UTC**: same fault signature, shorter.
- **2026-09-07, from 13:56 UTC onward**: user manually switched house
  power source to backup (real usage, not an anomaly) — exclusion left
  open-ended since the session length wasn't fully bounded.

## Data investigated

90 days of production data (`inverter_history`), `Normal (On-Grid)` +
`Connected to grid` only, all three events above excluded. Full-dataset
aggregate queries (`MAX()`/`COUNT()`) run directly against SQLite on
`raspberry4.local` via indexed `timestamp_epoch` range scans — never an
unbounded scan on the live 25GB table (see the "don't stall the live
service" constraint honored throughout).

Observed noise ceiling (`max(backup_ptotal)` per 10W inverter-output
bucket, full un-sampled dataset): ~16–19W near 0W load, rising smoothly to
~30–34W near max observed load (~8686W). No samples in a gray zone between
this ceiling and the 150W+ where real events start — clean separation.

## Calibration result

Requested exact endpoints — **18W @ 0W load, 35W @ full load** — give:

```
threshold(x) = 18 + 0.001957·x
```

This does **not** strictly hold: verified against the full 90-day set
(events excluded), **484 violations**, max excess **+1.77W**, concentrated
around x≈600–850W on 2026-06-09, 2026-06-17, 2026-07-06.

Bumping only the intercept to 20 (same slope):

```
threshold(x) = 20 + 0.001957·x   (≈20W @ 0W, ≈37W @ 8686W)
```

**0 violations** across the full 90-day set.

## Open decision (pending)

Use the exact-as-requested 18/35 line (small known overages, ≤1.77W) or
the strictly-safe 20-intercept line (no violations but doesn't hit the
literal 35W-at-full-load target)? Or gather more data / pick different
endpoints? Not decided yet.

## Visual aids (still on disk, LAN-served)

- `docs/superpowers/mockups/backup-threshold-calibration.html` — point
  cloud (1-in-5 subsample, ~791k points) + true max-per-10W-bucket line
  (full dataset) + both candidate threshold lines. Served via
  `python3 -m http.server 8321 --bind 0.0.0.0` from the repo root.
- `docs/superpowers/mockups/live-power-flow-dashboard-mockup-v4.html` —
  has adaptive-threshold UI controls (`adaptive-intercept` /
  `adaptive-slope` / `adaptive-margin`), currently still holding stale
  placeholder defaults (11.62/0.000877/7.5) — **needs updating** to
  whichever formula above is chosen once decided.
