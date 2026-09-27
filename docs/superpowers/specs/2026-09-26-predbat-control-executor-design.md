# Predbat → GoodWe control executor: design

Status: draft for review (2026-09-26). Background, decisions and raw spike
results: `docs/superpowers/notes/2026-09-26-predbat-control-path-brainstorm-state.md`.

## Goal

Let Predbat (or any other optimizer) drive the GW8KN-ET through goodwe_manager,
so the battery follows Predbat's plan, with an acknowledged, fail-safe command
path and the control state visible in the dashboard.

Success criteria:

- Every Predbat state (demand, charge, freeze charge, hold charge, export,
  freeze export) produces the measured inverter behaviour below.
- Whether a command was applied is visible within ~10 s in HA and the
  dashboard (read-back state, not "message sent"). Predbat itself does not
  verify service calls: it re-sends them every cycle (`repeat: true`) and
  re-plans from the measured SoC, so a missed command costs one cycle of
  plan deviation, not a stuck plan.
- No command outlives its sender: stale commands, crashes and restarts end in
  normal self-use (AUTO) with the user's own settings restored.
- The executor has no optimizer-specific logic; Predbat specifics live in
  HA/apps.yaml templates.

Non-goals:

- The Polish export energy budget (rule A) - a later Predbat planner patch.
- Delayed/Smart Charging (`smart_charging_enable`) and eco slots - untouched; eco slots stay
  the manual fallback. They also act in `auto`, so with `CONTROL_MODE=on`
  every enabled eco slot is reported as a warning in the state and dashboard
  (the user disables them; the executor never writes them).
- Inverter-side watchdog - not supported by this firmware (the API remote
  timeout registers ack writes but don't store them).

## Measured inverter behaviour (2026-09-26, GW8KN-ET DSP 12.197 / ARM 31)

Register names are the goodwe library's setting ids (`ems_mode`,
`ems_power_limit` = EMS power setpoint, `battery_charge_current`,
`battery_discharge_current`, `soc_upper_limit`, `work_mode`,
`battery_discharge_depth` = on-grid minimum SoC in %, shown inverted as DoD
in SolarGo); addresses are in the notes.

| Executor mode | Settings | Behaviour measured |
|---|---|---|
| `auto` | `ems_mode`=AUTO | Normal self-use (eco slots still apply) |
| `charge` P | `ems_mode`=CHARGE_BATTERY, `ems_power_limit`=P | Battery charges at exactly P (±7 W) whatever PV/load does; PV above P covers load, then exports; shortfall from grid |
| `export` P | `ems_mode`=DISCHARGE_PV, `ems_power_limit`=P | Battery discharges at exactly P; PV not curtailed; everything not used by the house is exported |
| `freeze_charge` | `ems_mode`=AUTO, `battery_discharge_depth`=SoC | No discharge (Standby); surplus PV charges the battery; deficit from grid. Off-grid the inverter uses its separate off-grid minimum SoC, so the backup side keeps full battery power (grid-breaker tests 2026-09-26) |
| `freeze_export` | `ems_mode`=AUTO, `battery_charge_current`=0 | No charging; surplus exported; battery covers deficit |

Not used: `battery_discharge_current` = 0 for freeze charge - it works
on-grid, but the inverter honours it off-grid too: in the grid-breaker test
the backup output collapsed and the inverter went to Fault within ~15 s.
CHARGE_PV - its setpoint is the max grid power for charging and it
sends all PV to the battery with the whole load from the grid;
`soc_upper_limit` = SoC works for freeze export too but depends on the SoC
reading, so `freeze_export` uses `battery_charge_current`.

All EMS modes considered for the freeze states (✓ measured, ○ from the
GoodWe protocol map's mode table only):

| EMS mode | Behaviour | Fit |
|---|---|---|
| CHARGE_PV ✓ | all PV to battery, load from grid, grid charging up to setpoint | no - imports the load even with PV |
| CONSERVE ○ | battery charged by PV only; "PV does not support the loads first"; discharge only off-grid | no - same load-from-grid problem as CHARGE_PV (it's an off-grid reserve mode) |
| BATTERY_STANDBY ○ | battery neither charges nor discharges | no - freeze charge must store surplus, freeze export must cover deficit; standby does neither |
| BUY_POWER / SELL_POWER ○ | battery balances to hold grid import/export at the setpoint | no - still charges and discharges |
| IMPORT_AC / EXPORT_AC / DISCHARGE_BATTERY ○ | grid- or battery-first, PV (MPPT) curtailed | no - curtails PV |
| AUTO + current limit 0 ✓ | one direction blocked, self-use otherwise | **yes** (both freezes) |

Other facts the design relies on:

- Mode changes take effect within one ~6 s sample.
- Some registers only show a new value a few seconds after the write ack.
- The dongle is sometimes unresponsive for ~20 s (reads fail after retries).
- `battery_charge_current`/`battery_discharge_current` are user-tuned limits (currently 19.0 A); BMS limit is 18 A
  (`battery_charge_limit`/`battery_discharge_limit`, already polled).
- EMS mode reportedly persists across inverter reboots (not verified here) -
  treated as persistent.

## Architecture

```
Predbat ──HA service templates──▶ mqtt.publish goodwe/control/set
                                              │
goodwe_manager                                ▼
  mqtt_bridge  ── subscribe ──▶ control.Executor (pure state machine)
  dashboard    ── override  ──▶        │  desired register targets
                                       ▼
  AsyncioThread poll loop (1 Hz): read runtime ─▶ executor.tick(sample, now)
                                  ─▶ write register diffs ─▶ verify by read-back
                                  ─▶ publish goodwe/control/state (retained)
HA mqtt sensors ◀── goodwe/control/state ──▶ Predbat compares each cycle
```

All inverter I/O stays on the existing single connection and event loop -
there is only ever one UDP client.

### Units

1. **`control.py` - `Executor`** (new, pure, no I/O). Inputs: commands
   (from MQTT or dashboard), runtime samples, time, control-register
   read-backs. Output: desired values for the control registers and the state
   document. Fully unit-testable with a fake clock.
2. **`control_io.py` - `ControlWriter`** (new). Given desired vs last
   read-back, writes only differing registers in a fixed order, schedules a
   verification read 3 s later, retries up to 3 times per register, then
   reports an error. Reads the control registers every 10 s (and after
   writes) - seven single-register reads (the five mode settings plus
   `soc_upper_limit` and `work_mode`), interleaved with polling.
3. **`mqtt_bridge.py`** (extended). Subscribes to `control/set` and
   `control/reserve/set` on (re)connect; a reader task parses JSON and hands
   commands to the executor; `publish_control_state()` publishes the
   retained state.
4. **`main.py`** (extended). Creates executor + writer; calls them once per
   poll iteration after `read_runtime_data()`; startup reconciliation before
   the loop; Flask routes for the dashboard panel/override.
5. **Dashboard** (`templates/`, `static/`). A control panel on the main
   page: current mode, power, target SoC, source, expiry, applied/error, raw
   register values (EMS mode/set, charge/discharge current, SoC upper limit);
   timed override form; config page shows the same registers read-only.

## Command protocol

`goodwe/control/set` (QoS 1, not retained):

```json
{"mode": "charge", "power_w": 3000, "target_soc": 80,
 "source": "predbat", "expires_at": "2026-09-26T15:10:00+02:00", "id": "optional"}
```

- `mode`: `auto | charge | export | freeze_charge | freeze_export`.
- `power_w`: required for `charge`/`export`; clamped to
  `[100, CONTROL_MAX_BATTERY_W]`; clamping is reported in the state. The live
  BMS limit (A × battery V) is only reported (`bms_charge_limit_w`,
  `bms_discharge_limit_w`), not applied: the inverter enforces it itself (in
  the spike, CHARGE_PV with grid headroom held the battery at the BMS max),
  and following it made the setpoint track voltage jitter.
- `target_soc`: optional for `charge`/`export` (see SoC targets).
- `expires_at` (ISO 8601 with offset) or `ttl_s` (seconds): one of them is
  required unless `mode` is `auto`; max 60 min ahead (Predbat re-sends each
  5-min cycle with `repeat: true`, so ~15 min is typical). Past, missing or
  too far → rejected.
- `stop` (optional, only with `mode: auto`): `"charge"` or `"export"`. A
  scoped stop only takes effect when the current command is in that domain
  (`charge`/`freeze_charge` resp. `export`/`freeze_export`); otherwise it is
  ignored. Predbat sends the opposite stop before every start
  (`discharge_stop` before `charge_start`, `charge_stop` before
  `discharge_start`), each cycle - unscoped, that would flip the inverter to
  `auto` and back every 5 minutes.
- Re-sending an identical command only refreshes `expires_at` - no writes.
- Invalid JSON/fields → rejected, logged, and reported in the state's
  `last_error`; the current command stays in force.

`goodwe/control/reserve/set` (retained, published by an HA MQTT number that
Predbat drives): integer SoC %, software reserve (below). Retained so a
restart picks it up.

`goodwe/control/state` (retained, published on every change and every 10 s):

```json
{"mode": "charge", "effective_mode": "freeze_charge", "power_w": 3000, "target_soc": 80,
 "source": "predbat", "expires_at": "...", "applied": true, "since": "...",
 "override": null, "reserve_soc": 25, "reason": "target_soc reached",
 "registers": {"ems_mode": 1, "ems_power_limit": 0, "battery_charge_current": 19.0,
               "battery_discharge_current": 19.0, "battery_discharge_depth": 81,
               "soc_upper_limit": 100, "work_mode": 3},
 "last_error": null, "warnings": [], "writes_today": 14}
```

`applied` is true only when the last read-back of every control register
matches the desired values. `effective_mode` differs from `mode` when the
executor substitutes a mode (SoC target reached, reserve, override).

## Executor rules

Order of precedence each tick:

1. **Disabled** (`CONTROL_MODE=off`): no writes, no subscriptions.
1a. **Off-grid** (see below) → `auto` with user currents and floor, above everything
   else including the override.
2. **Override** active (dashboard) → its mode; MQTT commands are recorded but
   not applied (`state.override` shows mode and end time).
3. **Command expired** (or none) → `auto`.
4. **SoC targets**:
   - `charge` with `target_soc`: once SoC ≥ target → `freeze_charge`
     (Predbat "hold charging"). Back to `charge` if SoC falls 3 points below
     target.
   - `export` with `target_soc`: once SoC ≤ target → `auto` (Predbat "hold
     exporting" = stop).
   - A target is "reached" only when the condition has held for 30 s (the
     BMS SoC jumps; single-sample garbage exists) - except on the first
     SoC sample after a new command, where an already-met target counts at
     once. Releasing a charge hold (and the reserve below) needs 30 s too.
5. **Software reserve**: in `auto`, if SoC ≤ reserve → `freeze_charge` until
   SoC ≥ reserve + 2. Values below 20 % are accepted but warned about in the
   dashboard (BMS SoC resyncs by ~4 points around 22-18 %). `CONTROL_MIN_SOC`
   stays the floor in every other mode.
6. Otherwise the commanded mode.

**Freeze floor**: entering `freeze_charge` sets `battery_discharge_depth` to
the current SoC (integer %, never below `CONTROL_MIN_SOC`) - the lowest SoC of
the last 30 s, so a single garbage high sample can't put the floor above the
real SoC. While frozen the floor follows a rising SoC in steps of 3 points
(surplus PV charging must not be discharged again later; again the lowest SoC
of the last 30 s) and never goes down. Without any SoC sample yet
(startup, read failures) `freeze_charge` is not applied - `auto` with reason
`waiting for SoC` - because a floor above the real SoC could make the
inverter's DoD Holding charge from the grid. Leaving the freeze writes
`CONTROL_MIN_SOC` back. The inverter only allows discharge again once SoC is
5 points above the floor; if SoC is within 5 points of `CONTROL_MIN_SOC`
when a freeze ends, discharge stays blocked until the battery charges - the
state carries a warning then.

Every mode fully specifies all control registers, so switching never leaves a
stale value behind:

| Mode | `ems_mode` | `ems_power_limit` | `battery_charge_current` | `battery_discharge_current` | `battery_discharge_depth` |
|---|---|---|---|---|---|
| `auto` | AUTO | 0 | user | user | min |
| `charge` P | CHARGE_BATTERY | P | user | user | min |
| `export` P | DISCHARGE_PV | P | user | user | min |
| `freeze_charge` | AUTO | 0 | user | user | freeze floor |
| `freeze_export` | AUTO | 0 | 0 | user | min |

`user` = `CONTROL_CHARGE_CURRENT_A` / `CONTROL_DISCHARGE_CURRENT_A`, `min` =
`CONTROL_MIN_SOC`, all from config (not captured from the inverter, so a crash
that left 0 A or a high floor there can never become the new "normal"). `soc_upper_limit` is only read and reported (must be 100;
a different value is shown as a warning, not changed).

Write order: restricting limits first (a current going to 0, the floor going
up), then EMS (setpoint first when entering a forced mode, mode first when
going back to AUTO), then relaxing limits (currents restored, floor lowered).

## Off-grid (grid outage)

The 1-year history has off-grid samples on 21 days - many are
seconds-long blips, but several outages lasted hours (13 h on 22-23 Apr
2026, 3-4 h on 8 Apr, 4 May and 20 Aug). `freeze_charge` via the on-grid
floor is safe by itself off-grid (measured), but `freeze_export` (charge
current 0) would block PV from charging the battery the house now depends
on, and forced EMS modes have no meaning without the grid.

- Detection from the runtime sample: `grid_mode` ≠ 1 (0 Not connected,
  2 Fault) or runtime `work_mode` = 2 (Normal Off-Grid). Seen in the
  history as `grid_mode` 2 with `work_mode` 2.
- Entering is immediate (no debounce): desired becomes `auto` with user
  currents, reason `off-grid`, state `off_grid: true`. Commands keep being
  recorded and expiring; the override and reserve are suspended.
- Leaving needs 60 s of on-grid samples in a row (the inverter itself
  waits before reconnecting; avoids flapping), then normal rules resume.
- The writer treats any change of desired values as a fresh start (clears
  retry counters, back-off and pending verification), so an off-grid
  restore is written on the next tick even after earlier write failures.
- Off-grid, the battery may go below the software reserve - that is what
  the reserve is for; `battery_discharge_depth_offline` stays the hard floor.
- Grid-breaker tests (2026-09-26, no PV, SoC 70-76 %): with
  `battery_discharge_current` = 0 the backup output collapsed and the
  inverter went to Fault in ~15 s (did not recover off-grid) - hence the
  floor-based `freeze_charge`. With the on-grid floor at the current SoC the
  inverter stayed in Normal off-grid and the battery supplied a 460 W house
  load on backup; on grid return it spends ~80 s in Check mode with the
  load bypassed to the grid, then reconnects. The Wi-Fi AP stayed up in both
  tests (the Pi has a UPS), so the executor keeps control during outages.
- Predbat has no notion of off-grid: it keeps planning and sending commands
  (if HA is still up). The executor's off-grid rule overrides them; the
  only Predbat-side outage feature is the Meteoalarm `keep` pre-charge.

## Fail-safe

- **Expiry** → `auto` with user currents and floor (above).
- **MQTT disconnect** does not change anything by itself; expiry handles it.
- **Startup reconciliation**: before polling starts, read the control
  registers. No valid command after a restart (commands are not persisted) →
  desired = `auto`, so anything a crash left behind is reverted within the
  first seconds. The retained reserve is re-read from MQTT.
- **Write failures**: 3 retries per register, then `last_error` set,
  `applied=false`, retried on the next change or every 60 s. A failed read is
  skipped (the dongle's ~20 s gaps), never an error on its own.
- **Pi/manager dead** with no restart: the inverter keeps the last mode
  (no inverter watchdog). Accepted risk; worst cases: `export` drains to the
  DoD floor, `charge` charges to 100 % from grid if needed, freezes block
  charging or discharging. Mitigation outside this spec: an HA automation
  alerting when `bridge/status` is offline while `control/state.mode != auto`.
- **External changes** (SolarGo): switching work mode in SolarGo writes
  `ems_mode`=AUTO / `ems_power_limit`=0 and `clearECOtime` (which also switches eco slots
  off). The executor sees the read-back differ and re-applies its desired
  registers within ~10 s, so manual SolarGo changes during an active command
  are overwritten; set a dashboard override to `auto` first. The executor
  reports `work_mode` and a changed value is a warning.
- **Flash wear**: writes only on change; `writes_today` in the state and a
  warning in the log above `CONTROL_MAX_WRITES_PER_DAY` (default 300). A
  typical Predbat day is expected to need 20-60 writes.

## Configuration (`.env`)

| Key | Default | Meaning |
|---|---|---|
| `CONTROL_MODE` | `off` | `off`, `shadow` (compute + publish state with `"shadow": true`, no writes), `on` |
| `CONTROL_CHARGE_CURRENT_A` | - (required when not `off`) | normal `battery_charge_current`, e.g. 19.0 |
| `CONTROL_DISCHARGE_CURRENT_A` | - (required when not `off`) | normal `battery_discharge_current` |
| `CONTROL_MIN_SOC` | - (required when not `off`) | normal on-grid minimum SoC % (`battery_discharge_depth`), e.g. 14 |
| `CONTROL_MAX_BATTERY_W` | 3600 | power clamp (the inverter enforces the live BMS limit itself) |
| `CONTROL_MAX_WRITES_PER_DAY` | 300 | warning threshold |

## Dashboard override

Form: mode, power (for charge/export), target SoC, duration (15 min - 12 h),
"Clear override". Stored in memory only (a restart clears it and falls back
to MQTT/auto). Shown in the state topic so Predbat/HA can see why its
command is not applied.

## Predbat / Home Assistant side

Documented in `MQTT_TOPICS.md` (examples copied from the deployed config
once working):

- HA MQTT sensors from `control/state` (mode, effective_mode, applied,
  target_soc, reserve), and an MQTT number for the reserve.
- apps.yaml custom inverter: `has_target_soc: true`,
  `support_charge_freeze: true`, `support_discharge_freeze: true`,
  `charge_control_immediate: true`, `has_timed_pause: false`,
  reserve → the MQTT number.
- Service templates with `repeat: true` call an HA script
  (`script.goodwe_control`) that builds the JSON and publishes it (HA's
  `mqtt.publish` no longer renders payload templates): `charge_start` →
  `charge` {power, target_soc}; `charge_freeze` → `freeze_charge`;
  `charge_stop` → `auto` with `stop: charge`; `discharge_start` → `export`
  {power, target_soc}; `discharge_freeze` → `freeze_export`;
  `discharge_stop` → `auto` with `stop: export`; each with `ttl_s: 900`.
- Hold charging arrives as `charge_start` with `target_soc` below the current
  SoC; the executor then goes to `freeze_charge` on the first sample (no
  debounce for a target that is already met when the command arrives).

## Predbat battery model at the low and high end

The BMS SoC is coulomb-counted but not linear at the ends (1-year analysis
in the notes): around 22-18 % it resyncs and ~4 points vanish; 25 % → 10 %
displayed delivers ~0.6 kWh instead of the nominal ~1.05 kWh; the 80-90 %
band also passes ~10 % faster than the middle. Predbat assumes kWh is
linear in SoC, so it would over-estimate what is left below ~25 %.

Changes in `apps.yaml` / Predbat settings (the HA repo, not this code):

- **Keep plans at the edge of the non-linear band**: `best_soc_min` (hard
  minimum the planner may target) = 20 % of `soc_max`, and `best_soc_keep`
  (soft floor, the user's "low only right before the next charge") = 25 %,
  tuned from experience.
- **Reserve** driven by Predbat into the executor's software reserve, never
  below 20 % (executor warns below 20 %); the inverter DoD stays the hard
  floor underneath.
- **Usable capacity**: leave `soc_max` at the nominal 7.1 kWh (the linear
  middle is what Predbat plans with) and do not model the bottom band at all
  - the ~0.45 kWh it lacks is inside the reserve and never planned against.
- **Low power charging/export**: enable Predbat's `set_charge_low_power` and
  `set_export_low_power` switches, so charge and export windows run at the
  lowest power that still reaches the target by the end of the window (less
  battery stress and conversion loss; the BMS limit of ~3.4 kW is rarely
  needed). The executor already takes any `power_w` from 100 W up to the
  clamp, and `charge`/`export` hold that power exactly, so low power rates
  are followed precisely.
- **Severe weather**: Predbat `alerts:` (Meteoalarm) with `keep` raises the
  floor dynamically, as decided in the brainstorm.
- Revisit after a month of executor data: compare Predbat's predicted SoC at
  the end of discharge windows with the measured one and adjust `best_soc_keep`
  or `battery_loss_discharge` if the error is systematic.

## Testing

- **Unit (`tests/test_control.py`)**: command validation and clamping;
  precedence (override > expiry > targets > reserve); SoC target debounce and
  hysteresis; register tables per mode; identical command refreshes expiry
  only; reserve warning; shadow mode produces no writes.
- **Writer (`tests/test_control_io.py`)** with a fake inverter: writes only
  diffs, order rules, delayed read-back (value appears after 2-8 s), retries
  and error reporting, failed reads skipped.
- **MQTT**: subscribe on reconnect, malformed payloads, retained reserve.
- **Startup**: registers left in `freeze_export`/`charge` by a "crash" are
  reverted to `auto` + user currents and floor.
- **Live acceptance** (short, supervised, reusing the spike's per-mode
  checks with 60 s windows): each mode via MQTT, expiry revert, restart
  revert, override precedence.

## Rollout

1. Merge with `CONTROL_MODE=off` (no behaviour change).
2. `shadow` with Predbat control enabled (Predbat must not be read-only, or
   it sends nothing) until each mode has been commanded at least once -
   using Predbat's manual plan overrides (force charge / export / freeze
   charge / freeze export slots) to trigger each mode on demand, so it
   takes about an hour rather than a day. Nothing is written, so Predbat simply sees the battery not
   following its plan and re-plans from the measured SoC each cycle; the
   check is that the published `effective_mode`/registers match what
   Predbat asked for, including expiry refreshes and target-reached
   switches.
3. Disable the eco slots, run the live acceptance test, then `on` with
   Predbat control enabled.
4. After a week of stable running: set the inverter DoD lower and let the
   software reserve manage the floor.
