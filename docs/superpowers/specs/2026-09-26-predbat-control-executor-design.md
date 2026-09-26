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
- Delayed/Smart Charging (47609) and eco slots - untouched; eco slots stay
  the manual fallback. They also act in `auto`, so with `CONTROL_MODE=on`
  every enabled eco slot is reported as a warning in the state and dashboard
  (the user disables them; the executor never writes them).
- Inverter-side watchdog - not supported by this firmware (47117/47118
  writes are acked but not stored).

## Measured inverter behaviour (2026-09-26, GW8KN-ET DSP 12.197 / ARM 31)

| Executor mode | Registers | Behaviour measured |
|---|---|---|
| `auto` | 47511=1 | Normal self-use (eco slots still apply) |
| `charge` P | 47511=11, 47512=P | Battery charges at exactly P (±7 W) whatever PV/load does; PV above P covers load, then exports; shortfall from grid |
| `export` P | 47511=3, 47512=P | Battery discharges at exactly P; PV not curtailed; everything not used by the house is exported |
| `freeze_charge` | 47511=1, 45355=0 | No discharge; surplus PV charges the battery; deficit from grid |
| `freeze_export` | 47511=1, 45353=0 | No charging; surplus exported; battery covers deficit |

Not used: CHARGE_PV (2) - its setpoint is the max grid power for charging and
it sends all PV to the battery with the whole load from the grid; SoC upper
limit 47760 = SoC works for freeze export too but depends on the SoC reading,
so `freeze_export` uses 45353.

Other facts the design relies on:

- Mode changes take effect within one ~6 s sample.
- Some registers only show a new value a few seconds after the write ack.
- The dongle is sometimes unresponsive for ~20 s (reads fail after retries).
- 45353/45355 are user-tuned limits (currently 19.0 A); BMS limit is 18 A
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
   writes) - five single-register reads, interleaved with polling.
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
  `[100, min(CONTROL_MAX_BATTERY_W, BMS limit A × battery V)]`; clamping is
  reported in the state.
- `target_soc`: optional for `charge`/`export` (see SoC targets).
- `expires_at`: required unless `mode` is `auto`; max 60 min ahead
  (Predbat re-sends each 5-min cycle with `repeat: true`, so ~15 min is
  typical). Past or missing → rejected.
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
 "registers": {"47511": 1, "47512": 0, "45353": 190, "45355": 0, "47760": 100},
 "last_error": null, "warnings": [], "writes_today": 14}
```

`applied` is true only when the last read-back of every control register
matches the desired values. `effective_mode` differs from `mode` when the
executor substitutes a mode (SoC target reached, reserve, override).

## Executor rules

Order of precedence each tick:

1. **Disabled** (`CONTROL_MODE=off`): no writes, no subscriptions.
2. **Override** active (dashboard) → its mode; MQTT commands are recorded but
   not applied (`state.override` shows mode and end time).
3. **Command expired** (or none) → `auto`.
4. **SoC targets**:
   - `charge` with `target_soc`: once SoC ≥ target → `freeze_charge`
     (Predbat "hold charging"). Back to `charge` if SoC falls 3 points below
     target.
   - `export` with `target_soc`: once SoC ≤ target → `auto` (Predbat "hold
     exporting" = stop).
   - A target is "reached" only after 2 consecutive samples ≥ 30 s apart (the
     BMS SoC jumps; single-sample garbage exists).
5. **Software reserve**: in `auto`, if SoC ≤ reserve → `freeze_charge` until
   SoC ≥ reserve + 2. Values below 25 % are accepted but warned about in the
   dashboard (BMS SoC resyncs by ~4 points around 22-18 %). The inverter
   depth-of-discharge (45356) stays the hard floor.
6. Otherwise the commanded mode.

Every mode fully specifies all control registers, so switching never leaves a
stale value behind:

| Mode | 47511 | 47512 | 45353 | 45355 |
|---|---|---|---|---|
| `auto` | 1 | 0 | user | user |
| `charge` P | 11 | P | user | user |
| `export` P | 3 | P | user | user |
| `freeze_charge` | 1 | 0 | user | 0 |
| `freeze_export` | 1 | 0 | 0 | user |

`user` = `CONTROL_CHARGE_CURRENT_A` / `CONTROL_DISCHARGE_CURRENT_A` from
config (not captured from the inverter, so a crash that left 0 there can
never become the new "normal"). 47760 is only read and reported (must be 100;
a different value is shown as a warning, not changed).

Write order: EMS mode first when leaving a forced mode, setpoint first when
entering one; currents last when leaving a freeze, first when entering one.

## Fail-safe

- **Expiry** → `auto` with user currents (above).
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
- **Flash wear**: writes only on change; `writes_today` in the state and a
  warning in the log above `CONTROL_MAX_WRITES_PER_DAY` (default 300). A
  typical Predbat day is expected to need 20-60 writes.

## Configuration (`.env`)

| Key | Default | Meaning |
|---|---|---|
| `CONTROL_MODE` | `off` | `off`, `shadow` (compute + publish state with `"shadow": true`, no writes), `on` |
| `CONTROL_CHARGE_CURRENT_A` | - (required when not `off`) | normal 45353 value, e.g. 19.0 |
| `CONTROL_DISCHARGE_CURRENT_A` | - (required when not `off`) | normal 45355 value |
| `CONTROL_MAX_BATTERY_W` | 3400 | power clamp |
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
- Service templates with `repeat: true`: `charge_start` → `charge`
  {power, target_soc}; `charge_freeze` → `freeze_charge`; `charge_stop` /
  `discharge_stop` → `auto`; `discharge_start` → `export` {power,
  target_soc}; `discharge_freeze` → `freeze_export`; each with
  `expires_at` = now + 15 min.

## Predbat battery model at the low and high end

The BMS SoC is coulomb-counted but not linear at the ends (1-year analysis
in the notes): around 22-18 % it resyncs and ~4 points vanish; 25 % → 10 %
displayed delivers ~0.6 kWh instead of the nominal ~1.05 kWh; the 80-90 %
band also passes ~10 % faster than the middle. Predbat assumes kWh is
linear in SoC, so it would over-estimate what is left below ~25 %.

Changes in `apps.yaml` / Predbat settings (the HA repo, not this code):

- **Keep plans out of the non-linear band**: `best_soc_min` (hard minimum the
  planner may target) = 25 % of `soc_max`, and `best_soc_keep` (soft floor,
  the user's "low only right before the next charge") starting at 30 %,
  tuned from experience. Predbat then only plans down to where its linear
  model is still right.
- **Reserve** driven by Predbat into the executor's software reserve, never
  below 22 % (executor warns below 25 %); the inverter DoD stays the hard
  floor underneath.
- **Usable capacity**: leave `soc_max` at the nominal 7.1 kWh (the linear
  middle is what Predbat plans with) and do not model the bottom band at all
  - the ~0.45 kWh it lacks is inside the reserve and never planned against.
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
  reverted to `auto` + user currents.
- **Live acceptance** (short, supervised, reusing the spike's per-mode
  checks with 60 s windows): each mode via MQTT, expiry revert, restart
  revert, override precedence.

## Rollout

1. Merge with `CONTROL_MODE=off` (no behaviour change).
2. `shadow` with Predbat control enabled (Predbat must not be read-only, or
   it sends nothing) until each mode has been commanded at least once -
   typically a few hours around a planned charge or export window, not a
   full day. Nothing is written, so Predbat simply sees the battery not
   following its plan and re-plans from the measured SoC each cycle; the
   check is that the published `effective_mode`/registers match what
   Predbat asked for, including expiry refreshes and target-reached
   switches. If the day's plan contains no such windows, skip straight to
   step 3.
3. Disable the eco slots, run the live acceptance test, then `on` with
   Predbat control enabled.
4. After a week of stable running: set the inverter DoD lower and let the
   software reserve manage the floor.
