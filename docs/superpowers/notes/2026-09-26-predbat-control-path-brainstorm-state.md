# Predbat → GoodWe control path: brainstorm state (2026-09-26)

Working notes from the brainstorm/spike session, kept so the session can be
compacted without losing decisions. Not the spec — the spec gets written
after the spike (step 3) results are in. Brainstorming path: **architectural**
(spec → user review → writing-plans).

## Decisions made (by the user)

- **Optimizer stays Predbat** (evaluated vs evcc — its optimizer is display-only
  as of 2026-07, rule-based battery control only; vs EMHASS — viable LP alternative,
  revisit if the Predbat net-settlement patch becomes too costly). Executor must be
  **optimizer-neutral** "as long as it doesn't add too much complexity".
- **Control via GoodWe EMS modes** (47511 EMSPowerMode / 47512 EMSPowerSet),
  eco slots only as fallback. Also **expose EMS mode + status in goodwe_manager
  dashboard/config**.
- **Transport: MQTT**, one non-retained JSON command topic `goodwe/control/set`
  `{mode, power_w, target_soc, source, expires_at}`, QoS1; ack = executor read-back
  published to retained `goodwe/control/state` → HA sensors → Predbat compares each
  cycle. Predbat side = **service API** (`charge_start/freeze/stop`,
  `discharge_start/freeze/stop` → `mqtt.publish`), custom inverter type in apps.yaml,
  `repeat: true` on templates (watchdog refresh). Not `has_mqtt_api` (Sofar-specific,
  retained multi-topic, overloaded semantics).
- **Watchdog**: executor reverts to AUTO when `expires_at` passes (~15 min / 3
  Predbat cycles). Inverter-side watchdog 47117/47118 being tested (step 3).
- **Manual override from dashboard: timed override (option A)** — pick mode +
  duration, Predbat commands ignored until expiry, visible in state topic.
- **Export energy budget (Polish prosumer rule, user's stretched reading)**:
  rule A = exported energy since the end of the previous forced export ≤ PV energy
  produced in that period (all export counts, incl. natural surplus). **Not in the
  executor** — to be a later **Predbat planner patch** (like the net-settlement patch).
- **Reserve**: user wants Predbat to manage it intelligently (low SoC only right
  before next charge = `best_soc_keep`; severe weather = Predbat Meteoalarm `alerts:`
  with `keep`). Inverter DoD (`battery_discharge_depth` 45356, currently 14 → min SoC
  14%) stays a fixed hard floor; user will change it once Predbat soft reserve is set.
  Predbat's reserve writes → executor **software reserve** (HA MQTT number, no flash).

## Predbat ↔ EMS mapping (from Predbat source + GoodWe map Table 8-16)

| Predbat state | Service called | EMS mode |
|---|---|---|
| Demand | charge_stop / discharge_stop | AUTO (1) |
| Charging R→T | charge_start {power, target_soc} | CHARGE_BATTERY (11) Xset=R (PV first, grid tops up = exact match of Predbat hybrid charge model); executor switches to hold at SoC ≥ T |
| Freeze charging / Hold for car / iBoost | charge_freeze | **AUTO + discharge current 0 A (45355=0)** — measured: surplus charges battery, deficit from grid, no discharge = exact match. NOT CHARGE_PV Xmax=0 (sends all PV to battery, whole load from grid) |
| Hold charging | charge_freeze or charge_start with target < SoC (needs `has_target_soc: true`) | executor "charge to target then hold" |
| Exporting R→T | discharge_start | DISCHARGE_PV (3) Xmax=R (not DISCHARGE_BATTERY 12 — that curtails PV) |
| Hold exporting | discharge_stop | AUTO |
| Freeze exporting | discharge_freeze | **AUTO + charge current 0 A (45353=0)** or soc_upper_limit=SoC (47760) — both measured: surplus exported, battery covers deficit = exact match |

Custom inverter def: `has_target_soc: true`, `support_charge_freeze: true`,
`support_discharge_freeze` per step 3, `has_timed_pause: false`, `charge_control_immediate: true`,
`set_reserve_enable` → software reserve.

## Hardware facts (GW8KN-ET, SN 98000ETU22CW2445)

- Firmware DSP 12.197, ARM 31 (02041-31-S00). 8 kW AC / 8.8 kVA; battery 180-600 V, 25 A max.
- Battery: 4 modules, 37 Ah, ~195 V, SoH 97%, **BMS limit 18 A** (~3.4-3.5 kW) — binding.
- Current config: work_mode 3 (Eco), 4 eco slots daily: 00:00-05:59 charge 1%,
  07:00-07:59 discharge 15%, 15:00-16:59 charge 25%, 19:00-19:59 discharge 25%
  (lib sign: negative = charge). ems_mode 1, ems_power_limit 5652 (leftover).
  Inverter-side current limits 45353/45355 now **19.0 A** (user changed from 18.5 on 2026-09-26).
- goodwe_manager talks to the dongle's own AP (Pi wlan0 → Solar-WiFi22CW2445,
  10.10.100.253), 1 Hz; only one UDP client at a time → stop the service for any spike.

## Spike findings so far

- Step 1 (read-only dump) done: `~/ems-spike/dump-baseline-20260925_194614.json` on Pi, copies in `/tmp/ems-spike/out/`.
- Step 2 Shadow Scan: SolarGo writes **only 45251** (1/0); goodwe lib's `shadow_scan` is correct.
  Battery not charging off-grid with Shadow Scan on = firmware behaviour.
- SolarGo captures (PCAPdroid on Android, connect SolarGo first then start capture —
  discovery broadcast fails through the VPN): `/tmp/ems-spike/solargo.pcap`, `solargo2.pcap`,
  decoder `/tmp/ems-spike/decode_pcap.py`.
  - **Restart = write 45221 = 361** (frame `f706b0a501696a01`); documented 45220 is only a grid reconnect.
  - **SolarGo writes the inverter clock (45200) on every connect** — explains clock jumps.
  - **Off-grid SOC recovery = 45287** (% SoC, currently 40); not in lib or map.
  - Battery current limits = 45353 / 45355 (0.1 A; already lib settings
    `battery_charge_current`/`battery_discharge_current`). **Discharge 0 A stops discharge** (observed).
  - 47832 polled by SolarGo, reads 65535 (unused on this unit).
- Protocol map: "ARM745 ESG2 ET30 Modbus protocol map 2022-12-31 v1", unofficial
  copy from ioBroker forum; local `/tmp/ems-spike/et-modbus-map.pdf` + `map.txt`.
  Notable: 47117/47118 API remote timeout (inverter watchdog, reads 65535),
  47038 StopModeSaveEn, 47609 smart charging enable, 47589-47594 group 8,
  47592 peak power sales limit (‰, 1000=100%), 47613 "PV sell first" (=1), 47615 battery current coff.
- EMS mode reportedly persists across reboots (evcc users) → flash-backed → minimise
  writes, fail-safe watchdog essential.

## Step 3 RESULTS (2026-09-26 13:40-14:57, logs ~/ems-spike/ems-test-20260926_*.json)

- CHARGE_BATTERY (11) Xset: battery charge power exact (±7 W) regardless of PV/load; PV beyond covers load then exports; shortfall from grid.
- DISCHARGE_PV (3) Xmax: battery discharge power exact; PV not curtailed; rest exported.
- CHARGE_PV (2) Xmax = max GRID power for battery charging (0 = PV only); battery takes PV first, load from grid, never discharges. Xmax=1000: grid tops up so battery stays at BMS max (~3.6 kW).
- soc_upper_limit < SoC: standby, no active discharge. = SoC: surplus exported, battery discharges for deficit (freeze export).
- charge current 45353=0: same as above (freeze export). discharge current 45355=0: surplus charges, deficit from grid (freeze charge).
- NOT supported on this firmware (write acked, read-back unchanged even after 13 s): 47117/47118 inverter watchdog, 47615 battery current coff. Executor watchdog only.
- Dongle occasionally unresponsive ~20 s (3 retries fail) - executor must tolerate failed reads.
- Mode changes take effect within one ~6 s sample.

## Step 3 plan (as run)

Script `/tmp/ems-spike/ems_test.py` + wrapper `run_ems_test.sh` (copies on Pi `~/ems-spike/`),
reviewed by a subagent, fixes applied (signed write echo, abort flag, ordered
independent restore, transient systemd unit). Sequence: 0 watchdog test (hold,
3 min silent) → 1 AUTO → 2 CHARGE_PV 0 → 3 CHARGE_BATTERY 1000 → 4 CHARGE_PV 1000 →
5 DISCHARGE_PV 1000 → 6 BatCurCoff 10 → 7 soc_upper=SoC → 8 soc_upper=SoC-5 →
9 charge current 0 A → restore/verify. Preflight: ems 1, work_mode 3, SoC 25-90,
start 08:05-14:10, PV > load+1 kW. Then manual: Delayed Charging via SolarGo
(sales limit 100%, PV-prioritise time 23:59) with dumps/observe. Run:
`cd ~/ems-spike && ./run_ems_test.sh run`, follow `journalctl --user -u ems-test -f`,
stop `systemctl --user kill -s INT ems-test`.
Optional: reboot persistence check of EMS mode.

## BMS SoC analysis (1-yr snapshot, 140 deep-discharge episodes)

- SoC is **Ah (coulomb) based**; charge 0.373-0.374 Ah/% flat 20-80% while V rises 1.7%.
- **Resync drop around 22-18%**: ~4 %-points vanish for ~0.4 Ah within 1-2 min at ~190.8 V.
  25%→10% displayed delivers ~3.1 Ah (~0.6 kWh) vs nominal 5.4 Ah (~1.05 kWh).
  Top 80-90% also ~10% fast. Occasional single-sample garbage (100→0→100).
- Design consequences: executor debounces SoC, doesn't trust SoC < ~25% as stop signal;
  Predbat soft floor ~22-25% (`best_soc_min`/`best_soc_keep`).
- Scripts: `/tmp/soc-analysis/{events,dwell,thresh,ahwh}.py`.

## Side threads

- Home Wi-Fi: dongle stops answering **broadcast ARP** some time after reconnect
  (router restart fixes it temporarily; Archer C6U has no DTIM setting). Probe:
  transient user timer `arp-probe.timer` on Pi → `~/ems-spike/arp-probe.log` (remove when done).
- goodwe library PRs (clone `~/Dev/goodwe`, origin = upstream, fork = piomar123):
  gh as personal account: `GH_CONFIG_DIR=~/.config/gh-piomar` (fish alias `gh-piomar`); default gh = work account!
  branches pushed to fork:
  1. `et-restart` — `Inverter.restart()`, ET writes 45221=361. **PR #152 opened.**
  2. `inverter-time-sync` — `get_time()`/`set_time()` on base Inverter + SolarGo clock note.
  3. `et-offgrid-soc-recovery` — `offgrid_soc_recovery` setting 45287. **PR #153 opened.**
  - PR 2 pending: add docstring note (tz-aware datetimes written as-is, no conversion), then show description.
  - goodwe#108 findings comment **posted** 2026-09-26 as piomar123 (work-account copy deleted):
    https://github.com/marcelblijleven/goodwe/issues/108#issuecomment-5845531151
    (promises: Delayed Charging capture confirmation follow-up + the three PRs).

## Still to design in the spec

Executor components (MQTT subscriber, write queue interleaved with 1 Hz polling,
read-back/state publisher, SoC-target enforcement + debounce, software reserve,
watchdog + startup reconciliation, eco-slot coexistence, power clamp to live BMS
limits), dashboard panel + config, HA MQTT entities, Predbat apps.yaml custom
inverter + service templates, testing approach, rollout (read_only → live).

## Delayed Charging capture (2026-09-26 15:29-15:42, two pcaps)

- Enable: group 8 47589..47594 = 00:00, 23:59, 0xFA7F (0xFA = smart charge enabled, days 0x7F), 1000 (100 %), 0, 0x0FFF;
  47613=0; 47000=0 (General); 47533=1 (clearECOtime); 47609=1.
- Disable: 47000=0; EMS 47511/47512 = 1/0; 47533=1; 47591=0x037F; 47609=0.
- clearECOtime disabled the eco slots (on/off byte 0xFF -> 0x00). SolarGo "Eco" writes 47000=3, EMS 1/0 and re-enables none; the 47549 (slot 1) writes in the capture were the user re-enabling slot 1 by hand.
- Draft #108 follow-up: /tmp/ems-spike/issue108-followup.md (post after user confirms).
