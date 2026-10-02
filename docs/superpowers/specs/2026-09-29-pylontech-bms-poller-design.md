# Pylontech BMS poller - design

Date: 2026-09-29. Status: approved (design in chat, spec self-reviewed at the
user's request).

## Goal

Record what the Pylontech Force H2 BMS knows and the GoodWe inverter doesn't
pass on: real cell temperatures, SOH, and every cell's voltage. It's for
monitoring and later analysis (battery ageing, temperature, the BMS SoC stall
above ~88 %). Nothing in Predbat or the executor uses it.

Why: the inverter's `battery_temperature` (register 37003) is a BMS board
reading (36 °C when the cells were 25.7-27.2 °C). The GoodWe cell-temperature
registers 37020/37021 read 0 with this battery (closed PR #41).

## Scope

In:

- An optional module in goodwe_manager that polls the BMS through the
  SolarMan logger attached to it, every 60 s.
- Storage in a separate SQLite file, `bms.db`.
- An MQTT topic, `goodwe/bms`, for Home Assistant.

Out:

- Dashboard UI and `/history` integration.
- Use as a control input (Predbat, executor).
- Any writes to the BMS or the logger.
- The HA sensor YAML and alerts: a follow-up commit in
  `home-assistant-raspberry4`.

## Background: how the BMS is reached

- The SolarMan Wi-Fi logger (serial 4060493924) is on the guest Wi-Fi at
  192.168.1.221. The Pi's `wlan0` is on that network, and 192.168.1.192/27
  is already routed through `wlan0`, so the Pi needs no changes.
- The logger answers the SolarMan V5 protocol on TCP 8899. V5 wraps
  Modbus RTU requests to the device on the logger's serial port.
- Modbus slave 1 is the BMS. It supports function 3 (read holding registers)
  only; function 4 returns IllegalFunction.
- Holding register 0x1000 reads as ASCII "Pylon" ... "Force_H2".
- 0x1400-0x143F mirrors 0x1100-0x113F.
- Blocks 0x1200 and 0x1300 return IllegalDataAddress.
- Library: `pysolarmanv5` 3.0.6 (`PySolarmanV5Async`), tested from the Pi on
  2026-09-29.

## Architecture

The poller is an async task on goodwe_manager's existing asyncio loop
(`AsyncioThread`), next to the inverter poll.

Other options weren't chosen:

- A separate thread would need thread-safe DB and MQTT access.
- A separate systemd service would mean a second deployment unit and doesn't
  fit "optional module".

### New module `bms_poller.py`

No import-time side effects, and no imports from `main.py` or storage.

- **Decoding:** pure functions from raw register lists to a `BmsSample`
  dataclass, plus the plausibility checks. No I/O.
- **`BmsPoller`:** the async loop.
  - It owns the `PySolarmanV5Async` client, the timeouts and the backoff.
  - It reports each accepted sample through one callback,
    `on_sample(sample)`. Failures are handled and logged inside the poller.
    Nothing outside needs them yet.
  - The client factory and the clock are injectable, for tests.
- **`pysolarmanv5` import:** only inside the client factory, so it's never
  imported when the module is disabled.

### Wiring in `main.py`

- **Config (`.env`, documented in `.env.example`):**

  | Key | Default | Notes |
  |---|---|---|
  | `BMS_LOGGER_HOST` | — | required to enable |
  | `BMS_LOGGER_SERIAL` | — | required to enable |
  | `BMS_LOGGER_PORT` | 8899 | |
  | `BMS_SLAVE_ID` | 1 | |
  | `BMS_POLL_SECONDS` | 60 | minimum 10 (lower values are clamped, with a warning), to protect the logger |

- **Disabled** (host or serial missing): no task and no `bms.db`. One INFO
  log line: "BMS poller disabled".
- **Invalid config** (serial, port or slave ID not an integer): one ERROR log
  line, and the module stays disabled. goodwe_manager itself still starts.
- **Enabled:**
  - `AsyncioThread.run()` creates the poller task in live mode (not with
    `--dry-run`, same as the inverter poll).
  - On shutdown it's cancelled and awaited with the other tasks. Then the
    client and the `bms.db` connection are closed.
- **`on_sample`:**
  - inserts a row into `bms_history` through a dedicated `aiosqlite`
    connection to `bms.db`. The file sits next to `data.db` (constant
    `BMS_DB_PATH = 'bms.db'`), uses `PRAGMA journal_mode = WAL` like the
    other databases, and gets `.gitignore` entries for `bms.db`, `-wal` and
    `-shm`;
  - calls `MqttBridge.publish_bms(payload)`.
- **Dependency:** `pysolarmanv5==3.0.6` and its dependency `umodbus==1.0.4`
  are added to `requirements.txt`, pinned like everything else there.

### Data flow

```
logger 192.168.1.221:8899
  -> BmsPoller (reads, 5 s timeout each)
  -> decode + plausibility checks
  -> on_sample
       -> bms.db / bms_history
       -> MQTT goodwe/bms (not retained)
```

Inverter polling, the executor, `data.db` and the existing MQTT topics are
unchanged.

## Data model

### Register map

These are all Modbus holding registers on slave 1. The first fields were
decoded 2026-09-29 against GoodWe telemetry and the SolarMan app; the rest
on 2026-10-02 from a day of 1-minute samples (about 1,050) paired with the
nearest `inverter_history` row. "32-bit" means high word first, signed.
Ids are 0-based.

| Field | Register | Scale | Unit | Confirmed by |
|---|---|---|---|---|
| `state` | 0x1100 bits 0-2 | 1 charge, 2 discharge, 3 idle | | current sign, all samples (bits 10-12 repeat it) |
| `pack_voltage` | 0x1103 | ÷10 | V | GoodWe vbattery1 (196.5) |
| `current` | 0x1104:0x1105 32-bit | ÷100 | A, + = charging | GoodWe ibattery1, r = −0.9995 (opposite sign) |
| `bms_temperature` | 0x1106 | ÷10 | °C | GoodWe battery_temperature (36) |
| `soc` | 0x1107 | 1 | % | GoodWe battery_soc (33) |
| `cycle_count` | 0x1108 | 1 | | SolarMan (684); stepped 683 → 684 |
| `charge_voltage_limit` | 0x1109 | ÷10 | V | 216.0 = 60 × 3.6 V |
| `charge_current_limit` | 0x110A:0x110B 32-bit | ÷100 | A | GoodWe battery_charge_limit (18.5/7.4/0 ↔ 18/7/0) |
| `discharge_voltage_limit` | 0x110C | ÷10 | V | 174.0 = 60 × 2.9 V |
| `discharge_current_limit` | 0x110D:0x110E 32-bit, negative | ÷100, abs | A | GoodWe battery_discharge_limit (18) |
| `cell_voltage_max` / `_min` | 0x1110 / 0x1111 | ÷1000 | V | the cell list |
| `cell_voltage_max_id` / `_min_id` | 0x1112 / 0x1113 | 1 | cell 0-59 | 0x111A/B agree in most samples |
| `cell_temp_max` / `_min` | 0x1114 / 0x1115 | ÷10 (1 °C steps) | °C | r = 0.99 with 0x111C/D |
| `cell_temp_max_id` / `_min_id` | 0x1116 / 0x1117 | 1 | sensor no. | ids in the module 0x111E/F name |
| `module_voltage_max` / `_min` | 0x1118 / 0x1119 | ÷100 | V | max ≥ min in every sample; matches the cell sum of the module 0x111A/B name |
| `module_voltage_max_id` / `_min_id` | 0x111A / 0x111B | 1 | module 0-1 | as above |
| `module_temp_max` / `_min` | 0x111C / 0x111D | ÷10 | °C | SolarMan (27.2 / 25.7) |
| `module_temp_max_id` / `_min_id` | 0x111E / 0x111F | 1 | module 0-1 | same layout as 0x1118-0x111B; constant 1 / 0 so far |
| `soh` | 0x1120 | 1 | % | SolarMan (97) |
| `remaining_capacity` | 0x1121:0x1122 32-bit | ÷100 | Ah | r = 0.999 with SoC; 70.97 at 100 % = 2 × 37 Ah × 97 % |
| `charge_today_wh` / `discharge_today_wh` | 0x1123:0x1124 / 0x1125:0x1126 32-bit | 1 | Wh | rise only while charging / discharging, reset at midnight; 10-15 % above GoodWe's counters |
| `charge_total_kwh` / `discharge_total_kwh` | 0x112B:0x112C / 0x112D:0x112E 32-bit | 1 | kWh | step +1 per ~1 kWh charged / discharged; 4941 kWh / 684 cycles = 7.2 kWh |
| `fully_charged` | 0x1138 | 0/1 | | 1 only at SoC 100 % |
| `module_voltages` | (cells) | sum of each 30 cells | V | not a register - see below |
| `cell_mv` | 0x1500 + n | 1 | mV | 60 cells, 3.273-3.277 V |

Module voltages:

- The BMS reports only the max and min module (0x1118/0x1119) and their ids
  (0x111A/0x111B), not a per-module list. The first version read
  0x1118 + n as "module n", which was wrong: 0x1118 ≥ 0x1119 in all 1,054
  samples, and 0x1118 matched module 2's cell sum in 103 of them. In a
  4-module layout 0x111A/0x111B would have been read as module voltages.
- `module_voltages` is therefore the sum of each run of 30 cells (a Force H2
  module has 30 cells), in V to the mV.
- Checks: the cells sum to within 2 % of `pack_voltage`, and
  `module_voltage_max` / `_min` are within 2 % of the largest / smallest
  cell sum.

Cell count:

- The number of values from 0x1500 up to the first zero (60 here; 0x153C
  onwards is 0).
- The cell block read covers 0x1500-0x15FF in chunks of at most 32
  registers.
- Reading stops after the first chunk that contains a zero.
- An IllegalDataAddress on a chunk after the first ends the cell list at that
  chunk rather than failing the poll. Only 0x1500-0x153F is verified to
  respond.

Not decoded (kept in `raw_1100`):

- 0x110F = 3, constant;
- 0x1127:0x1128 / 0x1129:0x112A: a second pair of daily charge/discharge
  counters within 1-3 % of the first; what differs isn't known;
- 0x1136 = 2 and 0x1137 = 60, constant: module and cell count (the code
  takes both from the cell list instead);
- 0x1101, 0x1102, 0x1130-0x1135, 0x1139-0x113F: always 0.

### Table `bms_history` in `bms.db`

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | |
| `timestamp` | TEXT | local `YYYY-MM-DD HH:MM:SS` at poll start, like `inverter_history` |
| `timestamp_epoch` | INTEGER | indexed, like `inverter_history` |
| `state` | TEXT | |
| every other scalar field above | REAL / INTEGER | `fully_charged` as 0/1 |
| `module_voltages` | TEXT | JSON array of V |
| `cell_mv` | TEXT | JSON array of mV, all cells |
| `raw_1100` | TEXT | JSON array, all 64 registers 0x1100-0x113F |

`PRAGMA user_version` holds the schema version (2). Opening a version-1
file (the first deployment, 2026-10-01) adds the new columns and re-decodes
every row from `raw_1100` and `cell_mv`, which also corrects `cell_temp_*`
and `module_voltages` in those rows. Rows that no longer decode are left as
they were.

Rules:

- A register gets its own column only once it's confirmed against a second
  source (GoodWe telemetry, SolarMan, or an unambiguous physical check).
- A newly decoded column is added with a migration that also fills in past
  rows from `raw_1100`.
- Size is about 1,440 rows a day, roughly 300 MB a year.
- Cross-DB analysis: `ATTACH 'bms.db' AS bms` from a `data.db` session,
  joining on `timestamp_epoch`.

### MQTT `goodwe/bms`

- Not retained, published on every accepted sample.
- The payload is a JSON object with `timestamp`, all decoded fields, and
  `cell_mv` and `module_voltages`. Values are JSON numbers (not strings,
  unlike `telemetry`).
- `raw_1100` isn't published.
- Documented in `MQTT_TOPICS.md`.

## Errors and availability

- **All-or-nothing poll:**
  - one poll = the summary block read plus the cell block reads;
  - connecting and each read have a 5 s timeout (`asyncio.wait_for`);
  - any timeout, Modbus/V5 exception or short response fails the whole poll.
    There's no row, no payload and no partial sample.
- **Plausibility checks.** A failing check fails the poll, and the log names
  the check.
  - SoC and SOH are in 0-100.
  - Pack voltage is in 100-500 V.
  - There's at least 1 cell, and every cell is in 2.0-4.0 V.
  - Temperatures are in -30 to 80 °C.
  - The module voltages add up to within 2 % of the pack voltage.
- **Reconnect:** after a failed poll, the client is closed and a new one is
  created for the next attempt. That covers a logger reboot or an IP change.
- **Backoff:**
  - After 3 consecutive failed polls, the interval doubles on each further
    failure: 60 → 120 → 240 → 480 → 600 s (cap).
  - One success resets it to `BMS_POLL_SECONDS`.
- **Logging on transitions only.** Single misses are expected, e.g. while
  the logger uploads to the SolarMan cloud, so they don't warn.
  - INFO on the first and second consecutive failure, with the reason;
  - WARNING on the third (entering backoff);
  - DEBUG for further failures while in backoff;
  - INFO on recovery, with the failure count and outage duration.
- **Startup:** an unreachable logger never blocks or delays goodwe_manager's
  startup. The poller just begins in its retry cycle.
- **DB write failure:** logged at WARNING; the sample is dropped and polling
  continues.
- **MQTT down:** handled by `MqttBridge` (publishes are dropped while
  disconnected).
- **HA availability (follow-up):** sensors use `expire_after: 300`, so they
  go unavailable instead of freezing.
- **Optional HA alerts (follow-up):** "BMS data unavailable > 30 min" and
  "cell temperature > 45 °C", through `script.alert_notify`.

## Effect on the inverter loop

- The poller only awaits non-blocking socket I/O. While a read waits, the
  loop keeps running the inverter poll, the executor and MQTT.
- CPU cost: decoding ~130 registers once a minute.
- DB writes go to a different file through a different `aiosqlite`
  connection, so they never queue behind `data.db` writes.
- The network path is separate:
  - inverter: 192.168.1.106 via `eth0`;
  - logger: 192.168.1.221 via `wlan0`;
  - the logger reads the BMS over its own RS485 port, not the inverter's CAN
    link.

## Testing

Unit tests only use fakes (no network, no real sleeps).

- **Decoding.** The fixtures are the real 2026-09-29 dumps.
  - Expected: pack 196.5 V, SoC 33, SOH 97, BMS temperature 36.0, cell
    temperatures 27.2 / 25.7, cells 3.276 / 3.273 V, modules
    [98.25, 98.25], 60 cells.
  - Trailing zeros are cut off.
  - An all-zero cell block is invalid.
  - One failing fixture per plausibility check.
- **Poller:**
  - success → `on_sample` once;
  - the second read failing → no callback;
  - a read that never returns → timed out (short timeout in tests) and
    counted as a failure;
  - backoff sequence and reset, with a fake clock;
  - a new client after a failure;
  - log levels: INFO on failures 1-2, WARNING on the 3rd, DEBUG after,
    INFO on recovery;
  - a connect that never completes is timed out like a read.
- **Loop safety:** a poller against a never-answering fake runs next to a
  task ticking every 10 ms; assert the ticks stay regular.
- **Wiring:**
  - host or serial missing → disabled, no task, `pysolarmanv5` not in
    `sys.modules` (checked in a subprocess, since other tests import it);
  - a non-integer serial → ERROR log, disabled, no exception;
  - `BMS_POLL_SECONDS=5` → clamped to 10;
  - defaults for port, slave ID and interval.
- **Storage:**
  - `bms.db` and the table and index are created on first use;
  - a row round-trips, including the JSON columns.
- **MQTT:** `publish_bms` publishes to `goodwe/bms`, not retained, with
  numeric values.

## Deployment checks (manual, on the Pi)

1. Add `BMS_LOGGER_HOST=192.168.1.221` and `BMS_LOGGER_SERIAL=4060493924` to
   `.env`. Restart between executor commands (restarts drop the active
   command until Predbat's next call).
2. Compare `bms_history` values with the SolarMan app, within a poll or two.
3. `inverter_history` rows per minute don't drop after enabling (compare a
   few hours before and after; this setup writes about 27/min).
4. The SolarMan app keeps updating after a day of polling. If it doesn't,
   raise `BMS_POLL_SECONDS`.
5. Probe the current register during a charge and during an export. Done
   2026-10-02: 0x1104:0x1105, see the register map.
