# MQTT topics published by the optional bridge

Reference for the topics/payloads `mqtt_bridge.py` publishes (and, for
battery control, subscribes to) when `MQTT_HOST` is set (see README.MD's "MQTT bridge" section for setup).
Every topic is prefixed with `MQTT_TOPIC_PREFIX` (default `goodwe`) -
paths below omit the prefix for brevity, e.g. `telemetry` is really
published to `goodwe/telemetry`.

## `bridge/status` (retained)

`online` while connected, `offline` on a clean shutdown or as the
connection's Last Will (published by the broker if the process dies
without disconnecting cleanly). Use this to detect a stale/missing
bridge rather than assuming silence means "nothing changed."

## `telemetry` (not retained, ~1Hz)

The same sample just written to `data.db`'s `inverter_history` row - all
of `sensors.SELECTED_SENSORS` plus `CalculatedValuesEvaluator`'s derived
fields (`_hourly_meter_export`, `_daily_load`, etc.), as a flat JSON
object. **Every value is a string** (mirrors how `inverter_history` rows
are read back) - HA/Predbat consumers must coerce numeric fields
themselves.

## `prices/export` (retained, published at startup and on day rollover)

```json
{"raw_today": [{"start": "2026-09-21T00:00:00+02:00", "end": "2026-09-21T00:15:00+02:00", "value": 0.42}, ...],
 "raw_tomorrow": [...]}
```

RCE day-ahead export prices in zł/kWh, with the 23% prosument VAT bonus
applied (see README.MD's `RCE_EXPORT_GRANULARITY`/
`RCE_EXPORT_NEGATIVE_PRICES` switches). `raw_tomorrow` is `[]` (not an
error) when tomorrow's RCE prices aren't cached yet - see
`export_price.build_export_price_payload`'s docstring.

## `prices/import` (retained, published at startup and on day rollover, only if `TARIFF_IMPORT_CONFIG` is set)

Same `{"raw_today": [...], "raw_tomorrow": [...]}` shape as above, band
values from `tariff_engine.bands_for_day()` (see `TARIFF_SCHEMA.md`) in
zł/kWh instead of RCE prices.

## `forecast/pv` (retained, published at startup and on day rollover)

```json
{"today": [{"period_start": "2026-09-21T06:00:00+02:00", "pv_estimate": 1.2, "pv_estimate10": 0.8, "pv_estimate90": 1.6}, ...],
 "tomorrow": [...]}
```

The combined Solcast forecast (`pv_forecast_payload.build_detailed_forecast`),
reusing whatever `ForecastPrefetchThread` already fetched into
`forecast_history.db` rather than calling Solcast again. `tomorrow` is
`[]` when tomorrow's forecast hasn't been fetched yet. Values are kWh
per period. This is the shape Predbat's `pv_forecast_today`/
`pv_forecast_tomorrow` config expects (as a `detailedForecast`-style
list), though the JSON keys here are just `today`/`tomorrow` - map them
to those two Predbat config keys in your HA template/sensor setup.

## Example Home Assistant sensor config

Excerpt from the actual deployed `configuration.yaml` on `raspberry4.local`
(see the `home-assistant-raspberry4` repo) - one `mqtt:` block can only have
a single `sensor:`/`binary_sensor:` key each, so these entries are merged
into whatever else that host's config already defines under those keys
rather than living in their own file.

```yaml
mqtt:
  sensor:
    - name: "Goodwe PV Power"
      unique_id: goodwe_pv_power
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json.ppv }}"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
    - name: "Goodwe Load Power"
      unique_id: goodwe_load_power
      state_topic: "goodwe/telemetry"
      # load_ptotal, not house_consumption: house_consumption is a
      # derived sum of several registers that goes visibly wrong (even
      # negative) during battery charge/discharge ramps, while
      # load_ptotal is a single raw register with no such error.
      value_template: "{{ value_json.load_ptotal }}"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
    - name: "Goodwe Grid Power"
      unique_id: goodwe_grid_power
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json.pgrid }}"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
    - name: "Goodwe Battery Power"
      unique_id: goodwe_battery_power
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json.pbattery1 }}"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
    - name: "Goodwe Battery SOC"
      unique_id: goodwe_battery_soc
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json.battery_soc }}"
      unit_of_measurement: "%"
      device_class: battery
      state_class: measurement
    # Daily energy counters - sourced from the inverter's own lifetime
    # counters via goodwe_manager's midnight-anchored deltas (e_day is
    # already daily), not integrated from power readings, so these don't
    # drift. These map directly to Predbat's
    # load_today/pv_today/import_today/export_today.
    - name: "Goodwe PV Today"
      unique_id: goodwe_pv_today
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json.e_day }}"
      unit_of_measurement: "kWh"
      device_class: energy
      state_class: total_increasing
    - name: "Goodwe Load Today"
      unique_id: goodwe_load_today
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json._daily_load }}"
      unit_of_measurement: "kWh"
      device_class: energy
      state_class: total_increasing
    - name: "Goodwe Import Today"
      unique_id: goodwe_import_today
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json._daily_meter_import }}"
      unit_of_measurement: "kWh"
      device_class: energy
      state_class: total_increasing
    - name: "Goodwe Export Today"
      unique_id: goodwe_export_today
      state_topic: "goodwe/telemetry"
      value_template: "{{ value_json._daily_meter_export }}"
      unit_of_measurement: "kWh"
      device_class: energy
      state_class: total_increasing
    # Price sensors for Predbat's metric_octopus_export/metric_octopus_import
    # (the generic, non-Octopus rate mechanism). json_attributes_topic with
    # no template pulls every top-level JSON key (raw_today, raw_tomorrow)
    # onto the entity as attributes directly.
    - name: "Goodwe Export Price"
      unique_id: goodwe_export_price
      state_topic: "goodwe/prices/export"
      value_template: "{{ (value_json.raw_today[0].value) if value_json.raw_today else 'unknown' }}"
      json_attributes_topic: "goodwe/prices/export"
      unit_of_measurement: "zł/kWh"
    - name: "Goodwe Import Price"
      unique_id: goodwe_import_price
      state_topic: "goodwe/prices/import"
      value_template: "{{ (value_json.raw_today[0].value) if value_json.raw_today else 'unknown' }}"
      json_attributes_topic: "goodwe/prices/import"
      unit_of_measurement: "zł/kWh"
    # PV forecast for Predbat's pv_forecast_today/pv_forecast_tomorrow +
    # *_attribute: detailedForecast config - two entities from the one
    # combined MQTT topic, each reshaped via json_attributes_template to
    # carry only its own day's list under the exact "detailedForecast" key
    # Predbat expects. The state must be the summed daily kWh, not a
    # period count: Predbat's solcast.py cross-checks sum(pv_estimate)
    # against the sensor's own state to detect kWh-per-slot vs.
    # kW-average-per-slot data (factor 1.0 vs 2.0/4.0) - a state that
    # isn't that sum matches neither factor and the forecast can't be
    # scaled correctly.
    - name: "Goodwe PV Forecast Today"
      unique_id: goodwe_pv_forecast_today
      state_topic: "goodwe/forecast/pv"
      value_template: "{{ value_json.today | sum(attribute='pv_estimate') | round(2) }}"
      unit_of_measurement: "kWh"
      device_class: energy
      json_attributes_topic: "goodwe/forecast/pv"
      json_attributes_template: "{{ {'detailedForecast': value_json.today} | tojson }}"
    - name: "Goodwe PV Forecast Tomorrow"
      unique_id: goodwe_pv_forecast_tomorrow
      state_topic: "goodwe/forecast/pv"
      value_template: "{{ value_json.tomorrow | sum(attribute='pv_estimate') | round(2) }}"
      unit_of_measurement: "kWh"
      device_class: energy
      json_attributes_topic: "goodwe/forecast/pv"
      json_attributes_template: "{{ {'detailedForecast': value_json.tomorrow} | tojson }}"

  binary_sensor:
    # Answers "is the goodwe_manager MQTT bridge alive" directly, rather
    # than inferring it from staleness of other goodwe/* topics - backed
    # by an MQTT Last Will (fires on an unclean disconnect) plus an
    # explicit publish on clean connect/disconnect.
    - name: "Goodwe Bridge Status"
      unique_id: goodwe_bridge_status
      state_topic: "goodwe/bridge/status"
      payload_on: "online"
      payload_off: "offline"
      device_class: connectivity
```

## `control/set` (subscribed, QoS 1, not retained) - only when `CONTROL_MODE` is `shadow` or `on`

```json
{"mode": "charge", "power_w": 3000, "target_soc": 80, "source": "predbat", "ttl_s": 900}
```

| Field | Meaning |
|---|---|
| `mode` | `auto`, `charge`, `export`, `freeze_charge`, `freeze_export` |
| `power_w` | required for `charge`/`export`; clamped to 100 W … min(`CONTROL_MAX_BATTERY_W`, live BMS limit) |
| `target_soc` | optional for `charge`/`export`: charge → `freeze_charge` once reached; export → `auto` once reached |

`freeze_charge` raises the on-grid minimum SoC (`battery_discharge_depth`) to the current SoC (never below `CONTROL_MIN_SOC`) and follows a rising SoC in 3-point steps; before the first SoC reading it stays `auto` (`reason: waiting for SoC`). The inverter only discharges again 5 points above its minimum, so a freeze ending near `CONTROL_MIN_SOC` shows a warning.
| `ttl_s` / `expires_at` | one required unless `mode` is `auto`; max 3600 s / 60 min ahead. Re-send before it runs out (Predbat: `repeat: true`) |
| `stop` | with `mode: auto` only: `charge` or `export` - clears the current command only if it is in that domain |
| `source`, `id` | free text, echoed in the state |

Invalid commands are rejected (the current one stays) and reported in `control/state.last_error`.

## `control/reserve/set` (subscribed, retained)

Integer SoC % software reserve; empty payload clears it. In `auto`, SoC at or below it (for 30 s) switches to `freeze_charge` until SoC is 2 points above.

## `control/state` (retained, on change and every 10 s)

```json
{"mode": "charge", "effective_mode": "freeze_charge", "power_w": 3000, "power_applied_w": 3000,
 "power_clamped": false, "target_soc": 80, "source": "predbat", "expires_at": "2026-09-26T15:10:00+02:00",
 "since": "2026-09-26T14:58:12+02:00", "override": null, "reserve_soc": 25, "reason": "target_soc reached",
 "shadow": false, "off_grid": false, "freeze_floor": 80, "applied": true, "last_error": null, "warnings": [],
 "writes_today": 14,
 "registers": {"ems_mode": 1, "ems_power_limit": 0, "battery_charge_current": 19.0,
               "battery_discharge_current": 19.0, "battery_discharge_depth": 80, "soc_upper_limit": 100,
               "work_mode": 3}}
```

`applied` is true only when the last read-back of all five mode settings matches what `effective_mode` needs. `off_grid` is true during a grid outage (and 60 s after it), when everything is held at `auto`.

## Home Assistant + Predbat example

```yaml
mqtt:
  sensor:
    - name: "Goodwe Control Mode"
      unique_id: goodwe_control_mode
      state_topic: "goodwe/control/state"
      value_template: "{{ value_json.effective_mode }}"
      json_attributes_topic: "goodwe/control/state"
  binary_sensor:
    - name: "Goodwe Control Applied"
      unique_id: goodwe_control_applied
      state_topic: "goodwe/control/state"
      value_template: "{{ 'ON' if value_json.applied else 'OFF' }}"
  number:
    - name: "Goodwe Reserve"
      unique_id: goodwe_reserve
      command_topic: "goodwe/control/reserve/set"
      state_topic: "goodwe/control/state"
      value_template: "{{ value_json.reserve_soc | int(0) }}"
      min: 0
      max: 100
      unit_of_measurement: "%"
      retain: true

script:
  goodwe_control:
    alias: "GoodWe control command"
    fields:
      mode: {}
      power: {}
      target_soc: {}
      stop: {}
    sequence:
      - action: mqtt.publish
        data:
          topic: "goodwe/control/set"
          qos: 1
          payload: >-
            {{ {'mode': mode, 'source': 'predbat', 'ttl_s': 900}
               | combine({'power_w': power | int} if power is defined and power not in ('', None) else {})
               | combine({'target_soc': target_soc | int} if target_soc is defined and target_soc not in ('', None) else {})
               | combine({'stop': stop} if stop is defined and stop else {})
               | to_json }}
```

Predbat `apps.yaml` (custom inverter section):

```yaml
  inverter:
    has_target_soc: true
    support_charge_freeze: true
    support_discharge_freeze: true
    charge_control_immediate: true
    has_timed_pause: false
  reserve: number.goodwe_reserve
  charge_start_service:
    service: script.goodwe_control
    mode: charge
    power: "{power}"
    target_soc: "{target_soc}"
    repeat: true
  charge_freeze_service:
    service: script.goodwe_control
    mode: freeze_charge
    repeat: true
  charge_stop_service:
    service: script.goodwe_control
    mode: auto
    stop: charge
    repeat: true
  discharge_start_service:
    service: script.goodwe_control
    mode: export
    power: "{power}"
    target_soc: "{target_soc}"
    repeat: true
  discharge_freeze_service:
    service: script.goodwe_control
    mode: freeze_export
    repeat: true
  discharge_stop_service:
    service: script.goodwe_control
    mode: auto
    stop: export
    repeat: true
```

Battery model settings from the spec: `best_soc_min` 20 % and `best_soc_keep` 25 % of `soc_max`, `set_charge_low_power` / `set_export_low_power` on.

## Failure behavior

A publish failure on any of the four data topics above is logged and
skipped, not retried until the next natural publish point (next 1Hz
tick for telemetry, next day rollover for the three retained ones) -
see `main.py`'s side-channel try/except blocks around each. A stale
retained price/forecast payload has no distinct "this is stale" signal
today; treat `bridge/status` going `offline` as the closest proxy.
