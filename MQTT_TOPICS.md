# MQTT topics published by the optional bridge

Reference for the topics/payloads `mqtt_bridge.py` publishes when
`MQTT_HOST` is set (see README.MD's "MQTT bridge" section for setup).
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
{"raw_today": [{"from": "2026-09-21T00:00:00+02:00", "to": "2026-09-21T00:15:00+02:00", "value": 0.42}, ...],
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
    # Predbat expects.
    - name: "Goodwe PV Forecast Today"
      unique_id: goodwe_pv_forecast_today
      state_topic: "goodwe/forecast/pv"
      value_template: "{{ value_json.today | length }}"
      unit_of_measurement: "periods"
      json_attributes_topic: "goodwe/forecast/pv"
      json_attributes_template: "{{ {'detailedForecast': value_json.today} | tojson }}"
    - name: "Goodwe PV Forecast Tomorrow"
      unique_id: goodwe_pv_forecast_tomorrow
      state_topic: "goodwe/forecast/pv"
      value_template: "{{ value_json.tomorrow | length }}"
      unit_of_measurement: "periods"
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

## Failure behavior

A publish failure on any of the four data topics above is logged and
skipped, not retried until the next natural publish point (next 1Hz
tick for telemetry, next day rollover for the three retained ones) -
see `main.py`'s side-channel try/except blocks around each. A stale
retained price/forecast payload has no distinct "this is stale" signal
today; treat `bridge/status` going `offline` as the closest proxy.
