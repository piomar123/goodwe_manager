# Predbat MQTT Bridge — Design

## Goal

Let Home Assistant run Predbat in **read-only** mode (plan evaluation only,
no inverter control yet) against this household's real Goodwe PV/battery
setup, without a second process ever talking to the inverter directly.
`goodwe_manager` already owns the sole connection to the inverter (a
single asyncio loop polling every ~1s); this design turns it into an MQTT
bridge that publishes everything Predbat needs to plan, sourced from data
`goodwe_manager` already fetches or already has cached.

Out of scope for this phase: any MQTT command/write topics (Predbat
writing charge/discharge schedules back to the inverter). That's a
natural follow-on once the read-only evaluation shows Predbat's plans are
worth acting on, using the same bridge in the opposite direction — but is
not designed here.

## Why a bridge instead of a second poller

Goodwe's inverter protocol doesn't tolerate two independent pollers
against the same device — concurrent connections cause comms errors on
the shared local link. `goodwe_manager` is already the sole owner of that
connection (see `main.py`'s `AsyncioThread`) and already exposes reads
(SSE `/listen`, `/history/*.json`) and writes (`/eco`, `/config` POST)
over HTTP. This design adds an MQTT publish path alongside that existing
HTTP surface, using data the polling loop already has in memory — no new
inverter reads, so no new race.

## Components

### 1. MQTT connection lifecycle

- New dependency: `aiomqtt` (async-native; fits the existing single
  event-loop design in `AsyncioThread` — no new thread, no blocking calls
  from the async loop).
- New env vars (all optional; unset `MQTT_HOST` disables the whole bridge
  feature — a fresh checkout or another user's fork behaves exactly as
  today with zero config):
  - `MQTT_HOST`, `MQTT_PORT` (default 1883), `MQTT_USERNAME`,
    `MQTT_PASSWORD`, `MQTT_TOPIC_PREFIX` (default `goodwe`).
- Client connects once when `AsyncioThread` starts, alongside the
  inverter connection. A publish failure is caught and logged, never
  propagated into the inverter polling loop — MQTT being down must never
  stop inverter polling/storage/SSE from working.
- **Reconnection**: a publish failure also marks the client as
  disconnected, and every subsequent publish opportunistically attempts
  to reconnect before giving up — so a Mosquitto restart on the Pi no
  longer requires restarting the whole `goodwe_manager` service to
  recover. Reconnect attempts are rate-limited (default: at most once
  per 30s, via `reconnect_interval_seconds`) since telemetry publishes
  at ~1Hz and hammering an unreachable broker every second would be
  wasteful and spam logs. Timing uses `time.monotonic()`, not wall-clock
  time, to stay correct across NTP adjustments.
- **Last Will and Testament**: client configured with LWT topic
  `<prefix>/bridge/status`, payload `offline`, retained. This is what
  fires if the process dies uncleanly (crash, network drop, `kill -9`) —
  per the MQTT spec, a clean client-initiated disconnect does **not**
  trigger the will.
- Because of that spec behavior, a clean shutdown needs its own explicit
  publish: `<prefix>/bridge/status` = `online` (retained) right after
  connecting, and `<prefix>/bridge/status` = `offline` (retained)
  explicitly published just before a graceful disconnect (service
  restart, `Ctrl-C`, etc.) — otherwise a deliberate stop would leave the
  retained topic stuck on `online` forever, hiding a real outage from
  Predbat/HA.
- HA side: a `binary_sensor` (`device_class: connectivity`) on this
  topic, so "is the bridge alive" is answered by this one entity rather
  than inferred from staleness of other topics.

### 2. Live telemetry + daily energy counters

- Every iteration of the existing polling loop (`_get_inverter_data`,
  `main.py:202-226`), right after `sensors_data_with_calculated` is
  computed, publish that dict as one JSON payload to
  `<prefix>/telemetry` (non-retained — always a fresh live reading).
- Predbat needs daily-resetting **energy** counters (`load_today`,
  `pv_today`, `import_today`, `export_today`), not power. The inverter
  already reports true cumulative counters (`e_day` for PV — already
  daily; `e_total_exp`/`e_total_imp`, `meter_e_total_exp`/
  `meter_e_total_imp`, `e_load_total`, `e_bat_charge_total`/
  `e_bat_discharge_total` — all lifetime totals). Using HA's built-in
  Riemann-sum integration over instantaneous watts would introduce
  drift/quantization error the inverter's own counters don't have.
- Instead, extend `CalculatedValuesEvaluator` (which already computes
  hour-start-anchored deltas for `hourly_summary` — see
  `_seed_hour_start_baseline`, `_hourly_meter_export`,
  `_hourly_meter_import`, `_hourly_load`) with the same technique
  anchored at **midnight**: `_daily_load`, `_daily_meter_import`,
  `_daily_meter_export`. `e_day` is reused as-is for PV (already daily).
  These four values ride along in the same `<prefix>/telemetry` payload.
- HA side: `mqtt: sensor:` entries with `value_template` pulling each
  field out of the JSON payload — same manual-sensor pattern already
  used for the GoHeishaMon integration in `home-assistant-raspberry4`.
  These map directly to Predbat's `load_today`/`pv_today`/
  `import_today`/`export_today` config.

### Units: zł throughout — no currency conversion in goodwe_manager

Everything — `tariff_engine.price_at()`, the tariff config file, the RCE
export band builder, `_calculate_income.py`, and the MQTT payloads
published to Predbat — stays denominated in **zł**, matching the invoice.
No conversion step, no implicit ×100 anywhere in the code.

Predbat's own optimizer math is scale-invariant (comparing import cost vs
export revenue to plan actions works identically regardless of whether
rates are in zł, grosz, or pence, as long as import and export match each
other). The one place scale genuinely matters is Predbat's **absolute
currency-valued threshold settings** — e.g. `metric_min_improvement`/
`metric_min_improvement_discharge` (default `0.1`), meant to mean
"ignore an improvement smaller than this, it's negligible." That default
is calibrated against Predbat's own example rates (7.5–40 pence) — left
at `0.1` against zł-scale rates (0.35–1.3 per kWh), `0.1` stops being
negligible (it's 10-30% of a typical price difference) and would cause
Predbat to silently discard real, worthwhile charge/discharge
opportunities.

So this is a **Predbat-side `apps.yaml` setup note**, not goodwe_manager
code: explicitly set `metric_min_improvement`, `metric_min_improvement_discharge`,
and any other absolute-currency threshold Predbat exposes to a
zł-appropriate value (e.g. `0.001` in place of the pence-tuned `0.1`)
when configuring Predbat against this bridge, rather than relying on its
shipped defaults. `currency_symbols: ['zł', 'zł']` (or just leaving the
default) is purely cosmetic and doesn't affect correctness.

### 3. Export price (RCE + prosument VAT bonus)

- `rce_prices.db` already caches day-ahead 15-minute prices, with a
  background thread (`rce_prefetch.py`) fetching tomorrow's prices daily
  at 14:15. This already matches the cadence Predbat needs.
- Predbat's generic (non-Octopus) rate mechanism (`metric_octopus_export`
  pointing at a custom HA sensor) expects that sensor to expose
  `raw_today`/`raw_tomorrow` attributes: a list of `{from, to, value}`
  bands. `from`/`to` must be ISO 8601 timestamps with an explicit UTC
  offset (Predbat's docs show e.g. `2025-11-08T00:00:00+01:00`) — it
  reads timestamped intervals, not a fixed slot count, so DST transition
  days (92 or 100 15-minute periods, per the existing comment in
  `rce_storage.is_cached`) resolve themselves correctly as long as every
  timestamp is timezone-aware (`Europe/Warsaw` via `zoneinfo`), never
  naive local time.
- New pure function in goodwe_manager: converts cached `rce_prices` rows
  for a given business date into that `{from, to, value}` band list,
  applying `value = rce_pln_per_mwh / 1000 * 1.23` (the prosument export
  VAT bonus), in zł — see the **Units** note below.
- Publish cadence: **not** every telemetry tick (would spam MQTT/HA
  history for a value that only changes twice a day). Publish
  `<prefix>/prices/export` (retained) at bridge startup (so a restart
  doesn't wait for the next natural change), at midnight rollover
  (yesterday's "tomorrow" becomes "today"), and whenever the day-ahead
  prefetch thread successfully fetches tomorrow's prices. Retained so a
  fresh HA/broker restart sees the last known prices immediately.
- HA side: a template sensor reshaping the payload into `raw_today`/
  `raw_tomorrow` attributes, referenced by Predbat's `metric_octopus_export`.
- **Tomorrow not cached yet**: between midnight and the prefetch thread's
  ~14:15 success (or later, on a retry day), tomorrow's RCE prices aren't
  known. The publish just omits `raw_tomorrow` (or publishes it empty) in
  that case rather than blocking or guessing — the next successful
  prefetch triggers a republish with `raw_tomorrow` filled in. This
  mirrors the existing `/prices` HTTP endpoint's own fallback behavior
  (`rce.get_rce_15min` always falls back to a live fetch on a cache miss),
  though the MQTT bridge publishes on the prefetch thread's cadence rather
  than doing its own live fetch per publish.

### 4. Import price (G12w, and configurable for other tariffs)

- G12w is a fixed schedule (not a market price), but it varies by season
  (summer/winter windows differ) and by **work day vs. non-work day**
  (weekends + Polish public holidays are treated as off-peak all day).
  Predbat's static `rates_import` schedule in `apps.yaml` does **not**
  support seasonal/date-range conditions (confirmed against Predbat's
  docs — only time-of-day and day-of-week repeat identically all year;
  `rates_import_override` is for one-off dated overrides, not an ongoing
  seasonal split).
- So import prices are computed the same way as export prices: a new
  tariff engine in `goodwe_manager` evaluates the configured schedule for
  a given calendar day and publishes `raw_today`/`raw_tomorrow` via MQTT,
  same `{from, to, value}` shape (ISO 8601 timestamps with explicit UTC
  offset, `value` in zł — see **Units** above) as the export price
  payload. Predbat consumes it via `metric_octopus_import`, the same
  generic mechanism used for export. This also means a future
  distributor/tariff change is a config edit in `goodwe_manager`, not a
  hand-edited Predbat schedule.
  - **No forced 15-minute slicing**: unlike the RCE export data, which is
    naturally 15-minute periods, G12w's bands are hours wide (e.g.
    15:00-17:00). The tariff engine emits one output interval per
    contiguous band as evaluated for that day — a 2-hour band becomes one
    `{from, to, value}` entry, not eight artificially-sliced 15-minute
    ones. Predbat's rate-interval mechanism doesn't require uniform
    slot width (Octopus Agile's own 30-minute bands are just a property
    of Octopus's data, not a Predbat requirement).
- **Config**: new env var `TARIFF_IMPORT_CONFIG=<path>` (unset = feature
  disabled). Points at a YAML file; the real file (e.g. `tariff_import.yaml`)
  is added to `.gitignore`, same pattern as `secrets.yaml` in
  `home-assistant-raspberry4` — so another user's fork never accidentally
  picks up this household's rates.

- **Distribution vs. sales tariffs can differ**: in Poland, the
  time-of-use *schedule* (which hours are cheap/expensive) is set by the
  distributor's tariff (e.g. PGE Dystrybucja's G12w), but the *seller*
  (sprzedawca) prices energy separately and isn't required to use the
  same zone boundaries — some sellers mirror the distributor's zones,
  some sell a flat rate, some define their own. The delivered price at
  any instant is the sum of whichever components apply. So the schema's
  top level is a map of independently-scheduled **components**, each
  using the same `prices`/`season_boundaries`/`bands`/`default_price`
  shape described below; the tariff engine evaluates every component for
  a given instant and sums their prices.

  For a bill where the distributor and seller happen to already be
  combined into one delivered per-zone price (this household's actual
  case — the invoice gives one blended rate per zone, not separate
  distribution/energy line items), a single component is enough:

  ```yaml
  components:
    total:
      prices:
        cheap: 0.6907      # off-peak (strefa tańsza), zł/kWh gross, all taxes incl.
        expensive: 1.3078  # peak (strefa droższa), zł/kWh gross, all taxes incl.

      season_boundaries:
        summer:
          start: "01.04"
          end: "30.09"
        winter:
          start: "01.10"
          end: "31.03"

      bands:
        default:                 # applies in both seasons — see "default bands" below
          - start: "22:00"
            end: "06:00"
            days: "Work"
            price: cheap
          - days: "Sa,Su,Holiday"
            price: cheap
        summer:
          - start: "15:00"
            end: "17:00"
            days: "Work"
            price: cheap
        winter:
          - start: "13:00"
            end: "15:00"
            days: "Work"
            price: cheap

      default_price: expensive
  ```

  This is the actual PGE Dystrybucja G12w schedule (their tariff card is
  the only one among Polish distributors that varies the midday off-peak
  window by season — confirmed via PGE's published tariff hours; night
  off-peak 22:00-06:00 is constant year-round). Zone rates above are this
  household's actual gross zł/kWh (all taxes included) from the latest
  invoice.

  A household whose seller genuinely prices differently from the
  distributor's zones would instead define two components and let the
  engine sum them, e.g.:

  ```yaml
  components:
    distribution:            # PGE Dystrybucja's G12w zones/fee
      prices: {cheap: 0.18, expensive: 0.35}
      bands: {default: [{days: "Sa,Su,Holiday"}], ...}  # abbreviated
      default_price: expensive
    sales:                   # seller's own flat-rate energy product
      prices: {flat: 0.55}
      bands:
        default:
          - price: flat       # no start/end/days — whole day, every day
      default_price: flat
  ```

  - **Default bands** (`bands.default`): checked for both seasons in
    addition to that season's own list, so a band that doesn't differ
    between summer and winter — here, the night off-peak window and the
    all-day weekend/holiday off-peak — is written once instead of copied
    into both `summer:` and `winter:`. Only the midday window, which PGE
    actually shifts by season, needs a season-specific entry. **Resolution
    order**: that season's own bands are checked first (in file order),
    then `bands.default` (in file order), then `default_price` — more
    specific wins, same convention as CSS specificity. This doesn't change
    the outcome for the PGE schedule above (nothing overlaps), but it's a
    real rule for any tariff whose seasonal and default bands do overlap
    (e.g. a season-specific band meant to override part of a default
    window).

  - **List format**: each season's `bands` is a YAML block list of
    mappings (as above), not inline flow-style `{...}` maps — easier to
    read and diff.
  - **Time format**: `HH:MM`, with `:SS` optional (`HH:MM:SS` still
    accepted for sub-minute precision if ever needed, but not required).
  - **`start`/`end` are optional together**: a band with neither key
    (see the `Sa,Su,Holiday` bands above) covers the entire day
    (00:00–24:00) — this is the normal way to write an all-day band, not
    a special-cased flag; writing `start: "00:00"` / `end: "24:00"`
    explicitly is equivalent and still supported for anyone who prefers
    to spell it out.
  - **Season boundary dates**: `dd.mm` format (e.g. `"01.04"`). Boundaries
    wrap the year (winter spans Oct→Mar across the year boundary) — the
    matching logic must handle a range whose start month is numerically
    after its end month.
  - **Named prices**: bands reference a price by key (`prices:` map)
    instead of repeating literals, so a rate change is a one-line edit.
    G12w only has two zones (`cheap`/`expensive`) — an earlier draft of
    this schema had a third, unused tier, dropped since it didn't
    correspond to anything G12w actually has.
  - **Day-spec grammar**: weekday abbreviations (`Mo,Tu,We,Th,Fr,Sa,Su`),
    ranges (`Mo-Fr`), comma lists (`Sa,Su`), and two keywords —
    `Work` (Mon–Fri excluding public holidays) and `Holiday` (Polish
    public holidays) — covering G12w's actual work-day/non-work-day
    rule without spelling out every combination.
  - Holiday dates come from the `holidays` PyPI package
    (`holidays.PL()`), not a maintained date list — it already handles
    Poland's fixed and Easter-based moving holidays correctly per year.
  - **Resolution**: bands checked first-match-wins in file order.
    `default_price` is a **required** field within each component
    (applies across both that component's seasons) — any slot no band
    in that component matches falls back to it, making "did I leave a
    coverage gap" an explicit, testable condition rather than silent
    undefined behavior. In the `total` schedule above it correctly
    resolves to `expensive` for all weekday hours outside the listed
    off-peak bands. The final published price for an instant is the sum
    of every component's resolved price (its own bands, or its own
    `default_price`) — for the single-component `total` case this sum
    is just that one value.
- This is meaningfully more parsing/validation surface than the rest of
  the bridge, so the implementation plan must call out explicit unit
  test cases per quirk: day-spec parsing (abbreviations, ranges, lists,
  both keywords), season-boundary wraparound, DST-day timestamp
  generation, holiday-date lookups, the `default_price` fallback path,
  and multi-component summation.

- **Example tariff definitions directory**: checked-in templates live
  under `tariff_examples/` (e.g. `tariff_examples/g12w_pge.yaml`), using
  real, publicly-documented distributor schedules (hours are published
  tariff-card information, not personal) — so this household's actual
  gitignored `tariff_import.yaml` starts as a copy of an example, not
  written from scratch, and another `goodwe_manager` user can add their
  own distributor's example alongside it. The example's rates are this
  household's real zł/kWh values rather than placeholders — electricity
  tariff rates aren't sensitive the way credentials are, and G12w's
  actual PLN figures are directly derivable from PGE's own published
  tariff card anyway, so there's no meaningful privacy reason to obscure
  them.

- **Reusable across scripts**: the parsing/evaluation logic (YAML
  loading, day-spec grammar, season/band resolution, multi-component
  summation) lives in its own module, `tariff_engine.py`, exposing a
  pure function `price_at(config, dt: datetime) -> float` (plus the
  `raw_today`/`raw_tomorrow` band-list builder used for MQTT
  publishing). Both the MQTT bridge and `_calculate_income.py` import
  this same module — no duplicated tariff logic. Concretely,
  `_calculate_income.py` currently values all imported energy at a
  single hardcoded flat rate (`IMPORT_PRICE_KWH = 1.1`), which doesn't
  reflect G12w's actual zone pricing at all; with `tariff_engine`
  available, `compute_hour_income` can look up the real per-hour
  component-summed rate via `tariff_engine.price_at(config, hour_dt)`
  when a `--tariff-config`/`TARIFF_IMPORT_CONFIG` is available, falling
  back to the existing flat constant when it isn't — so a fork with no
  tariff file configured keeps behaving exactly as it does today.

### 5. PV forecast sharing

- `goodwe_manager` already fetches and combines (east+west) Solcast
  forecasts via `solcast.py`, against Solcast's free-tier cap (10
  calls/day, shared budget). A second independent poller (e.g. HA's own
  Solcast integration) would consume that same shared budget for no
  benefit.
- Instead, publish the already-computed combined forecast via MQTT
  (`<prefix>/forecast/pv`, retained, republished whenever a new forecast
  fetch completes), as `{"today": [...], "tomorrow": [...]}`, each a
  half-hourly list built by `pv_forecast_payload.build_detailed_forecast`
  from `forecast_history.get_latest_merged`'s `{"HH:MM": {"c10", "c50",
  "c90"}}` shape. Both halves are needed — Predbat plans on a 48h
  horizon (e.g. whether tonight's cheap-rate charge is worth it depends
  on tomorrow's expected solar), and a single day's series alone can't
  answer that.
- Each list entry is shaped `{"period_start", "pv_estimate",
  "pv_estimate10", "pv_estimate90"}` (`period_start` tz-aware ISO8601,
  `c50`/`c10`/`c90` renamed to the `pv_estimate`/`pv_estimate10`/
  `pv_estimate90` field names Predbat/Solcast conventionally use) —
  this is the `detailedForecast` attribute shape Predbat's own
  real-world config templates (e.g. `templates/huawei.yaml`) expect
  behind two separate entities, `pv_forecast_today` and
  `pv_forecast_tomorrow`.
- HA side: two template sensors (`pv_forecast_today`/
  `pv_forecast_tomorrow`), each exposing its half of this topic's
  payload as its own `detailedForecast` attribute, referenced manually
  in Predbat's `apps.yaml` (not via the Solcast auto-discovery regex,
  since these aren't Solcast-native HA entities).

### 6. Battery/inverter efficiency estimation (offline analysis, not a live feature)

- New one-off script, `_estimate_battery_efficiency.py`, following the
  existing underscore-prefixed script convention (`_calculate_income.py`,
  `_backfill_hourly_summary.py`) — run manually against the ~2 years of
  `inverter_history` in `data.db`; outputs recommended values for you to
  paste into Predbat's `apps.yaml` yourself. Nothing auto-applies them.
- Predbat models the AC↔battery path as two lossy stages (inverter DC/AC
  conversion, then battery internal/chemical loss) and wants them
  separately: `battery_loss`, `inverter_loss_charge`,
  `inverter_loss_discharge` (decimal fractions).
- This split is measurable because `pbattery1` (`vbattery1 × ibattery1`)
  is read at the battery's own DC terminals, distinct from AC-side
  grid/load power:
  - **Inverter conversion loss** (charge and discharge computed
    separately): for each charge/discharge session (a contiguous run
    where `pbattery1` holds one sign above a noise threshold), compare
    AC-side energy attributable to that session against `pbattery1`
    energy integrated over the same window.
  - **Battery internal round-trip loss**: find matched cycle pairs where
    SOC returns to roughly its starting level within a short window (to
    limit self-discharge/calendar-aging skew), compare total DC energy
    in vs. out across each cycle, and average over as many such cycles
    as the 2 years of history provide.
- Output: a report (recommended values plus sample-size/confidence
  caveats, e.g. "based on N charge sessions, N cycle pairs") —
  deliberately approximate, since real-world PV/load noise means
  sessions aren't perfectly clean charge-only or discharge-only periods.

## Data flow summary

```
Inverter (Goodwe local API)
   │  (sole connection — goodwe_manager only)
   ▼
goodwe_manager AsyncioThread poll loop (~1s)
   │
   ├─► data.db (existing: inverter_history, hourly_summary)
   ├─► SSE /listen (existing, unchanged)
   │
   ├─► MQTT <prefix>/telemetry              (non-retained, every tick)
   ├─► MQTT <prefix>/bridge/status           (retained, LWT + explicit online/offline)
   ├─► MQTT <prefix>/prices/export           (retained, on change: RCE × 1.23)
   ├─► MQTT <prefix>/prices/import           (retained, on change: tariff engine)
   └─► MQTT <prefix>/forecast/pv             (retained, on new Solcast fetch)

rce_prices.db ──► export price band builder ──► MQTT prices/export
tariff_import.yaml (gitignored, copied from tariff_examples/*.yaml)
                ──► tariff_engine.py ──┬──► MQTT prices/import
                                       └──► _calculate_income.py (per-hour import valuation)
solcast.py combined forecast ──► MQTT forecast/pv

Mosquitto (native, existing)
   │
   ▼
Home Assistant (existing mqtt: sensor/binary_sensor + new template sensors)
   │
   ▼
Predbat (read-only plan evaluation — no write path in this phase)
```

## Testing

- Pure functions (RCE band conversion, tariff engine evaluation, daily
  delta calculation, MQTT payload building) are unit-testable without a
  real broker or inverter — same style as existing tests in `tests/`.
- Tariff engine gets dedicated test cases per parsing/resolution quirk
  listed in Component 4, including multi-component summation (distinct
  distribution + sales schedules producing the correctly-summed price).
- `_calculate_income.py`'s updated `compute_hour_income` gets a test
  case with a tariff config supplied (per-hour rate varies by G12w zone,
  not the flat `IMPORT_PRICE_KWH`) and one without (unchanged flat-rate
  behavior), so the fallback path is verified, not just the new one.
- Manual verification: `mosquitto_sub` on the Pi for each new topic,
  then confirming the corresponding HA entities populate, before
  pointing Predbat's `apps.yaml` at them.
- `_estimate_battery_efficiency.py` is verified by manual inspection of
  its output report against known plausible ranges (e.g. round-trip
  battery efficiency in the 85-95% range is plausible; wildly outside
  that signals a bug in session/cycle detection, not a real result).

## Non-goals (this phase)

- No MQTT command/write topics — Predbat stays strictly read-only against
  the real inverter.
- No changes to goodwe_manager's own eco-mode/price-driven charge logic —
  it keeps running exactly as today, independent of Predbat's evaluation.
- No credentials or genuinely sensitive data are hardcoded anywhere —
  `tariff_examples/g12w_pge.yaml` does carry this household's real
  zł/kWh rates (a deliberate choice; see Component 4's "Example tariff
  definitions directory" note on why that's not a privacy concern here),
  but MQTT credentials, the inverter's IP, and Solcast API keys stay in
  `.env`/gitignored files as before.
