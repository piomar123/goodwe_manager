# Tariff import YAML schema

Reference for the YAML format `tariff_engine.py` reads (`TARIFF_IMPORT_CONFIG`).
See `tariff_examples/g12w_pge.yaml` for a complete, real-world example
(PGE Dystrybucja's G12w schedule).

## Top level

```yaml
country: PL   # optional, defaults to PL
components:
  <component_name>:
    prices: { ... }
    season_boundaries: { ... }   # optional
    bands: { ... }
    default_price: <price_name>
```

`country` is an ISO country code the `holidays` PyPI package recognizes
- it controls which public holiday calendar the `Holiday`/`Work` day-spec
keywords use (see below). Defaults to `PL` if omitted.

`components` is a map of independently-scheduled tariff components,
each evaluated separately and **summed** to get the total price at a
given instant (`price_at()`). This is what lets you model Poland's
distribution tariff and sales tariff as two components with their own
schedules, or just use a single component (e.g. `total`) for a
combined rate. Component names are arbitrary and only used for the
YAML's own organization - they don't appear in the output.

## `prices`

A map of named price values (in zł/kWh, no unit conversion):

```yaml
prices:
  cheap: 0.6907
  expensive: 1.3078
```

Names are referenced by `bands[*].price` and `default_price` below.
Any number of names, any names you like.

## `season_boundaries` (optional)

```yaml
season_boundaries:
  summer:
    start: "01.04"
    end: "30.09"
  winter:
    start: "01.10"
    end: "31.03"
```

Dates are `dd.mm` (no year - re-evaluated every year). A range wraps
the year boundary automatically when `start > end` (winter above spans
Oct-Mar through New Year's). `29.02` falls back to the 28th in a
non-leap year rather than erroring. Omit this key entirely for a
season-independent component - `bands.default` becomes the only band
list then.

## `bands`

```yaml
bands:
  default: [ <band>, ... ]   # checked when no season-specific list matches
  summer: [ <band>, ... ]    # checked first when season_for_date() == "summer"
  winter: [ <band>, ... ]
```

Each season name here must match a key in `season_boundaries`. Every
component needs a `bands.default` list (checked for every day,
regardless of season) - put anything that doesn't vary by season there
to avoid duplicating it across `summer`/`winter`.

**Resolution order** at a given instant: the current season's own band
list first (in file order), then `bands.default` (in file order), then
`default_price` if nothing matched. First match wins - order your bands
from most specific to least specific.

### Band entry

```yaml
- days: "Work"       # optional; omit to match every day
  start: "22:00"     # optional
  end: "06:00"       # optional
  price: cheap       # required; must be a name from `prices`
```

- **`days`**: a comma-separated list of tokens, OR'd together (any
  token matching is enough). Tokens can be:
  - weekday abbreviations: `Mo`, `Tu`, `We`, `Th`, `Fr`, `Sa`, `Su`
  - ranges: `Mo-Fr`, wrapping the week if needed (e.g. `Fr-Mo` covers
    Friday through Monday)
  - `Work` - Monday-Friday, excluding public holidays (see `country`
    above)
  - `Holiday` - public holidays for `country` (via the `holidays`
    package), regardless of weekday
  - example: `"Sa,Su,Holiday"` matches weekends and holidays
  - omit `days` entirely to match every day
- **`start`/`end`**: `HH:MM` or `HH:MM:SS`, local time. `end` is
  exclusive. If `start > end` (e.g. `22:00`-`06:00`), the band wraps
  past midnight. Omit **both** together to mean "all day" - there's no
  way to specify one without the other.
- **`price`**: must be a key from this component's `prices` map.

## `default_price`

```yaml
default_price: expensive
```

Required. Used when no band (season-specific or `default`) matches the
instant - your final fallback, so every hour of every day always
resolves to a price.

## What the engine does with this

- `price_at(config, dt)` - the price at one instant (sums every
  component).
- `bands_for_day(config, day, tz)` - builds the day's price schedule as
  a list of `{"start", "end", "value"}` intervals (merging adjacent
  same-price minutes, no forced slicing), used for the MQTT bridge's
  `prices/import` publish (see `MQTT_TOPICS.md`) and Predbat's
  `metric_octopus_import`.
- `_calculate_income.py --tariff-config <path>` reuses the same engine
  for historical income calculations, per-hour, instead of the flat
  `IMPORT_PRICE_KWH` constant.
