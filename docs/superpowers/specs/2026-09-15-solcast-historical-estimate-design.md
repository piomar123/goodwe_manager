# Solcast historical estimate ("estimated_actuals") — design

## Goal

Add Solcast's `estimated_actuals` endpoint (its satellite-derived estimate of what a site actually
produced, as opposed to its forward-looking forecast) as a third data series on the `/forecast`
chart and table, for fully-elapsed past dates only — shown alongside the existing Meteosource
forecast, Solcast forecast, and real inverter "Actual" line. This supersedes the prior decision (see
`docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md`'s "Out of scope") to skip this
endpoint entirely; that decision was about using it as *ground truth for the accuracy delta*
(rejected, real inverter data serves that better and for free) — this is a separate, later request:
showing Solcast's own historical estimate for its own sake, as a point of curiosity/comparison, not
as a replacement for the existing real-inverter-based accuracy delta.

## Background: current state

- `solcast.py` currently only calls Solcast's forward-looking forecast endpoint
  (`fetch_solcast_forecast_30min`); the client has no historical-data capability at all.
- `forecast_history.py`'s `forecast_snapshots` table is already generic by `source` string — storing
  a third source needs no schema change.
- `forecast_prefetch.py` wakes 4 times/day (`06:00, 10:00, 14:15 [rce_prefetch.WAKE_TIME], 18:00`),
  fetching both Meteosource and Solcast's forecast at every slot (8 Solcast calls/day, 2/day
  headroom under the 10-calls/day Hobbyist cap).
- `/forecast`'s chart/table already show Meteosource, Solcast forecast (c10/c50/c90), and the app's
  own real inverter "Actual" line/column - see the prior spec's §1-§2.

## Solcast `estimated_actuals` — facts (verified against a real account, 2026-09-15)

- **Endpoint:** `GET /rooftop_sites/{resource_id}/estimated_actuals`, same auth (`api_key` query
  param) and per-site-per-call shape as the forecast endpoint.
- **Response shape:** `{"estimated_actuals": [{"pv_estimate": <kW avg over period>, "period_end":
  "<ISO8601, 7 fractional-second digits, same quirk as the forecast endpoint>", "period": "PT30M"},
  ...]}` - note the key is `estimated_actuals`, not `forecasts`, and each period carries a single
  `pv_estimate` only (no `pv_estimate10`/`pv_estimate90` - a historical estimate isn't a probabilistic
  range the way a forecast is).
- **`hours` parameter:** how far back a call reaches, **capped at 168** (exactly 7 days) - confirmed
  by a live `400` response (`"Hours must be between 0 and 168"`) when requesting more. One call with
  `hours=168` returns the full trailing week in one go (confirmed live: 337 periods, i.e. ~168.5
  hours of 30-minute periods), newest period first.
- Same `PERIOD_KW_TO_KWH` unit-conversion assumption as the forecast endpoint applies here too
  (`pv_estimate` is average kW over the period, converted to that period's kWh) - not separately
  re-verified against known real production, same caveat as the existing constant.
- Same 10-calls/day budget as the forecast endpoint - this is not a separate quota.

## Decisions

### 1. `solcast.py`: new `fetch_solcast_estimated_actuals_30min`

```python
def fetch_solcast_estimated_actuals_30min(resource_id: str, hours: int = 168) -> Dict[str, Dict[str, float]]:
    """Calls GET /rooftop_sites/{resource_id}/estimated_actuals at Solcast's
    native 30-minute period, covering the trailing `hours` hours (max 168 -
    7 days, Solcast's own cap). Returns {"YYYY-MM-DD": {"HH:MM": kwh}} -
    flat per-period kWh (unlike the forecast endpoint's {c10,c50,c90} - a
    historical estimate isn't a probabilistic range), same
    period_end-minus-30-minutes/local-time/PERIOD_KW_TO_KWH handling as
    fetch_solcast_forecast_30min.
    """
```

Mirrors `fetch_solcast_forecast_30min`'s parsing exactly (same `period_end` fractional-seconds
strip, same period-start-local conversion), reading `data['estimated_actuals']` instead of
`data['forecasts']` and `period['pv_estimate']` only (no c10/c90).

A new `sum_sites_flat(*site_payloads)` sibling to the existing `sum_sites` combines east+west into
one `{date: {time: kwh}}` — kept separate from `sum_sites` rather than generalizing it, since
`sum_sites` is typed specifically around the `{c10,c50,c90}` shape and forcing both shapes through
one function would need a shape-detection branch for no real reuse benefit (the summing loop itself
is three lines).

### 2. Storage: new `source` value, no schema change

Stored in the existing `forecast_snapshots` table with `source = 'solcast_actuals'`. `write_snapshot`,
`get_snapshot`, `get_latest_merged`, `get_fetch_times` are all already generic by `source` string -
none of them change. `payload` shape: `{"HH:MM": kwh}` (flat, matching Meteosource's convention, not
Solcast forecast's nested one).

**Write path:** mirrors `fetch_and_store_solcast` — fetch east+west, `sum_sites_flat`, then iterate
the (up to 7) dates the response covers and `write_snapshot(conn, 'solcast_actuals', date_str,
payload)` per date. Re-fetching the same trailing week daily is cheap on storage: `write_snapshot`'s
existing dedup (identical payload → bump `valid_until` instead of inserting a new row) means only
dates whose satellite estimate actually changed since yesterday's fetch produce a new row.

### 3. Fetch scheduling: new wake-time schedule, replacing all 4 current slots

**Forecast wake-times change from `(06:00, 10:00, 14:15, 18:00)` to `(06:00, 11:00, 21:00)`** — a
deliberate reschedule prompted by this feature, not just an addition to the existing schedule:

- **06:00** (dawn) — unchanged; satellite cloud imagery becomes usable at first light.
- **11:00** (was 10:00) — moved later, to sit with margin *before* the tariff's midday cheap-charging
  window (13:00-15:00 winter, 15:00-17:00 summer) in both seasons, so that window's charge-vs-PV
  decision has a same-morning-fresh forecast rather than one carried over from dawn.
- **21:00** (new, was 18:00) — the freshest possible forecast right before the 22:00-06:00 overnight
  cheap-charging window starts, so overnight grid-charge sizing can use tomorrow's latest forecast.
  This fills a gap the old schedule had entirely: its latest slot (18:00) was 4 hours too early for
  this decision.
- **Dropped: the `rce_prefetch.WAKE_TIME`-aligned slot (14:15).** The prior spec's §5 framed this
  alignment as deliberate groundwork for a future EMS correlating same-moment PV and price forecasts.
  That framing is explicitly superseded here: with only 3 forecast slots available (one must fund the
  new `solcast_actuals` fetch, see below), 21:00's overnight-charge-decision value was judged higher
  than preserving a same-timestamp PV/price correlation. A future EMS can still correlate a PV
  snapshot with a price snapshot by nearest-timestamp lookup across their two independent schedules
  when the time comes - it doesn't require them to share a wake time.

**New `ACTUALS_WAKE_TIME = 23:00`**, a separate one-a-day wake (not part of `FORECAST_WAKE_TIMES`),
calling the new `fetch_and_store_solcast_actuals(conn)`. 23:00 is safely after all three forecast
slots and past sunset even at midsummer (~21:30 latest in Poland) - though exact timing isn't
critical here: `get_latest_merged`'s existing period-level merge-across-snapshots already handles a
day not being fully final yet at fetch time (the same mechanism that makes "today" work for the
forward-looking forecast), so even a day fetched slightly before its last production hour gets
backfilled by the next day's 7-day-rolling re-fetch of the same date.

**Budget:** 3 forecast slots × 2 calls (east+west) + 1 actuals slot × 2 calls (east+west) = **8
Solcast calls/day**, identical to the current total - the same 2/day headroom is preserved, just
reallocated.

### 4. Read path (`main.py`): past dates only

`get_forecast()` and `get_forecast_hourly_json()` read the new source the same way they read
`solcast`: `_read_forecast_payload(conn, 'solcast_actuals', date_yyyymmdd, fetched_at)` — no change
needed to that helper, it's already generic by source name. The read is skipped entirely (treated as
unavailable) unless the viewed date is a fully elapsed past day (`date_yyyymmdd < today`, same
condition already used for the accuracy delta's gating) - Solcast's own estimate for today or a
future date would be an estimate of an estimate sitting next to the app's real "Actual" line, adding
clutter without adding information. `fetched_at`-snapshot mode works unmodified (same helper, same
semantics as the other two sources).

`get_forecast_hourly_json()`'s response gains:

```python
'solcast_actuals': {
    'available': bool(solcast_actuals_periods),  # False for today/future dates, or no data yet
    'periods': [{'time': t, 'kwh': kwh} for t, kwh in sorted(solcast_actuals_periods.items())],
},
```

matching the existing `'solcast'` key's `available`/`periods` convention.

### 5. Rendering: "Solcast Estimated Actual" line + table column

**Naming:** "Solcast Estimated Actual" - matches Solcast's own API/documentation terminology
(`estimated_actuals`) most directly, at the cost of placing the word "Actual" next to the app's own
real "Actual" line/label. This was a deliberate choice (over "Solcast Historical Estimate" or
"Solcast Satellite Estimate", both considered and rejected) in favor of matching Solcast's own
vocabulary for anyone cross-referencing their docs.

**Chart:** a new line dataset, own distinct color, dashed (visually distinguishing "estimate" from
the real "Actual" line's solid style), hourly-aggregated via a new `ForecastCalc` sibling function
(`aggregateSolcastActualsHourly`, mirroring `aggregateSolcastHourly`'s hour-bucketing but for this
source's flat `{time, kwh}` period shape instead of `{time, c10, c50, c90}`) rather than a shape-
generic aggregator - same rationale as `sum_sites_flat` in §1, and matching this codebase's existing
preference for small, specifically-named functions over one generalized-with-branches helper. Plotted
at the same `x = idx + 0.5` positions as Meteosource/Solcast/Actual (see the prior spec's §1
amendment for why uniform index-space matters for `interaction: {mode: 'index'}` tooltip alignment).
Only rendered when `data.solcast_actuals.available` is true (i.e. only for past dates with data) -
absent from the chart entirely otherwise, same "just don't draw it" pattern the existing
`solcast-unavailable-note` already uses for the Solcast forecast band.

**Table:** a new "Solcast Estimated Actual" column, alongside the existing
`Hour | Meteosource | Solcast c50 (c10-c90) | Actual | Actual cumul.` columns. Absent (column not
rendered at all, not shown-as-`—`) for dates where the series itself isn't available, consistent
with the chart's all-or-nothing availability rather than the existing per-hour `'—'` convention used
for individual missing hours within an otherwise-available series.

## Out of scope (YAGNI)

- Feeding this data into the existing accuracy-delta math (§3 of the prior spec) - that delta is
  deliberately anchored to real inverter measurement; this feature is a separate, purely
  display-oriented addition and doesn't change that decision.
- A Solcast forecast-vs-Solcast-estimated-actual "self-consistency" delta (how well Solcast's own
  forward forecast matched its own later retrospective estimate) - a real possible future use of this
  data, explicitly not built here; revisit only if there's a concrete need for it.
- Season-aware (real sunrise/sunset based) prefetch slot times - the fixed times chosen in §3 are a
  reasonable static approximation, same stance the prior spec already took for its own schedule.
- Backfilling more than 7 days of history on first rollout - Solcast's own `hours` cap means the
  first `ACTUALS_WAKE_TIME` fetch after this ships only ever reaches 7 days back; older dates simply
  have no `solcast_actuals` data, same graceful "just show what exists" degradation the prior spec's
  §7 already established for the other two sources.
