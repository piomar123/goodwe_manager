# Solcast PV production forecast — design

> **Amended 2026-09-14** (after live-testing with a real Solcast key): §1's chart now aggregates
> Solcast to hourly instead of its native 30-minute resolution (fixes a kWh-comparability issue and
> a Chart.js tooltip misalignment bug); §3's daily summary gained an accuracy delta against real
> inverter production and now shows Meteosource/Solcast on separate lines instead of one
> `·`-joined line. Original implementation (Tasks 1-7, everything except these amendments) is
> already committed on this branch.

## Goal

Add Solcast as a second PV production forecast source alongside the existing Meteosource scrape,
showing c10/c50/c90 probabilistic forecasts on the `/forecast` chart and table, summed across the
app's existing east+west orientation split. Along the way, also sum Meteosource's two orientations
into one series (currently shown as two separate `90°`/`270°` series) to reduce chart/table clutter,
and turn the ad-hoc request-time forecast cache into a real fetch history, since neither source
needs to be re-fetched anywhere near request time to stay useful.

## Background: current state

- `forecast.py` scrapes `solar.meteosource.com` per orientation (90°, 270°) for a given date; no
  API key, no rate limit, answers any date.
- `main.py`'s `_get_hourly_forecast_cached()` fans out both orientations, merges them per hour, and
  caches the merged result in an in-process dict for 300s (`_FORECAST_CACHE_TTL_SECONDS`) so a
  `/forecast` page view and its `/forecast/hourly.json` AJAX call don't double-scrape.
- `templates/forecast.html` renders a Chart.js chart (stacked 90°/270° bars + an "Actual" line
  sourced from `hourly_summary`) and a matching table, for a single selected date (`?date=`).
- `PV_ORIENTATIONS = (90, 270)` in `main.py` is the only place the two-orientation assumption is
  centralized; `ForecastData` (a namedtuple) and the template's summary line both hard-code exactly
  two angles.

## Solcast — free tier facts (verified)

- **Hobbyist plan:** free, 10 API calls/day (UTC day), max **2** rooftop sites per account — this
  maps exactly onto the existing east/west split (one Solcast "rooftop site" per orientation).
- Endpoint: `GET /rooftop_sites/{resource_id}/forecasts`, returns `pv_estimate` (c50/median),
  `pv_estimate10` (c10, cloudier scenario), `pv_estimate90` (c90, clearer scenario) per period.
  ([docs.solcast.com.au](https://docs.solcast.com.au/))
- One call returns a **rolling multi-day forecast** (today + several days ahead), not just one date
  — so one fetch per site refreshes many calendar days at once.
- Requested at its native **30-minute** period (not aggregated to hourly at fetch time) - fetched
  and stored at that resolution for the history's sake, but always aggregated to hourly before
  display; see §1's amendment for why.
- Forward-looking only — no historical/past-date data, and a given call only returns periods from
  roughly "now" onward, not the rest of today's already-elapsed periods either. A period outside
  the fetched window (past a call's start time, or beyond how far ahead it returned) simply has no
  Solcast data *from that call* - see §4's read-path merge for how today's already-elapsed periods
  still get shown from earlier calls.
- Neither Solcast nor Meteosource publish a "cheap update window" worth chasing: Solcast reissues
  NWP-based forecasts hourly (satellite nowcast every 5-15 min); Meteosource updates roughly every
  10 min. Both are near-continuously fresh — refresh cadence here is bound by Solcast's call quota,
  not by trying to align to either provider's own update schedule.

## Decisions

### 1. Chart: c50 line + shaded c10-c90 band ("Option A")

Meteosource becomes a single summed-E+W bar series (was two stacked 90°/270° bars). Solcast's c50
is drawn as a bold line; c10-c90 forms a soft shaded band around it (two invisible line datasets,
the upper one filled back to the lower one). The existing "Actual" line is unchanged.

Z-order (Chart.js `order`, lower value = drawn on top): Meteosource bar `order: 10` (bottom) <
Solcast band `order: 3` < Solcast c50 line `order: 2` < Actual `order: -1` (top, unchanged from
today). Without explicit `order` values, Chart.js does not use dataset array position for z-order
once any dataset sets one, so every dataset needs one set explicitly.

**Uniform hourly resolution (amended 2026-09-14, after live testing):** the original design plotted
Solcast at its native 30-minute points (48/day) while Meteosource's bars and the Actual line stayed
hourly (24/day). Live testing against a real Solcast key surfaced two problems this caused: (1) a
30-minute period's kWh is naturally half of an hourly bar's kWh for the same average power, making
Solcast look artificially smaller than Meteosource/Actual at a glance even when the underlying power
forecast agreed; and (2) Chart.js's `interaction: {mode: 'index'}` tooltip aligns datasets by array
*index*, not by x-value - with Solcast holding 48 points against the other two datasets' 24, the
tooltip's `dataIndex` lookup picked mismatched hours across series.

Solcast's chart datasets (band + c50 line) are now built from the same hourly-aggregated data as
the table (`ForecastCalc.aggregateSolcastHourly`, already used there), plotted at `x = idx + 0.5`
- the same points Meteosource's bar uses. All three chart series now share one 24-point index
space, fixing both problems: kWh values are directly comparable (every series is a full-hour sum),
and tooltip alignment is correct again. Raw 30-minute Solcast data is still fetched and stored in
`forecast_history` unchanged (see the free-tier facts above) - only the chart/table's rendering
resolution changed, not what's persisted. `ForecastCalc.solcastPeriodToX`, which computed the old
native-resolution x positions, is removed as dead code.

A working mockup comparing this against two rejected alternatives (three plain lines; c50-as-bars
with error-bar whiskers) lives in `docs/superpowers/mockups/solcast-chart-options/` (not committed
- see that directory's own throwaway-mockup convention already used by
`live-power-flow-dashboard-mockup*.html`). That mockup predates the resolution amendment above and
still shows the rejected native-30-minute rendering for the chart shape/z-order comparison itself -
the shape (band + c50 line, same z-order) is unchanged by this amendment, only the x-positions are.

### 2. Table columns

`Hour | Meteosource | Solcast c50 (c10-c90) | Actual | Actual cumul.` — the existing per-source
forecast cumulative columns are dropped. With two forecast sources plus the daily summary line
(below) already showing each source's day total, a running forecast total per hour added little;
the cumulative column that matters is Actual's (the one figure people actually compare against
either forecast's day total as production unfolds).

Solcast's row values are the **sum of its two 30-minute periods** for that hour (`c50` sums
cleanly, since it's a normal expected value). `c10`/`c90` are also just summed the same way for
display simplicity, even though summing two independent percentile estimates isn't statistically
exact (percentiles aren't additive in general) - close enough for a hobbyist dashboard table; the
chart (§1) shows the real 30-minute values for anyone who wants the precise figures.

### 3. Daily summary line

Replaces today's `90°: X kWh / 270°: X kWh / Total: X kWh`. **Amended 2026-09-14** (readability
feedback during live testing): Meteosource and Solcast are shown on separate lines rather than
joined with `·` on one line - easier to scan, and leaves room for the accuracy delta below without
the line growing unwieldy. Rendered with CSS `white-space: pre-line` on the summary `<p>` (so a
literal `\n` in the server-built string renders as a line break) rather than `| safe` + `<br>` -
avoids opting the template out of Jinja's autoescaping for a plain two-line label.

```
Meteosource: X kWh
Solcast: Y (Z-W) kWh
```

Z-W is Solcast's c10-c90 range for the day total.

**Accuracy delta (amended 2026-09-14, after live testing):** when the viewed date is a **fully
elapsed past day** (strictly before today) **and** has all 24 actual hours recorded (no
inverter-downtime gaps that day), each source's total also shows its delta against real measured
production - the app's own inverter telemetry (`_get_actual_hourly_pv_kwh`, the same data backing
the existing "Actual" chart line/table column), not a second Solcast API call. This was chosen over
fetching Solcast's `estimated_actuals` endpoint specifically because real inverter data is already
fetched, stored, and free - spending part of the 10-calls/day Solcast budget on a second estimate to
compare against a forecast, when a real measurement is sitting right there, added cost for no
accuracy benefit. `forecast_prefetch.py`'s schedule and call budget are unchanged by this addition.

```
Meteosource: X kWh (Δ +N% vs actual)
Solcast: Y (Z-W) kWh (Δ +N% vs actual)
```

`Δ = round((forecast_total - actual_total) / actual_total * 100)`, signed (a positive Δ means the
forecast overestimated; negative means it underestimated). On any date that doesn't meet the
"fully elapsed, no gaps" condition above (today, future dates, or a past date with incomplete
telemetry), the summary falls back to the plain `Meteosource: X kWh · Solcast: Y (Z-W) kWh` form
with no `Δ` - showing a delta against an incomplete or nonexistent actual total would be misleading
rather than informative.

### 4. Data model: fetch history, not a TTL cache

New table (new `forecast_history.db`, mirroring `rce_storage.py`'s one-db-per-concern pattern):

```sql
CREATE TABLE forecast_snapshots (
    source TEXT NOT NULL,        -- 'meteosource' | 'solcast'
    date TEXT NOT NULL,          -- YYYY-MM-DD, local calendar date being forecast
    fetched_at INTEGER NOT NULL, -- epoch seconds, first time this snapshot was seen
    valid_until INTEGER NOT NULL,-- epoch seconds, last time this snapshot was confirmed unchanged
    payload TEXT NOT NULL,       -- JSON: per-hour values (see below)
    PRIMARY KEY (source, date, fetched_at)
);
```

`payload` shape per source:
- `meteosource`: `{"HH:00": kwh, ...}` (already-summed east+west, matching the new single-series
  chart/table; still hourly - Meteosource's own resolution)
- `solcast`: `{"HH:00": {"c10": kwh, "c50": kwh, "c90": kwh}, "HH:30": {...}, ...}`
  (already-summed east+west, at Solcast's native 30-minute resolution - see §1)

**Write path:** every fetch (scheduled or fallback-live, see §5/§7) computes its payload and
compares it (rounded to the same 2-decimal precision already used for display) against the most
recent existing snapshot for that `(source, date)`. Identical -> update that row's `valid_until` to
now. Different (or no prior snapshot) -> insert a new row with `fetched_at = valid_until = now`.
Nothing is ever deleted, so every past date's forecast history stays queryable.

**Read path:** `/forecast` and `/forecast/hourly.json` query this table instead of the old
in-process `_forecast_cache` dict, which is removed entirely. Two read modes:

- **Specific snapshot** (fetch-time dropdown, §6, set to anything other than "latest"): return that
  `(source, date, fetched_at)` row's payload exactly as stored, gaps included. A snapshot fetched
  mid-morning genuinely has no periods for the hours before it ran - showing that gap is accurate
  history, not a bug.
- **"Latest" (the default view):** *merge per period across all of that date's snapshots*, taking
  each period's value from the most recent snapshot that actually reported one - not just the
  single newest `fetched_at`'s payload wholesale. This is what makes "today" work: Solcast's calls
  are forward-looking from call time (see free-tier facts above), so at 14:00 the 06:00 snapshot is
  the only one that ever covered 07:00-09:30, the 10:00 snapshot covers 10:00-13:30, and only
  14:00-onward comes from the freshest (14:15) snapshot. Rendering "latest" as a single snapshot's
  payload would show those earlier periods as missing even though they were legitimately forecast
  earlier in the day. Same merge logic applies to Meteosource, for consistency, even though its own
  scheduled+fallback fetches make gaps less likely there in practice.

### 5. Fetch scheduling

One background thread (new `forecast_prefetch.py`, modeled on `rce_prefetch.py`'s
plain-daemon-thread approach) wakes at **4 fixed local times/day**: `06:00`, `10:00`, the same time
as `rce_prefetch.WAKE_TIME` (currently 14:15 - imported directly from `rce_prefetch`, not
re-hardcoded, so the two stay in sync if RCE's publish time ever changes), and `18:00`.

At each wake, it fetches **both sources for both orientations** (2 Solcast calls + 2 Meteosource
scrapes) for "today" (each call's rolling window covers however many days ahead the provider
returns) and writes/dedupes snapshots per §4.

**Why these choices:**
- 4 slots x 2 Solcast calls = 8/day, leaving 2/day of headroom under Solcast's 10/day cap (for a
  failed slot, or an occasional manual/dev-time call against the same key) - the earlier 5-slot
  plan used the full 10/day with zero headroom.
- The slots are daylight-biased (no 2am fetch) rather than spread evenly across 24h, since a
  pre-dawn forecast update has no same-day decision value.
- The slot aligned with `rce_prefetch.WAKE_TIME` is deliberate groundwork for a **future** energy
  management system (not built here - see "Out of scope"): tomorrow's RCE prices and tomorrow's PV
  forecast, fetched at the same moment, share a `fetched_at`/timing anchor, which is what a future
  battery-scheduling EMS would want to jointly reason about. This spec only fetches and stores the
  data; no EMS logic is part of this feature.
- Meteosource riding on the same schedule as Solcast (rather than keeping its own request-time
  fetch) is a deliberate simplification requested during design: it means one fetch-time dropdown
  serves both sources instead of two independently-clocked ones. The trade-off is that Meteosource
  forecasts are now only as fresh as the last scheduled slot (~4h) instead of the current ~300s -
  acceptable since nothing here needs sub-4h freshness.
- On fetch failure (either source), log a warning and move on - no retry within the same slot (that
  would spend quota meant for the next slot). The next scheduled slot recovers naturally. This
  mirrors `rce_prefetch.py`'s "safe to fail" framing: the read path always has a fallback (§7), so a
  broken prefetch thread degrades gracefully rather than breaking `/forecast`.

### 6. Fetch-time selector UI

A single dropdown next to `/forecast`'s existing date `<input type=date>`, listing the selected
date's distinct `fetched_at` values (from either source having a snapshot for that date - since
both now fetch on the same schedule, in practice both sources share timestamps), newest first,
defaulting to latest. Changing it re-renders the chart/table using that specific `fetched_at`'s
snapshots instead of each source's latest.

### 7. Date scope / graceful degradation

- **Past dates:** always available (nothing is evicted from `forecast_snapshots`) for whichever
  dates were actually fetched. A past date that predates this feature's rollout, or that fell
  outside the prefetch's window, has no data - same "just show what exists" degradation the
  existing Actual-line/no-data handling already uses.
- **Future dates within Solcast's returned window:** both sources show data from the latest (or
  dropdown-selected) snapshot that covered that date.
- **Future dates beyond Solcast's window, or before this feature ever ran for that date:**
  Meteosource falls back to a **live** fetch (it can answer any date on demand) and persists the
  result as a new snapshot, same as a scheduled fetch would. Solcast cannot do this (forward-window
  API) - the UI shows an explicit "Solcast forecast unavailable for this date" note instead of
  silently omitting the series, so it's clear this is an API limitation rather than a bug.

### 8. Config & secrets

`.env.example` gains:

```
SOLCAST_API_KEY=
SOLCAST_SITE_EAST_ID=
SOLCAST_SITE_WEST_ID=
```

The two site IDs come from manually creating 2 rooftop sites in Solcast's own dashboard (one-time
setup, matching this app's real `PV_LAT`/`PV_LON`/`PV_TILT` and an azimuth of 90°/270°, each sized
at half `PV_POWER`) - not something this app can or should automate. The real `.env` (local dev and
the `raspberry4.local` deployment) gets edited directly by the user over SSH/local editor; nothing
here reads or displays the key's value.

## Out of scope (YAGNI)

- Any EMS/battery-scheduling logic that would consume the fetch-time alignment from §5 - that
  timing choice is groundwork only.
- Dynamic (season-aware) prefetch slot times based on real sunrise/sunset - the 4 fixed times are a
  reasonable static approximation; revisit only if real usage shows it matters.
- Automating Solcast rooftop site creation via its API.
- Fetching Solcast's `estimated_actuals` endpoint - considered for §3's accuracy delta, rejected in
  favor of the app's own free, already-stored real inverter telemetry (see §3's amendment); revisit
  only if a future need specifically requires Solcast's own retrospective estimate rather than real
  measured production (e.g. comparing Solcast's internal forecast-vs-nowcast consistency).
