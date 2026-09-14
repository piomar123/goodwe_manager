# Solcast Forecast Amendments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the Solcast chart's mixed-resolution comparability/tooltip bugs by rendering it hourly
(matching Meteosource/Actual), and add a daily summary accuracy delta against real inverter
production - both amendments to the already-implemented Solcast forecast feature, found during live
testing with a real Solcast key.

**Architecture:** Three small, independently-testable changes to already-existing files - no new
modules. `static/js/forecast-calc.js` loses its now-unused `solcastPeriodToX` (Task 1).
`main.py`'s `get_forecast()` gains a pure, unit-tested summary-building helper that adds the
accuracy delta and switches to a newline-separated format (Task 2). `templates/forecast.html`'s
chart switches Solcast's datasets from native-30-minute points to the same hourly-aggregated data
already used for the table, and picks up a small pre-existing bug fix found while touching this
code: the y-axis still had `stacked: true` left over from the original two-orientation chart,
which - per Chart.js's stacking behavior for filled line datasets - inflates the Solcast band's
rendered fill position independently of the resolution fix; removing it was verified during the
original feature's mockup work but the production chart's y-axis was missed then (only the x-axis
was fixed). Fixing it now, since Task 3 is already rewriting this exact function (Task 3).

**Tech Stack:** Python 3 (Flask, unchanged deps), `unittest` (existing convention), vanilla JS +
Chart.js 4.4.4, Node's built-in `node:test`.

**Spec:** `docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md` (see the "Amended
2026-09-14" banner at the top and §1/§3's amendment notes)

## Global Constraints

- Solcast's chart and table both show only hourly-aggregated values (`ForecastCalc.aggregateSolcastHourly`);
  raw 30-minute data keeps being fetched and stored in `forecast_history` unchanged - only rendering
  resolution changes, not what's persisted.
- The accuracy delta is computed **only** when the viewed date is strictly before today (a fully
  elapsed past day) **and** has all 24 actual hours recorded (no inverter-downtime gaps) - otherwise
  the summary falls back to its plain (no-delta) form.
- Delta formula: `round((forecast_total - actual_total) / actual_total * 100)`, signed (positive =
  forecast overestimated).
- No new Solcast API calls - the accuracy delta uses the app's own already-stored real inverter
  Actual data (`_get_actual_hourly_pv_kwh`), not Solcast's `estimated_actuals` endpoint.
  `forecast_prefetch.py`'s schedule and call budget are unchanged.
- Summary line: Meteosource and Solcast render on separate lines via a literal `\n` in the
  server-built string plus CSS `white-space: pre-line` on the summary `<p>` - not `| safe` + `<br>`.

---

## Task 1: `static/js/forecast-calc.js` - remove `solcastPeriodToX`

**Files:**
- Modify: `static/js/forecast-calc.js`
- Modify: `tests/js/forecast_calc.test.js`

**Interfaces:**
- Consumes: nothing new.
- Produces: `ForecastCalc` now exports only `aggregateSolcastHourly` (unchanged signature/behavior)
  - `solcastPeriodToX` is removed. Task 3 relies on `aggregateSolcastHourly` still being exported
    exactly as before.

- [ ] **Step 1: Remove `solcastPeriodToX`'s test**

In `tests/js/forecast_calc.test.js`, remove the `solcastPeriodToX` import and its test:

```javascript
const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  aggregateSolcastHourly,
} = require('../../static/js/forecast-calc.js');

test('aggregateSolcastHourly sums each hour\'s two 30-minute periods', () => {
  const periods = [
    { time: '07:00', c10: 1.0, c50: 2.0, c90: 3.0 },
    { time: '07:30', c10: 0.5, c50: 1.0, c90: 1.5 },
    { time: '08:00', c10: 2.0, c50: 3.0, c90: 4.0 },
  ];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result, [
    { time: '07:00', c10: 1.5, c50: 3.0, c90: 4.5 },
    { time: '08:00', c10: 2.0, c50: 3.0, c90: 4.0 },
  ]);
});

test('aggregateSolcastHourly treats a missing half-hour as zero', () => {
  const periods = [{ time: '07:30', c10: 0.5, c50: 1.0, c90: 1.5 }];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result, [{ time: '07:00', c10: 0.5, c50: 1.0, c90: 1.5 }]);
});

test('aggregateSolcastHourly returns hours in ascending order', () => {
  const periods = [
    { time: '09:00', c10: 1, c50: 1, c90: 1 },
    { time: '07:00', c10: 1, c50: 1, c90: 1 },
  ];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result.map(r => r.time), ['07:00', '09:00']);
});
```

- [ ] **Step 2: Run the tests to verify they still pass**

Run: `node --test tests/js/forecast_calc.test.js`
Expected: PASS (3 tests - down from 4, `solcastPeriodToX`'s own test is gone)

- [ ] **Step 3: Remove `solcastPeriodToX` from the implementation**

In `static/js/forecast-calc.js`, remove the function and its export:

```javascript
// Pure per-request math for the /forecast chart/table - no DOM, no
// Chart.js - so it can be unit tested the same way static/js/diagram-calc.js
// already is (see tests/js/forecast_calc.test.js). Loaded as a plain
// <script> in forecast.html (browser global ForecastCalc), and required
// directly in tests (Node's CommonJS module.exports below).

// Sums each hour's two 30-minute Solcast periods into one hourly row - both
// the table and the chart (forecast.html) render Solcast at this hourly
// resolution, even though the underlying data is fetched/stored at
// Solcast's native 30-minute period (see
// docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md §1's
// amendment for why the chart no longer plots the native resolution
// directly). A missing half (e.g. the very first/last period of a fetch
// window) is treated as 0, same convention forecast.py/solcast.py already
// use for a missing orientation.
function aggregateSolcastHourly(periods) {
  const byHour = {};
  for (const p of periods) {
    const hour = p.time.split(':')[0] + ':00';
    const bucket = byHour[hour] || { time: hour, c10: 0, c50: 0, c90: 0 };
    bucket.c10 = Math.round((bucket.c10 + p.c10) * 100) / 100;
    bucket.c50 = Math.round((bucket.c50 + p.c50) * 100) / 100;
    bucket.c90 = Math.round((bucket.c90 + p.c90) * 100) / 100;
    byHour[hour] = bucket;
  }
  return Object.keys(byHour).sort().map(h => byHour[h]);
}

const ForecastCalc = { aggregateSolcastHourly };
if (typeof module !== 'undefined' && module.exports) {
  module.exports = ForecastCalc;
} else {
  window.ForecastCalc = ForecastCalc;
}
```

- [ ] **Step 4: Run the tests to verify they still pass**

Run: `node --test tests/js/forecast_calc.test.js`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add static/js/forecast-calc.js tests/js/forecast_calc.test.js
git commit -m "forecast-calc.js: remove solcastPeriodToX, now unused

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 2: `main.py` - accuracy delta + newline-separated summary

**Files:**
- Modify: `main.py:527-572` (the `_read_forecast_payload`/`_solcast_daily_totals`/`get_forecast`
  block)
- Test: `tests/test_main_forecast_summary.py` (new)

**Interfaces:**
- Consumes: `_get_actual_hourly_pv_kwh(date_yyyymmdd) -> Dict[str, float]` (existing, unchanged -
  see `main.py:575`), `_solcast_daily_totals(periods_dict) -> (c10, c50, c90)` (existing, unchanged).
- Produces: `_accuracy_delta_pct(forecast_total: float, actual_total: Optional[float]) -> Optional[int]`,
  `_build_forecast_summary(meteosource_total: float, solcast_totals: Optional[Tuple[float, float, float]], actual_total: Optional[float]) -> str`
  - both pure functions, no later task depends on them beyond `get_forecast()` itself (this is the
    last task touching `main.py`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_main_forecast_summary.py`:

```python
import unittest

from main import _accuracy_delta_pct, _build_forecast_summary


class AccuracyDeltaPctTest(unittest.TestCase):
    def test_forecast_higher_than_actual_is_a_positive_delta(self):
        self.assertEqual(_accuracy_delta_pct(10.0, 8.0), 25)

    def test_forecast_lower_than_actual_is_a_negative_delta(self):
        self.assertEqual(_accuracy_delta_pct(6.0, 8.0), -25)

    def test_zero_actual_total_returns_none(self):
        self.assertIsNone(_accuracy_delta_pct(10.0, 0.0))

    def test_none_actual_total_returns_none(self):
        self.assertIsNone(_accuracy_delta_pct(10.0, None))


class BuildForecastSummaryTest(unittest.TestCase):
    def test_meteosource_only_no_solcast_no_actual(self):
        summary = _build_forecast_summary(10.0, None, None)
        self.assertEqual(summary, "Meteosource: 10.0 kWh")

    def test_meteosource_and_solcast_no_actual(self):
        summary = _build_forecast_summary(10.0, (3.0, 5.0, 7.0), None)
        self.assertEqual(summary, "Meteosource: 10.0 kWh\nSolcast: 5.0 (3.0-7.0) kWh")

    def test_meteosource_and_solcast_with_actual_shows_both_deltas(self):
        summary = _build_forecast_summary(10.0, (4.0, 6.0, 10.0), 8.0)
        self.assertEqual(
            summary,
            "Meteosource: 10.0 kWh (Δ +25% vs actual)\nSolcast: 6.0 (4.0-10.0) kWh (Δ -25% vs actual)",
        )

    def test_meteosource_only_with_actual_shows_one_delta_and_no_solcast_line(self):
        summary = _build_forecast_summary(10.0, None, 8.0)
        self.assertEqual(summary, "Meteosource: 10.0 kWh (Δ +25% vs actual)")


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `venv/bin/python3 -m pytest tests/test_main_forecast_summary.py -v`
Expected: FAIL - `ImportError: cannot import name '_accuracy_delta_pct' from 'main'`

- [ ] **Step 3: Add the two helpers and rewire `get_forecast()`**

In `main.py`, replace the existing `get_forecast()` (and add the two new helpers just above it,
right after `_solcast_daily_totals`):

```python
def _solcast_daily_totals(periods_dict):
    """periods_dict: {"HH:MM": {"c10":.., "c50":.., "c90":..}}. Returns
    (c10_total, c50_total, c90_total) day sums, rounded to 1 decimal to
    match the existing summary line's precision."""
    c10 = sum(v['c10'] for v in periods_dict.values())
    c50 = sum(v['c50'] for v in periods_dict.values())
    c90 = sum(v['c90'] for v in periods_dict.values())
    return round(c10, 1), round(c50, 1), round(c90, 1)


def _accuracy_delta_pct(forecast_total, actual_total):
    """Δ = round((forecast_total - actual_total) / actual_total * 100),
    signed - positive means the forecast overestimated, negative means it
    underestimated. None if actual_total is falsy (0 or None) - a real
    zero-production day (e.g. total snow cover) can't be divided into, and
    get_forecast() already only passes a real actual_total when one is
    computable (see its own gating logic)."""
    if not actual_total:
        return None
    return round((forecast_total - actual_total) / actual_total * 100)


def _build_forecast_summary(meteosource_total, solcast_totals, actual_total):
    """meteosource_total: day-total Meteosource kWh. solcast_totals:
    (c10_total, c50_total, c90_total) tuple, or None if Solcast has no data
    for this date. actual_total: day-total real Actual kWh if the accuracy
    delta is computable for this date (a fully elapsed past day with all 24
    hours recorded - see get_forecast's gating), else None to omit deltas
    entirely. Returns the summary string - one line if Solcast has no data,
    two `\n`-joined lines otherwise; forecast.html renders it with CSS
    `white-space: pre-line` so the `\n` becomes a real line break without
    needing `| safe` + `<br>`.
    """
    meteosource_line = f"Meteosource: {meteosource_total} kWh"
    meteosource_delta = _accuracy_delta_pct(meteosource_total, actual_total)
    if meteosource_delta is not None:
        meteosource_line += f" (Δ {meteosource_delta:+d}% vs actual)"
    lines = [meteosource_line]

    if solcast_totals is not None:
        c10_total, c50_total, c90_total = solcast_totals
        solcast_line = f"Solcast: {c50_total} ({c10_total}-{c90_total}) kWh"
        solcast_delta = _accuracy_delta_pct(c50_total, actual_total)
        if solcast_delta is not None:
            solcast_line += f" (Δ {solcast_delta:+d}% vs actual)"
        lines.append(solcast_line)

    return "\n".join(lines)


@app.get('/forecast')
def get_forecast():
    date_param = request.args.get('date', default='t')
    date = parse_date(date_param)
    date_yyyymmdd = date.strftime('%Y-%m-%d')
    fetched_at = request.args.get('fetched_at', type=int)
    logger.debug(f"Fetching forecast for {date_yyyymmdd} (fetched_at={fetched_at})")

    with _forecast_history_connection() as conn:
        meteosource = _read_forecast_payload(conn, 'meteosource', date_yyyymmdd, fetched_at)
        solcast_periods = _read_forecast_payload(conn, 'solcast', date_yyyymmdd, fetched_at)

    meteosource_total = round(sum(meteosource.values()), 1)
    solcast_totals = _solcast_daily_totals(solcast_periods) if solcast_periods else None

    # Accuracy delta only for a fully elapsed past date with complete
    # telemetry - see this plan's Global Constraints and spec §3's
    # amendment for why (a partial/incomplete actual total would make the
    # delta misleading, not informative).
    is_past_date = date_yyyymmdd < datetime.now().strftime('%Y-%m-%d')
    actual_by_hour = _get_actual_hourly_pv_kwh(date_yyyymmdd) if is_past_date else {}
    actual_total = round(sum(actual_by_hour.values()), 1) if is_past_date and len(actual_by_hour) == 24 else None

    summary = _build_forecast_summary(meteosource_total, solcast_totals, actual_total)
    return flask.render_template('forecast.html', date=date_yyyymmdd, fetched_at=fetched_at, summary=summary)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `venv/bin/python3 -m pytest tests/test_main_forecast_summary.py -v`
Expected: PASS (8 tests)

Then run the full test suite to check nothing else broke:

Run: `venv/bin/python3 -m pytest tests/ -v --ignore=tests/js`
Expected: PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add main.py tests/test_main_forecast_summary.py
git commit -m "main.py: add accuracy delta vs real actual, newline-separated summary

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 3: `templates/forecast.html` - hourly Solcast chart, y-axis stacking fix, summary CSS

**Files:**
- Modify: `templates/forecast.html`

**Interfaces:**
- Consumes: `ForecastCalc.aggregateSolcastHourly` (Task 1, unchanged signature), the two-line `\n`
  summary string from `_build_forecast_summary` (Task 2).
- Produces: the rendered page - no new interfaces for later tasks (this is the last task).

- [ ] **Step 1: Make the summary `<p>` render its `\n` as a line break**

Replace:

```html
    <div class="row">
      <div class="col">
        <p>{{ summary }}</p>
      </div>
    </div>
```

with:

```html
    <div class="row">
      <div class="col">
        <p id="forecast-summary">{{ summary }}</p>
      </div>
    </div>
```

And add to the existing `<style>` block (next to `#forecast-chart`'s rules):

```css
    /* main.py's _build_forecast_summary joins Meteosource/Solcast with a
       literal \n - this renders that as a real line break without opting
       the template out of Jinja's autoescaping (no `| safe` + `<br>`
       needed for a plain two-line label). */
    #forecast-summary { white-space: pre-line; }
```

- [ ] **Step 2: Switch `solcastDatasets()` to hourly-aggregated points**

Replace:

```javascript
      function solcastDatasets() {
        if (!data.solcast.available) return [];
        // Native 30-minute points, unlike Meteosource's hourly bars - see
        // spec §1 "Mixed resolution".
        const points = key => data.solcast.periods.map(p => ({ x: ForecastCalc.solcastPeriodToX(p.time), y: p[key] }));
        return [
          { type: 'line', label: 'Solcast c10', data: points('c10'), borderWidth: 0, pointRadius: 0, fill: false, order: 3 },
          { type: 'line', label: 'Solcast c10–c90 range', data: points('c90'), borderWidth: 0, pointRadius: 0, fill: '-1', backgroundColor: ChartTheme.withAlpha(c50Color, 0.18), order: 3 },
          { type: 'line', label: 'Solcast c50', data: points('c50'), borderColor: c50Color, backgroundColor: c50Color, borderWidth: 2, pointRadius: 0, fill: false, order: 2 },
        ];
      }
```

with:

```javascript
      function solcastDatasets() {
        if (!data.solcast.available) return [];
        // Hourly-aggregated, same resolution and same x-positions
        // (idx + 0.5) as the Meteosource bar - see
        // docs/superpowers/specs/2026-09-14-solcast-pv-forecast-design.md
        // §1's amendment. solcastByHour is already built above (table
        // section) from the same ForecastCalc.aggregateSolcastHourly call;
        // reused here instead of a second aggregation pass. A missing hour
        // (e.g. the very edge of a merged snapshot window) reads as 0,
        // same convention the table's row-building already uses.
        const points = key => hourLabels.map((t, idx) => ({ x: idx + 0.5, y: (solcastByHour[t] || {})[key] ?? 0 }));
        return [
          { type: 'line', label: 'Solcast c10', data: points('c10'), borderWidth: 0, pointRadius: 0, fill: false, order: 3 },
          { type: 'line', label: 'Solcast c10–c90 range', data: points('c90'), borderWidth: 0, pointRadius: 0, fill: '-1', backgroundColor: ChartTheme.withAlpha(c50Color, 0.18), order: 3 },
          { type: 'line', label: 'Solcast c50', data: points('c50'), borderColor: c50Color, backgroundColor: c50Color, borderWidth: 2, pointRadius: 0, fill: false, order: 2 },
        ];
      }
```

- [ ] **Step 3: Remove the leftover y-axis `stacked: true`**

This is a pre-existing bug found while touching this exact code: the mockup that originally
validated this chart shape (see spec §1) removed `stacked: true` from *both* the x- and y-axis,
since with `fill`-based line datasets (the Solcast band, the Actual line), it's the **y-axis's**
`stacked` flag that makes Chart.js accumulate fill positions across datasets on that axis - the
x-axis's `stacked` flag (already removed in the original implementation) doesn't affect this. The
production chart's y-axis was never updated to match, so the Solcast band's filled area has been
rendering at an inflated/offset position relative to its own c50 line (which uses `fill: false` and
so isn't affected by the stacking) - this is very likely what looked like "c50 being 2x smaller
than the c10-c90 range" during live testing, on top of the resolution mismatch fixed by Step 2.

Replace:

```javascript
              y: {
                stacked: true,
                beginAtZero: true,
```

with:

```javascript
              y: {
                beginAtZero: true,
```

- [ ] **Step 4: Manually verify against the real app**

Run the app in dry-run mode:

```bash
venv/bin/python3 main.py --dry-run
```

Then, with real `SOLCAST_API_KEY`/`SOLCAST_SITE_EAST_ID`/`SOLCAST_SITE_WEST_ID` already configured
in `.env` (per the earlier manual post-implementation step), trigger one fetch so there's Solcast
data to look at:

```bash
venv/bin/python3 -c "
import dotenv
dotenv.load_dotenv()
import forecast_history, forecast_prefetch
conn = forecast_history.init_db()
forecast_prefetch.fetch_and_store_solcast(conn)
conn.close()
"
```

Then open `http://<LAN IP>:5001/forecast` in a browser (see the `local-dryrun-url-format` memory
convention - use the LAN IP, not localhost) and confirm:
- The summary shows Meteosource and Solcast on two separate lines.
- The Solcast c50 line sits visibly inside its own c10-c90 shaded band (not offset above/below it).
- The Solcast series' hourly values are in the same ballpark as Meteosource's for the same hour
  (no more artificial ~2x gap from the old 30-minute-vs-hourly mismatch).
- Hovering the chart shows a tooltip where Meteosource/Solcast/Actual all agree on which hour is
  being shown (no more cross-series hour mismatch).
- For a **past date** with a full day of recorded Actual data (e.g. yesterday, if the inverter has
  been running - in `--dry-run` there may be no real telemetry, in which case the summary will
  correctly show no `Δ` at all, which is also worth confirming), the summary shows `(Δ ±N% vs
  actual)` on both lines; for today, it does not.

- [ ] **Step 5: Commit**

```bash
git add templates/forecast.html
git commit -m "forecast.html: hourly Solcast chart, fix leftover y-axis stacking bug, summary CSS

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```
