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
