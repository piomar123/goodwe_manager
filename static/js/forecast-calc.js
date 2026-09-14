// Pure per-request math for the /forecast chart/table - no DOM, no
// Chart.js - so it can be unit tested the same way static/js/diagram-calc.js
// already is (see tests/js/forecast_calc.test.js). Loaded as a plain
// <script> in forecast.html (browser global ForecastCalc), and required
// directly in tests (Node's CommonJS module.exports below).

function solcastPeriodToX(hhmm) {
  const [h, m] = hhmm.split(':').map(Number);
  return h + m / 60;
}

// Sums each hour's two 30-minute Solcast periods into one hourly row - the
// table shows hourly figures even though the chart (forecast.html) plots
// Solcast at its native 30-minute resolution. A missing half (e.g. the
// very first/last period of a fetch window) is treated as 0, same
// convention forecast.py/solcast.py already use for a missing orientation.
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

const ForecastCalc = { solcastPeriodToX, aggregateSolcastHourly };
if (typeof module !== 'undefined' && module.exports) {
  module.exports = ForecastCalc;
} else {
  window.ForecastCalc = ForecastCalc;
}
