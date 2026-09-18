// Pure per-request math for the /forecast chart/table - no DOM, no
// Chart.js - so it can be unit tested the same way static/js/diagram-calc.js
// already is (see tests/js/forecast_calc.test.js). Loaded as a plain
// <script> in forecast.html (browser global ForecastCalc), and required
// directly in tests (Node's CommonJS module.exports below).

// Sums each hour's two 30-minute Solcast periods into one hourly row - both
// the table and the chart (forecast.html) render Solcast at this hourly
// resolution, even though the underlying data is fetched/stored at
// Solcast's native 30-minute period (see
// https://github.com/piomar123/goodwe_manager/pull/26 for why the chart no
// longer plots the native resolution directly - the design spec doc itself
// was removed from the tree, but is still visible in that PR's history).
// A missing half (e.g. the very first/last period of a fetch
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

// Same hour-bucketing convention as aggregateSolcastHourly, but for Solcast
// estimated_actuals' flat {time, kwh} period shape (a historical estimate
// is a single value, not a {c10,c50,c90} range) - kept as a separate
// function rather than generalizing one aggregator across both shapes, for
// the same reason solcast.py's sum_sites/sum_sites_flat stay separate. See
// https://github.com/piomar123/goodwe_manager/pull/26 for the design
// rationale (the design spec doc itself was removed from the tree, but is
// still visible in that PR's history).
function aggregateSolcastActualsHourly(periods) {
  const byHour = {};
  for (const p of periods) {
    const hour = p.time.split(':')[0] + ':00';
    const bucket = byHour[hour] || { time: hour, kwh: 0 };
    bucket.kwh = Math.round((bucket.kwh + p.kwh) * 100) / 100;
    byHour[hour] = bucket;
  }
  return Object.keys(byHour).sort().map(h => byHour[h]);
}

// Builds one row per hour for the /forecast table, in either mode of the
// page's Cumulative switch: hourly (that hour's own value) or cumulative
// (the running total through that hour). A missing hour (absent from the
// corresponding *ByHour map) shows '—' and is skipped from the running
// total - so a gap doesn't reset the total, it just doesn't advance it.
// Solcast's c10/c50/c90 are each summed independently when cumulative, so
// the displayed range stays a real (min, median, max) of running totals,
// not just the c50 total with the last hour's spread tacked on.
function buildForecastTableRows({
  hourLabels, meteosourceByHour, solcastByHour, solcastActualsByHour, actualByHour,
  currentHourIndex, partialKwh, solcastActualsAvailable, cumulative,
}) {
  let cumMeteo = 0, cumC10 = 0, cumC50 = 0, cumC90 = 0, cumSolcastActuals = 0, cumActual = 0;
  return hourLabels.map((t, idx) => {
    const row = { Hour: t };

    const m = meteosourceByHour[t];
    if (m != null) {
      cumMeteo += m;
      row['Meteosource'] = (cumulative ? cumMeteo : m).toFixed(2);
    } else {
      row['Meteosource'] = '—';
    }

    const s = solcastByHour[t];
    if (s) {
      cumC10 += s.c10; cumC50 += s.c50; cumC90 += s.c90;
      const c10 = cumulative ? cumC10 : s.c10;
      const c50 = cumulative ? cumC50 : s.c50;
      const c90 = cumulative ? cumC90 : s.c90;
      row['Solcast'] = `${c50.toFixed(2)} (${c10.toFixed(2)}–${c90.toFixed(2)})`;
    } else {
      row['Solcast'] = '—';
    }

    if (solcastActualsAvailable) {
      const sa = solcastActualsByHour[t];
      if (sa) {
        cumSolcastActuals += sa.kwh;
        row['Solcast Estimated Actual'] = (cumulative ? cumSolcastActuals : sa.kwh).toFixed(2);
      } else {
        row['Solcast Estimated Actual'] = '—';
      }
    }

    if (actualByHour[t] != null) {
      cumActual += actualByHour[t];
      row['Actual'] = (cumulative ? cumActual : actualByHour[t]).toFixed(2);
    } else if (idx === currentHourIndex && partialKwh != null) {
      const val = cumulative ? cumActual + partialKwh : partialKwh;
      row['Actual'] = `${val.toFixed(2)} (so far)`;
    } else {
      // Not a completed hour and not the live partial one either - no
      // meaningful "actual production through this hour" figure yet
      // (covers future hours, and any hour past-due but still missing
      // data, e.g. no inverter connection).
      row['Actual'] = '—';
    }
    return row;
  });
}

const ForecastCalc = { aggregateSolcastHourly, aggregateSolcastActualsHourly, buildForecastTableRows };
if (typeof module !== 'undefined' && module.exports) {
  module.exports = ForecastCalc;
} else {
  window.ForecastCalc = ForecastCalc;
}
