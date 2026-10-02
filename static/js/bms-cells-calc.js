// Pure calculation functions for the "Battery cells" panel (Pylontech BMS
// sample, see bms_poller.py). Same pattern as diagram-calc.js: no DOM
// access, loaded in the browser as a plain <script> (window.BmsCellsCalc)
// and under Node for `node --test tests/js/*.test.js`.
(function (root) {
  'use strict';

  var CELLS_PER_MODULE = 30;  // Force H2 module
  var STALE_AFTER_S = 300;    // same as the HA sensors' expire_after

  // y-axis range: real min/max plus padding, at least minSpanMv wide, so a
  // few mV of spread still shows as visibly different bars.
  function cellScale(cellMv, padMv, minSpanMv) {
    padMv = padMv === undefined ? 3 : padMv;
    minSpanMv = minSpanMv === undefined ? 10 : minSpanMv;
    var lo = Math.min.apply(null, cellMv) - padMv;
    var hi = Math.max.apply(null, cellMv) + padMv;
    if (hi - lo < minSpanMv) {
      var mid = (hi + lo) / 2;
      lo = mid - minSpanMv / 2;
      hi = mid + minSpanMv / 2;
    }
    return { lo: lo, hi: hi };
  }

  function cellExtremes(cellMv) {
    var minIdx = 0, maxIdx = 0;
    for (var i = 1; i < cellMv.length; i++) {
      if (cellMv[i] < cellMv[minIdx]) minIdx = i;
      if (cellMv[i] > cellMv[maxIdx]) maxIdx = i;
    }
    return { minIdx: minIdx, maxIdx: maxIdx };
  }

  // Cell indices where a further module starts (a divider goes before them).
  function moduleBoundaries(cellCount, perModule) {
    perModule = perModule || CELLS_PER_MODULE;
    var out = [];
    for (var i = perModule; i < cellCount; i += perModule) out.push(i);
    return out;
  }

  function barHeight(mv, scale, height) {
    var h = (mv - scale.lo) / (scale.hi - scale.lo) * height;
    return Math.max(1, Math.min(height, h));
  }

  function sampleAgeSeconds(sampleEpochS, nowMs) {
    return Math.max(0, Math.round(nowMs / 1000 - sampleEpochS));
  }

  function isStale(ageS) {
    return ageS > STALE_AFTER_S;
  }

  function formatAge(ageS) {
    return ageS < 120 ? ageS + ' s' : Math.round(ageS / 60) + ' min';
  }

  // "charging 15.2 A" from the BMS state and current (A, + = charging).
  function formatFlow(state, current) {
    var amps = Math.abs(current).toFixed(1) + ' A';
    if (state === 'charge') return 'charging ' + amps;
    if (state === 'discharge') return 'discharging ' + amps;
    if (state === 'idle') return 'idle';
    return state + ' ' + amps;
  }

  // Bar tooltip: "Module 2 cell 5: 3.301 V, 32 °C" (cellIdx 0-based; the
  // temperature is left out for samples without cell_temps).
  function cellLabel(cellIdx, mv, tempC) {
    var label = 'Module ' + (Math.floor(cellIdx / CELLS_PER_MODULE) + 1) + ' cell ' +
      (cellIdx % CELLS_PER_MODULE + 1) + ': ' + (mv / 1000).toFixed(3) + ' V';
    return tempC === undefined || tempC === null ? label : label + ', ' + tempC + ' °C';
  }

  var BmsCellsCalc = {
    CELLS_PER_MODULE: CELLS_PER_MODULE,
    cellScale: cellScale,
    cellExtremes: cellExtremes,
    moduleBoundaries: moduleBoundaries,
    barHeight: barHeight,
    sampleAgeSeconds: sampleAgeSeconds,
    isStale: isStale,
    formatAge: formatAge,
    formatFlow: formatFlow,
    cellLabel: cellLabel,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = BmsCellsCalc;
  } else {
    root.BmsCellsCalc = BmsCellsCalc;
  }
})(typeof window !== 'undefined' ? window : this);
