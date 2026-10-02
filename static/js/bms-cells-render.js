// "Battery cells" panel: the latest Pylontech BMS sample (bms_poller.py),
// sent as a sticky `bms` SSE event once a minute and replayed on connect.
// Calculations live in bms-cells-calc.js (unit-tested); this file only
// touches the DOM.
(function () {
  'use strict';
  var C = window.BmsCellsCalc;
  var SVG_NS = 'http://www.w3.org/2000/svg';
  var COLORS = { cell: '#6c8ebf', min: '#d9534f', max: '#2e8b57', divider: '#999', label: '#6c757d' };
  var HEIGHT = 140, LEFT = 52, TOP = 6, BOTTOM = 6;
  var last = null;

  function el(name, attrs, text) {
    var node = document.createElementNS(SVG_NS, name);
    Object.keys(attrs).forEach(function (k) { node.setAttribute(k, attrs[k]); });
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function renderSummary(s, ageS) {
    var spread = Math.round((s.cell_voltage_max - s.cell_voltage_min) * 1000);
    var modules = s.module_voltages.map(function (v) { return v.toFixed(2); }).join(' / ');
    document.getElementById('bms-summary').textContent =
      C.formatFlow(s.state, s.current) + ' · SOH ' + s.soh + '%, ' + s.cycle_count + ' cycles · ' +
      'cells ' + s.module_temp_min.toFixed(1) + '–' + s.module_temp_max.toFixed(1) + ' °C' +
      ' (BMS board ' + s.bms_temperature.toFixed(1) + ' °C) · ' +
      s.cell_voltage_min.toFixed(3) + '–' + s.cell_voltage_max.toFixed(3) + ' V, spread ' + spread + ' mV' +
      ' · modules ' + modules + ' V · ' + C.formatAge(ageS) + ' ago' +
      (C.isStale(ageS) ? ' (stale)' : '');
  }

  function renderChart(cells, temps) {
    var svg = document.getElementById('bms-cells');
    var width = Math.max(svg.clientWidth || 600, LEFT + cells.length * 2);
    svg.setAttribute('viewBox', '0 0 ' + width + ' ' + HEIGHT);
    while (svg.firstChild) svg.removeChild(svg.firstChild);

    var scale = C.cellScale(cells);
    var extremes = C.cellExtremes(cells);
    var plotH = HEIGHT - TOP - BOTTOM;
    var step = (width - LEFT) / cells.length;
    var gap = step > 6 ? 2 : 1;

    svg.appendChild(el('text', { x: LEFT - 4, y: TOP + 10, 'text-anchor': 'end', 'font-size': 11, fill: COLORS.label },
      (scale.hi / 1000).toFixed(3) + ' V'));
    svg.appendChild(el('text', { x: LEFT - 4, y: HEIGHT - BOTTOM, 'text-anchor': 'end', 'font-size': 11, fill: COLORS.label },
      (scale.lo / 1000).toFixed(3) + ' V'));

    cells.forEach(function (mv, i) {
      var h = C.barHeight(mv, scale, plotH);
      var color = i === extremes.minIdx ? COLORS.min : (i === extremes.maxIdx ? COLORS.max : COLORS.cell);
      var bar = el('rect', { x: LEFT + i * step + gap / 2, y: TOP + plotH - h, width: Math.max(1, step - gap), height: h, fill: color });
      bar.appendChild(el('title', {}, C.cellLabel(i, mv, temps ? temps[i] : undefined)));
      svg.appendChild(bar);
    });

    C.moduleBoundaries(cells.length).forEach(function (i) {
      var x = LEFT + i * step;
      svg.appendChild(el('line', { x1: x, x2: x, y1: TOP, y2: HEIGHT - BOTTOM, stroke: COLORS.divider, 'stroke-dasharray': '4 3' }));
    });
  }

  function render() {
    if (!last) return;
    var panel = document.getElementById('bms-panel');
    panel.style.display = '';
    var ageS = C.sampleAgeSeconds(last.timestamp_epoch, Date.now());
    panel.style.opacity = C.isStale(ageS) ? 0.5 : 1;
    renderSummary(last, ageS);
    renderChart(last.cell_mv, last.cell_temps);
  }

  window.eventSource.addEventListener('bms', function (e) {
    last = JSON.parse(e.data);
    render();
  });
  setInterval(render, 10000);  // keeps the age (and the stale greying) current
  window.addEventListener('resize', render);
})();
