// Wires diagram-calc.js's pure logic to the DOM: draws the connecting
// lines as hand-built SVG (ported from the mockups under
// docs/superpowers/mockups/ - several rounds of using the LeaderLine
// library there ran into curved/mis-routed paths and label-offset issues,
// so this draws everything directly from box positions instead), updates
// node text/colors on every SSE message, and handles the inline-expand
// detail toggles. Manually verified against the running app - no DOM
// available under `node --test`, unlike diagram-calc.js.
(function () {
  'use strict';

  var LINE_COLORS = { grey: '#6c757d', green: '#28a745', orange: '#fd7e14', red: '#dc3545', pink: '#e83e8c', yellow: '#ffc107' };

  function byId(id) { return document.getElementById(id); }
  function fmtW(w) { return Math.round(w) + ' W'; }

  function setNodeColor(nodeId, colorName) {
    var el = byId(nodeId);
    el.className = el.className.replace(/\bdiagram-node--\w+\b/g, '').trim();
    el.classList.add('diagram-node--' + colorName);
  }

  // --- Hand-drawn SVG connections ------------------------------------
  //
  // Every edge in this diagram is either purely horizontal or purely
  // vertical by design (matching the CSS grid layout), so a "point" is
  // enough to describe each connection - no path routing, no library
  // sockets. Backup/Load aren't centered under Inverter/Junction's actual
  // rendered column (grid-column sizing can shift slightly with content),
  // so their point deliberately uses the *other* element's center
  // coordinate for one axis instead of their own.

  var SVG_NS = 'http://www.w3.org/2000/svg';

  function svgEl(tag, attrs) {
    var el = document.createElementNS(SVG_NS, tag);
    Object.keys(attrs).forEach(function (k) { el.setAttribute(k, attrs[k]); });
    return el;
  }

  function diagramRect() {
    return document.querySelector('.diagram').getBoundingClientRect();
  }

  function centerX(el) {
    var c = diagramRect(), r = el.getBoundingClientRect();
    return (r.left + r.right) / 2 - c.left;
  }
  function centerY(el) {
    var c = diagramRect(), r = el.getBoundingClientRect();
    return (r.top + r.bottom) / 2 - c.top;
  }
  function edgeY(el, which) {
    var c = diagramRect(), r = el.getBoundingClientRect();
    return (which === 'top' ? r.top : r.bottom) - c.top;
  }
  function edgeX(el, which) {
    var c = diagramRect(), r = el.getBoundingClientRect();
    return (which === 'left' ? r.left : r.right) - c.left;
  }

  // Returns the six edge geometries fresh from current DOM positions -
  // called on every render/resize, cheap enough for ~6 lines.
  function computeEdges() {
    var pv = byId('node-pv'), battery = byId('node-battery'), inverter = byId('node-inverter'),
        junction = byId('node-junction'), backup = byId('node-backup'), grid = byId('node-grid'), load = byId('node-load');
    return {
      pv: { x1: centerX(inverter), y1: edgeY(pv, 'bottom'), x2: centerX(inverter), y2: edgeY(inverter, 'top') },
      battery: { x1: edgeX(battery, 'right'), y1: centerY(inverter), x2: edgeX(inverter, 'left'), y2: centerY(inverter) },
      backup: { x1: centerX(backup), y1: edgeY(inverter, 'bottom'), x2: centerX(backup), y2: edgeY(backup, 'top') },
      bus: { x1: edgeX(inverter, 'right'), y1: centerY(inverter), x2: edgeX(junction, 'left'), y2: centerY(inverter) },
      grid: { x1: edgeX(junction, 'right'), y1: centerY(inverter), x2: edgeX(grid, 'left'), y2: centerY(inverter) },
      load: { x1: centerX(junction), y1: centerY(junction), x2: centerX(junction), y2: edgeY(load, 'top') },
    };
  }

  // Moves a point `amount` px along `dir` ('up'/'down'/'left'/'right').
  function moveAlong(x, y, dir, amount) {
    switch (dir) {
      case 'right': return [x + amount, y];
      case 'left': return [x - amount, y];
      case 'down': return [x, y + amount];
      case 'up': return [x, y - amount];
    }
  }

  // dir here is the direction FROM the back corners TO the tip.
  function arrowheadPolygonPoints(tipX, tipY, dir, size) {
    var half = size * 0.6;
    var back = moveAlong(tipX, tipY, dir, -size);
    if (dir === 'right' || dir === 'left') return [[back[0], back[1] - half], [tipX, tipY], [back[0], back[1] + half]];
    return [[back[0] - half, back[1]], [tipX, tipY], [back[0] + half, back[1]]];
  }

  function drawEdge(svg, geo, colorName, thicknessPx, reversed, opacity, directionKnown) {
    var hasFlow = thicknessPx > 0;
    var color = LINE_COLORS[colorName] || LINE_COLORS.grey;
    var strokeWidth = hasFlow ? thicknessPx : 1;
    var horizontal = geo.y1 === geo.y2;
    var hasDirection = hasFlow && directionKnown !== false;
    var arrowAtStart = hasDirection && reversed;
    var arrowAtEnd = hasDirection && !reversed;
    var alpha = opacity === undefined ? 1 : opacity;

    var size = Math.max(8, 6 + strokeWidth * 2.2);
    var endDir = horizontal ? (geo.x2 > geo.x1 ? 'right' : 'left') : (geo.y2 > geo.y1 ? 'down' : 'up');
    var startDir = horizontal ? (endDir === 'right' ? 'left' : 'right') : (endDir === 'down' ? 'up' : 'down');

    var endTip = [geo.x2, geo.y2];
    var startTip = [geo.x1, geo.y1];
    var lineEnd = arrowAtEnd ? moveAlong(endTip[0], endTip[1], endDir, -size) : [geo.x2, geo.y2];
    var lineStart = arrowAtStart ? moveAlong(startTip[0], startTip[1], startDir, -size) : [geo.x1, geo.y1];

    svg.appendChild(svgEl('line', {
      x1: lineStart[0], y1: lineStart[1], x2: lineEnd[0], y2: lineEnd[1],
      stroke: color, 'stroke-width': strokeWidth, 'stroke-linecap': 'round', opacity: alpha,
    }));

    if (!hasFlow) return;
    if (arrowAtEnd) {
      svg.appendChild(svgEl('polygon', { points: arrowheadPolygonPoints(endTip[0], endTip[1], endDir, size).map(function (p) { return p.join(','); }).join(' '), fill: color, opacity: alpha }));
    }
    if (arrowAtStart) {
      svg.appendChild(svgEl('polygon', { points: arrowheadPolygonPoints(startTip[0], startTip[1], startDir, size).map(function (p) { return p.join(','); }).join(' '), fill: color, opacity: alpha }));
    }
  }

  function drawCross(svg, geo) {
    var midX = (geo.x1 + geo.x2) / 2, midY = (geo.y1 + geo.y2) / 2, r = 6;
    var color = LINE_COLORS.red;
    [[-r, -r, r, r], [-r, r, r, -r]].forEach(function (d) {
      svg.appendChild(svgEl('line', {
        x1: midX + d[0], y1: midY + d[1], x2: midX + d[2], y2: midY + d[3],
        stroke: color, 'stroke-width': 2, 'stroke-linecap': 'round',
      }));
    });
  }

  var lastData = null;

  function redrawLines(calc, data) {
    var svg = byId('diagram-svg');
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    var edges = computeEdges();

    var pv = calc.pvState(data);
    var battery = calc.batteryState(data);
    var backup = calc.backupState(data);
    var bus = calc.inverterBusState(data);
    var grid = calc.gridState(data);
    var load = calc.loadState(data);

    drawEdge(svg, edges.pv, pv.active ? 'green' : 'grey', calc.arrowThickness(pv.watts), false, 1, true);
    if (!battery.noBattery) {
      drawEdge(svg, edges.battery, battery.color, calc.arrowThickness(battery.watts), battery.direction === 'charge', 1, battery.direction !== 'none');
    }
    drawEdge(svg, edges.backup, backup.active ? 'orange' : 'grey', calc.arrowThickness(backup.watts), false, 1, true);
    // The Inverter-Junction "bus" line is crossed when the inverter isn't
    // outputting to the shared bus (grid Fault/Not-connected - see
    // gridState's `crossed`), and genuinely carries 0W whenever that's the
    // case, regardless of what pgrid would otherwise say. Half-opacity
    // when crossed, so the line reads as "inactive" rather than a normal
    // idle grey line.
    drawEdge(svg, edges.bus, grid.crossed ? 'red' : 'grey', grid.crossed ? 0 : calc.arrowThickness(bus.watts), false, grid.crossed ? 0.5 : 1, true);
    drawEdge(svg, edges.grid, grid.color, calc.arrowThickness(grid.watts), grid.color === 'orange', 1, grid.directionKnown);
    drawEdge(svg, edges.load, load.watts > 0 ? 'orange' : 'grey', calc.arrowThickness(load.watts), false, 1, true);
    if (grid.crossed) drawCross(svg, edges.bus);
  }

  function render(data) {
    lastData = data;
    var calc = window.DiagramCalc;

    var pv = calc.pvState(data);
    byId('pv-watts').textContent = fmtW(pv.watts);
    byId('pv-string1').textContent = fmtW(calc.toNumber(data.ppv1)) + ', ' + calc.toNumber(data.vpv1).toFixed(1) + ' V';
    byId('pv-string2').textContent = fmtW(calc.toNumber(data.ppv2)) + ', ' + calc.toNumber(data.vpv2).toFixed(1) + ' V';
    setNodeColor('node-pv', pv.active ? 'green' : 'grey');

    var battery = calc.batteryState(data);
    byId('battery-soc').textContent = calc.toNumber(data.battery_soc) + ' %';
    byId('battery-watts').textContent = fmtW(battery.watts);
    byId('battery-voltage').textContent = calc.toNumber(data.vbattery1).toFixed(1);
    byId('battery-temp').textContent = calc.toNumber(data.battery_temperature);
    setNodeColor('node-battery', battery.color);
    byId('battery-fill').style.height = calc.toNumber(data.battery_soc) + '%';
    // No battery hardware at all - the whole node fades out, distinct
    // from Standby (a real, connected battery just not actively charging
    // or discharging right now).
    byId('node-battery').style.opacity = battery.noBattery ? 0.5 : 1;

    var inverter = calc.inverterState(data);
    byId('inverter-status').textContent = inverter.label;
    setNodeColor('node-inverter', inverter.color);
    byId('inverter-temp-air').textContent = calc.toNumber(data.temperature_air);
    byId('inverter-temp').textContent = calc.toNumber(data.temperature);

    var backup = calc.backupState(data);
    byId('backup-watts').textContent = fmtW(backup.watts);
    setNodeColor('node-backup', backup.active ? 'orange' : 'grey');
    ['backup-i1', 'backup-i2', 'backup-i3'].forEach(function (id, i) {
      byId(id).textContent = backup.phaseCurrents[i].toFixed(1);
      byId(id.replace('backup-i', 'backup-line')).classList.toggle('diagram-alert', backup.phaseAlerts[i]);
    });

    var grid = calc.gridState(data);
    byId('grid-watts').textContent = fmtW(grid.watts);
    setNodeColor('node-grid', grid.color);
    byId('grid-meter').textContent = [data.meter_active_power1, data.meter_active_power2, data.meter_active_power3]
      .map(function (v) { return Math.round(calc.toNumber(v)); }).join(' / ') + ' W';
    byId('grid-voltages').textContent = [data.vgrid, data.vgrid2, data.vgrid3]
      .map(function (v) { return calc.toNumber(v).toFixed(1); }).join(' / ') + ' V';
    byId('grid-freqs').textContent = [data.fgrid, data.fgrid2, data.fgrid3]
      .map(function (v) { return calc.toNumber(v).toFixed(2); }).join(' / ') + ' Hz';

    var load = calc.loadState(data);
    byId('load-watts').textContent = fmtW(load.watts);
    setNodeColor('node-load', load.watts > 0 ? 'orange' : 'grey');
    byId('load-p1').textContent = fmtW(calc.toNumber(data.load_p1));
    byId('load-p2').textContent = fmtW(calc.toNumber(data.load_p2));
    byId('load-p3').textContent = fmtW(calc.toNumber(data.load_p3));

    redrawLines(calc, data);
  }

  function initToggles() {
    document.querySelectorAll('[data-toggle-details]').forEach(function (trigger) {
      trigger.addEventListener('click', function () {
        var key = trigger.getAttribute('data-toggle-details');
        byId(key + '-details').classList.toggle('diagram-details--open');
        // Expanding/collapsing details changes a node's box height, which
        // shifts where the lines need to connect.
        if (lastData) redrawLines(window.DiagramCalc, lastData);
      });
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    initToggles();
    eventSource.addEventListener('message', function (e) {
      render(JSON.parse(e.data));
    });
    window.addEventListener('resize', function () {
      if (lastData) redrawLines(window.DiagramCalc, lastData);
    });
  });
})();
