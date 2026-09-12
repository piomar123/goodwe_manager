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

  // Explicit whitelist rather than a \bdiagram-node--\w+\b regex strip - an
  // earlier version had a diagram-node--translucent modifier class that a
  // blanket regex would wipe on every render().
  var NODE_COLOR_NAMES = ['grey', 'green', 'orange', 'red', 'pink', 'yellow'];
  function setNodeColor(nodeId, colorName) {
    var el = byId(nodeId);
    NODE_COLOR_NAMES.forEach(function (name) { el.classList.remove('diagram-node--' + name); });
    el.classList.add('diagram-node--' + colorName);
  }

  // Shared by render() (node color) and redrawLines() (arrow color) so
  // Backup's own arrow can never drift from the node it feeds - the arrow
  // used to be colored by a separately re-derived orange/grey-only
  // expression that never accounted for the phase-overload case, so the
  // node could show red while its arrow still read plain orange.
  function backupNodeColor(backup, isBackupActive) {
    return backup.phaseAlerts.some(Boolean) ? 'red' : (isBackupActive ? 'orange' : 'grey');
  }

  // --- Hand-drawn SVG connections ------------------------------------
  //
  // Every edge in this diagram is either purely horizontal or purely
  // vertical by design (matching the CSS grid layout), so a "point" is
  // enough to describe each connection. Backup/Load aren't centered under
  // Inverter/Junction's actual rendered column (grid-column sizing can
  // shift slightly with content), so their point deliberately uses the
  // *other* element's center coordinate for one axis instead of their own.
  // Backup is the one exception - it's fed from two different, non-aligned
  // places depending on grid mode (Junction when grid-connected, Inverter
  // when islanded/faulted - see backupSource()), so its edge needs a real
  // bent (Manhattan) path rather than a single straight line; see
  // computeBackupEdge/manhattanSegments below.

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

  // Point + leave/enter direction on a given border - only Backup's edge
  // needs this (it bends), everything else stays a plain two-point line.
  var OPPOSITE_SIDE = { left: 'right', right: 'left', top: 'bottom', bottom: 'top' };
  var LEAVE_DIR = { left: 'left', right: 'right', top: 'up', bottom: 'down' };
  function sideAnchor(el, side) {
    return (side === 'left' || side === 'right') ? { x: edgeX(el, side), y: centerY(el) } : { x: centerX(el), y: edgeY(el, side) };
  }
  function enterDir(side) { return LEAVE_DIR[OPPOSITE_SIDE[side]]; }

  // Junction sits in its own row directly below Inverter, between it and
  // Load, with each of its four sides used by exactly one flow: top=
  // Inverter (bus in), left=Backup (grid-bypass), right=Grid, bottom=Load
  // (see the .diagram grid-template-areas comment in diagram.css). That
  // makes every edge through Junction a plain straight line - only the
  // inverter-fed Backup path (Inverter and Backup aren't adjacent) still
  // bends, entering Backup's *right* border (reads as "fed from the
  // left," consistent with the grid-bypass case, which also enters via
  // Backup's right since Junction sits to its right).
  function computeEdges() {
    var pv = byId('node-pv'), battery = byId('node-battery'), inverter = byId('node-inverter'),
        junction = byId('node-junction'), backup = byId('node-backup'), grid = byId('node-grid'), load = byId('node-load');
    return {
      pv: { p0: sideAnchor(pv, 'bottom'), dir0: 'down', p1: sideAnchor(inverter, 'top'), dir1: enterDir('top') },
      battery: { p0: sideAnchor(battery, 'right'), dir0: 'right', p1: sideAnchor(inverter, 'left'), dir1: enterDir('left') },
      bus: { p0: sideAnchor(inverter, 'bottom'), dir0: 'down', p1: sideAnchor(junction, 'top'), dir1: enterDir('top') },
      grid: { p0: sideAnchor(junction, 'right'), dir0: 'right', p1: sideAnchor(grid, 'left'), dir1: enterDir('left') },
      load: { p0: sideAnchor(junction, 'bottom'), dir0: 'down', p1: sideAnchor(load, 'top'), dir1: enterDir('top') },
      backupBypass: { p0: sideAnchor(junction, 'left'), dir0: 'left', p1: sideAnchor(backup, 'right'), dir1: enterDir('right') },
      backupIslanding: { p0: sideAnchor(inverter, 'bottom'), dir0: 'down', p1: sideAnchor(backup, 'right'), dir1: enterDir('right') },
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

  // Direction of travel for a Manhattan segment, inferred from its own
  // coordinates.
  function segDir(geo) {
    if (geo.y1 === geo.y2) return geo.x2 > geo.x1 ? 'right' : 'left';
    return geo.y2 > geo.y1 ? 'down' : 'up';
  }
  var DIR_VECTOR = { right: { x: 1, y: 0 }, left: { x: -1, y: 0 }, down: { x: 0, y: 1 }, up: { x: 0, y: -1 } };
  // A 90 degree rotation of the direction vector (perp = {dy, -dx}), used
  // to lay out stripes by direction of travel rather than by raw
  // horizontal/vertical axis - a fixed "vertical segments order left-to-
  // right" rule would flip which physical side a given stripe lands on
  // depending on which way the segment happens to travel. Sign chosen so
  // stripe index 0 (battery/yellow, by convention) lands on the left for a
  // downward-traveling edge, matching the screen order the source boxes
  // sit in (Battery left, PV middle, Grid right) - e.g. Load's arrow reads
  // yellow/green/orange left-to-right.
  function perpVector(dir) {
    var u = DIR_VECTOR[dir];
    return { x: u.y, y: -u.x };
  }

  function manhattanSegments(p0, dir0, p1, dir1) {
    if (p0.x === p1.x || p0.y === p1.y) return [{ x1: p0.x, y1: p0.y, x2: p1.x, y2: p1.y }];
    var horiz0 = dir0 === 'left' || dir0 === 'right';
    var horiz1 = dir1 === 'left' || dir1 === 'right';
    if (horiz0 !== horiz1) {
      var corner = horiz0 ? { x: p1.x, y: p0.y } : { x: p0.x, y: p1.y };
      return [
        { x1: p0.x, y1: p0.y, x2: corner.x, y2: corner.y },
        { x1: corner.x, y1: corner.y, x2: p1.x, y2: p1.y },
      ];
    }
    if (horiz0) {
      var midX = (p0.x + p1.x) / 2;
      return [
        { x1: p0.x, y1: p0.y, x2: midX, y2: p0.y },
        { x1: midX, y1: p0.y, x2: midX, y2: p1.y },
        { x1: midX, y1: p1.y, x2: p1.x, y2: p1.y },
      ];
    }
    var midY = (p0.y + p1.y) / 2;
    return [
      { x1: p0.x, y1: p0.y, x2: p0.x, y2: midY },
      { x1: p0.x, y1: midY, x2: p1.x, y2: midY },
      { x1: p1.x, y1: midY, x2: p1.x, y2: p1.y },
    ];
  }

  // reversed flips which end is the real destination by reversing the
  // segment list and swapping each segment's own endpoints, rather than
  // needing separate direction math - the "last segment carries the
  // arrowhead" rule then naturally lands on the true destination either
  // way.
  function drawManhattanEdge(svg, p0, dir0, p1, dir1, colorName, thicknessPx, reversed, directionKnown, opacity, stripes) {
    var segments = manhattanSegments(p0, dir0, p1, dir1);
    if (reversed) segments = segments.slice().reverse().map(function (seg) { return { x1: seg.x2, y1: seg.y2, x2: seg.x1, y2: seg.y1 }; });
    if (stripes && drawStripedManhattanEdge(svg, segments, stripes, thicknessPx)) return;
    segments.forEach(function (seg, i) {
      drawEdge(svg, seg, colorName, thicknessPx, false, opacity, i === segments.length - 1 ? directionKnown : false);
    });
  }

  // "Color mixing" via stripes: rather than blending source colors into
  // one muddy hue, split the arrow's own width into parallel same-color
  // stripes, one per contributing source, each sized proportionally to
  // that source's watts. stripes is ordered left-to-right (for a vertical
  // edge) or top-to-bottom (for a horizontal one) - callers pass them in
  // the same left-to-right order the source boxes sit on screen (Battery/
  // PV/Grid). Per-px integer widths via the largest-remainder method - no
  // fractional-px stripes. This is a proportional-to-available-wattage
  // approximation (doesn't account for a source's watts being shared with
  // other concurrent sinks), not a strict energy-balance readout. Returns
  // null (not an empty array) when there's nothing to draw, so callers can
  // tell "no stripes" apart from "stripes summing to zero width."
  function stripeWidths(stripes, thicknessPx) {
    var total = stripes.reduce(function (sum, s) { return sum + s.watts; }, 0);
    if (total <= 0 || thicknessPx <= 0) return null;
    var raw = stripes.map(function (s) { return thicknessPx * s.watts / total; });
    var widths = raw.map(Math.floor);
    var used = widths.reduce(function (a, b) { return a + b; }, 0);
    var remainder = thicknessPx - used;
    var byFrac = raw.map(function (r, i) { return { i: i, frac: r - Math.floor(r) }; }).sort(function (a, b) { return b.frac - a.frac; });
    for (var k = 0; k < remainder; k++) widths[byFrac[k % byFrac.length].i]++;
    return widths;
  }

  var stripedGradientCounter = 0;

  // Fills the arrowhead triangle with a single hard-stop gradient rather
  // than separate per-stripe shapes (a wedge-fan sharing the tip vertex,
  // or a clip-path + rects, both left visible seams/were a stack of
  // separate shapes) - there's structurally nothing left to seam this way,
  // since the whole tip is exactly one shape with exactly one fill. The
  // gradient's own axis runs along perpVector(dir) (perpendicular to
  // travel), spanning the triangle's back-edge half-width, so color stays
  // constant along the direction of travel and only varies across it - the
  // triangle's own taper (not a separate clip) is what narrows the bands
  // toward the tip.
  function drawStripedArrowheadGradient(svg, tipX, tipY, dir, size, stripes, thicknessPx, widths) {
    var perp = perpVector(dir);
    var half = size * 0.6;
    var back = moveAlong(tipX, tipY, dir, -size);
    var gx1 = back[0] - perp.x * half, gy1 = back[1] - perp.y * half;
    var gx2 = back[0] + perp.x * half, gy2 = back[1] + perp.y * half;
    var gradId = 'stripe-arrowhead-grad-' + (++stripedGradientCounter);
    var grad = svgEl('linearGradient', { id: gradId, x1: gx1, y1: gy1, x2: gx2, y2: gy2, gradientUnits: 'userSpaceOnUse' });
    var offset = 0; // fraction 0..1 across the full thicknessPx width
    stripes.forEach(function (s, i) {
      var w = widths[i];
      if (w <= 0) return;
      var color = LINE_COLORS[s.colorName] || LINE_COLORS.grey;
      var o1 = offset / thicknessPx, o2 = (offset + w) / thicknessPx;
      // Duplicate stops at each boundary (not one gradual stop) - a hard
      // cut between bands, not a blend.
      grad.appendChild(svgEl('stop', { offset: (o1 * 100) + '%', 'stop-color': color }));
      grad.appendChild(svgEl('stop', { offset: (o2 * 100) + '%', 'stop-color': color }));
      offset += w;
    });
    svg.appendChild(grad);
    svg.appendChild(svgEl('polygon', {
      points: arrowheadPolygonPoints(tipX, tipY, dir, size).map(function (p) { return p.join(','); }).join(' '),
      fill: 'url(#' + gradId + ')',
    }));
  }

  // Draws an entire striped Manhattan edge - every bent segment plus the
  // arrowhead - as one continuous shape per stripe, instead of separate
  // per-segment lines stitched together at the joints (independent lines,
  // even bridged by an overshoot or a patch at each joint, always left a
  // visible gap or crossing artifact right at the bend). This computes one
  // exact polygon per stripe that traces the *entire* bent centerline,
  // offset by that stripe's own perpendicular distance on each side, with
  // a mathematically exact miter at every interior corner - for a
  // 90-degree Manhattan turn, the miter point works out to simply
  // `corner + perpIncoming*offset + perpOutgoing*offset`, since the two
  // segments' perpendiculars are always exactly one x-only and one
  // y-only, so summing them can't double-count either axis.
  function drawStripedManhattanEdge(svg, segments, stripes, thicknessPx) {
    var widths = stripeWidths(stripes, thicknessPx);
    if (!widths) return false;

    var dirs = segments.map(segDir);
    var points = [{ x: segments[0].x1, y: segments[0].y1 }];
    segments.forEach(function (seg) { points.push({ x: seg.x2, y: seg.y2 }); });

    var tip = points[points.length - 1];
    var endDir = dirs[dirs.length - 1];
    var size = Math.max(8, 6 + thicknessPx * 2.2);
    var back = moveAlong(tip.x, tip.y, endDir, -size);
    // Shaft's own path stops at the arrowhead's back edge, not the true
    // tip - the (wider) triangle takes over from there.
    var shaftPoints = points.slice(0, -1).concat([{ x: back[0], y: back[1] }]);

    function offsetPoint(i, offset) {
      var p = shaftPoints[i];
      if (i === 0) {
        var perp0 = perpVector(dirs[0]);
        return { x: p.x + perp0.x * offset, y: p.y + perp0.y * offset };
      }
      if (i === shaftPoints.length - 1) {
        var perpN = perpVector(dirs[dirs.length - 1]);
        return { x: p.x + perpN.x * offset, y: p.y + perpN.y * offset };
      }
      // Interior corner - exact miter (see this function's own comment).
      var perpA = perpVector(dirs[i - 1]), perpB = perpVector(dirs[i]);
      return { x: p.x + perpA.x * offset + perpB.x * offset, y: p.y + perpA.y * offset + perpB.y * offset };
    }

    var offset = -thicknessPx / 2;
    stripes.forEach(function (s, idx) {
      var w = widths[idx];
      if (w <= 0) return;
      var lo = offset, hi = offset + w;
      var low = [], high = [];
      for (var i = 0; i < shaftPoints.length; i++) { low.push(offsetPoint(i, lo)); high.push(offsetPoint(i, hi)); }
      var poly = low.concat(high.reverse());
      svg.appendChild(svgEl('polygon', {
        points: poly.map(function (p) { return p.x + ',' + p.y; }).join(' '),
        fill: LINE_COLORS[s.colorName] || LINE_COLORS.grey,
        'shape-rendering': 'crispEdges',
      }));
      offset += w;
    });

    drawStripedArrowheadGradient(svg, tip.x, tip.y, endDir, size, stripes, thicknessPx, widths);
    return true;
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

  // Backup can be fed via a straight bypass relay (Junction, when grid_mode
  // is Connected) or synthesized by the inverter itself (islanding/fault) -
  // see DiagramCalc.backupSource. Combined with gridState's `crossed` here
  // (not in diagram-calc.js) so backupState stays a pure function of
  // Backup's own data.
  function isBackupActive(backup, grid) {
    return grid.crossed || backup.active;
  }

  var lastData = null;

  function redrawLines(calc, data) {
    var svg = byId('diagram-svg');
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    var edges = computeEdges();

    // Hoisted ahead of battery/backup (both need it for their own source
    // mixes) - grid.color/watts don't depend on anything computed below.
    var grid = calc.gridState(data);
    var gridImportW = grid.color === 'orange' ? grid.watts : 0;

    var pv = calc.pvState(data);
    drawManhattanEdge(svg, edges.pv.p0, edges.pv.dir0, edges.pv.p1, edges.pv.dir1, pv.active ? 'green' : 'grey', calc.arrowThickness(pv.watts), false, true);

    var battery = calc.batteryState(data);
    if (!battery.noBattery) {
      var eBattery = edges.battery;
      if (battery.direction === 'charge') {
        // Battery-as-sink: charging can be PV-sourced, grid-sourced (e.g.
        // the cheap-tariff overnight case), or both at once - stripe it
        // the same way Load's arrow does, instead of a flat green that
        // implies "always from PV."
        var chargeMix = [{ colorName: 'green', watts: pv.watts }, { colorName: 'orange', watts: gridImportW }];
        drawManhattanEdge(svg, eBattery.p0, eBattery.dir0, eBattery.p1, eBattery.dir1, battery.flowColor, calc.arrowThickness(battery.watts), true, true, 1, chargeMix);
      } else {
        drawManhattanEdge(svg, eBattery.p0, eBattery.dir0, eBattery.p1, eBattery.dir1, battery.flowColor, calc.arrowThickness(battery.watts), false, battery.direction !== 'none');
      }
    }

    var backup = calc.backupState(data);
    var active = isBackupActive(backup, grid);
    var backupColor = backupNodeColor(backup, active);
    var backupThickness = calc.arrowThickness(backup.watts);
    // Battery discharge and PV are the two things that can actually feed
    // Backup while inverter-fed (islanding) - same mix reused for the bus
    // edge below, since both cases draw from the same pair.
    var battDischargeW = (!battery.noBattery && battery.direction === 'discharge') ? battery.watts : 0;
    var sourceMix = [{ colorName: 'yellow', watts: battDischargeW }, { colorName: 'green', watts: pv.watts }];
    if (calc.backupSource(data) === 'junction') {
      // Grid-bypass: Backup taps the exact same grid-side line the Bus/
      // Grid edges read below, so it follows the same rule they do -
      // grid.color as the flat color (orange import / grey idle), or the
      // PV+battery sourceMix when grid.color is green (net export) - not
      // a separately-computed status color, which only reflects the
      // node's own alert/idle state (fine for setNodeColor, wrong for an
      // arrow - a status color isn't a flow color).
      var eb = edges.backupBypass;
      drawManhattanEdge(svg, eb.p0, eb.dir0, eb.p1, eb.dir1, grid.color, backupThickness, false, true, 1, grid.color === 'green' ? sourceMix : null);
    } else {
      var ei = edges.backupIslanding;
      drawManhattanEdge(svg, ei.p0, ei.dir0, ei.p1, ei.dir1, backupColor, backupThickness, false, true, 1, active ? sourceMix : null);
    }

    // The Inverter<->Junction ("bus") edge used to be approximated as
    // arrowThickness(load) with direction hardcoded to "inverter exports" -
    // wrong whenever the inverter is a net importer (e.g. charging the
    // battery from grid power), and a vanishing arrow whenever load
    // happens to be 0 even though real power is crossing the bus (e.g. all
    // surplus going to grid export/backup/battery-charge, none via Load).
    // Fixed with Kirchhoff's law at the Junction node: it only has three
    // edges (bus, grid, load), so whatever the bus brings in must equal
    // what grid and load take out - bus = load - meterSigned. (pgrid/
    // pgrid2/pgrid3 measure the inverter's grid-tie AC port specifically,
    // which is near-zero during both off-grid islanding and grid-bypass -
    // exactly when Backup is fed via one of its two other paths - so
    // they're the wrong signal for this edge.)
    var load = calc.loadState(data);
    var meterSigned = grid.color === 'orange' ? grid.watts : -grid.watts;
    var netBus = load.watts - meterSigned;
    var busThickness = grid.crossed ? 0 : calc.arrowThickness(netBus);
    // When the inverter is genuinely exporting onto the bus (netBus>=0),
    // that flow is exactly the PV+battery-discharge mix - stripe it, same
    // as Backup's inverter-fed case above. When it's a net importer
    // instead (e.g. charging battery from grid), the bus is grid-sourced,
    // a single color - orange, consistent with orange meaning grid-import
    // everywhere else in this diagram.
    var eBus = edges.bus;
    drawManhattanEdge(svg, eBus.p0, eBus.dir0, eBus.p1, eBus.dir1,
      grid.crossed ? 'red' : (netBus < 0 ? 'orange' : 'grey'), grid.crossed ? 0 : busThickness, netBus < 0, true, grid.crossed ? 0.5 : 1,
      (!grid.crossed && netBus >= 0) ? sourceMix : null);

    // Same mix again for Grid's edge, only when actually exporting - an
    // import is already single-source (grid itself), correct as a plain
    // orange line, no striping needed.
    var gridThickness = calc.arrowThickness(grid.watts);
    var eGrid = edges.grid;
    drawManhattanEdge(svg, eGrid.p0, eGrid.dir0, eGrid.p1, eGrid.dir1,
      grid.color, gridThickness, grid.color === 'orange', grid.directionKnown, 1,
      grid.color === 'green' ? sourceMix : null);

    // Load is the one sink that can genuinely draw from all three sources
    // at once (battery discharge, PV, grid import), and the stripe order
    // matches the order the boxes actually sit in on screen (Battery left,
    // PV/Inverter center, Grid right).
    var loadThickness = calc.arrowThickness(load.watts);
    var eLoad = edges.load;
    drawManhattanEdge(svg, eLoad.p0, eLoad.dir0, eLoad.p1, eLoad.dir1, load.watts > 0 ? 'orange' : 'grey', loadThickness, false, true, 1, [
      { colorName: 'yellow', watts: battDischargeW },
      { colorName: 'green', watts: pv.watts },
      { colorName: 'orange', watts: gridImportW },
    ]);

    if (grid.crossed) drawCross(svg, { x1: eBus.p0.x, y1: eBus.p0.y, x2: eBus.p1.x, y2: eBus.p1.y });

    setNodeColor('node-backup', backupColor);
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
    // Inverter is now an icon-only square (see #node-inverter in
    // diagram.css) - its status label lives in this line above the
    // diagram instead, colored to match the node's own status color.
    byId('inverter-status-line-text').textContent = inverter.label;
    byId('inverter-status-line-text').style.color = LINE_COLORS[inverter.color] || '';
    setNodeColor('node-inverter', inverter.color);
    byId('inverter-temp-air').textContent = calc.toNumber(data.temperature_air);
    byId('inverter-temp').textContent = calc.toNumber(data.temperature);

    var backup = calc.backupState(data);
    var grid = calc.gridState(data);
    byId('backup-watts').textContent = fmtW(backup.watts);
    // node color is finished by redrawLines() (backupColor there also
    // covers the phase-overload case) - avoid computing/setting it twice.
    ['backup-i1', 'backup-i2', 'backup-i3'].forEach(function (id, i) {
      byId(id).textContent = backup.phaseCurrents[i].toFixed(1);
      byId(id.replace('backup-i', 'backup-line')).classList.toggle('diagram-alert', backup.phaseAlerts[i]);
    });

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
    if (window.BACKUP_ACTIVE_THRESHOLD_W !== undefined) {
      window.DiagramCalc.setBackupActiveThreshold(window.BACKUP_ACTIVE_THRESHOLD_W);
    }
    initToggles();
    eventSource.addEventListener('message', function (e) {
      render(JSON.parse(e.data));
    });
    window.addEventListener('resize', function () {
      if (lastData) redrawLines(window.DiagramCalc, lastData);
    });
  });
})();
