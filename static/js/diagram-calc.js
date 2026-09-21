// Pure calculation functions for the live power-flow diagram (see the
// mockups under docs/superpowers/mockups/ for the visual design this was
// ported from). No DOM access here on purpose - this file is loaded both
// in the browser (as a plain <script>, exposing window.DiagramCalc) and
// under Node for `node --test tests/js`, so it stays testable without
// adding any build tooling or npm dependency to the project.
(function (root) {
  'use strict';

  var BATTERY_MODE = { NO_BATTERY: 0, STANDBY: 1, DISCHARGE: 2, CHARGE: 3, TO_BE_CHARGED: 4, TO_BE_DISCHARGED: 5 };
  var GRID_IN_OUT = { IDLE: 0, EXPORTING: 1, IMPORTING: 2 };
  var GRID_MODE = { NOT_CONNECTED: 0, CONNECTED: 1, FAULT: 2 };
  var WORK_MODE = { WAIT: 0, NORMAL_ON_GRID: 1, NORMAL_OFF_GRID: 2, FAULT: 3, FLASH: 4, CHECK: 5 };
  var WORK_MODE_COLORS = { 0: 'grey', 1: 'green', 2: 'pink', 3: 'red', 4: 'orange', 5: 'yellow' };

  var BACKUP_CURRENT_ALERT_THRESHOLD_A = 13.5;
  // Backup output above this is real usage, not CT-crosstalk noise (see
  // docs/superpowers/notes/2026-09-08-backup-threshold-investigation.md - a
  // flat constant for now, pending the adaptive-threshold formula explored
  // there). Overridable at runtime via setBackupActiveThreshold, since the
  // real value comes from a server-side env var (main.py's
  // BACKUP_ACTIVE_THRESHOLD_W) injected into the page after this file loads.
  var BACKUP_ACTIVE_THRESHOLD_W = 35;
  // Reference power that maps to full arrow thickness. Chosen from this
  // system's observed range during the design investigation (PV up to
  // ~7.5kW, battery charge up to ~4kW), not from an inverter capacity
  // sensor - none is present in SELECTED_SENSORS.
  var FULL_THICKNESS_WATTS = 6000;
  var MIN_THICKNESS_PX = 1;
  var MAX_THICKNESS_PX = 12;

  function toNumber(value) {
    if (value === null || value === undefined || value === '') return 0;
    var n = Number(value);
    return Number.isNaN(n) ? 0 : n;
  }

  function setBackupActiveThreshold(watts) {
    BACKUP_ACTIVE_THRESHOLD_W = toNumber(watts) || 0;
  }

  // Square root sits between a linear scale (small real-world wattages all
  // look equally hair-thin next to multi-kW ones) and a log scale (over-
  // compresses the high end) - see the mockup's "ugly thickness
  // quantization" checklist item. Math.max(MIN, ratio*MAX) rather than
  // MIN + ratio*(MAX-MIN): the curve already passes through the origin, so
  // floor-clamping it at MIN keeps every bucket's watt-range evenly scaled;
  // an additive MIN offset instead squeezed the first real bucket's range
  // to roughly half its neighbors'.
  function arrowThickness(watts) {
    var w = Math.abs(toNumber(watts));
    if (w === 0) return 0;
    var ratio = Math.min(Math.sqrt(w) / Math.sqrt(FULL_THICKNESS_WATTS), 1);
    return Math.max(MIN_THICKNESS_PX, ratio * MAX_THICKNESS_PX);
  }

  function pvState(data) {
    var watts = toNumber(data.ppv);
    return { watts: watts, active: watts > 0 };
  }

  // Not used by the production render any more - diagram-render.js derives
  // the Inverter<->Junction bus edge via Kirchhoff's law instead (bus =
  // load - meterSigned), since pgrid/pgrid2/pgrid3 read near-zero during
  // both off-grid islanding and grid-bypass. Kept for the mockup, which
  // still uses it standalone; its still-passing tests only cover this
  // function in isolation, not whether the shipped diagram is correct.
  function inverterBusState(data) {
    var watts = toNumber(data.pgrid) + toNumber(data.pgrid2) + toNumber(data.pgrid3);
    return { watts: watts, active: watts > 0 };
  }

  // color/flowColor (the *status*) come from the numeric battery_mode -
  // direction comes from pbattery1's own sign instead, decoupled from mode.
  // Verified against 90 days of production data: ~19% of all Discharge-mode
  // samples (34% within the +/-200W noise band around 0) actually read
  // negative (charging-direction) even though mode still says Discharge -
  // a mode-only direction misrepresents the real flow on a large fraction
  // of exactly these near-zero samples, which is when direction is visually
  // most noticeable (a thin, easy-to-stare-at arrow). Standby's ~-30W BMS
  // self-consumption trickle is real and now shows as a (tiny) charge
  // direction instead of being suppressed to 'none' - that's more accurate,
  // not a regression: it really is flowing that way.
  // color here is a *status* color (can be red at the reserve floor) for
  // setNodeColor('node-battery', ...) only - flowColor (never red; see
  // below) is what any arrow/stripe fed by the battery should use instead,
  // so an alert state never gets misread as "an alert is flowing."
  function batteryState(data) {
    var rawWatts = toNumber(data.pbattery1);
    var watts = Math.abs(rawWatts);
    var mode = toNumber(data.battery_mode);
    var dischargeLimit = toNumber(data.battery_discharge_limit);
    var direction = rawWatts > 0 ? 'discharge' : rawWatts < 0 ? 'charge' : 'none';
    var flowColor = 'grey';
    if (mode === BATTERY_MODE.CHARGE || mode === BATTERY_MODE.TO_BE_CHARGED) {
      flowColor = 'green';
    } else if (mode === BATTERY_MODE.DISCHARGE || mode === BATTERY_MODE.TO_BE_DISCHARGED) {
      // Yellow, not orange - orange already means grid-import throughout
      // this diagram, so a battery-discharge stripe sitting next to a
      // grid-import stripe in the same arrow (e.g. Load's) would be
      // indistinguishable otherwise.
      flowColor = 'yellow';
    }
    var noBattery = mode === BATTERY_MODE.NO_BATTERY;
    // The reserve floor is hit when battery_discharge_limit (amperes)
    // itself reads 0A, not by comparing it against battery_soc (a %) - see
    // docs/superpowers/notes/2026-09-08-backup-threshold-investigation.md.
    // The old `soc <= dischargeLimit` check was a units mismatch: it both
    // missed the real case (soc:10, dischargeLimit:0 never trips 10<=0)
    // and false-positived on unrelated ones (soc:11, dischargeLimit:25
    // trips 11<=25 despite the floor not being hit at all). soc/
    // dischargeLimit are meaningless with no battery installed (both
    // typically read 0), so this only applies when a battery is present.
    var color = (!noBattery && dischargeLimit === 0) ? 'red' : flowColor;
    return { watts: watts, direction: direction, color: color, flowColor: flowColor, noBattery: noBattery };
  }

  function inverterState(data) {
    var mode = toNumber(data.work_mode);
    return {
      color: WORK_MODE_COLORS.hasOwnProperty(mode) ? WORK_MODE_COLORS[mode] : 'grey',
      label: data.work_mode_label || '',
    };
  }

  // color (the *status*) comes from the numeric grid_in_out/grid_mode -
  // the arrow's `reversed` direction comes from meter_active_power_total's
  // own sign instead (positive = exporting, negative = importing - verified
  // against 90 days of production data at |meter_active_power_total| >
  // 500W: sign disagrees with grid_in_out's label only 0.16% of Exporting
  // samples and 1.35% of Importing ones, i.e. the meter's sign is reliable
  // enough to drive the arrow directly, same rationale as batteryState()).
  // directionKnown is false during a Fault: red already flags the problem,
  // and there's no reliable way to assert import-vs-export direction on
  // top of that (verified: the 'red' color previously defaulted to the
  // "export" arrow direction whenever the state wasn't explicitly 'orange',
  // which is a fabricated direction during a fault, the same class of bug
  // fixed for the battery arrow above - see the mockup's Junction-Grid
  // arrow fix).
  //
  // directionKnown stays true during NOT_CONNECTED (deliberate, since
  // PR #9) even though crossed/importing/exporting are all forced false
  // for that state below - confirmed against the full production history
  // that NOT_CONNECTED has never actually occurred on this install, see
  // docs/superpowers/notes/2026-09-13-grid-not-connected-investigation.md.
  function gridState(data) {
    var raw = toNumber(data.meter_active_power_total);
    var watts = Math.abs(raw);
    var inOut = toNumber(data.grid_in_out);
    var mode = toNumber(data.grid_mode);
    var crossed = mode === GRID_MODE.FAULT || mode === GRID_MODE.NOT_CONNECTED;
    // importing/exporting are the semantic *status* other code should read
    // (calc.gridState(data).importing, not calc.gridState(data).color ===
    // 'orange') - meaningful only while the grid is actually connected and
    // not faulted: grid_in_out can still read Importing/Exporting during a
    // Fault (verified against production data), and is meaningless while
    // disconnected, so both crossed cases force both flags false rather
    // than trusting the raw code.
    var importing = !crossed && inOut === GRID_IN_OUT.IMPORTING;
    var exporting = !crossed && inOut === GRID_IN_OUT.EXPORTING;
    var color = mode === GRID_MODE.FAULT ? 'red' : importing ? 'orange' : exporting ? 'green' : 'grey';
    var directionKnown = mode !== GRID_MODE.FAULT;
    return {
      watts: watts,
      color: color,
      crossed: crossed,
      importing: importing,
      exporting: exporting,
      // Gated on !crossed, not directionKnown - directionKnown deliberately
      // stays true during NOT_CONNECTED for historical reasons (see the
      // comment above), but the meter's sign is just as meaningless while
      // disconnected as grid_in_out is, so reversed must use the same gate
      // as importing/exporting above, not directionKnown's looser one.
      reversed: !crossed && raw < 0,
      directionKnown: directionKnown,
    };
  }

  function loadState(data) {
    return { watts: toNumber(data.load_ptotal) };
  }

  // active is threshold-based (BACKUP_ACTIVE_THRESHOLD_W), not just
  // watts > 0 - see the backup-threshold investigation note. This doesn't
  // account for the grid-fault/off-grid case (backup reads real even at
  // low wattage there) - callers combine this with gridState's `crossed`
  // themselves, same as the mockup's backupActive() did, so this stays a
  // pure function of backup's own data.
  function backupState(data) {
    var watts = toNumber(data.backup_ptotal);
    var phaseCurrents = [toNumber(data.backup_i1), toNumber(data.backup_i2), toNumber(data.backup_i3)];
    var phaseAlerts = phaseCurrents.map(function (amps) {
      return amps >= BACKUP_CURRENT_ALERT_THRESHOLD_A;
    });
    return { watts: watts, active: watts > BACKUP_ACTIVE_THRESHOLD_W, phaseCurrents: phaseCurrents, phaseAlerts: phaseAlerts };
  }

  // Backup can be fed two different ways depending on the relay matrix's
  // position: grid-bypass whenever grid_mode reads Connected (relay ties
  // Backup straight to grid, skipping the inverter), or inverter-fed
  // otherwise (islanding/fault - the inverter synthesizes Backup's output
  // itself from PV/battery). Confirmed against real history data
  // (2026-08-20 fault event) - see
  // docs/superpowers/notes/2026-09-08-backup-threshold-investigation.md.
  function backupSource(data) {
    return toNumber(data.grid_mode) === GRID_MODE.CONNECTED ? 'junction' : 'inverter';
  }

  // The bus edge (Inverter->Junction) and the inverter-fed Backup edge
  // both leave Inverter's bottom border, and the islanding edge then runs
  // straight down through Junction's own position before bending into
  // Backup - without an offset between the two exit points, they draw on
  // exactly the same pixels, making an inverter-fed Backup visually
  // indistinguishable from a grid-bypassed one (confirmed against a real
  // Pi sample, 2026-09-13 19:57:52: grid_mode Fault/work_mode Off-Grid,
  // load 725W, backup 726W, battery discharging 700W - the diagram drew
  // Backup's flow as if it came from Junction even though grid_mode
  // wasn't Connected). Ported from the v5 mockup's BACKUP_ISLANDING_SHIFT
  // (docs/superpowers/mockups/live-power-flow-dashboard-mockup-v5.html),
  // which offsets the bus's exit point right and the islanding edge's own
  // exit point left by this same amount, and shifts Junction/Load right
  // by it too, so the two lines run alongside each other instead of on
  // top of each other - see diagram-render.js's computeEdges/redrawLines,
  // which apply the actual pixel offsets (DOM geometry, not testable
  // here).
  var BACKUP_ISLANDING_SHIFT_PX = 10;

  function backupIslandingShiftPx(data) {
    return backupSource(data) === 'inverter' ? BACKUP_ISLANDING_SHIFT_PX : 0;
  }

  // Kirchhoff's law at the Junction node: whatever the Inverter->Junction
  // ("bus") edge brings in must equal what Junction's other edges take
  // out. Junction has *four* edges, not three, whenever Backup is
  // grid-bypass-fed (bus, grid, load, backupBypass) - omitting
  // backupBypassW here previously under-reported the bus's real flow by
  // Backup's own wattage whenever grid-bypass was active. Extracted as a
  // pure function (rather than left inline in diagram-render.js, which
  // only draws the result) specifically so it's covered by a plain
  // node --test regression test - this is arithmetic, not DOM.
  function busFlow(data) {
    var grid = gridState(data);
    var load = loadState(data);
    var backup = backupState(data);
    var backupBypassW = backupSource(data) === 'junction' ? backup.watts : 0;
    var meterSigned = grid.importing ? grid.watts : -grid.watts;
    return { netBus: load.watts + backupBypassW - meterSigned, backupBypassW: backupBypassW };
  }

  // Grid power can only ever reach the battery by first flowing backward
  // over the bus edge (netBus < 0) - never by being credited wholesale
  // just because the household happens to be net-importing somewhere
  // else (Load/Backup, served directly at Junction) at the same moment.
  // Verified against real history (2026-09-13 08:08:23): PV 2584W alone
  // covered a 2209W charge while 73W of unrelated grid import was
  // happening at the same time - crediting that 73W to the charge arrow
  // painted a grid stripe on what was actually a 100%-PV charge.
  // Takes the already-computed netBus (from busFlow(data).netBus) rather
  // than data itself, so callers that also need netBus for the bus edge
  // (diagram-render.js does) don't re-derive gridState/loadState/
  // backupState a second time for the same immutable data.
  function batteryChargeGridWatts(netBus) {
    return Math.max(0, -netBus);
  }

  // backup.active (the wattage threshold) only distinguishes real usage
  // from CT-crosstalk noise while genuinely Normal (On-Grid) - it was only
  // ever calibrated against samples in that mode (see
  // docs/superpowers/notes/2026-09-08-backup-threshold-investigation.md).
  // grid.crossed (Fault/Not-connected) already forces active regardless of
  // wattage, but that alone misses Check Mode: grid_mode still reads
  // Connected there (the bypass relay ties Backup straight to grid - see
  // backupSource's own comment), so grid.crossed is false, yet Check Mode
  // is just as "not normal" as Fault/Off-Grid for the threshold's purposes.
  function isBackupActive(backup, grid, data) {
    if (grid.crossed) return true;
    var normalOnGrid = toNumber(data.work_mode) === WORK_MODE.NORMAL_ON_GRID;
    return normalOnGrid ? backup.active : true;
  }

  // Node status color - a real phase overload always turns the Backup
  // node red as an alert flag. The arrow itself deliberately never uses
  // this (see backupArrowFallbackColor): it represents the physical
  // source mix, not alarm status.
  function backupNodeColor(backup, isBackupActive) {
    return backup.phaseAlerts.some(Boolean) ? 'red' : (isBackupActive ? 'orange' : 'grey');
  }

  // Backup arrow's flat fallback color, used only when there's no source
  // mix to stripe (inactive/idle). Never red: unlike the node, the arrow
  // communicates what's flowing, not alarm status.
  function backupArrowFallbackColor(isBackupActive) {
    return isBackupActive ? 'orange' : 'grey';
  }

  // Full per-edge draw decisions (color/thickness/direction/opacity/
  // stripes) for every arrow in the diagram - previously computed inline
  // in diagram-render.js's redrawLines(), intermixed with the actual SVG
  // drawing calls, which meant none of it could be tested without a
  // browser (only the underlying netBus/mix arithmetic was covered).
  // Pulled out here as a single pure function of `data` so every arrow's
  // exact decision is covered by node --test - diagram-render.js now
  // just maps this onto computeEdges()'s DOM geometry and calls
  // drawManhattanEdge. thicknessPx already has arrowThickness applied
  // (including edge-specific overrides, e.g. the bus edge forcing 0
  // while grid.crossed), so callers don't need arrowThickness at all.
  function edgeStates(data) {
    var grid = gridState(data);
    var gridImportW = grid.importing ? grid.watts : 0;
    var load = loadState(data);
    var pv = pvState(data);
    var battery = batteryState(data);
    var backup = backupState(data);
    var active = isBackupActive(backup, grid, data);
    var netBus = busFlow(data).netBus;

    // See fullSourceMix/inverterOutputMix in the old redrawLines() for
    // the full "why two mixes, not one" writeup: fullSourceMix covers
    // anything tied to the shared grid line at Junction (Load, Backup's
    // grid-bypass, Grid's own export), inverterOutputMix covers the
    // inverter's own output (the bus edge, Backup's inverter-fed path) -
    // never grid-sourced, since grid reaches Junction via its own edge.
    var battDischargeW = (!battery.noBattery && battery.direction === 'discharge') ? battery.watts : 0;
    var inverterOutputMix = [
      { colorName: 'yellow', watts: battDischargeW },
      { colorName: 'green', watts: pv.watts },
    ];
    var fullSourceMix = [
      { colorName: 'yellow', watts: battDischargeW },
      { colorName: 'green', watts: pv.watts },
      { colorName: 'orange', watts: gridImportW },
    ];

    var pvEdge = { colorName: pv.active ? 'green' : 'grey', thicknessPx: arrowThickness(pv.watts), reversed: false, directionKnown: true, opacity: 1, stripes: null };

    var batteryEdge = null;
    if (!battery.noBattery) {
      if (battery.direction === 'charge') {
        // The grid stripe is capped to -netBus (the bus edge actually
        // running backward), not the household's whole gridImportW - see
        // batteryChargeGridWatts.
        var chargeMix = [{ colorName: 'green', watts: pv.watts }, { colorName: 'orange', watts: batteryChargeGridWatts(netBus) }];
        batteryEdge = { colorName: battery.flowColor, thicknessPx: arrowThickness(battery.watts), reversed: true, directionKnown: true, opacity: 1, stripes: chargeMix };
      } else {
        batteryEdge = { colorName: battery.flowColor, thicknessPx: arrowThickness(battery.watts), reversed: false, directionKnown: battery.direction !== 'none', opacity: 1, stripes: null };
      }
    }

    var backupIsJunction = backupSource(data) === 'junction';
    var backupEdge = {
      isJunction: backupIsJunction,
      nodeColor: backupNodeColor(backup, active),
      colorName: backupArrowFallbackColor(active),
      thicknessPx: arrowThickness(backup.watts),
      reversed: false,
      directionKnown: true,
      opacity: 1,
      stripes: active ? (backupIsJunction ? fullSourceMix : inverterOutputMix) : null,
    };

    var busEdge = {
      colorName: grid.crossed ? 'red' : (netBus < 0 ? 'orange' : 'grey'),
      thicknessPx: grid.crossed ? 0 : arrowThickness(netBus),
      reversed: netBus < 0,
      directionKnown: true,
      opacity: grid.crossed ? 0.5 : 1,
      stripes: (!grid.crossed && netBus >= 0) ? inverterOutputMix : null,
      crossed: grid.crossed,
    };

    var gridEdge = {
      colorName: grid.color,
      thicknessPx: arrowThickness(grid.watts),
      // Arrow direction from grid.reversed (the meter's own sign), not
      // grid.importing (the status flag used for colorName/stripes above) -
      // see gridState()'s own comment for why these are deliberately
      // decoupled now.
      reversed: grid.reversed,
      directionKnown: grid.directionKnown,
      opacity: 1,
      stripes: grid.exporting ? fullSourceMix : null,
    };

    var loadEdge = {
      colorName: load.watts > 0 ? 'orange' : 'grey',
      thicknessPx: arrowThickness(load.watts),
      reversed: false,
      directionKnown: true,
      opacity: 1,
      stripes: fullSourceMix,
    };

    return { pv: pvEdge, battery: batteryEdge, backup: backupEdge, bus: busEdge, grid: gridEdge, load: loadEdge };
  }

  var DiagramCalc = {
    BATTERY_MODE: BATTERY_MODE,
    GRID_IN_OUT: GRID_IN_OUT,
    GRID_MODE: GRID_MODE,
    WORK_MODE: WORK_MODE,
    BACKUP_CURRENT_ALERT_THRESHOLD_A: BACKUP_CURRENT_ALERT_THRESHOLD_A,
    toNumber: toNumber,
    setBackupActiveThreshold: setBackupActiveThreshold,
    arrowThickness: arrowThickness,
    pvState: pvState,
    inverterBusState: inverterBusState,
    batteryState: batteryState,
    inverterState: inverterState,
    gridState: gridState,
    loadState: loadState,
    backupState: backupState,
    backupSource: backupSource,
    BACKUP_ISLANDING_SHIFT_PX: BACKUP_ISLANDING_SHIFT_PX,
    backupIslandingShiftPx: backupIslandingShiftPx,
    busFlow: busFlow,
    batteryChargeGridWatts: batteryChargeGridWatts,
    isBackupActive: isBackupActive,
    backupNodeColor: backupNodeColor,
    backupArrowFallbackColor: backupArrowFallbackColor,
    edgeStates: edgeStates,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = DiagramCalc;
  } else {
    root.DiagramCalc = DiagramCalc;
  }
})(typeof window !== 'undefined' ? window : this);
