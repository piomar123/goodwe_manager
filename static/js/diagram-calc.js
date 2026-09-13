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

  // Direction/color come from the numeric battery_mode, never from the
  // sign of pbattery1 - verified unreliable against production data.
  // Standby/No-battery get direction 'none': Standby's ~-30W idle
  // trickle is real (BMS self-consumption), but has no defined direction,
  // and there's no trustworthy way to know which way it's flowing -
  // diagram-render.js renders 'none' as an undirected line, not a
  // fabricated arrow.
  // color here is a *status* color (can be red at the reserve floor) for
  // setNodeColor('node-battery', ...) only - flowColor (never red; see
  // below) is what any arrow/stripe fed by the battery should use instead,
  // so an alert state never gets misread as "an alert is flowing."
  function batteryState(data) {
    var watts = Math.abs(toNumber(data.pbattery1));
    var mode = toNumber(data.battery_mode);
    var dischargeLimit = toNumber(data.battery_discharge_limit);
    var direction = 'none';
    var flowColor = 'grey';
    if (mode === BATTERY_MODE.CHARGE || mode === BATTERY_MODE.TO_BE_CHARGED) {
      direction = 'charge';
      flowColor = 'green';
    } else if (mode === BATTERY_MODE.DISCHARGE || mode === BATTERY_MODE.TO_BE_DISCHARGED) {
      direction = 'discharge';
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

  // color/directionKnown come from the numeric grid_in_out/grid_mode, not
  // the sign of meter_active_power_total. directionKnown is false during
  // a Fault: red already flags the problem, and there's no reliable way
  // to assert import-vs-export direction on top of that (verified: the
  // 'red' color previously defaulted to the "export" arrow direction
  // whenever the state wasn't explicitly 'orange', which is a fabricated
  // direction during a fault, the same class of bug as the battery one
  // above - see the mockup's Junction-Grid arrow fix).
  function gridState(data) {
    var watts = Math.abs(toNumber(data.meter_active_power_total));
    var inOut = toNumber(data.grid_in_out);
    var mode = toNumber(data.grid_mode);
    var crossed = mode === GRID_MODE.FAULT || mode === GRID_MODE.NOT_CONNECTED;
    // importing/exporting are the semantic source of truth other code
    // should read (calc.gridState(data).importing, not
    // calc.gridState(data).color === 'orange') - direction is only
    // meaningful while the grid is actually connected and not faulted:
    // grid_in_out can still read Importing/Exporting during a Fault
    // (verified against production data), and is meaningless while
    // disconnected, so both crossed cases force both flags false rather
    // than trusting the raw code.
    var importing = !crossed && inOut === GRID_IN_OUT.IMPORTING;
    var exporting = !crossed && inOut === GRID_IN_OUT.EXPORTING;
    var color = mode === GRID_MODE.FAULT ? 'red' : importing ? 'orange' : exporting ? 'green' : 'grey';
    return {
      watts: watts,
      color: color,
      crossed: crossed,
      importing: importing,
      exporting: exporting,
      directionKnown: mode !== GRID_MODE.FAULT,
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
    busFlow: busFlow,
    batteryChargeGridWatts: batteryChargeGridWatts,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = DiagramCalc;
  } else {
    root.DiagramCalc = DiagramCalc;
  }
})(typeof window !== 'undefined' ? window : this);
