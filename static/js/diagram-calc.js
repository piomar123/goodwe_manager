// Pure calculation functions for the live power-flow diagram (see
// docs/superpowers/specs/2026-09-03-live-power-flow-dashboard-design.md
// and its accompanying mockups under docs/superpowers/mockups/). No DOM
// access here on purpose - this file is loaded both in the browser (as a
// plain <script>, exposing window.DiagramCalc) and under Node for
// `node --test tests/js`, so it stays testable without adding any build
// tooling or npm dependency to the project.
(function (root) {
  'use strict';

  var BATTERY_MODE = { NO_BATTERY: 0, STANDBY: 1, DISCHARGE: 2, CHARGE: 3, TO_BE_CHARGED: 4, TO_BE_DISCHARGED: 5 };
  var GRID_IN_OUT = { IDLE: 0, EXPORTING: 1, IMPORTING: 2 };
  var GRID_MODE = { NOT_CONNECTED: 0, CONNECTED: 1, FAULT: 2 };
  var WORK_MODE_COLORS = { 0: 'grey', 1: 'green', 2: 'pink', 3: 'red', 4: 'orange', 5: 'yellow' };

  var BACKUP_CURRENT_ALERT_THRESHOLD_A = 13.5;
  // Reference power that maps to full arrow thickness. Chosen from this
  // system's observed range during the design investigation (PV up to
  // ~7.5kW, battery charge up to ~4kW), not from an inverter capacity
  // sensor - none is present in SELECTED_SENSORS.
  var FULL_THICKNESS_WATTS = 6000;
  var MIN_THICKNESS_PX = 2;
  var MAX_THICKNESS_PX = 9;

  function toNumber(value) {
    if (value === null || value === undefined || value === '') return 0;
    var n = Number(value);
    return Number.isNaN(n) ? 0 : n;
  }

  function arrowThickness(watts) {
    var w = Math.abs(toNumber(watts));
    if (w === 0) return 0;
    var ratio = Math.min(w / FULL_THICKNESS_WATTS, 1);
    return MIN_THICKNESS_PX + ratio * (MAX_THICKNESS_PX - MIN_THICKNESS_PX);
  }

  function pvState(data) {
    var watts = toNumber(data.ppv);
    return { watts: watts, active: watts > 0 };
  }

  function inverterBusState(data) {
    var watts = toNumber(data.pgrid) + toNumber(data.pgrid2) + toNumber(data.pgrid3);
    return { watts: watts, active: watts > 0 };
  }

  // Direction/color come from the numeric battery_mode, never from the
  // sign of pbattery1 - verified unreliable against production data (see
  // spec). Standby/No-battery get direction 'none': Standby's ~-30W idle
  // trickle is real (BMS self-consumption), but has no defined direction,
  // and there's no trustworthy way to know which way it's flowing -
  // diagram-render.js renders 'none' as an undirected line, not a
  // fabricated arrow.
  function batteryState(data) {
    var watts = Math.abs(toNumber(data.pbattery1));
    var mode = toNumber(data.battery_mode);
    var soc = toNumber(data.battery_soc);
    var dischargeLimit = toNumber(data.battery_discharge_limit);
    var direction = 'none';
    var color = 'grey';
    if (mode === BATTERY_MODE.CHARGE || mode === BATTERY_MODE.TO_BE_CHARGED) {
      direction = 'charge';
      color = 'green';
    } else if (mode === BATTERY_MODE.DISCHARGE || mode === BATTERY_MODE.TO_BE_DISCHARGED) {
      direction = 'discharge';
      color = 'orange';
    }
    var noBattery = mode === BATTERY_MODE.NO_BATTERY;
    // soc/battery_discharge_limit are meaningless with no battery
    // installed (both typically read 0, which would otherwise trip this
    // check every time via 0 <= 0).
    if (!noBattery && soc <= dischargeLimit) color = 'red';
    return { watts: watts, direction: direction, color: color, noBattery: noBattery };
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
    var color = 'grey';
    if (inOut === GRID_IN_OUT.EXPORTING) color = 'green';
    else if (inOut === GRID_IN_OUT.IMPORTING) color = 'orange';
    var crossed = false;
    if (mode === GRID_MODE.FAULT) {
      color = 'red';
      crossed = true;
    } else if (mode === GRID_MODE.NOT_CONNECTED) {
      color = 'grey';
      crossed = true;
    }
    return { watts: watts, color: color, crossed: crossed, directionKnown: mode !== GRID_MODE.FAULT };
  }

  function loadState(data) {
    return { watts: toNumber(data.load_ptotal) };
  }

  function backupState(data) {
    var watts = toNumber(data.backup_ptotal);
    var phaseCurrents = [toNumber(data.backup_i1), toNumber(data.backup_i2), toNumber(data.backup_i3)];
    var phaseAlerts = phaseCurrents.map(function (amps) {
      return amps >= BACKUP_CURRENT_ALERT_THRESHOLD_A;
    });
    return { watts: watts, active: watts > 0, phaseCurrents: phaseCurrents, phaseAlerts: phaseAlerts };
  }

  var DiagramCalc = {
    BATTERY_MODE: BATTERY_MODE,
    GRID_IN_OUT: GRID_IN_OUT,
    GRID_MODE: GRID_MODE,
    BACKUP_CURRENT_ALERT_THRESHOLD_A: BACKUP_CURRENT_ALERT_THRESHOLD_A,
    toNumber: toNumber,
    arrowThickness: arrowThickness,
    pvState: pvState,
    inverterBusState: inverterBusState,
    batteryState: batteryState,
    inverterState: inverterState,
    gridState: gridState,
    loadState: loadState,
    backupState: backupState,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = DiagramCalc;
  } else {
    root.DiagramCalc = DiagramCalc;
  }
})(typeof window !== 'undefined' ? window : this);
