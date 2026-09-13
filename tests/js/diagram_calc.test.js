const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  toNumber, arrowThickness, pvState, inverterBusState, batteryState,
  inverterState, gridState, loadState, backupState, backupSource,
  setBackupActiveThreshold, BACKUP_CURRENT_ALERT_THRESHOLD_A,
  busFlow, batteryChargeGridWatts, isBackupActive, backupNodeColor,
  backupArrowFallbackColor, edgeStates, backupIslandingShiftPx,
  BACKUP_ISLANDING_SHIFT_PX,
} = require('../../static/js/diagram-calc.js');

test('toNumber parses numeric strings', () => {
  assert.equal(toNumber('123.4'), 123.4);
});

test('toNumber treats null/undefined/empty as 0', () => {
  assert.equal(toNumber(null), 0);
  assert.equal(toNumber(undefined), 0);
  assert.equal(toNumber(''), 0);
});

test('toNumber treats non-numeric strings as 0', () => {
  assert.equal(toNumber('not-a-number'), 0);
});

test('arrowThickness returns 0 for exactly 0W', () => {
  assert.equal(arrowThickness(0), 0);
});

test('arrowThickness never goes below the minimum floor for nonzero power', () => {
  assert.ok(arrowThickness(1) >= 1);
  assert.ok(arrowThickness(30) >= 1);
});

test('arrowThickness caps at the maximum for very large power', () => {
  assert.equal(arrowThickness(50000), 12);
});

test('arrowThickness scales roughly linearly between the floor and cap', () => {
  const half = arrowThickness(3000); // half of FULL_THICKNESS_WATTS (6000)
  assert.ok(half > arrowThickness(500) && half < arrowThickness(6000));
});

test('pvState reports combined PV watts and active flag', () => {
  assert.deepEqual(pvState({ ppv: '1234' }), { watts: 1234, active: true });
});

test('pvState is inactive at 0W', () => {
  assert.deepEqual(pvState({ ppv: '0' }), { watts: 0, active: false });
});

test('inverterBusState sums all three grid-port phases', () => {
  const result = inverterBusState({ pgrid: '100', pgrid2: '110', pgrid3: '90' });
  assert.equal(result.watts, 300);
  assert.equal(result.active, true);
});

test('inverterBusState is inactive when the sum is 0', () => {
  const result = inverterBusState({ pgrid: '0', pgrid2: '0', pgrid3: '0' });
  assert.equal(result.active, false);
});

test('batteryState: Charge mode is green, direction charge, magnitude from abs(pbattery1)', () => {
  const result = batteryState({ pbattery1: '-364', battery_mode: '3', battery_soc: '52', battery_discharge_limit: '10' });
  assert.deepEqual(result, { watts: 364, direction: 'charge', color: 'green', flowColor: 'green', noBattery: false });
});

test('batteryState: Discharge mode is yellow (not orange - orange means grid-import elsewhere), direction discharge, sign of pbattery1 ignored', () => {
  // Both a "Charge" and "Discharge" sample can carry a negative pbattery1
  // (verified against production data - see spec) - direction/color must
  // come purely from battery_mode, never the raw sign.
  const result = batteryState({ pbattery1: '-33', battery_mode: '2', battery_soc: '50', battery_discharge_limit: '10' });
  assert.equal(result.direction, 'discharge');
  assert.equal(result.color, 'yellow');
  assert.equal(result.flowColor, 'yellow');
  assert.equal(result.watts, 33);
});

test('batteryState: To be charged / to be discharged map to charge/discharge', () => {
  assert.equal(batteryState({ pbattery1: '0', battery_mode: '4', battery_soc: '50', battery_discharge_limit: '10' }).direction, 'charge');
  assert.equal(batteryState({ pbattery1: '0', battery_mode: '5', battery_soc: '50', battery_discharge_limit: '10' }).direction, 'discharge');
});

test('batteryState: Standby is grey with no direction, even with real nonzero wattage', () => {
  const standby = batteryState({ pbattery1: '-30', battery_mode: '1', battery_soc: '100', battery_discharge_limit: '10' });
  assert.deepEqual(standby, { watts: 30, direction: 'none', color: 'grey', flowColor: 'grey', noBattery: false });
});

test('batteryState: No battery is grey/none and flags noBattery', () => {
  const result = batteryState({ pbattery1: '0', battery_mode: '0', battery_soc: '0', battery_discharge_limit: '0' });
  assert.equal(result.direction, 'none');
  assert.equal(result.color, 'grey');
  assert.equal(result.noBattery, true);
});

test('batteryState: red (status only) overrides discharge color when battery_discharge_limit is 0A (reserve floor hit)', () => {
  // battery_discharge_limit is amperes, not a SoC % - comparing it against
  // battery_soc was a units-mismatch bug (see
  // docs/superpowers/notes/2026-09-08-backup-threshold-investigation.md).
  // The reserve floor is hit when dischargeLimit itself reads 0A.
  const result = batteryState({ pbattery1: '50', battery_mode: '2', battery_soc: '10', battery_discharge_limit: '0' });
  assert.equal(result.color, 'red');
  assert.equal(result.flowColor, 'yellow');
  assert.equal(result.direction, 'discharge');
});

test('batteryState: a low but nonzero discharge_limit does not falsely trip red', () => {
  // Old buggy check (`soc <= dischargeLimit`) would have tripped here
  // (11 <= 25) despite the reserve floor not being hit at all.
  const result = batteryState({ pbattery1: '50', battery_mode: '2', battery_soc: '11', battery_discharge_limit: '25' });
  assert.equal(result.color, 'yellow');
});

test('inverterState maps each work_mode code to its color, keeps the label as-is', () => {
  assert.deepEqual(
    inverterState({ work_mode: '1', work_mode_label: 'Normal (On-Grid)' }),
    { color: 'green', label: 'Normal (On-Grid)' }
  );
  assert.equal(inverterState({ work_mode: '0', work_mode_label: 'Wait Mode' }).color, 'grey');
  assert.equal(inverterState({ work_mode: '2', work_mode_label: 'Normal (Off-Grid)' }).color, 'pink');
  assert.equal(inverterState({ work_mode: '3', work_mode_label: 'Fault Mode' }).color, 'red');
  assert.equal(inverterState({ work_mode: '4', work_mode_label: 'Flash Mode' }).color, 'orange');
  assert.equal(inverterState({ work_mode: '5', work_mode_label: 'Check Mode' }).color, 'yellow');
});

test('inverterState falls back to grey for an unrecognized code', () => {
  assert.equal(inverterState({ work_mode: '99', work_mode_label: 'Unknown' }).color, 'grey');
});

test('gridState: Exporting is green, magnitude from abs(meter_active_power_total)', () => {
  const result = gridState({ meter_active_power_total: '-500', grid_in_out: '1', grid_mode: '1' });
  assert.deepEqual(result, { watts: 500, color: 'green', crossed: false, importing: false, exporting: true, directionKnown: true });
});

test('gridState: Importing is orange', () => {
  const result = gridState({ meter_active_power_total: '385', grid_in_out: '2', grid_mode: '1' });
  assert.deepEqual(result, { watts: 385, color: 'orange', crossed: false, importing: true, exporting: false, directionKnown: true });
});

test('gridState: Idle is grey', () => {
  const result = gridState({ meter_active_power_total: '0', grid_in_out: '0', grid_mode: '1' });
  assert.deepEqual(result, { watts: 0, color: 'grey', crossed: false, importing: false, exporting: false, directionKnown: true });
});

test('gridState: Fault forces red and crossed, and direction becomes unknown', () => {
  // grid_in_out could still say Importing during a fault - color/crossed
  // correctly override to red, and directionKnown must go false too
  // (this was the bug: defaulting to the "export" arrow whenever color
  // wasn't 'orange', which silently asserted export during a fault).
  // importing/exporting must both go false too - a Fault makes direction
  // itself unreliable, not just the color.
  const result = gridState({ meter_active_power_total: '200', grid_in_out: '2', grid_mode: '2' });
  assert.deepEqual(result, { watts: 200, color: 'red', crossed: true, importing: false, exporting: false, directionKnown: false });
});

test('gridState: Not connected forces grey and crossed, direction stays known', () => {
  const result = gridState({ meter_active_power_total: '0', grid_in_out: '1', grid_mode: '0' });
  assert.deepEqual(result, { watts: 0, color: 'grey', crossed: true, importing: false, exporting: false, directionKnown: true });
});

test('loadState reports total load watts', () => {
  assert.deepEqual(loadState({ load_ptotal: '960' }), { watts: 960 });
});

test('backupState reports watts, active flag, and per-phase currents', () => {
  const result = backupState({ backup_ptotal: '500', backup_i1: '2.1', backup_i2: '2.2', backup_i3: '2.0' });
  assert.equal(result.watts, 500);
  assert.equal(result.active, true);
  assert.deepEqual(result.phaseCurrents, [2.1, 2.2, 2.0]);
  assert.deepEqual(result.phaseAlerts, [false, false, false]);
});

test('backupState flags a phase red at/above the 13.5A threshold', () => {
  const result = backupState({ backup_ptotal: '3000', backup_i1: '13.5', backup_i2: '13.4', backup_i3: '14.0' });
  assert.deepEqual(result.phaseAlerts, [true, false, true]);
  assert.equal(BACKUP_CURRENT_ALERT_THRESHOLD_A, 13.5);
});

test('backupState is inactive at 0W', () => {
  assert.equal(backupState({ backup_ptotal: '0', backup_i1: '0', backup_i2: '0', backup_i3: '0' }).active, false);
});

test('backupState treats output at/below the active threshold (default 35W) as noise, not real usage', () => {
  assert.equal(backupState({ backup_ptotal: '22', backup_i1: '0', backup_i2: '0', backup_i3: '0' }).active, false);
  assert.equal(backupState({ backup_ptotal: '36', backup_i1: '0', backup_i2: '0', backup_i3: '0' }).active, true);
});

test('setBackupActiveThreshold overrides the default threshold', () => {
  setBackupActiveThreshold(100);
  assert.equal(backupState({ backup_ptotal: '50', backup_i1: '0', backup_i2: '0', backup_i3: '0' }).active, false);
  setBackupActiveThreshold(35); // restore default for any other test relying on it
});

test('backupSource: grid-bypass (junction) when grid_mode is Connected, inverter-fed otherwise', () => {
  assert.equal(backupSource({ grid_mode: '1' }), 'junction');
  assert.equal(backupSource({ grid_mode: '0' }), 'inverter');
  assert.equal(backupSource({ grid_mode: '2' }), 'inverter');
});

// The bus edge and the inverter-fed Backup edge both leave Inverter's
// bottom border - without an offset between them, the islanding edge (which
// then runs straight down through Junction's position before bending into
// Backup) draws exactly on top of the bus/backupBypass edges, making an
// inverter-fed Backup visually indistinguishable from a grid-bypassed one.
// Real Pi sample confirming this state (2026-09-13 19:57:52, grid_mode
// Fault/work_mode Off-Grid): load 725W, backup 726W, battery discharging
// 700W, meter/pgrid all ~0W.
test('backupIslandingShiftPx: nonzero only while Backup is inverter-fed (islanding)', () => {
  assert.equal(backupIslandingShiftPx({ grid_mode: '1' }), 0); // grid-bypass
  assert.equal(backupIslandingShiftPx({ grid_mode: '0' }), BACKUP_ISLANDING_SHIFT_PX); // not connected
  assert.equal(backupIslandingShiftPx({ grid_mode: '2' }), BACKUP_ISLANDING_SHIFT_PX); // fault
});

test('busFlow: real Pi sample (2026-09-13 08:08:23) - PV covers the whole battery charge while grid imports for something else', () => {
  // PV 2584W, battery charging 2209W, grid importing 73W, load 435W,
  // backup 9W (grid-bypass-fed). netBus must be positive (inverter is
  // exporting onto the bus, sourced entirely by PV) - the 73W import is
  // going straight to Load/Backup at Junction, never through the
  // inverter, so it must NOT show up as a grid contribution to the
  // battery charge. This is the exact data that exposed the bug: the
  // battery-charge arrow rendered a spurious orange sliver even though
  // PV (2584W) alone exceeds the charge (2209W).
  const data = {
    meter_active_power_total: '73', grid_in_out: '2', grid_mode: '1',
    load_ptotal: '435', backup_ptotal: '9', backup_i1: '0', backup_i2: '0', backup_i3: '0',
  };
  assert.equal(busFlow(data).netBus, 435 + 9 - 73);
  assert.equal(batteryChargeGridWatts(busFlow(data).netBus), 0);
});

test('busFlow: Junction has four edges, not three - backupBypassW must count toward netBus when Backup is grid-bypass-fed', () => {
  // Old (buggy) formula was netBus = load - meterSigned, silently
  // dropping backup's own wattage whenever it's tied straight to the
  // grid line (junction-fed). With grid idle (meterSigned 0), the bus
  // must carry load AND backup's draw, not just load.
  const data = {
    meter_active_power_total: '0', grid_in_out: '0', grid_mode: '1',
    load_ptotal: '200', backup_ptotal: '100', backup_i1: '0', backup_i2: '0', backup_i3: '0',
  };
  assert.equal(busFlow(data).backupBypassW, 100);
  assert.equal(busFlow(data).netBus, 300);
});

test('busFlow: backupBypassW is 0 when Backup is inverter-fed (islanding/fault), even with real backup wattage', () => {
  const data = {
    meter_active_power_total: '0', grid_in_out: '0', grid_mode: '0',
    load_ptotal: '200', backup_ptotal: '100', backup_i1: '0', backup_i2: '0', backup_i3: '0',
  };
  assert.equal(busFlow(data).backupBypassW, 0);
  assert.equal(busFlow(data).netBus, 200);
});

test('batteryChargeGridWatts: genuine grid-charging (netBus negative) is reported, capped to the actual reversed-bus amount', () => {
  // Load 100W, grid importing 900W, no backup draw - far more import than
  // the house needs, so the surplus must be flowing backward across the
  // bus to charge the battery (the only other thing at Junction).
  const data = {
    meter_active_power_total: '900', grid_in_out: '2', grid_mode: '1',
    load_ptotal: '100', backup_ptotal: '0', backup_i1: '0', backup_i2: '0', backup_i3: '0',
  };
  assert.equal(busFlow(data).netBus, -800);
  assert.equal(batteryChargeGridWatts(busFlow(data).netBus), 800);
});

test('batteryChargeGridWatts: pure function of netBus - 0 when the bus is exporting (netBus >= 0), -netBus when reversed', () => {
  assert.equal(batteryChargeGridWatts(371), 0);
  assert.equal(batteryChargeGridWatts(0), 0);
  assert.equal(batteryChargeGridWatts(-800), 800);
});

test('WORK_MODE exposes the numeric work_mode codes, matching WORK_MODE_COLORS ordering', () => {
  const { WORK_MODE } = require('../../static/js/diagram-calc.js');
  assert.deepEqual(WORK_MODE, { WAIT: 0, NORMAL_ON_GRID: 1, NORMAL_OFF_GRID: 2, FAULT: 3, FLASH: 4, CHECK: 5 });
});

test('isBackupActive: grid.crossed forces true regardless of wattage', () => {
  const backup = { active: false };
  assert.equal(isBackupActive(backup, { crossed: true }, { work_mode: '1' }), true);
});

test('isBackupActive: gates on backup.active only in Normal (On-Grid); every other work_mode forces true', () => {
  const grid = { crossed: false };
  assert.equal(isBackupActive({ active: false }, grid, { work_mode: '1' }), false);
  assert.equal(isBackupActive({ active: true }, grid, { work_mode: '1' }), true);
  // Check Mode (5) still reads grid_mode Connected (grid.crossed false),
  // but isn't "normal" - the threshold shouldn't apply there.
  assert.equal(isBackupActive({ active: false }, grid, { work_mode: '5' }), true);
});

test('backupNodeColor: a phase alert is red regardless of active, otherwise active/inactive maps to orange/grey', () => {
  assert.equal(backupNodeColor({ phaseAlerts: [false, true, false] }, false), 'red');
  assert.equal(backupNodeColor({ phaseAlerts: [false, false, false] }, true), 'orange');
  assert.equal(backupNodeColor({ phaseAlerts: [false, false, false] }, false), 'grey');
});

test('backupArrowFallbackColor: never red, unlike backupNodeColor - just active/inactive', () => {
  assert.equal(backupArrowFallbackColor(true), 'orange');
  assert.equal(backupArrowFallbackColor(false), 'grey');
});

test('edgeStates: real Pi sample (2026-09-13 08:08:23) - battery charge and bus are 100% green, no spurious grid stripe', () => {
  // Same data as the busFlow regression test above: PV 2584W, battery
  // charging 2209W, grid importing 73W (unrelated to charging), load
  // 435W, backup 9W (grid-bypass-fed).
  const data = {
    ppv: '2584', pbattery1: '-2209', battery_mode: '3', battery_soc: '52', battery_discharge_limit: '25',
    meter_active_power_total: '73', grid_in_out: '2', grid_mode: '1',
    load_ptotal: '435', backup_ptotal: '9', backup_i1: '0.1', backup_i2: '0.1', backup_i3: '0.1',
    work_mode: '1',
  };
  const e = edgeStates(data);
  assert.deepEqual(e.battery.stripes, [{ colorName: 'green', watts: 2584 }, { colorName: 'orange', watts: 0 }]);
  assert.equal(e.bus.colorName, 'grey'); // exporting onto the bus (netBus >= 0), not grid-sourced
  assert.deepEqual(e.bus.stripes, [{ colorName: 'yellow', watts: 0 }, { colorName: 'green', watts: 2584 }]);
  assert.equal(e.grid.colorName, 'orange');
  assert.equal(e.backup.isJunction, true);
  // 9W is below the default 35W noise threshold - inactive, no stripes.
  assert.equal(e.backup.stripes, null);
  assert.equal(e.backup.colorName, 'grey');
});

test('edgeStates: genuine grid-charging (netBus negative) puts the grid stripe on both battery and bus', () => {
  // Load 100W, grid importing 900W, no PV/backup - the surplus must be
  // flowing backward across the bus to charge the battery.
  const data = {
    ppv: '0', pbattery1: '-800', battery_mode: '3', battery_soc: '50', battery_discharge_limit: '25',
    meter_active_power_total: '900', grid_in_out: '2', grid_mode: '1',
    load_ptotal: '100', backup_ptotal: '0', backup_i1: '0', backup_i2: '0', backup_i3: '0',
    work_mode: '1',
  };
  const e = edgeStates(data);
  assert.deepEqual(e.battery.stripes, [{ colorName: 'green', watts: 0 }, { colorName: 'orange', watts: 800 }]);
  assert.equal(e.bus.colorName, 'orange');
  assert.equal(e.bus.stripes, null); // single-color flat orange, not striped
});

test('edgeStates: a real phase overload never turns the Backup arrow red - only the node', () => {
  // High phase current (>= 13.5A alert threshold) with plenty of real
  // wattage (well above the 35W active threshold).
  const data = {
    ppv: '0', pbattery1: '0', battery_mode: '0', battery_soc: '0', battery_discharge_limit: '0',
    meter_active_power_total: '0', grid_in_out: '0', grid_mode: '1',
    load_ptotal: '0', backup_ptotal: '3000', backup_i1: '14.0', backup_i2: '13.0', backup_i3: '13.0',
    work_mode: '1',
  };
  const e = edgeStates(data);
  assert.equal(e.backup.nodeColor, 'red');
  assert.notEqual(e.backup.colorName, 'red');
  assert.equal(e.backup.colorName, 'orange');
});

test('edgeStates: Backup inverter-fed (islanding/fault) never stripes in a grid contribution', () => {
  const data = {
    ppv: '1000', pbattery1: '-500', battery_mode: '2', battery_soc: '50', battery_discharge_limit: '25',
    meter_active_power_total: '0', grid_in_out: '0', grid_mode: '0', // not connected
    load_ptotal: '200', backup_ptotal: '800', backup_i1: '1', backup_i2: '1', backup_i3: '1',
    work_mode: '2',
  };
  const e = edgeStates(data);
  assert.equal(e.backup.isJunction, false);
  assert.deepEqual(e.backup.stripes, [{ colorName: 'yellow', watts: 500 }, { colorName: 'green', watts: 1000 }]);
});

test('edgeStates: grid Fault forces the bus edge to a crossed, half-opacity red line with no flow', () => {
  const data = {
    ppv: '500', pbattery1: '0', battery_mode: '1', battery_soc: '50', battery_discharge_limit: '25',
    meter_active_power_total: '200', grid_in_out: '2', grid_mode: '2', // fault
    load_ptotal: '300', backup_ptotal: '0', backup_i1: '0', backup_i2: '0', backup_i3: '0',
    work_mode: '3',
  };
  const e = edgeStates(data);
  assert.equal(e.bus.colorName, 'red');
  assert.equal(e.bus.crossed, true);
  assert.equal(e.bus.thicknessPx, 0);
  assert.equal(e.bus.opacity, 0.5);
  assert.equal(e.bus.stripes, null);
});

test('edgeStates: no battery installed means no battery edge to draw', () => {
  const data = {
    ppv: '500', pbattery1: '0', battery_mode: '0', battery_soc: '0', battery_discharge_limit: '0',
    meter_active_power_total: '0', grid_in_out: '0', grid_mode: '1',
    load_ptotal: '500', backup_ptotal: '0', backup_i1: '0', backup_i2: '0', backup_i3: '0',
    work_mode: '1',
  };
  assert.equal(edgeStates(data).battery, null);
});
