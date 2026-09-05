const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  toNumber, arrowThickness, pvState, inverterBusState, batteryState,
  inverterState, gridState, loadState, backupState, BACKUP_CURRENT_ALERT_THRESHOLD_A,
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
  assert.ok(arrowThickness(1) >= 2);
  assert.ok(arrowThickness(30) >= 2);
});

test('arrowThickness caps at the maximum for very large power', () => {
  assert.equal(arrowThickness(50000), 9);
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
  assert.deepEqual(result, { watts: 364, direction: 'charge', color: 'green', noBattery: false });
});

test('batteryState: Discharge mode is orange, direction discharge, sign of pbattery1 ignored', () => {
  // Both a "Charge" and "Discharge" sample can carry a negative pbattery1
  // (verified against production data - see spec) - direction/color must
  // come purely from battery_mode, never the raw sign.
  const result = batteryState({ pbattery1: '-33', battery_mode: '2', battery_soc: '50', battery_discharge_limit: '10' });
  assert.equal(result.direction, 'discharge');
  assert.equal(result.color, 'orange');
  assert.equal(result.watts, 33);
});

test('batteryState: To be charged / to be discharged map to charge/discharge', () => {
  assert.equal(batteryState({ pbattery1: '0', battery_mode: '4', battery_soc: '50', battery_discharge_limit: '10' }).direction, 'charge');
  assert.equal(batteryState({ pbattery1: '0', battery_mode: '5', battery_soc: '50', battery_discharge_limit: '10' }).direction, 'discharge');
});

test('batteryState: Standby is grey with no direction, even with real nonzero wattage', () => {
  const standby = batteryState({ pbattery1: '-30', battery_mode: '1', battery_soc: '100', battery_discharge_limit: '10' });
  assert.deepEqual(standby, { watts: 30, direction: 'none', color: 'grey', noBattery: false });
});

test('batteryState: No battery is grey/none and flags noBattery', () => {
  const result = batteryState({ pbattery1: '0', battery_mode: '0', battery_soc: '0', battery_discharge_limit: '0' });
  assert.equal(result.direction, 'none');
  assert.equal(result.color, 'grey');
  assert.equal(result.noBattery, true);
});

test('batteryState: red overrides charge/discharge color at/below the SoC floor', () => {
  const result = batteryState({ pbattery1: '50', battery_mode: '2', battery_soc: '10', battery_discharge_limit: '10' });
  assert.equal(result.color, 'red');
  assert.equal(result.direction, 'discharge');
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
  assert.deepEqual(result, { watts: 500, color: 'green', crossed: false, directionKnown: true });
});

test('gridState: Importing is orange', () => {
  const result = gridState({ meter_active_power_total: '385', grid_in_out: '2', grid_mode: '1' });
  assert.deepEqual(result, { watts: 385, color: 'orange', crossed: false, directionKnown: true });
});

test('gridState: Idle is grey', () => {
  const result = gridState({ meter_active_power_total: '0', grid_in_out: '0', grid_mode: '1' });
  assert.deepEqual(result, { watts: 0, color: 'grey', crossed: false, directionKnown: true });
});

test('gridState: Fault forces red and crossed, and direction becomes unknown', () => {
  // grid_in_out could still say Importing during a fault - color/crossed
  // correctly override to red, and directionKnown must go false too
  // (this was the bug: defaulting to the "export" arrow whenever color
  // wasn't 'orange', which silently asserted export during a fault).
  const result = gridState({ meter_active_power_total: '200', grid_in_out: '2', grid_mode: '2' });
  assert.deepEqual(result, { watts: 200, color: 'red', crossed: true, directionKnown: false });
});

test('gridState: Not connected forces grey and crossed, direction stays known', () => {
  const result = gridState({ meter_active_power_total: '0', grid_in_out: '1', grid_mode: '0' });
  assert.deepEqual(result, { watts: 0, color: 'grey', crossed: true, directionKnown: true });
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
