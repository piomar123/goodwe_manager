const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  cellScale, cellExtremes, moduleBoundaries, barHeight, sampleAgeSeconds, isStale, formatAge, formatFlow,
} = require('../../static/js/bms-cells-calc.js');

test('cellScale pads the real min/max by 3 mV', () => {
  assert.deepEqual(cellScale([3308, 3312, 3310]), { lo: 3305, hi: 3315 });
});

test('cellScale keeps at least a 10 mV span for perfectly balanced cells', () => {
  assert.deepEqual(cellScale([3300, 3300]), { lo: 3295, hi: 3305 });
});

test('cellExtremes returns the first lowest and highest cell index', () => {
  assert.deepEqual(cellExtremes([3310, 3308, 3312, 3308, 3312]), { minIdx: 1, maxIdx: 2 });
});

test('moduleBoundaries marks where each further 30-cell module starts', () => {
  assert.deepEqual(moduleBoundaries(60), [30]);
  assert.deepEqual(moduleBoundaries(120), [30, 60, 90]);
  assert.deepEqual(moduleBoundaries(30), []);
});

test('barHeight maps a cell voltage into the chart height, never below 1 px', () => {
  const scale = { lo: 3300, hi: 3320 };
  assert.equal(barHeight(3310, scale, 100), 50);
  assert.equal(barHeight(3320, scale, 100), 100);
  assert.equal(barHeight(3300, scale, 100), 1);
});

test('sampleAgeSeconds and isStale use the sample epoch', () => {
  const age = sampleAgeSeconds(1000, 1000 * 1000 + 61500);
  assert.equal(age, 62);
  assert.equal(isStale(age), false);
  assert.equal(isStale(301), true);
  assert.equal(sampleAgeSeconds(1000, 999 * 1000), 0);  // clock skew never goes negative
});

test('formatAge shows seconds under 2 min, minutes after', () => {
  assert.equal(formatAge(45), '45 s');
  assert.equal(formatAge(119), '119 s');
  assert.equal(formatAge(600), '10 min');
});

test('formatFlow names the direction and the current magnitude', () => {
  assert.equal(formatFlow('charge', 15.183), 'charging 15.2 A');
  assert.equal(formatFlow('discharge', -17.81), 'discharging 17.8 A');
  assert.equal(formatFlow('idle', 0), 'idle');
  assert.equal(formatFlow('unknown (5)', 0.4), 'unknown (5) 0.4 A');
});
