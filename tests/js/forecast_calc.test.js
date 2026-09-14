const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  aggregateSolcastHourly,
} = require('../../static/js/forecast-calc.js');

test('aggregateSolcastHourly sums each hour\'s two 30-minute periods', () => {
  const periods = [
    { time: '07:00', c10: 1.0, c50: 2.0, c90: 3.0 },
    { time: '07:30', c10: 0.5, c50: 1.0, c90: 1.5 },
    { time: '08:00', c10: 2.0, c50: 3.0, c90: 4.0 },
  ];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result, [
    { time: '07:00', c10: 1.5, c50: 3.0, c90: 4.5 },
    { time: '08:00', c10: 2.0, c50: 3.0, c90: 4.0 },
  ]);
});

test('aggregateSolcastHourly treats a missing half-hour as zero', () => {
  const periods = [{ time: '07:30', c10: 0.5, c50: 1.0, c90: 1.5 }];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result, [{ time: '07:00', c10: 0.5, c50: 1.0, c90: 1.5 }]);
});

test('aggregateSolcastHourly returns hours in ascending order', () => {
  const periods = [
    { time: '09:00', c10: 1, c50: 1, c90: 1 },
    { time: '07:00', c10: 1, c50: 1, c90: 1 },
  ];
  const result = aggregateSolcastHourly(periods);
  assert.deepEqual(result.map(r => r.time), ['07:00', '09:00']);
});
