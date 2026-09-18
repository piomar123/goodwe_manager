const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  aggregateSolcastHourly,
  aggregateSolcastActualsHourly,
  buildForecastTableRows,
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

test('aggregateSolcastActualsHourly sums each hour\'s two 30-minute periods', () => {
  const periods = [
    { time: '07:00', kwh: 1.0 },
    { time: '07:30', kwh: 0.5 },
    { time: '08:00', kwh: 2.0 },
  ];
  const result = aggregateSolcastActualsHourly(periods);
  assert.deepEqual(result, [
    { time: '07:00', kwh: 1.5 },
    { time: '08:00', kwh: 2.0 },
  ]);
});

test('aggregateSolcastActualsHourly treats a missing half-hour as zero', () => {
  const periods = [{ time: '07:30', kwh: 0.5 }];
  const result = aggregateSolcastActualsHourly(periods);
  assert.deepEqual(result, [{ time: '07:00', kwh: 0.5 }]);
});

test('aggregateSolcastActualsHourly returns hours in ascending order', () => {
  const periods = [
    { time: '09:00', kwh: 1 },
    { time: '07:00', kwh: 1 },
  ];
  const result = aggregateSolcastActualsHourly(periods);
  assert.deepEqual(result.map(r => r.time), ['07:00', '09:00']);
});

function baseTableArgs(overrides) {
  return Object.assign({
    hourLabels: ['07:00', '08:00', '09:00'],
    meteosourceByHour: { '07:00': 1, '08:00': 2, '09:00': 3 },
    solcastByHour: {
      '07:00': { c10: 0.5, c50: 1, c90: 1.5 },
      '08:00': { c10: 1, c50: 2, c90: 3 },
      '09:00': { c10: 1.5, c50: 3, c90: 4.5 },
    },
    solcastActualsByHour: {},
    actualByHour: {},
    currentHourIndex: -1,
    partialKwh: null,
    solcastActualsAvailable: false,
    cumulative: false,
  }, overrides);
}

test('buildForecastTableRows hourly mode shows each hour\'s own value', () => {
  const rows = buildForecastTableRows(baseTableArgs({ actualByHour: { '07:00': 0.9 } }));
  assert.equal(rows[0].Meteosource, '1.00');
  assert.equal(rows[0].Solcast, '1.00 (0.50–1.50)');
  assert.equal(rows[0].Actual, '0.90');
  assert.equal(rows[1].Actual, '—'); // no measured data for 08:00
});

test('buildForecastTableRows cumulative mode sums Meteosource, Solcast (each percentile independently), and Actual', () => {
  const rows = buildForecastTableRows(baseTableArgs({
    actualByHour: { '07:00': 0.9, '08:00': 1.8 },
    cumulative: true,
  }));
  assert.equal(rows[1].Meteosource, '3.00'); // 1 + 2
  assert.equal(rows[1].Solcast, '3.00 (1.50–4.50)'); // c50: 1+2, c10: 0.5+1, c90: 1.5+3
  assert.equal(rows[1].Actual, '2.70'); // 0.9 + 1.8
  assert.equal(rows[2].Meteosource, '6.00'); // 1 + 2 + 3
});

test('buildForecastTableRows skips a missing hour from the running total instead of resetting it', () => {
  const rows = buildForecastTableRows(baseTableArgs({
    meteosourceByHour: { '07:00': 1, '09:00': 3 }, // 08:00 missing
    cumulative: true,
  }));
  assert.equal(rows[0].Meteosource, '1.00');
  assert.equal(rows[1].Meteosource, '—');
  assert.equal(rows[2].Meteosource, '4.00'); // continues from 1, not reset by the gap
});

test('buildForecastTableRows omits Solcast Estimated Actual when unavailable', () => {
  const rows = buildForecastTableRows(baseTableArgs());
  assert.equal(rows[0]['Solcast Estimated Actual'], undefined);
});

test('buildForecastTableRows includes Solcast Estimated Actual, cumulative, when available', () => {
  const rows = buildForecastTableRows(baseTableArgs({
    solcastActualsByHour: { '07:00': { kwh: 0.4 }, '08:00': { kwh: 0.6 } },
    solcastActualsAvailable: true,
    cumulative: true,
  }));
  assert.equal(rows[0]['Solcast Estimated Actual'], '0.40');
  assert.equal(rows[1]['Solcast Estimated Actual'], '1.00');
  assert.equal(rows[2]['Solcast Estimated Actual'], '—'); // no snapshot for 09:00
});

test('buildForecastTableRows marks the current hour\'s partial reading as "so far", hourly and cumulative', () => {
  const args = baseTableArgs({
    actualByHour: { '07:00': 0.9 }, // 08:00 (currentHourIndex) not yet complete
    currentHourIndex: 1,
    partialKwh: 0.3,
  });
  const hourly = buildForecastTableRows(args);
  assert.equal(hourly[1].Actual, '0.30 (so far)');

  const cumulative = buildForecastTableRows(Object.assign({}, args, { cumulative: true }));
  assert.equal(cumulative[1].Actual, '1.20 (so far)'); // 0.9 + 0.3
});
