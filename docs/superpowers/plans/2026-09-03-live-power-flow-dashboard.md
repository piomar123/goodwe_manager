# Live Power-Flow Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the plain-text runtime-data block at the top of `templates/index.html` with a graphical, physically-accurate power-flow diagram (PV/Battery/Inverter/Backup/Grid/Load) driven by the existing SSE stream.

**Architecture:** Pure calculation logic (parsing raw SSE sensor values into per-node color/direction/magnitude) lives in a small dependency-free JS module, unit-tested with Node's built-in test runner (`node --test`) since the project has no npm/build tooling and none should be introduced. DOM rendering and arrow drawing (via the LeaderLine CDN library) is a separate, manually-verified glue module, consistent with how the rest of `index.html`'s SSE-driven UI (freshness badges) is already untested. No backend changes.

**Tech Stack:** Vanilla JS (no build step, no framework), LeaderLine (CDN, for SVG arrows between HTML nodes), existing Bootstrap 5 CSS already loaded in `templates/index.html`, Node's built-in `node:test`/`node:assert` for unit tests (Node v26 confirmed available; no `package.json` needed).

**Spec:** `docs/superpowers/specs/2026-09-03-live-power-flow-dashboard-design.md`

## Global Constraints

- No backend, SSE payload, or database changes — every field used already exists in `SELECTED_SENSORS` (`sensors.py`) and already streams via `/listen`.
- No new npm dependency or build step — plain `<script>` tags via CDN (matching how Bootstrap and Chart.js are already loaded), matching this project's existing zero-build-tooling convention.
- Power values are always displayed in whole watts (`Math.round`), no kW switching, everywhere on the diagram.
- Arrow thickness scales linearly with `Math.abs(watts)`, with a minimum visible floor thickness — see `arrowThickness()` in Task 1.
- All color/state decisions use the **numeric** enum fields (`work_mode`, `grid_mode`, `battery_mode`, `grid_in_out`), never the `_label` strings.
- Battery direction/color comes from `battery_mode`, never from the sign of `pbattery1` (documented as unreliable near zero in the spec).
- Extra per-node details (voltages, temperatures, per-phase splits) are shown via inline expand/collapse under each node, not tooltips or a shared panel.
- The raw JSON dump below the existing `<hr>` (`#event-target`) and the SSE freshness badges (`#age-badge`/`#read-badge`/`#delivery-badge`) are untouched.

**Scope note (flag to reviewer before merging):** the current text block being replaced also shows `diagnose_result_label` and the three hourly rollup fields (`_hourly_meter_export`/`_hourly_meter_import`/`_hourly_load`). These aren't part of the new diagram (the spec's node/edge table doesn't include them) and aren't available anywhere else on this page (there's a separate `/history` page for hourly summaries, but not this exact live view). Task 2 keeps them as a small plain-text line under the diagram rather than silently dropping them — confirm this is still wanted, or tell the implementer to drop them if not.

---

## File Structure

- **Create** `static/js/diagram-calc.js` — pure functions turning a raw SSE data object into per-node/edge display state (watts, color, direction, thresholds). No DOM access. Exports via `module.exports` when running under Node (tests), and as a `window.DiagramCalc` global in the browser.
- **Create** `tests/js/diagram_calc.test.js` — Node built-in test runner tests for every function in `diagram-calc.js`.
- **Create** `static/css/diagram.css` — layout (CSS grid matching the spec's node arrangement) and node/detail styling, including the color classes referenced by `diagram-render.js`.
- **Create** `static/js/diagram-render.js` — DOM wiring: creates the LeaderLine arrows once, listens for SSE messages, updates node text/colors/arrow thickness, and handles the inline-expand toggles. Not unit tested (no DOM in the test environment) — verified manually per Task 4.
- **Modify** `templates/index.html` — replace the plain-text block (current lines 12–34) with the new diagram markup; add `<link>`/`<script>` tags for the new CSS/JS and the LeaderLine CDN script.
- **Modify** `CHANGELOG.md` — add an `Unreleased` entry describing the new dashboard.

---

### Task 1: `diagram-calc.js` pure calculation module

**Files:**
- Create: `static/js/diagram-calc.js`
- Create: `tests/js/diagram_calc.test.js`

**Interfaces:**
- Produces (consumed by Task 3's `diagram-render.js`, and by the test file):
  - `toNumber(value): number`
  - `arrowThickness(absWatts: number): number` (returns 0 for exactly 0 W, else clamped between `MIN_THICKNESS_PX` and `MAX_THICKNESS_PX`)
  - `pvState(data): { watts: number, active: boolean }`
  - `inverterBusState(data): { watts: number, active: boolean }`
  - `batteryState(data): { watts: number, direction: 'charge'|'discharge'|'none', color: 'green'|'orange'|'red'|'grey' }`
  - `inverterState(data): { color: 'grey'|'green'|'pink'|'red'|'orange'|'yellow', label: string }`
  - `gridState(data): { watts: number, color: 'grey'|'green'|'orange'|'red', crossed: boolean }`
  - `loadState(data): { watts: number }`
  - `backupState(data): { watts: number, active: boolean, phaseCurrents: number[3], phaseAlerts: boolean[3] }`
  - `BACKUP_CURRENT_ALERT_THRESHOLD_A` (constant, `13.5`)

- [ ] **Step 1: Write the failing tests for `toNumber` and `arrowThickness`**

Create `tests/js/diagram_calc.test.js`:

```js
const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  toNumber, arrowThickness,
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
  assert.equal(arrowThickness(50000), 12);
});

test('arrowThickness scales roughly linearly between the floor and cap', () => {
  const half = arrowThickness(3000); // half of FULL_THICKNESS_WATTS (6000)
  assert.ok(half > arrowThickness(500) && half < arrowThickness(6000));
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `node --test tests/js`
Expected: FAIL — `Cannot find module '../../static/js/diagram-calc.js'`

- [ ] **Step 3: Create `static/js/diagram-calc.js` with `toNumber` and `arrowThickness`**

```js
// Pure calculation functions for the live power-flow diagram (see
// docs/superpowers/specs/2026-09-03-live-power-flow-dashboard-design.md).
// No DOM access here on purpose - this file is loaded both in the browser
// (as a plain <script>, exposing window.DiagramCalc) and under Node for
// `node --test tests/js` (via module.exports), so it stays testable
// without adding any build tooling or npm dependency to the project.
(function (root) {
  'use strict';

  var BATTERY_MODE = { NO_BATTERY: 0, STANDBY: 1, DISCHARGE: 2, CHARGE: 3, TO_BE_CHARGED: 4, TO_BE_DISCHARGED: 5 };
  var GRID_IN_OUT = { IDLE: 0, EXPORTING: 1, IMPORTING: 2 };
  var GRID_MODE = { NOT_CONNECTED: 0, CONNECTED: 1, FAULT: 2 };
  var WORK_MODE_COLORS = { 0: 'grey', 1: 'green', 2: 'pink', 3: 'red', 4: 'orange', 5: 'yellow' };

  var BACKUP_CURRENT_ALERT_THRESHOLD_A = 13.5;
  // Reference power that maps to full arrow thickness. Chosen from this
  // system's observed range during the design investigation (PV up to
  // ~2.3kW, battery charge up to ~3.7kW over a 14-day sample), with
  // headroom for a full-sun day. Not derived from an inverter capacity
  // sensor - none is present in SELECTED_SENSORS.
  var FULL_THICKNESS_WATTS = 6000;
  var MIN_THICKNESS_PX = 2;
  var MAX_THICKNESS_PX = 12;

  function toNumber(value) {
    if (value === null || value === undefined || value === '') return 0;
    var n = Number(value);
    return Number.isNaN(n) ? 0 : n;
  }

  function arrowThickness(absWatts) {
    var watts = Math.abs(toNumber(absWatts));
    if (watts === 0) return 0;
    var ratio = Math.min(watts / FULL_THICKNESS_WATTS, 1);
    return MIN_THICKNESS_PX + ratio * (MAX_THICKNESS_PX - MIN_THICKNESS_PX);
  }

  var DiagramCalc = {
    BATTERY_MODE: BATTERY_MODE,
    GRID_IN_OUT: GRID_IN_OUT,
    GRID_MODE: GRID_MODE,
    BACKUP_CURRENT_ALERT_THRESHOLD_A: BACKUP_CURRENT_ALERT_THRESHOLD_A,
    toNumber: toNumber,
    arrowThickness: arrowThickness,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = DiagramCalc;
  } else {
    root.DiagramCalc = DiagramCalc;
  }
})(typeof window !== 'undefined' ? window : this);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test tests/js`
Expected: PASS (7 tests)

- [ ] **Step 5: Write the failing tests for `pvState` and `inverterBusState`**

Append to `tests/js/diagram_calc.test.js`:

```js
const { pvState, inverterBusState } = require('../../static/js/diagram-calc.js');

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
```

(Note: this step's `require` is added as a second `require` line near the top-level one from Step 1 for clarity in this plan; when writing the file, add these names to the single existing `require(...)` destructuring instead of a second `require` call.)

- [ ] **Step 6: Run the tests to verify they fail**

Run: `node --test tests/js`
Expected: FAIL — `pvState is not a function`

- [ ] **Step 7: Implement `pvState` and `inverterBusState`**

Add to `static/js/diagram-calc.js`, before the `DiagramCalc` object literal:

```js
  function pvState(data) {
    var watts = toNumber(data.ppv);
    return { watts: watts, active: watts > 0 };
  }

  function inverterBusState(data) {
    var watts = toNumber(data.pgrid) + toNumber(data.pgrid2) + toNumber(data.pgrid3);
    return { watts: watts, active: watts > 0 };
  }
```

Add `pvState: pvState, inverterBusState: inverterBusState,` to the `DiagramCalc` object literal.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `node --test tests/js`
Expected: PASS (11 tests)

- [ ] **Step 9: Write the failing tests for `batteryState`**

Append to `tests/js/diagram_calc.test.js` (add `batteryState` to the imports):

```js
test('batteryState: Charge mode is green, direction charge, magnitude from abs(pbattery1)', () => {
  const result = batteryState({ pbattery1: '-364', battery_mode: '3', battery_soc: '52', battery_discharge_limit: '10' });
  assert.deepEqual(result, { watts: 364, direction: 'charge', color: 'green' });
});

test('batteryState: Discharge mode is orange, direction discharge', () => {
  const result = batteryState({ pbattery1: '75', battery_mode: '2', battery_soc: '50', battery_discharge_limit: '10' });
  assert.deepEqual(result, { watts: 75, direction: 'discharge', color: 'orange' });
});

test('batteryState: To be charged / to be discharged map to charge/discharge', () => {
  assert.equal(batteryState({ pbattery1: '0', battery_mode: '4', battery_soc: '50', battery_discharge_limit: '10' }).direction, 'charge');
  assert.equal(batteryState({ pbattery1: '0', battery_mode: '5', battery_soc: '50', battery_discharge_limit: '10' }).direction, 'discharge');
});

test('batteryState: Standby/No battery is grey with no direction', () => {
  const standby = batteryState({ pbattery1: '-30', battery_mode: '1', battery_soc: '100', battery_discharge_limit: '10' });
  assert.deepEqual(standby, { watts: 30, direction: 'none', color: 'grey' });
});

test('batteryState: red overrides charge/discharge color at/below the SoC floor', () => {
  const result = batteryState({ pbattery1: '50', battery_mode: '2', battery_soc: '10', battery_discharge_limit: '10' });
  assert.equal(result.color, 'red');
  assert.equal(result.direction, 'discharge');
});
```

- [ ] **Step 10: Run the tests to verify they fail**

Run: `node --test tests/js`
Expected: FAIL — `batteryState is not a function`

- [ ] **Step 11: Implement `batteryState`**

Add to `static/js/diagram-calc.js`:

```js
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
    if (soc <= dischargeLimit) color = 'red';
    return { watts: watts, direction: direction, color: color };
  }
```

Add `batteryState: batteryState,` to the `DiagramCalc` object literal.

- [ ] **Step 12: Run the tests to verify they pass**

Run: `node --test tests/js`
Expected: PASS (16 tests)

- [ ] **Step 13: Write the failing tests for `inverterState`**

Append to `tests/js/diagram_calc.test.js` (add `inverterState` to the imports):

```js
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
```

- [ ] **Step 14: Run the tests to verify they fail**

Run: `node --test tests/js`
Expected: FAIL — `inverterState is not a function`

- [ ] **Step 15: Implement `inverterState`**

Add to `static/js/diagram-calc.js`:

```js
  function inverterState(data) {
    var mode = toNumber(data.work_mode);
    return {
      color: WORK_MODE_COLORS.hasOwnProperty(mode) ? WORK_MODE_COLORS[mode] : 'grey',
      label: data.work_mode_label || '',
    };
  }
```

Add `inverterState: inverterState,` to the `DiagramCalc` object literal.

- [ ] **Step 16: Run the tests to verify they pass**

Run: `node --test tests/js`
Expected: PASS (18 tests)

- [ ] **Step 17: Write the failing tests for `gridState`**

Append to `tests/js/diagram_calc.test.js` (add `gridState` to the imports):

```js
test('gridState: Exporting is green, magnitude from abs(meter_active_power_total)', () => {
  const result = gridState({ meter_active_power_total: '-500', grid_in_out: '1', grid_mode: '1' });
  assert.deepEqual(result, { watts: 500, color: 'green', crossed: false });
});

test('gridState: Importing is orange', () => {
  const result = gridState({ meter_active_power_total: '385', grid_in_out: '2', grid_mode: '1' });
  assert.deepEqual(result, { watts: 385, color: 'orange', crossed: false });
});

test('gridState: Idle is grey', () => {
  const result = gridState({ meter_active_power_total: '0', grid_in_out: '0', grid_mode: '1' });
  assert.deepEqual(result, { watts: 0, color: 'grey', crossed: false });
});

test('gridState: Fault forces red and crossed, overriding import/export color', () => {
  const result = gridState({ meter_active_power_total: '200', grid_in_out: '2', grid_mode: '2' });
  assert.deepEqual(result, { watts: 200, color: 'red', crossed: true });
});

test('gridState: Not connected forces grey and crossed', () => {
  const result = gridState({ meter_active_power_total: '0', grid_in_out: '1', grid_mode: '0' });
  assert.deepEqual(result, { watts: 0, color: 'grey', crossed: true });
});
```

- [ ] **Step 18: Run the tests to verify they fail**

Run: `node --test tests/js`
Expected: FAIL — `gridState is not a function`

- [ ] **Step 19: Implement `gridState`**

Add to `static/js/diagram-calc.js`:

```js
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
    return { watts: watts, color: color, crossed: crossed };
  }
```

Add `gridState: gridState,` to the `DiagramCalc` object literal.

- [ ] **Step 20: Run the tests to verify they pass**

Run: `node --test tests/js`
Expected: PASS (23 tests)

- [ ] **Step 21: Write the failing tests for `loadState` and `backupState`**

Append to `tests/js/diagram_calc.test.js` (add `loadState`, `backupState`, `BACKUP_CURRENT_ALERT_THRESHOLD_A` to the imports):

```js
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
});

test('backupState is inactive at 0W', () => {
  assert.equal(backupState({ backup_ptotal: '0', backup_i1: '0', backup_i2: '0', backup_i3: '0' }).active, false);
});
```

- [ ] **Step 22: Run the tests to verify they fail**

Run: `node --test tests/js`
Expected: FAIL — `loadState is not a function`

- [ ] **Step 23: Implement `loadState` and `backupState`**

Add to `static/js/diagram-calc.js`:

```js
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
```

Add `loadState: loadState, backupState: backupState,` to the `DiagramCalc` object literal.

- [ ] **Step 24: Run the full test suite to verify it passes**

Run: `node --test tests/js`
Expected: PASS (27 tests, 0 failures)

- [ ] **Step 25: Commit**

```bash
git add static/js/diagram-calc.js tests/js/diagram_calc.test.js
git commit -m "Add diagram-calc.js pure module with Node test coverage

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 2: Diagram HTML markup and CSS

**Files:**
- Modify: `templates/index.html:12-34` (the current plain-text runtime-data block)
- Create: `static/css/diagram.css`

**Interfaces:**
- Consumes: nothing (static markup only in this task; Task 3 wires it up).
- Produces (DOM element IDs that Task 3's `diagram-render.js` depends on):
  - Node containers: `#node-pv`, `#node-battery`, `#node-inverter`, `#node-junction`, `#node-backup`, `#node-grid`, `#node-load`
  - Primary-value text targets: `#pv-watts`, `#battery-watts`, `#battery-soc`, `#inverter-status`, `#backup-watts`, `#grid-watts`, `#load-watts`
  - Detail-toggle triggers: any element with `data-toggle-details="<key>"`, paired with a `#<key>-details` container (keys: `pv`, `battery`, `inverter`, `backup`, `grid`, `load`)
  - Detail text targets: `#pv-string1`, `#pv-string2`, `#battery-voltage`, `#battery-temp`, `#inverter-temp-air`, `#inverter-temp`, `#backup-i1`, `#backup-i2`, `#backup-i3`, `#grid-voltages`, `#grid-freqs`, `#load-p1`, `#load-p2`, `#load-p3`
  - CSS color classes applied by Task 3: `.diagram-node--grey/green/orange/red/pink/yellow` on `#node-inverter` and `#node-grid`
  - CSS state classes: `.diagram-details--open` (toggled on `#<key>-details`), `.diagram-alert` (toggled on the backup current spans)

- [ ] **Step 1: Create `static/css/diagram.css`**

```css
/* Live power-flow diagram (see
   docs/superpowers/specs/2026-09-03-live-power-flow-dashboard-design.md).
   Plain CSS grid, no build step - matches this project's existing
   convention of loading everything via <link>/<script> tags. */

.diagram {
  display: grid;
  grid-template-columns: repeat(4, minmax(70px, 1fr));
  grid-template-areas:
    ".        pv        .         ."
    "battery  inverter  junction  grid"
    ".        backup    load      .";
  gap: 24px 12px;
  max-width: 480px;
  margin: 24px auto;
}

.diagram-node {
  grid-area: var(--area);
  border: 2px solid #6c757d;
  border-radius: 8px;
  padding: 8px;
  text-align: center;
  font-size: 0.85rem;
  background: var(--bs-body-bg, #fff);
  cursor: pointer;
}

#node-pv { --area: pv; }
#node-battery { --area: battery; }
#node-inverter { --area: inverter; }
#node-junction { --area: junction; border-style: dotted; padding: 2px; font-size: 0.7rem; }
#node-backup { --area: backup; }
#node-grid { --area: grid; }
#node-load { --area: load; }

.diagram-node--grey { border-color: #6c757d; }
.diagram-node--green { border-color: #28a745; }
.diagram-node--orange { border-color: #fd7e14; }
.diagram-node--red { border-color: #dc3545; }
.diagram-node--pink { border-color: #e83e8c; }
.diagram-node--yellow { border-color: #ffc107; }

.diagram-details {
  display: none;
  font-size: 0.75rem;
  margin-top: 4px;
  color: #adb5bd;
}

.diagram-details--open {
  display: block;
}

.diagram-alert {
  color: #dc3545;
  font-weight: bold;
}

@media (max-width: 400px) {
  .diagram {
    grid-template-columns: repeat(4, minmax(56px, 1fr));
    gap: 16px 6px;
    font-size: 0.7rem;
  }
}
```

- [ ] **Step 2: Replace the plain-text block in `templates/index.html`**

Replace this block (current lines 12–34):

```html
  <div class="container">
    <h1>Inverter runtime data</h1>
    <div class="row">
      <div class="col">
        <div><span data-inverter="timestamp"></span></div>
        <div>
          <span id="age-badge" class="badge" data-bs-toggle="tooltip" title="Time since the last SSE update arrived in this browser. Keeps ticking even if the connection stalls - a growing number here means updates have actually stopped, not just slowed down."></span>
          <span id="read-badge" class="badge" data-bs-toggle="tooltip" title="How long the server's read_runtime_data() call to the inverter itself took. Poor wifi to the inverter is the most likely cause of a high value here."></span>
          <span id="delivery-badge" class="badge" data-bs-toggle="tooltip" title="Extra delay delivering an update from the server to this browser, on top of the best-case delivery seen recently (clock-skew corrected, so it's a relative delay, not an absolute one)."></span>
        </div>
        <div>PV: <span data-inverter="ppv"></span>W</div>
        <div>Battery: <span data-inverter="pbattery1"></span>W, <span data-inverter="battery_soc"></span>%, <span data-inverter="vbattery1"></span>V</div>
        <div>Load: <span data-inverter="load_ptotal"></span>W (<span data-inverter="load_p1"></span> / <span data-inverter="load_p2"></span> / <span data-inverter="load_p3"></span> W)</div>
        <div>Backup: <span data-inverter="backup_ptotal"></span>W (<span data-inverter="backup_p1"></span> / <span data-inverter="backup_p2"></span> / <span data-inverter="backup_p3"></span> W)</div>
        <div>Output: <span data-inverter="pgrid"></span> / <span data-inverter="pgrid2"></span> / <span data-inverter="pgrid3"></span> W</div>
        <div>Grid: <span data-inverter="vgrid"></span> / <span data-inverter="vgrid2"></span> / <span data-inverter="vgrid3"></span> V, <span data-inverter="fgrid"></span> / <span data-inverter="fgrid2"></span> / <span data-inverter="fgrid3"></span> Hz</div>
        <div>Meter: <span data-inverter="meter_active_power_total"></span>W (<span data-inverter="meter_active_power1"></span> / <span data-inverter="meter_active_power2"></span> / <span data-inverter="meter_active_power3"></span> W)</div>
        <div>Diagnose: <span data-inverter="diagnose_result_label"></span></div>
        <div>Hourly export: <span data-inverter="_hourly_meter_export"></span> kWh</div>
        <div>Hourly import: <span data-inverter="_hourly_meter_import"></span> kWh</div>
        <div>Hourly load: <span data-inverter="_hourly_load"></span> kWh</div>
      </div>
    </div>
```

with:

```html
  <div class="container">
    <h1>Inverter runtime data</h1>
    <div class="row">
      <div class="col">
        <div><span data-inverter="timestamp"></span></div>
        <div>
          <span id="age-badge" class="badge" data-bs-toggle="tooltip" title="Time since the last SSE update arrived in this browser. Keeps ticking even if the connection stalls - a growing number here means updates have actually stopped, not just slowed down."></span>
          <span id="read-badge" class="badge" data-bs-toggle="tooltip" title="How long the server's read_runtime_data() call to the inverter itself took. Poor wifi to the inverter is the most likely cause of a high value here."></span>
          <span id="delivery-badge" class="badge" data-bs-toggle="tooltip" title="Extra delay delivering an update from the server to this browser, on top of the best-case delivery seen recently (clock-skew corrected, so it's a relative delay, not an absolute one)."></span>
        </div>
      </div>
    </div>
    <div class="row">
      <div class="col">
        <div class="diagram">
          <div id="node-pv" class="diagram-node" data-toggle-details="pv">
            <div>☀️ PV</div>
            <div id="pv-watts">- W</div>
            <div id="pv-details" class="diagram-details">
              <div>String 1: <span id="pv-string1">-</span></div>
              <div>String 2: <span id="pv-string2">-</span></div>
            </div>
          </div>
          <div id="node-battery" class="diagram-node diagram-node--grey" data-toggle-details="battery">
            <div>🔋 Battery</div>
            <div id="battery-watts">- W</div>
            <div id="battery-soc">- %</div>
            <div id="battery-details" class="diagram-details">
              <div><span id="battery-voltage">-</span> V</div>
              <div><span id="battery-temp">-</span> °C</div>
            </div>
          </div>
          <div id="node-inverter" class="diagram-node diagram-node--grey" data-toggle-details="inverter">
            <div>⚡ Inverter</div>
            <div id="inverter-status">-</div>
            <div id="inverter-details" class="diagram-details">
              <div>Air: <span id="inverter-temp-air">-</span> °C</div>
              <div>Internal: <span id="inverter-temp">-</span> °C</div>
            </div>
          </div>
          <div id="node-junction" class="diagram-node">⏚</div>
          <div id="node-backup" class="diagram-node" data-toggle-details="backup">
            <div>🔌 Backup</div>
            <div id="backup-watts">- W</div>
            <div id="backup-details" class="diagram-details">
              <div>L1: <span id="backup-i1">-</span> A</div>
              <div>L2: <span id="backup-i2">-</span> A</div>
              <div>L3: <span id="backup-i3">-</span> A</div>
            </div>
          </div>
          <div id="node-grid" class="diagram-node diagram-node--grey" data-toggle-details="grid">
            <div>🏭 Grid</div>
            <div id="grid-watts">- W</div>
            <div id="grid-details" class="diagram-details">
              <div id="grid-voltages">-</div>
              <div id="grid-freqs">-</div>
            </div>
          </div>
          <div id="node-load" class="diagram-node" data-toggle-details="load">
            <div>🏠 Load</div>
            <div id="load-watts">- W</div>
            <div id="load-details" class="diagram-details">
              <div>L1: <span id="load-p1">-</span></div>
              <div>L2: <span id="load-p2">-</span></div>
              <div>L3: <span id="load-p3">-</span></div>
            </div>
          </div>
        </div>
        <div class="small text-muted">
          Diagnose: <span data-inverter="diagnose_result_label"></span> |
          Hourly export: <span data-inverter="_hourly_meter_export"></span> kWh |
          Hourly import: <span data-inverter="_hourly_meter_import"></span> kWh |
          Hourly load: <span data-inverter="_hourly_load"></span> kWh
        </div>
      </div>
    </div>
```

- [ ] **Step 3: Add the CSS `<link>` and LeaderLine/diagram `<script>` tags**

In the `<head>`, after the existing `bootstrap.min.css` link:

```html
  <link href="/static/css/diagram.css" rel="stylesheet">
```

Just before the closing `</body>`, after the existing script blocks (LeaderLine must load before `diagram-render.js`, and `diagram-calc.js` before `diagram-render.js`; `diagram-render.js` depends on the page's `eventSource` variable, defined in the existing inline `<script>` block earlier in the file, so it must load after that):

```html
  <script src="https://cdn.jsdelivr.net/npm/leader-line@1.0.7/leader-line.min.js"></script>
  <script src="/static/js/diagram-calc.js"></script>
  <script src="/static/js/diagram-render.js"></script>
```

- [ ] **Step 4: Manually verify the static layout**

Run: `python main.py --dry-run` (or the app's existing local run method) and open `/` in a browser at both a desktop width and a narrow (≤400px) mobile viewport (browser devtools device toolbar). Confirm: the 7 node boxes appear in the PV-top/Battery-Inverter-Junction-Grid row/Backup-Load-row layout from the spec, nothing overlaps or overflows horizontally at 375px width, and clicking a node toggles its (currently static placeholder) details text.

- [ ] **Step 5: Commit**

```bash
git add static/css/diagram.css templates/index.html
git commit -m "Add diagram markup and CSS layout (static, not yet wired to SSE data)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 3: `diagram-render.js` — SSE wiring, arrows, and detail toggles

**Files:**
- Create: `static/js/diagram-render.js`

**Interfaces:**
- Consumes: every function/constant produced by Task 1 (`window.DiagramCalc.*`), every DOM ID produced by Task 2, and the page-global `eventSource` (`var eventSource = new EventSource('/listen')`, declared in `templates/index.html`'s existing inline `<script>` — a top-level `var` in a classic script is a `window` property, so it's visible here as long as this script tag loads after that one, per Task 2 Step 3's ordering), and the global `LeaderLine` constructor from the CDN script.
- Produces: nothing consumed by later tasks (this is the outermost UI layer).

- [ ] **Step 1: Create `static/js/diagram-render.js`**

```js
// Wires the diagram-calc.js pure logic to the DOM: creates the LeaderLine
// arrows once, updates them (and all node text) on every SSE message, and
// handles the inline-expand detail toggles. Manually verified (Task 4) -
// no DOM available under `node --test`, unlike diagram-calc.js.
(function () {
  'use strict';

  var LINE_COLORS = {
    grey: '#6c757d',
    green: '#28a745',
    orange: '#fd7e14',
    red: '#dc3545',
    pink: '#e83e8c',
    yellow: '#ffc107',
  };

  var lines = {};

  function byId(id) {
    return document.getElementById(id);
  }

  function createLines() {
    var opts = { color: LINE_COLORS.grey, size: 2, endPlug: 'arrow1' };
    lines.pv = new LeaderLine(byId('node-pv'), byId('node-inverter'), opts);
    lines.battery = new LeaderLine(byId('node-battery'), byId('node-inverter'), opts);
    lines.backup = new LeaderLine(byId('node-inverter'), byId('node-backup'), opts);
    lines.bus = new LeaderLine(byId('node-inverter'), byId('node-junction'), opts);
    lines.grid = new LeaderLine(byId('node-junction'), byId('node-grid'), opts);
    lines.load = new LeaderLine(byId('node-junction'), byId('node-load'), opts);
  }

  // fromEl/toEl let bidirectional edges (battery, grid) flip which end the
  // arrowhead points at, redundantly with color, per the spec.
  function setLine(line, fromEl, toEl, colorName, thicknessPx, crossed) {
    line.start = fromEl;
    line.end = toEl;
    line.color = LINE_COLORS[colorName] || LINE_COLORS.grey;
    line.size = thicknessPx > 0 ? thicknessPx : 1;
    line.dash = thicknessPx === 0 ? { animation: false } : false;
    line.middleLabel = crossed ? LeaderLine.pathLabel('✕', { color: LINE_COLORS.red }) : '';
  }

  function setNodeColor(nodeId, colorName) {
    var el = byId(nodeId);
    el.className = el.className.replace(/\bdiagram-node--\w+\b/g, '').trim();
    el.classList.add('diagram-node--' + colorName);
  }

  function formatWatts(watts) {
    return Math.round(watts) + ' W';
  }

  function updateFromData(data) {
    var calc = window.DiagramCalc;

    var pv = calc.pvState(data);
    byId('pv-watts').textContent = formatWatts(pv.watts);
    setLine(lines.pv, byId('node-pv'), byId('node-inverter'), 'grey', calc.arrowThickness(pv.watts), false);
    byId('pv-string1').textContent = formatWatts(calc.toNumber(data.ppv1)) + ', ' + calc.toNumber(data.vpv1).toFixed(1) + ' V';
    byId('pv-string2').textContent = formatWatts(calc.toNumber(data.ppv2)) + ', ' + calc.toNumber(data.vpv2).toFixed(1) + ' V';

    var battery = calc.batteryState(data);
    byId('battery-watts').textContent = formatWatts(battery.watts);
    byId('battery-soc').textContent = calc.toNumber(data.battery_soc) + ' %';
    byId('battery-voltage').textContent = calc.toNumber(data.vbattery1).toFixed(1);
    byId('battery-temp').textContent = calc.toNumber(data.battery_temperature);
    var batteryFrom = battery.direction === 'discharge' ? byId('node-battery') : byId('node-inverter');
    var batteryTo = battery.direction === 'discharge' ? byId('node-inverter') : byId('node-battery');
    setLine(lines.battery, batteryFrom, batteryTo, battery.color, calc.arrowThickness(battery.watts), false);

    var inverter = calc.inverterState(data);
    byId('inverter-status').textContent = inverter.label;
    setNodeColor('node-inverter', inverter.color);
    byId('inverter-temp-air').textContent = calc.toNumber(data.temperature_air);
    byId('inverter-temp').textContent = calc.toNumber(data.temperature);

    var backup = calc.backupState(data);
    byId('backup-watts').textContent = formatWatts(backup.watts);
    setLine(lines.backup, byId('node-inverter'), byId('node-backup'), 'grey', calc.arrowThickness(backup.watts), false);
    ['backup-i1', 'backup-i2', 'backup-i3'].forEach(function (id, i) {
      var el = byId(id);
      el.textContent = backup.phaseCurrents[i].toFixed(1);
      el.classList.toggle('diagram-alert', backup.phaseAlerts[i]);
    });

    var bus = calc.inverterBusState(data);
    setLine(lines.bus, byId('node-inverter'), byId('node-junction'), 'grey', calc.arrowThickness(bus.watts), false);

    var grid = calc.gridState(data);
    byId('grid-watts').textContent = formatWatts(grid.watts);
    setNodeColor('node-grid', grid.color);
    var gridInOut = calc.toNumber(data.grid_in_out);
    var gridFrom = gridInOut === calc.GRID_IN_OUT.IMPORTING ? byId('node-grid') : byId('node-junction');
    var gridTo = gridInOut === calc.GRID_IN_OUT.IMPORTING ? byId('node-junction') : byId('node-grid');
    setLine(lines.grid, gridFrom, gridTo, grid.color, calc.arrowThickness(grid.watts), grid.crossed);
    byId('grid-voltages').textContent = [data.vgrid, data.vgrid2, data.vgrid3]
      .map(function (v) { return calc.toNumber(v).toFixed(1); }).join(' / ') + ' V';
    byId('grid-freqs').textContent = [data.fgrid, data.fgrid2, data.fgrid3]
      .map(function (v) { return calc.toNumber(v).toFixed(2); }).join(' / ') + ' Hz';

    var load = calc.loadState(data);
    byId('load-watts').textContent = formatWatts(load.watts);
    setLine(lines.load, byId('node-junction'), byId('node-load'), 'grey', calc.arrowThickness(load.watts), false);
    byId('load-p1').textContent = formatWatts(calc.toNumber(data.load_p1));
    byId('load-p2').textContent = formatWatts(calc.toNumber(data.load_p2));
    byId('load-p3').textContent = formatWatts(calc.toNumber(data.load_p3));
  }

  function initToggles() {
    document.querySelectorAll('[data-toggle-details]').forEach(function (trigger) {
      trigger.addEventListener('click', function () {
        var key = trigger.getAttribute('data-toggle-details');
        byId(key + '-details').classList.toggle('diagram-details--open');
      });
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    createLines();
    initToggles();
    eventSource.addEventListener('message', function (e) {
      updateFromData(JSON.parse(e.data));
    });
    window.addEventListener('resize', function () {
      Object.keys(lines).forEach(function (key) { lines[key].position(); });
    });
  });
})();
```

- [ ] **Step 2: Commit**

```bash
git add static/js/diagram-render.js
git commit -m "Wire diagram to live SSE data: arrows, colors, detail toggles

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 4: Manual end-to-end verification and CHANGELOG

This task has no automated tests (none of the DOM/SSE wiring is unit-testable without introducing a browser-test framework, which the spec's Technical Approach section deliberately avoided). Verification is manual, against the running app.

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Run the app locally against the real inverter (or replay real historical data if `--dry-run`/offline)**

Run: `python main.py` (drop `--dry-run` if you want live inverter data; the design's example values throughout this plan came from `raspberry4.local`'s production `data.db`, so replaying a few of those rows through a manual `EventSource`-shaped test message is an acceptable substitute if the inverter isn't reachable from your dev machine).

- [ ] **Step 2: Verify each documented scenario renders correctly**

Using either live data or by temporarily pointing the browser's devtools at a mocked `/listen` response built from these `data.db` rows (all pulled during the design phase, see the spec):

- PV producing + battery charging: `ppv=1367, pgrid=318, pgrid2=324, pgrid3=312, meter_active_power_total≈9, grid_in_out=0 (Idle), battery_mode=3 (Charge), pbattery1=-364, load_ptotal≈960` → PV arrow visibly thick, Battery arrow green pointing Inverter→Battery, Grid node grey/idle, Junction→Load arrow present.
- Overnight discharge covering load from grid: `ppv=0, pbattery1=-73, battery_mode=2 (Discharge but small/noisy — see spec caveat), grid_in_out=2 (Importing), meter_active_power_total=-385` → Grid arrow orange pointing Grid→Junction, Load arrow present, PV arrow at floor/dashed.
- Idle/full battery: `battery_soc=100, pbattery1=-31, battery_mode` whatever the live system reports at idle → confirm a thin but visible Battery arrow (not hidden), matching the spec's "render as-is" decision.
- Grid fault: manually set `grid_mode=2` in a mocked payload → Grid node turns red, the Inverter–Junction–Grid line shows the ✕ cross-out.
- Grid not connected: `grid_mode=0` → line crossed out, Grid node stays grey.
- Inverter off-grid: `work_mode=2` → Inverter node turns pink, status text shows "Normal (Off-Grid)".
- Backup overloaded: mock `backup_i1=14.0` → that phase's current renders in red in the Backup node's expanded details.
- Mobile: resize the browser to 375px width (or use devtools device toolbar) → no horizontal scroll, all 7 nodes and their labels stay legible, tapping a node opens its details without layout breaking.

- [ ] **Step 3: Confirm the existing behavior is untouched**

Check the freshness badges (`#age-badge`/`#read-badge`/`#delivery-badge`) still update every second, the raw JSON dump below the `<hr>` still shows all fields, and the nav buttons (`/eco`, `/config`, `/prices`, `/forecast`, `/history`) still work.

- [ ] **Step 4: Add a CHANGELOG entry**

Add under the `## Unreleased` → a new `### Added` section (or append to `### Changed` if one fits better by the time this lands) in `CHANGELOG.md`:

```markdown
### Added

- Replaced the plain-text runtime-data block on the main page with a
  graphical live power-flow diagram (PV/Battery/Inverter/Backup/Grid/Load),
  showing direction and relative magnitude via arrow color/thickness, with
  tap-to-expand extra details (per-string PV, grid voltage/frequency,
  per-phase load and backup current). See
  `docs/superpowers/specs/2026-09-03-live-power-flow-dashboard-design.md`
  for the design, including a couple of inverter-data quirks it works
  around (`pbattery1`'s sign isn't reliable near zero, `pgrid` reflects the
  inverter's own AC output rather than actual grid import/export).
```

- [ ] **Step 5: Commit**

```bash
git add CHANGELOG.md
git commit -m "Document live power-flow dashboard in CHANGELOG

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** Layout (Task 2), data mapping table (Task 1 functions + Task 3 wiring covers every row), color/status rules including the Fault/Not-connected cross-out and backup current threshold (Task 1 `gridState`/`backupState` + Task 3), inline-expand details interaction (Task 2 markup + Task 3 `initToggles`), LeaderLine technical approach (Task 3), idle-battery "render as-is" decision (Task 4 Step 2 explicitly re-verifies it), the known accepted magnitude/direction transition-skew artifact (documented in the spec, deliberately not engineered around per the user's "keep it simple" decision — no task attempts to fix it). All covered.
- **Placeholder scan:** no TBD/TODO markers; every step has real, complete code.
- **Type consistency:** all `DiagramCalc.*` function names/return shapes used in Task 3 match exactly what Task 1 defines and tests.
