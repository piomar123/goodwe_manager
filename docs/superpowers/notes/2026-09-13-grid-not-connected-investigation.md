# `grid_mode = NOT_CONNECTED` — is `directionKnown` ever wrong for it?

Status: **resolved, 2026-09-13. No code change needed.**

## Question

A subagent branch review flagged that `diagram-calc.js`'s `gridState()`
sets `directionKnown: mode !== GRID_MODE.FAULT` — i.e. it reports
direction as "known" while `grid_mode === NOT_CONNECTED` (0), even though
`crossed` (and therefore `importing`/`exporting`) is forced false in that
same state. Is that an inconsistency worth fixing, or does it matter in
practice?

## Finding #1 — it's intentional, not an oversight

`git log -S"direction stays known"` traces this to the very first PR
(#9) that introduced the diagram: there's a test from that PR explicitly
asserting direction stays "known" in this case. It was a deliberate
design decision from day one, not a defect introduced later.

## Finding #2 — and it has never actually been reachable on this install

Ran the full-history aggregate directly against the live production
`data.db` (34.36M rows, ~2 years of history). SQLite's WAL mode gives
readers a non-blocking snapshot, so this ran as a plain long-lived query
against the live file with zero disruption to the running service - no
snapshot/copy step needed:

```sql
SELECT grid_mode, grid_mode_label, COUNT(*), MIN(timestamp), MAX(timestamp)
FROM inverter_history GROUP BY grid_mode, grid_mode_label;
```

Result:

| grid_mode | label              | rows       |
|-----------|--------------------|------------|
| NULL      | (pre-tracking era) | 12,240,613 |
| 1.0       | Connected to grid  | 22,047,958 |
| 2.0       | Fault              | 75,371     |
| 0.0       | Not connected      | **0**      |

`NOT_CONNECTED` has never occurred once in this installation's ~2 years
of history. The flagged `directionKnown` behavior for that state is
provably dead code for this deployment.

## Conclusion

Left `gridState()` as-is. Not worth adding special-casing for a state
that has never been observed; revisit only if a different install (or
this one, after a config/firmware change) ever actually reports
`grid_mode = 0`.

## Related, smaller ambiguity (flagged, not acted on)

`toNumber(undefined) === 0 === GRID_MODE.NOT_CONNECTED`, so if
`data.grid_mode` were ever missing/undefined (as opposed to genuinely
`0`), it would silently read as `NOT_CONNECTED` rather than "unknown".
No evidence this happens in practice (the field is always populated in
the historical data above); not worth guarding against speculatively.
