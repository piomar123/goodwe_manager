"""
_raw_registers.py - list register changes in runtime_raw.db (runtime_raw.py)
over a time window, to spot undocumented flags. Read-only.

  python _raw_registers.py --from "2026-11-20 14:00" --to "2026-11-20 14:30"
  python _raw_registers.py --from ... --to ... --regs 35212-35219,35124
  python _raw_registers.py --from ... --to ... --all       # every register
  python _raw_registers.py --from ... --to ... --show 35212,35213   # values per sample

Default registers: runtime_raw.UNDECODED. Times are local.
"""
import argparse
import sqlite3
from datetime import datetime

import runtime_raw


def parse_regs(text: str) -> list:
    out = []
    for part in text.split(','):
        part = part.strip()
        if '-' in part:
            a, b = part.split('-')
            out.extend(range(int(a), int(b) + 1))
        elif part:
            out.append(int(part))
    return out


def epoch(text: str) -> int:
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
        try:
            return int(datetime.strptime(text, fmt).timestamp())
        except ValueError:
            pass
    raise SystemExit(f"bad time {text!r}, use 'YYYY-MM-DD HH:MM[:SS]'")


def local(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--db', default=runtime_raw.RAW_DB_PATH)
    p.add_argument('--from', dest='start', required=True)
    p.add_argument('--to', dest='end', required=True)
    p.add_argument('--regs', help='e.g. 35212-35219,35124 (default: the undecoded ones)')
    p.add_argument('--all', action='store_true', help='all 125 registers of the block')
    p.add_argument('--show', help='print these registers for every sample instead of changes')
    args = p.parse_args()

    conn = sqlite3.connect(f'file:{args.db}?mode=ro', uri=True)
    rows = conn.execute(
        'SELECT timestamp_epoch, block FROM runtime_raw WHERE timestamp_epoch BETWEEN ? AND ? '
        'ORDER BY timestamp_epoch', (epoch(args.start), epoch(args.end))).fetchall()
    print(f'{len(rows)} samples')
    if args.show:
        regs = parse_regs(args.show)
        print('time                 ' + ' '.join(f'{r:>7}' for r in regs))
        for ts, block in rows:
            print(local(ts) + '  ' + ' '.join(f'{runtime_raw.register(block, r):#06x}' for r in regs))
        return
    if args.all:
        regs = list(range(runtime_raw.FIRST_REGISTER, runtime_raw.FIRST_REGISTER + runtime_raw.REGISTER_COUNT))
    else:
        regs = parse_regs(args.regs) if args.regs else list(runtime_raw.UNDECODED)
    if rows:
        first = rows[0][1]
        print('start  ' + ', '.join(f'{r}={runtime_raw.register(first, r):#06x}' for r in regs))
    for ts, reg, old, new in runtime_raw.changes(rows, regs):
        print(f'{local(ts)}  {reg}  {old:#06x} -> {new:#06x}  ({old} -> {new}, bits changed {old ^ new:#06x})')


if __name__ == '__main__':
    main()
