"""
shadow_scan_guard.py
Turns the inverter's shadow scan (PV MPPT global scan) off while the
inverter is off-grid - it misbehaves there - and puts the previous value
back once the grid has stayed up for a cooldown: outages come in bursts,
the grid often drops again minutes after returning (2025-09-23: up to
~15 min). Independent of battery control (CONTROL_MODE). Runs on the
asyncio loop thread; step() never raises, so a problem here can't stop
inverter polling.
"""
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Mapping, Optional

logger = logging.getLogger(__name__)

SETTING = 'shadow_scan'
OFF = 0
VERIFY_DELAY_S = 3.0  # a write can take a few seconds to show in the read-back
READ_RETRY_S = 60.0
MAX_ATTEMPTS = 3
FIRST_BACKOFF_S = 300.0
MAX_BACKOFF_S = 3600.0  # a register that acks but never takes the value: a few writes an hour


@dataclass(frozen=True)
class GuardConfig:
    cooldown: timedelta


def config_from_env(env: Mapping[str, str]) -> Optional[GuardConfig]:
    mode = (env.get('OFF_GRID_SHADOW_SCAN_GUARD') or 'off').strip().lower()
    if mode == 'off':
        return None
    if mode != 'on':
        raise ValueError(f'OFF_GRID_SHADOW_SCAN_GUARD must be on or off, not {mode!r}')
    minutes = float(env.get('OFF_GRID_SHADOW_SCAN_COOLDOWN_MIN') or 15)
    if minutes < 0:
        raise ValueError('OFF_GRID_SHADOW_SCAN_COOLDOWN_MIN must be >= 0')
    return GuardConfig(timedelta(minutes=minutes))


class ShadowScanGuard:
    def __init__(self, config: GuardConfig, *, state_path: Optional[str] = None,
                 mono_fn: Callable[[], float] = time.monotonic):
        self._cooldown_s = config.cooldown.total_seconds()
        self._state_path = state_path
        self._mono = mono_fn
        # holding: shadow scan is kept off (off-grid, or on-grid cooldown).
        # restore: the value to put back, None until read this outage.
        # Both survive restarts, so a deploy mid-outage still restores it.
        self._holding = False
        self._restore: Optional[int] = None
        self._on_grid_since: Optional[float] = None
        self._target: Optional[int] = None
        self._applied: Optional[int] = None  # target confirmed by read-back - stop reading
        self._next_try = 0.0
        self._attempts = 0
        self._backoff_s = FIRST_BACKOFF_S
        # Loaded on the first step: main.py builds the guard at import, before
        # logging is configured, and the load logs what it resumed.
        self._loaded = False

    async def step(self, inverter, off_grid: bool) -> None:
        """Call once per poll with the current goodwe object (it is new after
        every reconnect) and whether the inverter is off-grid now."""
        try:
            await self._step(inverter, off_grid)
        except Exception as e:
            logger.warning(f'Shadow scan guard step failed: {e}')

    async def _step(self, inverter, off_grid: bool) -> None:
        if not self._loaded:
            self._loaded = True
            self._load()
        now = self._mono()
        restoring = False
        if off_grid:
            self._on_grid_since = None
            if not self._holding:
                logger.info('Off-grid: holding shadow scan off')
                self._holding, self._restore = True, None
                self._save()
            target = OFF
        elif self._holding:
            if self._on_grid_since is None:
                self._on_grid_since = now
            if now - self._on_grid_since < self._cooldown_s:
                target = OFF
            elif self._restore is None:
                # Never read during the outage (link down): nothing known to restore.
                logger.warning('Grid stable, but shadow scan was never read during the outage - left as is')
                self._finish()
                return
            else:
                target, restoring = self._restore, True
        else:
            return
        if target != self._target:
            self._target, self._applied = target, None
            self._next_try, self._attempts, self._backoff_s = 0.0, 0, FIRST_BACKOFF_S
        if self._applied != target and now >= self._next_try:
            await self._apply(inverter, target, now)
        if restoring and self._applied == target:
            logger.info(f'Grid stable for {self._cooldown_s / 60:g} min: shadow scan back to {target}')
            self._finish()

    async def _apply(self, inverter, target: int, now: float) -> None:
        """One read (and maybe one write) towards target; sets _applied once
        the read-back shows it."""
        try:
            value = await inverter.read_setting(SETTING)
        except Exception as e:
            logger.debug(f'Could not read {SETTING}: {e}')
            value = None
        if value is None:
            self._next_try = now + READ_RETRY_S
            return
        value = int(value)
        if self._holding and self._restore is None:
            self._restore = value  # first read of this outage: what to put back
            self._save()
        if value == target:
            self._applied, self._attempts, self._backoff_s = target, 0, FIRST_BACKOFF_S
            return
        if self._attempts >= MAX_ATTEMPTS:
            logger.warning(f'{SETTING}: not applied after {self._attempts} attempts (wanted {target}, read {value})')
            self._next_try = now + self._backoff_s
            self._backoff_s = min(self._backoff_s * 2, MAX_BACKOFF_S)
            self._attempts = 0
            return
        self._attempts += 1
        try:
            await inverter.write_setting(SETTING, target)
            logger.info(f'Wrote {SETTING} = {target}')
        except Exception as e:
            logger.warning(f'Writing {SETTING} = {target} failed: {e}')
        self._next_try = now + VERIFY_DELAY_S

    def _finish(self) -> None:
        self._holding, self._restore, self._on_grid_since = False, None, None
        self._target = self._applied = None
        self._save()

    def _load(self) -> None:
        if not self._state_path:
            return
        try:
            with open(self._state_path) as f:
                saved = json.load(f)
            self._holding = bool(saved['holding'])
            self._restore = None if saved['restore'] is None else int(saved['restore'])
            if self._holding:
                logger.info(f'Shadow scan guard resumed (restore value {self._restore})')
        except FileNotFoundError:
            pass
        except Exception as e:
            self._holding, self._restore = False, None
            logger.warning(f'Ignoring unreadable shadow scan guard state {self._state_path}: {e}')

    def _save(self) -> None:
        if not self._state_path:
            return
        try:
            tmp = self._state_path + '.tmp'
            with open(tmp, 'w') as f:
                json.dump({'holding': self._holding, 'restore': self._restore}, f)
            os.replace(tmp, self._state_path)
        except OSError as e:
            logger.warning(f'Could not save shadow scan guard state {self._state_path}: {e}')
