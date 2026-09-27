"""
control_runtime.py
Glue between control.Executor, control_io.ControlWriter, MQTT and main.py's
1 Hz poll loop. Every method runs on the asyncio loop thread; step() never
raises, so a control problem can't stop inverter polling.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Callable, Optional

from control import Command, CommandError, ControlConfig, Executor, Sample, compute_warnings, parse_command
from control_io import ControlWriter

logger = logging.getLogger(__name__)


class ControlRuntime:
    def __init__(self, config: ControlConfig, mqtt, *,
                 now_fn: Callable[[], datetime] = lambda: datetime.now().astimezone(),
                 mono_fn: Callable[[], float] = time.monotonic,
                 eco_check_interval_s: float = 300.0, state_interval_s: float = 10.0):
        self._config = config
        self._mqtt = mqtt
        self._now = now_fn
        self._mono = mono_fn
        self._eco_interval = eco_check_interval_s
        self._state_interval = state_interval_s
        self.executor = Executor(config)
        self._writer: Optional[ControlWriter] = None
        self._inverter = None
        self._eco_enabled: Optional[list] = None
        self._next_eco = 0.0
        self._initial_work_mode: Optional[int] = None
        self._last_state: Optional[dict] = None
        self._next_state = 0.0

    @property
    def last_state(self) -> Optional[dict]:
        return self._last_state

    def attach(self, inverter) -> None:
        """Called after every inverter (re)connect - the goodwe object is new
        each time, the executor (commands, override, reserve) is kept."""
        self._inverter = inverter
        old = self._writer
        self._writer = ControlWriter(inverter, shadow=self._config.mode == 'shadow',
                                     max_writes_per_day=self._config.max_writes_per_day, now_fn=self._mono)
        if old is not None:
            self._writer.carry_state_from(old)
        self._next_eco = 0.0

    def on_mqtt_message(self, topic_suffix: str, payload: bytes) -> None:
        if topic_suffix == 'control/set':
            try:
                self.executor.submit(parse_command(payload, self._now()))
            except CommandError as e:
                logger.warning(f'Rejected control command: {e}')
                self.executor.reject(str(e))
        elif topic_suffix == 'control/reserve/set':
            text = payload.decode(errors='replace').strip() if isinstance(payload, bytes) else str(payload).strip()
            if text == '':
                self.executor.set_reserve(None)
                return
            try:
                value = int(float(text))
            except ValueError:
                logger.warning(f'Ignored reserve {text!r}: not a number')
                return
            if not 0 <= value <= 100:
                logger.warning(f'Ignored reserve {value}: outside 0-100')
                return
            self.executor.set_reserve(value)

    def set_override(self, cmd: Command) -> None:
        self.executor.set_override(cmd)

    def clear_override(self) -> None:
        self.executor.clear_override()

    async def step(self, runtime_data: dict) -> dict:
        now = self._now()
        desired = None
        try:
            was_off_grid = self.executor.snapshot(now)['off_grid']
            desired = self.executor.tick(Sample.from_runtime(runtime_data), now)
            if self._writer is not None:
                if self.executor.snapshot(now)['off_grid'] and not was_off_grid:
                    self._writer.restart()  # the backup side can't wait out a back-off
                await self._writer.step(desired)
                if self._initial_work_mode is None:
                    self._initial_work_mode = self._writer.readback.get('work_mode')
                await self._maybe_read_eco()
        except Exception as e:
            logger.warning(f'Control step failed: {e}')
        state = self._build_state(now, desired)
        await self._maybe_publish(state)
        return state

    async def _maybe_read_eco(self) -> None:
        mono = self._mono()
        if mono < self._next_eco:
            return
        self._next_eco = mono + self._eco_interval
        enabled = []
        for i in range(1, 5):
            try:
                slot = await self._inverter.read_setting(f'eco_mode_{i}')
                enabled.append(slot is not None and slot.on_off < 0)
            except Exception as e:
                logger.debug(f'Could not read eco_mode_{i}: {e}')
                return  # keep the previous list rather than a partial one
        self._eco_enabled = enabled

    def _build_state(self, now: datetime, desired: Optional[dict]) -> dict:
        snap = self.executor.snapshot(now)
        writer = self._writer
        readback = dict(writer.readback) if writer is not None else {}
        state = {k: v for k, v in snap.items() if k != 'command_error'}
        state.update({
            'applied': writer.applied(desired) if writer is not None else False,
            'registers': readback,
            'last_error': (writer.last_error if writer is not None else None) or snap['command_error'],
            'warnings': compute_warnings(readback, self._eco_enabled, self.executor.reserve, self._initial_work_mode,
                                         self._config.min_soc if self.executor.min_soc_hold else None),
            'writes_today': writer.writes_today if writer is not None else 0,
        })
        return state

    async def _maybe_publish(self, state: dict) -> None:
        mono = self._mono()
        if state == self._last_state and mono < self._next_state:
            return
        self._last_state = state
        self._next_state = mono + self._state_interval
        try:
            await asyncio.wait_for(self._mqtt.publish_control_state(state), timeout=5)
        except Exception as e:
            logger.warning(f'Could not publish control state: {e}')
