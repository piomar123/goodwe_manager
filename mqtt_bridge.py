"""
mqtt_bridge.py
Optional MQTT publish path for goodwe_manager - turns the existing
inverter polling loop's already-fetched data into the topics Home
Assistant/Predbat need, without a second connection to the inverter. See
PR #35.

Disabled entirely (every publish_* method becomes a no-op) when no
MQTT_HOST is configured - a fresh checkout or another user's fork with no
new env vars set behaves exactly as before this feature existed.
"""
import json
import logging
import time
from typing import Any, Callable, Optional

import aiomqtt

logger = logging.getLogger(__name__)


class MqttBridge:
    def __init__(self, host: Optional[str], port: int = 1883, username: Optional[str] = None,
                 password: Optional[str] = None, topic_prefix: str = 'goodwe',
                 client_factory: Optional[Callable[..., Any]] = None,
                 reconnect_interval_seconds: float = 30.0,
                 now_fn: Optional[Callable[[], float]] = None):
        """client_factory, if given, is called with the same kwargs
        aiomqtt.Client would take, and must return an object supporting
        `async with` and an `async def publish(topic, payload, retain)`
        method - used by tests to inject a fake client instead of a real
        aiomqtt.Client. Defaults to aiomqtt.Client itself.

        reconnect_interval_seconds rate-limits the opportunistic
        reconnect attempts _publish makes when the broker has dropped -
        telemetry publishes at ~1Hz, so without this an unreachable
        broker would get hammered with a connection attempt (and a log
        line) every single second.

        now_fn, if given, replaces time.monotonic() as the clock used to
        track reconnect attempts - used by tests to control time without
        real sleeps. Deliberately not time.time(), which can jump on NTP
        adjustments (this codebase already uses time.monotonic()
        elsewhere for exactly this reason, e.g. main.py's
        `read_start = time.monotonic()`)."""
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._prefix = topic_prefix
        self._client_factory = client_factory or aiomqtt.Client
        self._client = None
        self._reconnect_interval_seconds = reconnect_interval_seconds
        self._now_fn = now_fn or time.monotonic
        self._last_connect_attempt = None

    @property
    def enabled(self) -> bool:
        return self._host is not None

    def _topic(self, suffix: str) -> str:
        return f'{self._prefix}/{suffix}'

    async def connect(self) -> None:
        """Explicit connect entry point - called once by main.py at
        startup. Records the attempt time (so the opportunistic
        reconnect path in _publish doesn't immediately retry right after
        this) and delegates to _do_connect(), which raises on failure so
        this call site's own try/except (in main.py) can log it once."""
        self._last_connect_attempt = self._now_fn()
        await self._do_connect()

    async def _do_connect(self) -> None:
        if not self.enabled:
            return
        if self._client is not None:
            # Already connected - a no-op. main.py's inverter polling
            # loop re-invokes _get_inverter_data() (which calls
            # connect()) on every inverter reconnect, which happens every
            # 5 seconds while the inverter link is flaky (expected/normal
            # per this project's README). The MQTT broker connection is
            # unrelated to the inverter connection and doesn't need to be
            # torn down and rebuilt just because the inverter bounced -
            # rebuilding it here would leak the previous client's open
            # socket/thread every single reconnect, since nothing ever
            # calls __aexit__ on the old one.
            return
        will = aiomqtt.Will(topic=self._topic('bridge/status'), payload='offline', retain=True)
        client = self._client_factory(
            hostname=self._host, port=self._port,
            username=self._username, password=self._password, will=will,
        )
        try:
            await client.__aenter__()
        except Exception:
            # A failed __aenter__ must not leave self._client pointing at a
            # client that never actually connected - every subsequent
            # publish_* call's `self._client is None` check (in _publish)
            # is what keeps this bridge a no-op when disconnected, and that
            # check only works if self._client stays None here. Re-raise so
            # the caller still sees the failure - connect() lets it
            # propagate to main.py's startup try/except, and the
            # opportunistic reconnect path in _publish catches it locally
            # instead.
            self._client = None
            raise
        self._client = client
        await self._publish('bridge/status', 'online', retain=True)

    async def publish_offline_and_disconnect(self) -> None:
        """Explicit offline publish before a clean disconnect - the MQTT
        Will above only fires on an *unclean* disconnect (spec's Units/
        Component 1 note), so a deliberate shutdown needs this to avoid
        leaving the retained status topic stuck on 'online'."""
        if not self.enabled or self._client is None:
            return
        await self._publish('bridge/status', 'offline', retain=True)
        try:
            await self._client.__aexit__(None, None, None)
        except Exception as e:
            logger.warning(f"Error disconnecting MQTT client: {e}")

    async def publish_telemetry(self, payload: dict) -> None:
        await self._publish('telemetry', json.dumps(payload), retain=False)

    async def publish_export_prices(self, payload: dict) -> None:
        await self._publish('prices/export', json.dumps(payload), retain=True)

    async def publish_import_prices(self, payload: dict) -> None:
        await self._publish('prices/import', json.dumps(payload), retain=True)

    async def publish_pv_forecast(self, series: dict) -> None:
        await self._publish('forecast/pv', json.dumps(series), retain=True)

    async def _publish(self, topic_suffix: str, payload, retain: bool) -> None:
        if not self.enabled:
            return
        if self._client is None:
            await self._maybe_reconnect()
            if self._client is None:
                return
        try:
            await self._client.publish(self._topic(topic_suffix), payload, retain=retain)
        except Exception as e:
            # A broker hiccup must never take down inverter polling - see
            # PR #35: "MQTT being down must never stop inverter
            # polling/storage/SSE from working." Mark the client as
            # disconnected so the `self._client is None` check above
            # (re)triggers a rate-limited reconnect attempt next time,
            # instead of silently treating this stale client as usable
            # forever.
            logger.warning(f"MQTT publish to {topic_suffix} failed: {e}")
            self._client = None

    async def _maybe_reconnect(self) -> None:
        """Opportunistic reconnect, tried from within _publish whenever
        self._client is None. Rate-limited to at most once per
        reconnect_interval_seconds so an unreachable broker doesn't get
        hammered with a connection attempt (and a log line) on every ~1Hz
        telemetry publish. Unlike connect(), a failure here must not
        raise - it's swallowed the same way a publish failure already is,
        leaving self._client as None for the next scheduled attempt."""
        now = self._now_fn()
        if (self._last_connect_attempt is not None
                and now - self._last_connect_attempt < self._reconnect_interval_seconds):
            return
        self._last_connect_attempt = now
        try:
            await self._do_connect()
        except Exception as e:
            logger.warning(f"MQTT reconnect attempt failed: {e}")
