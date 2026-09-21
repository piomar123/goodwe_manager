"""
tests/test_mqtt_bridge.py
MqttBridge is tested against a FakeMqttClient rather than a real broker -
it records every publish call (topic, payload, retain) so tests can
assert on exact topic names and retain flags without any network I/O.
"""
import asyncio
import json
import unittest

import mqtt_bridge


class FakeMqttClient:
    def __init__(self):
        self.published = []
        self.connected = False
        self.disconnected = False
        self.aenter_count = 0

    async def __aenter__(self):
        self.connected = True
        self.aenter_count += 1
        return self

    async def __aexit__(self, *exc_info):
        self.disconnected = True

    async def publish(self, topic, payload, retain=False):
        self.published.append((topic, payload, retain))


class MqttBridgeDisabledTest(unittest.TestCase):
    def test_disabled_when_no_host_given(self):
        bridge = mqtt_bridge.MqttBridge(host=None)
        self.assertFalse(bridge.enabled)

    def test_publish_is_a_no_op_when_disabled(self):
        bridge = mqtt_bridge.MqttBridge(host=None)
        asyncio.run(bridge.connect())
        asyncio.run(bridge.publish_telemetry({'ppv': '100'}))  # must not raise


class FailingAenterMqttClient:
    """Simulates a client whose connection attempt fails inside __aenter__
    (e.g. an invalid host/port) - used to verify that MqttBridge.connect()
    doesn't leave self._client pointing at a client that never actually
    connected, which would make every later publish_* call think it's
    connected and log a spurious "not currently connected" warning per
    call instead of quietly no-op'ing via the `self._client is None` check."""
    async def __aenter__(self):
        raise ValueError("Invalid host.")

    async def __aexit__(self, *exc_info):
        pass

    async def publish(self, topic, payload, retain=False):
        raise AssertionError("publish should never be called on a client that failed to connect")


class MqttBridgeFailedConnectTest(unittest.TestCase):
    def setUp(self):
        self.bridge = mqtt_bridge.MqttBridge(host='localhost', client_factory=lambda **kwargs: FailingAenterMqttClient())

    def test_connect_raises_but_leaves_client_none(self):
        with self.assertRaises(ValueError):
            asyncio.run(self.bridge.connect())
        self.assertIsNone(self.bridge._client)

    def test_publish_after_failed_connect_is_a_silent_no_op(self):
        with self.assertRaises(ValueError):
            asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))  # must not raise, and must not touch the failed client


class MqttBridgeEnabledTest(unittest.TestCase):
    def setUp(self):
        self.fake_client = FakeMqttClient()
        self.captured_factory_kwargs = {}

        def capturing_client_factory(**kwargs):
            self.captured_factory_kwargs.update(kwargs)
            return self.fake_client

        self.bridge = mqtt_bridge.MqttBridge(host='localhost', port=1884,
                                             username='u', password='p',
                                             topic_prefix='goodwe',
                                             client_factory=capturing_client_factory)

    def test_connect_passes_host_port_credentials_and_will_to_client_factory(self):
        asyncio.run(self.bridge.connect())

        self.assertEqual(self.captured_factory_kwargs['hostname'], 'localhost')
        self.assertEqual(self.captured_factory_kwargs['port'], 1884)
        self.assertEqual(self.captured_factory_kwargs['username'], 'u')
        self.assertEqual(self.captured_factory_kwargs['password'], 'p')

        will = self.captured_factory_kwargs['will']
        self.assertEqual(will.topic, 'goodwe/bridge/status')
        self.assertEqual(will.payload, 'offline')
        self.assertTrue(will.retain)

    def test_connect_publishes_online_status_retained(self):
        asyncio.run(self.bridge.connect())

        self.assertTrue(self.fake_client.connected)
        self.assertIn(('goodwe/bridge/status', 'online', True), self.fake_client.published)

    def test_publish_offline_and_disconnect_publishes_offline_status_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_offline_and_disconnect())

        self.assertIn(('goodwe/bridge/status', 'offline', True), self.fake_client.published)
        self.assertTrue(self.fake_client.disconnected)

    def test_publish_telemetry_is_not_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/telemetry')
        self.assertEqual(json.loads(payload), {'ppv': '100'})
        self.assertFalse(retain)

    def test_publish_export_prices_is_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_export_prices({'raw_today': [], 'raw_tomorrow': []}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/prices/export')
        self.assertTrue(retain)

    def test_publish_import_prices_is_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_import_prices({'raw_today': [], 'raw_tomorrow': []}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/prices/import')
        self.assertTrue(retain)

    def test_publish_pv_forecast_is_retained(self):
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.publish_pv_forecast({'12:00': 1.5}))

        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/forecast/pv')
        self.assertTrue(retain)

    def test_second_connect_when_already_connected_does_not_rebuild_client(self):
        # Regression test for a client leak: main.py's inverter polling
        # loop calls mqtt.connect() again on every inverter reconnect
        # (every 5s while the inverter is unreachable, which is normal
        # per this project's README). Before the idempotence guard, each
        # call built a brand-new client and __aenter__'d it without ever
        # __aexit__'ing the previous one, leaking a socket/thread per
        # reconnect. connect() should be a no-op once already connected.
        asyncio.run(self.bridge.connect())
        asyncio.run(self.bridge.connect())

        self.assertEqual(self.fake_client.aenter_count, 1)
        self.assertEqual(self.fake_client.published.count(('goodwe/bridge/status', 'online', True)), 1)

    def test_a_publish_failure_is_swallowed_not_raised(self):
        async def raising_publish(*args, **kwargs):
            raise ConnectionError("broker unreachable")
        self.fake_client.publish = raising_publish
        asyncio.run(self.bridge.connect())

        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))  # must not raise

    def test_a_publish_failure_marks_the_client_as_disconnected(self):
        async def raising_publish(*args, **kwargs):
            raise ConnectionError("broker unreachable")
        self.fake_client.publish = raising_publish
        asyncio.run(self.bridge.connect())

        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))

        self.assertIsNone(self.bridge._client)


class FakeClock:
    """Controllable clock injected as MqttBridge's now_fn, so reconnect
    rate-limiting tests can advance time without a real sleep."""
    def __init__(self, start: float = 0.0):
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class MqttBridgeReconnectTest(unittest.TestCase):
    def setUp(self):
        self.fake_client = FakeMqttClient()
        self.factory_call_count = 0

        def counting_client_factory(**kwargs):
            self.factory_call_count += 1
            return self.fake_client

        self.clock = FakeClock()
        self.bridge = mqtt_bridge.MqttBridge(
            host='localhost', client_factory=counting_client_factory,
            reconnect_interval_seconds=30.0, now_fn=self.clock,
        )

    def _break_publish(self):
        async def raising_publish(*args, **kwargs):
            raise ConnectionError("broker unreachable")
        self.fake_client.publish = raising_publish

    def _fix_publish(self):
        async def working_publish(topic, payload, retain=False):
            self.fake_client.published.append((topic, payload, retain))
        self.fake_client.publish = working_publish

    def test_publish_before_reconnect_interval_elapsed_does_not_reconnect(self):
        asyncio.run(self.bridge.connect())
        self.assertEqual(self.factory_call_count, 1)
        self._break_publish()
        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))  # fails, client -> None

        self.clock.advance(10.0)  # less than the 30s reconnect_interval_seconds
        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))

        self.assertEqual(self.factory_call_count, 1)  # no new client built
        self.assertIsNone(self.bridge._client)

    def test_publish_after_reconnect_interval_elapsed_reconnects_and_publish_succeeds(self):
        asyncio.run(self.bridge.connect())
        self.assertEqual(self.factory_call_count, 1)
        self._break_publish()
        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))  # fails, client -> None

        self.clock.advance(31.0)  # past the 30s reconnect_interval_seconds
        self._fix_publish()
        asyncio.run(self.bridge.publish_telemetry({'ppv': '200'}))

        self.assertEqual(self.factory_call_count, 2)  # reconnected
        self.assertIsNotNone(self.bridge._client)
        topic, payload, retain = self.fake_client.published[-1]
        self.assertEqual(topic, 'goodwe/telemetry')
        self.assertEqual(json.loads(payload), {'ppv': '200'})

    def test_failed_reconnect_attempt_is_swallowed_not_raised(self):
        asyncio.run(self.bridge.connect())
        self._break_publish()
        asyncio.run(self.bridge.publish_telemetry({'ppv': '100'}))  # fails, client -> None

        self.clock.advance(31.0)

        async def failing_aenter(*args, **kwargs):
            raise ConnectionError("still unreachable")
        self.fake_client.__aenter__ = failing_aenter

        asyncio.run(self.bridge.publish_telemetry({'ppv': '300'}))  # must not raise

        self.assertIsNone(self.bridge._client)

    def test_explicit_connect_still_raises_on_failure_after_reconnect_fix(self):
        # Regression test: the new opportunistic reconnect path inside
        # _publish must not change connect()'s documented behavior of
        # raising on failure so main.py's startup try/except can log it.
        failing_bridge = mqtt_bridge.MqttBridge(
            host='localhost',
            client_factory=lambda **kwargs: FailingAenterMqttClient(),
        )
        with self.assertRaises(ValueError):
            asyncio.run(failing_bridge.connect())
        self.assertIsNone(failing_bridge._client)


if __name__ == '__main__':
    unittest.main()
