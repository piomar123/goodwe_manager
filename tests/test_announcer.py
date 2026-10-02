import queue
import unittest

from announcer import MessageAnnouncer


class MessageAnnouncerBackpressureTest(unittest.TestCase):
    def test_evicts_oldest_message_and_disconnects_listener_when_queue_is_full(self):
        announcer = MessageAnnouncer()
        listener = announcer.listen()  # maxsize=16

        for i in range(16):
            announcer.announce(f"msg-{i}")
        self.assertTrue(listener.full())

        announcer.announce("overflow")

        # the slow listener is dropped so future announces don't block/raise
        self.assertNotIn(listener, announcer.listeners)

        # oldest message (msg-0) was evicted to make room for the stop signal;
        # the remaining buffered messages are still delivered in order, ending
        # with a None sentinel telling the SSE stream to close
        drained = []
        while True:
            item = listener.get_nowait()
            drained.append(item)
            if item is None:
                break

        self.assertEqual([m.data for m in drained[:-1]], [f"msg-{i}" for i in range(1, 16)])
        self.assertIsNone(drained[-1])
        # the message that triggered the overflow was never delivered to this listener
        self.assertTrue(all(m.data != "overflow" for m in drained if m is not None))

    def test_delivers_messages_to_a_listener_with_room(self):
        announcer = MessageAnnouncer()
        listener = announcer.listen()

        announcer.announce("hello")

        self.assertEqual(listener.get_nowait().data, "hello")
        self.assertIn(listener, announcer.listeners)


class MessageAnnouncerStickyTest(unittest.TestCase):
    """Sticky events (the once-a-minute BMS sample) are sent only when they
    change, so a browser connecting in between gets the latest one replayed."""

    def test_new_listener_gets_latest_sticky_event_and_last_regular_message(self):
        announcer = MessageAnnouncer()
        announcer.announce('{"bms": 1}', event='bms', sticky=True)
        announcer.announce('{"bms": 2}', event='bms', sticky=True)
        announcer.announce('telemetry')

        listener = announcer.listen()

        replayed = [listener.get_nowait() for _ in range(listener.qsize())]
        self.assertEqual([(m.event, m.data) for m in replayed], [('bms', '{"bms": 2}'), (None, 'telemetry')])

    def test_sticky_event_does_not_replace_last_regular_message(self):
        announcer = MessageAnnouncer()
        announcer.announce('telemetry')
        announcer.announce('{"bms": 1}', event='bms', sticky=True)

        listener = announcer.listen()

        self.assertEqual([listener.get_nowait().data for _ in range(listener.qsize())], ['{"bms": 1}', 'telemetry'])

    def test_sticky_event_reaches_connected_listeners_once(self):
        announcer = MessageAnnouncer()
        listener = announcer.listen()

        announcer.announce('{"bms": 1}', event='bms', sticky=True)

        msg = listener.get_nowait()
        self.assertEqual((msg.event, msg.data), ('bms', '{"bms": 1}'))
        self.assertTrue(listener.empty())
        self.assertEqual(str(msg), 'event: bms\ndata: {"bms": 1}\n\n')


if __name__ == '__main__':
    unittest.main()
