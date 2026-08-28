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


if __name__ == '__main__':
    unittest.main()
