import queue
from typing import Optional


class Message:
    __slots__ = ('data', 'event')

    def __init__(self, data: str, event=None):
        self.data = data
        self.event = event

    def __str__(self):
        msg = f'data: {self.data}\n\n'
        if self.event is not None:
            msg = f'event: {self.event}\n{msg}'
        return msg


class MessageAnnouncer:
    """
    https://maxhalford.github.io/blog/flask-sse-no-deps/
    TODO: single queue for all listeners
    TODO: asyncio.Queue?
    """

    def __init__(self):
        self.listeners: set[queue.Queue] = set()
        self.last_msg = None
        # latest message per sticky event, replayed to every new listener
        self._sticky: dict = {}

    def listen(self):
        q = queue.Queue(maxsize=16)
        for msg in list(self._sticky.values()):
            q.put_nowait(msg)
        if self.last_msg:
            q.put_nowait(self.last_msg)
        self.listeners.add(q)
        return q

    def announce(self, data: str, event: Optional[str] = None, sticky: bool = False):
        """sticky: for events sent only when they change (e.g. the
        once-a-minute BMS sample) - the latest one per event is replayed to
        new listeners, without replacing the last regular message."""
        msg = Message(data, event)
        if sticky:
            self._sticky[event] = msg
        else:
            self.last_msg = msg
        for listener in set(self.listeners):  # using a copy to avoid concurrent modifications
            try:
                listener.put_nowait(msg)
            except queue.Full:
                # drop the oldest buffered message to make room for a stop
                # signal, so a slow/stuck listener gets disconnected instead
                # of blocking future announces (see tests/test_announcer.py)
                listener.get_nowait()
                listener.put_nowait(None)  # signal the listener to stop
                self.listeners.remove(listener)

    def unsubscribe(self, q):
        if q in self.listeners:
            self.listeners.remove(q)
