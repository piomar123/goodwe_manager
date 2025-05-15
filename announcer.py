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

    def listen(self):
        q = queue.Queue(maxsize=16)
        if self.last_msg:
            q.put_nowait(self.last_msg)
        self.listeners.add(q)
        return q

    def announce(self, data: str, event: Optional[str] = None):
        msg = Message(data, event)
        self.last_msg = msg
        for listener in set(self.listeners):  # using a copy to avoid concurrent modifications
            try:
                listener.put_nowait(msg)
            except queue.Full:
                # TODO: check if this works
                listener.get_nowait()
                listener.put_nowait(None)  # signal the listener to stop
                self.listeners.remove(listener)

    def unsubscribe(self, q):
        if q in self.listeners:
            self.listeners.remove(q)
