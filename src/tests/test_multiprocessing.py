# -*- coding: utf-8 -*-

from ktz.multiprocessing import Actor
from ktz.multiprocessing import Relay
from ktz.multiprocessing import Handler
from ktz.multiprocessing import Control

import pytest

import queue
import multiprocessing as mp


HELO = "hello!"


class Sender(Actor):

    n: int

    def __init__(self, n: int, *args, **kwargs):
        """Send n messages."""
        super().__init__(*args, **kwargs)
        self.n = n

    def loop(self):
        for x in range(self.n):
            self.send(x)


class Receiver(Actor):

    results: mp.Queue

    def __init__(
        self,
        results: mp.Queue,
        send_on_startup: bool = False,
        send_on_shutdown: bool = False,
        *args,
        **kwargs,
    ):
        """Send n messages."""
        super().__init__(*args, **kwargs)
        self.results = results
        self._start_msg = send_on_startup
        self._stop_msg = send_on_shutdown

    def recv(self, msg):
        self.results.put(msg)

    def startup(self):
        if self._start_msg:
            self.results.put(HELO)

    def shutdown(self):
        if self._stop_msg:
            self.results.put(Control.eol)


class Proxy(Actor):
    def recv(self, msg):
        self.send(msg + 1)


class AggregatorHandler(Handler):

    results: set

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.results = set()

    def run(self):
        while True:
            msg = self.q.get()
            if msg == Control.eol:
                return

            self.results.add(msg)


class TestRelay:
    def test_single_actor(self):
        # at least two args must be provided
        with pytest.raises(AssertionError):
            Relay().connect(Sender(n=1))

        # at least two args must be provided
        with pytest.raises(AssertionError):
            Relay().connect(foo=Sender(n=1))

    def test_1_1(self):

        sender = Sender(n=1)
        receiver = Receiver(
            results=mp.Queue(),
            send_on_startup=True,
            send_on_shutdown=True,
        )

        relay = Relay()
        relay.connect(sender, receiver)

        assert not sender.receiver
        assert sender.sender

        assert receiver.receiver
        assert not receiver.sender

        relay.start()

        assert receiver.results.get_nowait() == HELO
        assert receiver.results.get_nowait() == 0
        assert receiver.results.get_nowait() == Control.eol

        with pytest.raises(queue.Empty):
            receiver.results.get_nowait()

    def test_1_n(self):
        sender = Sender(n=2)
        results = mp.Queue()
        receiver = [Receiver(results=results) for _ in range(3)]

        relay = Relay()
        relay.connect(sender, receiver)
        relay.start()

        received = set([results.get_nowait() for _ in range(2)])

        assert received == {0, 1}
        with pytest.raises(queue.Empty):
            results.get_nowait()

    def test_n_1(self):
        sender = [Sender(n=1) for _ in range(2)]
        receiver = Receiver(results=mp.Queue())

        relay = Relay()
        relay.connect(sender, receiver)
        relay.start()

        assert receiver.results.get_nowait() == 0
        assert receiver.results.get_nowait() == 0

        with pytest.raises(queue.Empty):
            receiver.results.get_nowait()

    def test_n_m(self):
        results = mp.Queue()

        sender = [Sender(n=2) for _ in range(2)]
        receiver = [Receiver(results=results) for _ in range(3)]

        relay = Relay()
        relay.connect(sender, receiver)
        relay.start()

        received = [results.get_nowait() for _ in range(4)]
        assert sorted(received) == [0, 0, 1, 1]

        with pytest.raises(queue.Empty):
            results.get_nowait()

    def test_1_n_1(self):
        sender = Sender(n=5)
        worker = [Proxy() for _ in range(3)]
        receiver = Receiver(results=mp.Queue())

        relay = Relay()
        relay.connect(sender, worker, receiver)
        relay.start()

        received = [receiver.results.get_nowait() for _ in range(5)]
        assert set(received) == set(range(1, 6))

    def test_handler(self):

        handler = AggregatorHandler()

        sender = Sender(n=2)
        receiver = Receiver(results=handler.q, send_on_shutdown=True)

        relay = Relay()
        relay.connect(sender, receiver)
        relay.start(handler)

        assert handler.results == {0, 1}
