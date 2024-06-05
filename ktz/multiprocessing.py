# -*- coding: utf-8 -*-

"""Multiprocessing goodies."""

import abc
import enum
import logging
import logging.handlers
import multiprocessing as mp
import threading
from collections.abc import Iterable
from typing import Any

log = logging.getLogger(__name__)
ctx = mp.get_context()


# some good reads:
#   Why threads must not be mixed with forked processes:
#     - https://rachelbythebay.com/w/2011/06/07/forked/
#     - https://pythonspeed.com/articles/python-multiprocessing/


class Control(enum.Enum):
    """Internally used control messages."""

    # from upstream
    poison = enum.auto()

    # inside a group for shutdown
    # and for management queues (e.g. _q_log)
    eol = enum.auto()


class Actor(abc.ABC, ctx.Process):
    """
    Process abstraction used with a Relay.

    Actor instances are wired together using
    a Relay and then send messages to each other.
    Each actor is spawned in a separate process.
    Please see Relay to understand their usage.

    - Actors who are senders may invoke send()
    - Actors who receivers must implement recv()
    - Actors who are not receivers must implement loop()

    There are callback functions invoked at various
    stages of an Actor's lifecycle (implemented in run()):

    - startup(): Before loop()
    - loop(): Must be overwritten if not receiver
    - shutdown(): After loop()

    Actors MUST not be iterable! A Relay is required to properly
    initialize an Actor.

    """

    group: str

    _inbox: mp.Queue
    _outbox: mp.Queue

    _q_log: mp.Queue
    _log_name: str

    # TODO how to type this?
    #   - it is mp.Value (Synchronized[int])
    #   - https://github.com/python/typeshed/pull/11833
    #   - https://github.com/python/typeshed/issues/4266
    _received_poison: Any
    _expected_poison: int
    _peer_count: int

    def _add_poison(self):
        poison = self._received_poison
        expected = self._expected_poison

        with poison.get_lock():
            poison.value += 1

            self.log(
                f"received {poison.value}/{expected} poison pills",
                level=logging.DEBUG,
            )

            if poison.value == expected:
                for _ in range(self._peer_count):
                    self._inbox.put(Control.eol)

    # ---

    @property
    def sender(self) -> bool:
        """
        Whether the actor can send messages.

        If the Actor is last in a Relay it has
        no recipient and thus can not send messages.
        If Actor.sender is True, the send() message
        can be invoked.

        """
        try:
            self._outbox
            return True
        except AttributeError:
            return False

    @property
    def receiver(self) -> bool:
        """
        Whether the Actor can receive messages.

        If the Actor is first in a Relay it has
        no senders attached which can send it
        messages. Such Actors must implement a
        loop() which decides their lifetime and
        must not implement recv().

        """
        try:
            self._inbox
            return True
        except AttributeError:
            return False

    def run(self):
        """
        Control an Actor's lifecycle.

        Implementation of mp.Process.run. It is not invoked
        directly but called through Process.start() in a
        Relay.

        """
        self.log("starting up")
        self.startup()

        self.log("running loop", level=logging.DEBUG)
        self.loop()
        self.log("leaving loop", level=logging.DEBUG)

        if self.sender:
            self._outbox.put(Control.poison)

        self.log("shutting down", level=logging.DEBUG)
        self.shutdown()
        self.log("shut down complete")

    # ---

    def _init_log(self, name: str, q: mp.Queue):
        handler = logging.handlers.QueueHandler(q)

        root = logging.getLogger()
        root.addHandler(handler)

        self._log = logging.getLogger(name)

    def log(self, msg, *args: Any, level: int = logging.INFO):
        """
        Log in main process.

        Logging is not process-safe. As such a thread
        in the parent process handles logging and this
        function handles communication with this thread.

        Parameters
        ----------
        msg : str
            Log message
        level : int
            Log level

        """
        msg = f"[{self.group}] ({self.name}) {msg}"
        self._q_log.put((self._log_name, level, msg, args))

    def send(self, *args, **kwargs):
        """
        Send a message to consuming actors.

        The recv() handler of any one of the receiving
        Actors is called with the provided args and kwargs.

        """
        assert self.sender
        self._outbox.put((args, kwargs))

    def loop(self):
        """
        Keep the Actor alive.

        This function needs to be overwritten if the
        Actor is not a receiver. Handles message passing
        otherwise.

        """
        assert self.receiver

        while True:
            data = self._inbox.get()

            # upstream eol notification
            if data is Control.poison:
                self._add_poison()
                continue

            # in-group eol notification
            if data is Control.eol:
                break

            # all "non-system" messages are
            # always (args, kwargs) tuples
            args, kwargs = data
            self.recv(*args, **kwargs)

    # required handler

    def recv(self, msg):
        ...

    # optional handler

    def startup(self):
        """Implement as callback before loop()."""
        pass

    def shutdown(self):
        """Implement as callback after loop()."""
        pass


class Handler(abc.ABC):
    """
    Execute code in the main process.

    The handler maintains a queue. After control is given to the relay
    to handle the Actor's life cycles a handler instance may be used
    to gain control back in the main process. Note that the user is
    then responsible to leave the run() method and give control back
    to the relay to join on the actor processes.

    Usually it is a good idea to send and react to
    a poison pill (see demo/multiprocessing_relay.py)
    for an example.

    """

    q: mp.Queue

    def __init__(self):
        """Create a handler."""
        self.q = ctx.Queue()

    @abc.abstractmethod
    def run(self):
        """Execute code in the main process."""
        pass


class Relay:
    """
    Wire Actors together.

    Control backpressure with maxsize.


    Development Notes
    -----------------

    Logging is not multiprocess-, but thread-safe.
    (https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes)
    The relay starts a separate logging thread which handles
    incoming log data sent by the spawned Actor processes.

    Topologies

    * 1 - 1
    * n - 1
    * 1 - n
    * n - m

    """

    groups: dict[str, list[Actor]]
    maxsize: int | None

    def __init__(
        self,
        maxsize: int | None = None,
        log: str | None = None,
    ):
        """Create a new Relay.

        Use the connect() method to define a processing pipeline of
        Actors. To apply backpressure, use the maxsize parameter which
        limits how many messages may reside in the connecting
        queue. This effectively applies backpressure if consuming
        Actors are too slow.

        Parameters
        ----------
        maxsize : int | None
            Maximum messages queued between senders and receivers.
        log : str | None
            Name of the logger, defaults to implementers module

        """
        self.actors = []
        self.maxsize = maxsize

        self._log = log
        self._q_log = ctx.Queue()  # log thread communication

    def connect(
        self,
        *args: Actor | Iterable[Actor],
        **kwargs: Actor | Iterable[Actor],
    ):
        """
        Connect Actors to form a processing pipeline.

        Provide a sequence of Actors or sets of Actors to define
        message flow. All topologies are possible: 1-1, 1-n, n-1 and
        n-m.

        Parameters
        ----------
        *args : Actor | Iterable[Actor]
            Actors or sets of Actors.

        """

        def ensure_list(obj) -> list[Actor]:
            try:
                return list(iter(obj))
            except TypeError:
                return [obj]

        assert len(args) + len(kwargs) > 1

        # take all args and prepend them to kwargs
        groups = {f"group-{i}": a for i, a in enumerate(args)} | kwargs
        groups = {group: ensure_list(actors) for group, actors in groups.items()}

        # connect pairs of actors by a single queue.
        # the receivers need to know how many poison
        # pills they need to expect.
        instances: list[list[Actor]] = list(groups.values())

        for sender, receiver in zip(instances, instances[1:]):
            q = ctx.Queue(self.maxsize) if self.maxsize else ctx.Queue()

            for actor in sender:
                actor._outbox = q

            for actor in receiver:
                actor._inbox = q
                actor._peer_count = len(receiver)
                actor._expected_poison = len(sender)
                actor._received_poison = ctx.Value("I", 0)  # I: uint

        self.groups = groups
        log.info(f"relay: maintaining {len(self.groups)} groups")

    # logthread

    def _start_logthread(self):
        log.info("relay: starting logthread")

        def _log_thread(q):
            while True:
                data = q.get()
                if data == Control.eol:
                    break

                name, level, msg, args = data
                log = logging.getLogger(name)
                log.log(level, msg, *args)

        self._t_log = threading.Thread(target=_log_thread, args=(self._q_log,))
        self._t_log.start()

    def _join_logthread(self):
        log.info("relay: waiting for log thread")
        self._q_log.put(Control.eol)
        self._t_log.join()

    # actors

    def _start_actors(self):
        log.info("relay: starting processes")
        procs = []

        for group, actors in self.groups.items():
            for actor in actors:
                actor._q_log = self._q_log
                actor._log_name = self._log or actor.__class__.__module__
                actor.group = group

                actor.start()
                procs.append(actor)

        return procs

    def _join_actors(self, procs: list[Actor]):
        log.info(f"relay: waiting for {len(procs)} processes to finish")
        while procs:
            proc, procs = procs[0], procs[1:]
            log.info(f"relay: waiting for {proc.name}")
            proc.join()

    # lifecycle

    def start(self, handler: Handler | None = None):
        """
        Start the relay.

        This spawns all Actor processes and blocks until all these
        processes terminated. If a handler is provided, control is
        given back to the user in the main process.

        """
        # processes MUST be started before any threads (when forked)
        procs = self._start_actors()
        self._start_logthread()

        if handler:
            handler.run()

        # wait for processes; join fifo like the poison propagates
        self._join_actors(procs)
        self._join_logthread()

        log.info("relay: finished, exiting")
