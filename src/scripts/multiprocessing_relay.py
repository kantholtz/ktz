#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Demonstrating a Relay with tqdm progress bars.

Each process takes some configurable time to process the messages.
There are two worker pools ("preprocessing" and "postprocessing"),
a producer which sends work downstream and a single consumer process
which writes the results to disk.

The topology is as follows (resulting in N*M*K+1 processes):

  group:       prod    prep    pod    cons
  class:   Producer   Worker  Worker  Consumer
              ┌───┐   ┌───┐   ┌───┐   ┌───┐
              │ N │ → │ M │ → │ K │ → │ 1 │
              └───┘   └───┘   └───┘   └───┘
                │       │       │       │
                └───────┴───┬───┴───────┘
                            │
                            ▼
                          ┌───┐
                          │ 1 │
                          └───┘
                       TQDMHandler
               (executed in main process)


In this example configuration with
  N = 3   delay = 0.1
  M = 10  delay = 1.0
  K = 5   delay = 0.5

On average:
  producer send 3 * 10 messages per second
  prep/Worker handle 10*1/s
  post/Worker handle 5*2/s

With the given maxsize of 50, back-pressure is applied
to the producer and the consumer should report around
10/s messages.

 ⯈ ./multiprocessing_relay.py
writing logfile to ktz.log
prod :  85%|█████████████████████████████     | 256/300 [00:20<00:05, 8.06it/s]
prep : 194it [00:20,  8.93it/s]
post : 189it [00:20, 11.55it/s]
cons :  63%|█████████████████████▍            | 189/300 [00:20<00:09, 11.55it/s]

The progress bars must not be maintained in the processes
for tqdm to work correctly (race conditions will lead to
wonky/spurious placements of the bars). Control is handed
back to the main processes using the registered handler.

Note: The user is now responsible to hand-off control
back to the relay after work is done. Here we simply send
a poison pill in the Consumer.shutdown callback and
leave the handler's loop in TQDMHandler.run.

See main() to adjust the configuration. A logfile is written to ktz.log
and the results to multiprocessing_relay.txt.
"""


import logging
import logging.config
import multiprocessing as mp
import time
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial

import yaml
from ktz.multiprocessing import Actor, Control, Handler, Relay
from tqdm import tqdm as _tqdm

LOG = "ktz.demo"
tqdm = partial(_tqdm, ncols=80)


@dataclass
class Stat:
    """Increment origin identifier."""

    group: str


def _setup_logging():
    with open("../conf/logging.yaml", mode="r", encoding="utf-8") as fd:
        conf = yaml.safe_load(fd)

    conf["loggers"]["root"]["level"] = "DEBUG"
    conf["loggers"]["root"]["handlers"] = ["logfile"]

    print(f"writing logfile to {conf['handlers']['logfile']['filename']}")

    logging.config.dictConfig(conf)
    return logging.getLogger(LOG)


log = _setup_logging()


class Base(Actor):
    """Base class shared by all pipeline actors."""

    delay: float
    sink: mp.Queue

    def __init__(self, sink: mp.Queue, delay: float, *args, **kwargs):  # noqa: D107
        super().__init__(*args, **kwargs)
        self.sink = sink
        self.delay = delay

    def incr(self):
        """Increment the counter for handled messages."""
        self.sink.put(self.group)


class Producer(Base):
    """Produce n messages."""

    wait: float
    amount: int

    def __init__(self, amount: int, *args, **kwargs):
        """Create a producer.

        Parameters
        ----------
        wait : float
            Time (in seconds) to wait between each message
        amount : int
            No of messages to send
        """
        super().__init__(*args, **kwargs)

        self.amount = amount

    def loop(self):
        """Send n messages downstream."""
        for x in range(self.amount):
            time.sleep(self.delay)
            self.incr()
            self.send(f"Message {x} from {self.name}")


class Consumer(Base):
    """Consumer that writes all messages."""

    def __init__(self, filename: str, *args, **kwargs):
        """Create a consumer."""
        super().__init__(*args, **kwargs)
        self.filename = filename

    def startup(self):
        """Before the loop."""
        self.fd = open(self.filename, mode="w", encoding="utf-8")

    def shutdown(self):
        """After the loop."""
        self.fd.close()
        self.sink.put(Control.eol)

    def recv(self, msg):
        """Write and wait."""
        self.fd.write(f"{msg}\n")
        time.sleep(self.delay)
        self.incr()


class Worker(Base):
    """Relay messages further down the line."""

    def recv(self, msg: str):
        """Relays a message with delay."""
        time.sleep(self.delay)
        self.incr()
        self.send(f"{msg} handled by {self.group}/{self.name}")


class TQDMHandler(Handler):
    """Simple handler updating progress bars per group."""

    groups: dict

    def __init__(self, groups: dict[str, dict]):
        """Create a tqdm handler.

        Expects groups to be given. Each group can be configured with
        a dictionary. If the dictionary contains a "total" key, its
        value is used to set the total for tqdm.

        Parameters
        ----------
        groups : Iterable[dict]
            Keys expected that Base.incr() sends.

        """
        super().__init__()

        self.groups = {}
        for position, group in enumerate(groups, 1):
            total = groups[group].get("total", None)
            self.groups[group] = tqdm(desc=f"{group} ", total=total)

    def run(self):
        """Run the handlers loop."""
        while True:
            group = self.q.get()
            if group == Control.eol:
                break

            self.groups[group].update()


def main():
    """Create a fan-in/out pipeline with two processing steps."""
    P = 3
    N = 100

    config = dict(
        prod=dict(
            count=P,
            Klass=Producer,
            kwargs=dict(delay=0.1, amount=N),
            total=P * N,
        ),
        prep=dict(
            count=10,
            Klass=Worker,
            kwargs=dict(delay=1),
        ),
        post=dict(
            count=5,
            Klass=Worker,
            kwargs=dict(delay=0.5),
        ),
        cons=dict(
            count=1,
            Klass=Consumer,
            kwargs=dict(delay=0, filename="multiprocessing_relay.txt"),
            total=P * N,
        ),
    )

    stats = TQDMHandler(groups=config)

    relay = Relay(log=LOG, maxsize=50)
    relay.connect(
        **{
            # instantiate "count" many Actor instances and add sink queue for stats
            group: [v["Klass"](sink=stats.q, **v["kwargs"]) for _ in range(v["count"])]  # type: ignore
            for group, v in config.items()
        }
    )

    relay.start(handler=stats)


if __name__ == "__main__":
    # mp.set_start_method("spawn")
    main()
