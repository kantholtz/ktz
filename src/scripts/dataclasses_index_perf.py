"""
Test performance problems with the dataclasses.Index implementation.
"""


import cProfile
from dataclasses import dataclass
from datetime import datetime
from random import random
from timeit import timeit

from ktz.dataclasses import Index


@dataclass(frozen=True)
class Dispro:
    broad: int  # nearly unique
    narrow: int  # nearly equal


def log(msg: str):
    print(datetime.now().strftime("%H:%M:%S"), msg)


def test_disproportion():
    """Disproportionate reverse indices."""

    N = 10_000_000  # len(idx) ca. N + NP
    P = 0.01  # percentage of narrow spread

    log("create index")
    idx = Index(Dispro)
    for broad in range(N):
        idx.add(Dispro(broad=broad, narrow=0))
        if random() < P:
            idx.add(Dispro(broad=broad, narrow=1))

    keys = set(range(int(N * P)))
    narrow = 0

    # with cProfile.Profile() as pr:
    #     res = idx.con(broad=keys, narrow=narrow)
    # pr.print_stats(sort="cumulative")
    # log(f"con: {len(res)}/{int(N * P)}")

    log(f"query index (workaround) - {len(keys)}/{len(idx)}")
    res = []
    with cProfile.Profile() as pr:
        for broad in keys:
            dp = {dp.narrow: dp for dp in idx.get(broad=broad)}[narrow]
            res.append(dp)
    pr.print_stats(sort="tottime")

    log(f"query index (supposed)- {len(keys)}/{len(idx)}")
    res = []
    with cProfile.Profile() as pr:
        for broad in keys:
            dps = idx.con(broad=broad, narrow=narrow)
            assert len(dps) == 1
            res.append(dps.pop())
    pr.print_stats(sort="tottime")


def timeit_set_intersection():
    s1, s2, s3 = set(range(1)), set(range(100)), set(range(100_000))

    print("s1/s2", timeit(lambda: set.intersection(*[s1, s2])))
    print("s1/s3", timeit(lambda: set.intersection(*[s1, s3])))
    print("s1/s2/s3", timeit(lambda: set.intersection(*[s1, s2, s3])))


def timeit_union_vs_add():
    N = 10_000

    def union():
        target, source = set(), set(range(N))
        target |= source
        return target

    def add():
        target, source = set(), set(range(N))
        for x in source:
            target.add(x)
        return target

    print("union", timeit(union))  # 582.660
    print("add", timeit(add))  # 692.460


if __name__ == "__main__":
    test_disproportion()
    # timeit_set_intersection()
    # timeit_union_vs_add()

# NOTES


# workaround
# - → Index.get
# - → Index.dis
# - → Index._agg
#   - key, val loop (1)
#     - set()
#     - _idxs.get()
#     - agg.append() set of size 1
# - ← Index.dis
#   - Set.union
# - ← test_dispro
#   - dict()
#   - res.append()

# supposed
# - → Index.con
# - → Index._agg
#   - key, val loop (1)
#     - set()
#     - _idxs.get()
#     - agg.append() set of size 1
#   - key, val loop (2)
#     - set()
#     - _idxs.get()
#     - agg.append() set of size 1000
# - ← Index.con
#   - Set.intersection
# - ← test_dispro
#   - res.append()
