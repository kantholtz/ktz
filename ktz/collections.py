# -*- coding: utf-8 -*-


import logging
from itertools import chain
from collections import defaultdict

from typing import Any
from typing import Union
from typing import Callable
from typing import Optional

from collections.abc import Iterable
from collections.abc import Collection


log = logging.getLogger(__name__)


#
#   PRIMITIVES
#


def decode_line(encoded: bytes, sep: str) -> list[str]:
    return list(map(str.strip, encoded.decode("unicode_escape").split(sep)))


def encode_line(data: list[str], sep: str) -> bytes:
    assert all(sep not in s for s in data)
    return ((f" {sep} ").join(data)).encode("unicode_escape") + b"\n"


#
#   COLLECTIONS
#

A, B, C, D = Any, Any, Any, Any


def agg(col: Iterable[tuple[A, B]]) -> dict[A, tuple[B]]:
    dic = defaultdict(list)

    for k, v in col:
        dic[k].append(v)

    return {k: tuple(v) for k, v in dic.items()}


def partition(col: Iterable[Any], key: Callable[[Any], Any]) -> dict[Any, Any]:
    dic = defaultdict(list)

    for it in col:
        k = key(it)
        dic[k].append(it)

    return dic


def split(col: Iterable[Any], key: Callable[[Any], bool]) -> tuple[list]:
    a, b = [], []
    for it in col:
        lis = a if key(it) else b
        lis.append(it)

    return a, b


def buckets(
    col: Collection[A],
    fn: Callable[[int, A], tuple[B, C]],
    reductor: Optional[Callable[[list[C]], D]] = None,
) -> Union[dict[B, list[C]], dict[B, D]]:

    dic = defaultdict(list)

    for i, elem in enumerate(col):
        k, v = fn(i, elem)
        dic[k].append(v)

    if reductor:
        dic = {k: reductor(v) for k, v in dic.items()}

    return dict(dic)


def unbucket(buckets: dict[A, list[B]]) -> list[tuple[A, B]]:
    yield from ((key, el) for key, lis in buckets.items() for el in lis)


def flat(col: Collection[Any], depth: int = 2) -> Collection[Any]:
    yield from col if depth == 1 else chain(
        *(flat(lis, depth=depth - 1) for lis in col)
    )
