# -*- coding: utf-8 -*-


import logging
from itertools import chain
from collections import defaultdict

from typing import Any
from typing import Union
from typing import Callable
from typing import Optional

from collections.abc import Iterable
from collections.abc import Generator
from collections.abc import Collection


log = logging.getLogger(__name__)


#
#   COLLECTIONS
#

A, B, C, D = Any, Any, Any, Any
Index = int


# python 3.10
# def buckets(
#     col: Collection[A] | Collection[tuple[B, C]],
#     key: Optional[Callable[[Index, A], tuple[B, C]]] = None,
#     mapper: Optional[Callable[[tuple[C]], D]] = None,
# ) -> dict[B, list[C]] | dict[B, D]:
def buckets(
    col: Union[Collection[A], Collection[tuple[B, C]]],
    key: Optional[Callable[[Index, A], tuple[B, C]]] = None,
    mapper: Optional[Callable[[tuple[C]], D]] = None,
) -> Union[dict[B, list[C]], dict[B, D]]:
    dic = defaultdict(list)

    for i, elem in enumerate(col):
        k, v = elem if key is None else key(i, elem)
        dic[k].append(v)

    if mapper:
        dic = {k: mapper(v) for k, v in dic.items()}

    return dict(dic)


def unbucket(
    buckets: dict[A, list[B]],
) -> list[tuple[A, B]]:
    return [(key, el) for key, lis in buckets.items() for el in lis]


Nested = Union[Iterable[A], A]


def flat(
    col: Iterable[Nested],
    depth: int = -1,
) -> Generator[A, None, None]:

    if depth == 0:
        yield col
        return

    try:
        for elem in col:
            yield from flat(col=elem, depth=depth - 1)
    except TypeError:
        yield col
