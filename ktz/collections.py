# -*- coding: utf-8 -*-

"""Tools for working with collection types."""


import logging
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
    """
    Sort data into buckets.

    Takes a collection and sorts the data into buckets based on a
    provided function. The resulting buckets can then optionally be
    mapped (e.g. to be reduced).

    Parameters
    ----------
    col : Union[Collection[A], Collection[tuple[B, C]]]
        Collection to be partitioned
    key : Optional[Callable[[Index, A], tuple[B, C]]]
        Optional function that returns (key, value) tuples
    mapper : Optional[Callable[[tuple[C]], D]]
        Optional function that takes a bucket and maps it

    Returns
    -------
    Union[dict[B, list[C]], dict[B, D]]
        A dictionary which maps bucket identifieres to their data

    Examples
    --------
    >>> from ktz.collections import buckets
    >>> data = [1, 2, 3, 4, 5]
    >>> buckets(col=data, key=lambda i, x: (x % 2 ==0 , x))
    {False: [1, 3, 5], True: [2, 4]}
    >>> buckets(col=data, key=lambda i, x: (x % 2 ==0 , x), mapper=sum)
    {False: 9, True: 6}

    """
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
    """
    Flattens a bucket dictionary.

    Partitioned data is joined back up together and
    presented as tuples in a flat list.

    Parameters
    ----------
    buckets : dict[A, list[B]]
        Bucket dictionary

    Returns
    -------
    list[tuple[A, B]]
        Flattened collection

    Examples
    --------
    >>> from ktz.collections import buckets
    >>> from ktz.collections import unbucket
    >>> data = [1, 2, 3, 4, 5]
    >>> kcol.buckets(col=data, key=lambda i, x: (x % 2 ==0 , x))
    {False: [1, 3, 5], True: [2, 4]}
    >>> parts = kcol.buckets(col=data, key=lambda i, x: (x % 2 ==0 , x))
    >>> unbucket(parts)
    [(False, 1), (False, 3), (False, 5), (True, 2), (True, 4)]

    """
    return [(key, el) for key, lis in buckets.items() for el in lis]


Nested = Union[Iterable[A], A]


def flat(
    col: Iterable[Nested],
    depth: int = -1,
) -> Generator[A, None, None]:
    """Flattens a collection.

    Consumes the given iterable and flattens it up to n level deep or
    completely.

    Parameters
    ----------
    col : Iterable[Nested]
        Nested collection
    depth : int
        Maximum depth to flatten

    Returns
    -------
    Generator[A, None, None]
        Generator with flattened collection

    Examples
    --------
    >>> from ktz.collections import flat
    >>> flat([[1], [[2]]], depth=2)
    <generator object flat at 0x7f2886aeccf0>
    >>> list(flat([[1], [[2]]], depth=2))
    [1, [2]]

    """
    if depth == 0:
        yield col
        return

    try:
        for elem in col:
            yield from flat(col=elem, depth=depth - 1)
    except TypeError:
        yield col
