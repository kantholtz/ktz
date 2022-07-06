# -*- coding: utf-8 -*-

"""Tools for working with collection types."""


import logging
from collections import defaultdict
from collections.abc import Collection, Generator, Iterable, Mapping
from functools import partial
from itertools import count
from pathlib import Path
from typing import Any, Callable, Optional, Union

import yaml

from ktz.filesystem import path as kpath

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


class Incrementer(dict):
    """
    Automatically assign unique ids.

    Examples
    --------
    >>> from ktz.collections import Incrementer
    >>> incr = Incrementer()
    >>> incr['a']
    0
    >>> incr['b']
    1
    >>> incr['a']
    0
    >>> incr['c']
    2
    >>> incr.freeze()
    >>> incr['d']
    NameError: Key 'd' not present and incrementer is frozen.
    >>> incr.unfreeze()
    >>> incr['d']
    3
    """

    def __init__(self, *args, fn: Iterable[A] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.unfreeze()

        # iter() is idempotent and iterators are iterable
        self._iterator = count() if fn is None else iter(fn)

    def __setitem__(self, key: Any, val: Any):
        raise KeyError("must not set values explicitly")

    def __getitem__(self, key: Any) -> Union[int, A]:
        if self._frozen and key not in self:
            raise NameError(f"Key '{key}' not present and incrementer is frozen.")

        if key not in self:
            val = next(self._iterator)
            super().__setitem__(key, val)

        return super().__getitem__(key)

    def freeze(self):
        """
        Freeze the incrementer.

        It is no longer possible to automatically create new keys.

        """
        self._frozen = True

    def unfreeze(self):
        """Unfreeze the incrementer.

        Allows the creation of new keys again

        """
        self._frozen = False


# --


def drslv(
    dic: dict,
    chain: str,
    sep: str = " ",
    skiplast: Optional[int] = None,
    default: Any = KeyError,
):
    """
    with sep="." and skiplast=0
    foo.bar.baz -> dic['foo']['bar']['baz']
    """
    crumbs = chain.split(sep)
    if skiplast:  # neither None nor 0
        crumbs = crumbs[:-skiplast]

    try:
        for key in crumbs:
            dic = dic[key]
    except KeyError as err:
        if default == KeyError:
            raise err

        return default

    return dic


def dflat(dic, sep: str = " "):
    """
    Flatten a deep dictionary with string keys.
    """

    def r(src, tar, trail):
        for k, v in src.items():
            assert isinstance(k, str)

            k = f"{trail}{sep}{k}" if trail else k

            if isinstance(v, dict):
                r(v, tar, k)
            else:
                tar[k] = v

        return tar

    return r(dic, {}, None)


def dmerge(d1: Mapping, d2: Mapping):
    """
    Deeply Merge two mappings.

    Values of the the second mapping overwrite the former
    unless they are set to None.

    """
    d1 = d1.copy()
    for k, v in d2.items():
        if k in d1 and v is None:
            continue

        if k not in d1 or type(v) is not dict:
            d1[k] = v

        else:
            d1[k] = dmerge(d1[k] or {}, d2[k])

    return d1


def ryaml(*configs: Union[Path, str], **overwrites) -> dict:
    """
    Load and join configurations from yaml and kwargs.

    First, all provided configuration files are loaded
    and joined together. Afterwards, all provided kwargs
    overwrite the joined configuration dict.

    """
    as_path = partial(kpath, is_file=True, message="loading {path_abbrv}")

    # first join all yaml configs into one dictionary;
    # later dictionaries overwrite earlier ones
    result = {}
    for path in map(as_path, configs):
        with path.open(mode="r") as fd:
            new = yaml.safe_load(fd)

            if new is None:
                log.warn(f"{fd.name} is empty!")
                new = {}

            result = dmerge(result, new)

    return dmerge(result, overwrites)
