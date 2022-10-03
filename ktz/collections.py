# -*- coding: utf-8 -*-
"""Tools for working with collection types."""


import copy
import logging
import warnings
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
    Union[Mapping[B, list[C]], Mapping[B, D]]
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
    buckets: Mapping[A, list[B]],
) -> list[tuple[A, B]]:
    """
    Flattens a bucket dictionary.

    Partitioned data is joined back up together and
    presented as tuples in a flat list.

    Parameters
    ----------
    buckets : Mapping[A, list[B]]
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

    Consumes the given iterable and flattens it up to
    n levels deep or completely.

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
    dic: Mapping,
    chain: str,
    sep: str = ".",
    default: Any = KeyError,
    collapse: Optional[int] = None,
    # deprecated: use collapse
    skiplast: Optional[int] = None,
):
    """
    Resolve string trails in deep dictionaries.

    For example, with sep="." and collapse=0
    foo.bar.baz -> dic['foo']['bar']['baz']. Setting
    collapse=1 would return dic['foo']['bar'] = {'baz': ...}

    Parameters:
    -----------
    dic : Mapping
        Data to be looked up
    chain : str
        Query string
    sep : str
        How the chain needs to be split
    collapse : Optional[int]
        Return an n-level deep dict instead
    default : Any
        For missing keys; defaults to raising a KeyError

    Examples
    --------
    >>> from ktz.collections import drslv
    >>> dic = dict(foo=dict(bar=dict(a=1,b=2),c=3),d=4)
    >>> drslv(dic, 'foo.bar.a')
    1
    >>> drslv(dic, 'foo bar a', sep=' ')
    1
    >>> drslv(dic, 'foo.bar.a', collapse=1)
    {'a': 1, 'b': 2}
    >>> drslv(dic, 'not.there')
    Traceback (most recent call last):
      Input In [15] in <cell line: 1>
        drslv(dic, 'not.there')
      File ~/Complex/scm/ktz/ktz/collections.py:267 in drslv
        raise err
      File ~/Complex/scm/ktz/ktz/collections.py:264 in drslv
        dic = dic[key]
    KeyError: 'not'

    >>> drslv(dic, 'not.there', default=None)
    >>> drslv(dic, 'not.there', default=1)
    1

    """
    if skiplast:
        warnings.warn(
            "'skiplast' is deprecated; use 'collapse' instead",
            DeprecationWarning,
            stacklevel=2,
        )
        collapse = skiplast

    crumbs = chain.split(sep)

    try:
        trail = []
        for key in crumbs:
            trail.append(dic)
            dic = dic[key]

    except KeyError as err:
        if default == KeyError:
            raise err

        return default

    return trail[-collapse] if collapse else dic


def dflat(
    dic,
    sep: str = ".",
    only: Optional[int] = None,
    # skiplast: Optional[int] = None, TODO add skiplast
):
    """
    Flatten a deep dictionary with string keys.

    Takes a deeply nested dictionary and flattens it by concatenating
    its keys using the provided separator. For example a dictionary
    d['foo']['bar'] = 3 becomes d['foo.bar'] = 3. Keys are transformed
    to strings either by __str__ or __repr__ if __str__ is not defined.

    Parameters
    ----------
    dic : Mapping[str, XXXX]
        The dictionary to be flattened
    sep : str
        Separator to concatenate the keys with
    only : Optional[int]
        Stops flattening after the provided depth

    Examples
    --------
    >>> from ktz.collections import dflat
    >>> dic = dict(foo=dict(bar=dict(a=1,b=2),c=3),d=4)
    >>> dflat(dic)
    {'foo.bar.a': 1, 'foo.bar.b': 2, 'foo.c': 3, 'd': 4}
    >>> dflat(dic, sep=' ')
    {'foo bar a': 1, 'foo bar b': 2, 'foo c': 3, 'd': 4}
    >>> dflat(dic, only=2)
    {'foo.bar': {'a': 1, 'b': 2}, 'foo.c': 3, 'd': 4}

    """

    def descend(v, depth):
        if not isinstance(v, Mapping):
            return False

        if only is None or depth < only:
            return True

        return False

    def rec(src: Mapping, tar: Mapping, trail: str, depth: int):
        for k, v in src.items():
            assert isinstance(k, str)

            k = f"{trail}{sep}{k}" if trail else k

            if descend(v, depth):
                rec(v, tar, k, depth + 1)
            else:
                tar[k] = v

        return tar

    return rec(dic, {}, None, 1)


def dmerge(*ds: Mapping):
    """
    Deeply merge mappings.

    A new deep copy is created from the keys and values from the
    provided mappings. Values of the the next mapping overwrite the
    former unless they are set to None.

    Parameters
    ----------
    ds : Mapping
        Deep mappings to be merged

    Examples
    --------
    FIXME: Add docs.

    """
    if len(ds) == 0:
        return {}

    if len(ds) == 1:
        return copy.deepcopy(ds[0])

    work = list(ds)
    last = work.pop()
    while work:
        curr = work.pop().copy()
        for k, v in last.items():
            if k in curr and v is None:
                continue

            if k not in curr or not isinstance(v, Mapping):
                curr[k] = v

            else:
                curr[k] = dmerge(curr[k] or {}, last[k])

        last = curr
    return curr


def ryaml(*configs: Union[Path, str], **overwrites) -> dict:
    """
    Load and join configurations from yaml and kwargs.

    First, all provided configuration files are loaded
    and joined together. Afterwards, all provided kwargs
    overwrite the joined configuration dict.

    """
    as_path = partial(kpath, is_file=True)

    # first join all yaml configs into one dictionary;
    # later dictionaries overwrite earlier ones
    loaded = []
    for path in map(as_path, configs):
        with path.open(mode="r") as fd:
            loaded.append(yaml.safe_load(fd) or {})

    work = loaded + [overwrites]
    return dmerge(*work)
