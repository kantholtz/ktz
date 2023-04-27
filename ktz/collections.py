# -*- coding: utf-8 -*-
"""Tools for working with collection types."""


import copy
import logging
import warnings
from collections import defaultdict
from collections.abc import Collection, Generator, Iterable, Mapping
from functools import partial
from inspect import signature
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


# cannot get Union[list, tuple] to be a Generic
Nested = Union[list[A], A]


def lflat(
    col: Nested,
    depth: int = -1,
) -> Generator[A, None, None]:
    """Flattens a tuple or list.

    Consumes the given sequence and flattens it up to
    n levels deep or completely.

    Parameters
    ----------
    col : Nested
        Nested list or tuple
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
    >>> list(flat([["foo"], [["bar"]]]))
    ["foo", "bar"]

    """
    isseq = isinstance(col, list) or isinstance(col, tuple)
    if depth == 0 or not isseq:
        yield col
        return

    try:
        for elem in col:
            yield from lflat(col=elem, depth=depth - 1)
    except TypeError:
        yield col


class Incrementer(dict):
    """Automatically assign unique ids.

    This is basically a defaultdict using a state which remembers the
    latest assigned id and assigns its increment when queried for a
    missing item. It can be frozen to error out on unknown keys. You
    can overwrite the built-in incrementer by providing your own iterable
    upon instantiation using the fn kwarg.

    Parameters
    ----------
    dict : dict
        Base dictionary
    fn : Iterable
        Custom iterable to use instead of count()

    Raises
    ------
    NameError
        Thrown if the dict is frozen and an unknown key is accessed
    KeyError
        Thrown for invalid explicit setting of values
    StopIteration
        Thrown for depleted custom iterators given to __init__

    Examples
    --------
    >>> from ktz.collections import Incrementer
    >>> # using a custom fn to control the assigned ids
    >>> from itertools import count
    >>> ids = Incrementer(fn=count(10))
    >>> ids[4]
    10
    >>> ids[10]
    11
    >>> ids
    {4: 10, 10: 11}
    >>> ids.freeze()
    >>> ids[3]
    NameError: Key '3' not present and incrementer is frozen.

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
        """Freeze the incrementer.

        It is no longer possible to automatically create new keys.

        Examples
        --------
        >>> from ktz.collections import Incrementer
        >>> ids = Incrementer()
        >>> ids[1]
        0
        >>> ids.freeze()
        >>> ids[1]
        0
        >>> ids[2]
        NameError: Key '2' not present and incrementer is frozen.

        """
        self._frozen = True

    def unfreeze(self):
        """Unfreeze the incrementer.

        Allows the creation of new keys again

        Examples
        --------
        >>> from ktz.collections import Incrementer
        >>> ids = Incrementer()
        >>> ids.freeze()
        >>> ids[2]
        NameError: Key '2' not present and incrementer is frozen.
        >>> ids.unfreeze()
        >>> ids[2]
        0

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

    Parameters
    ----------
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
    >>> drslv(dic, 'foo.*.a')  # only works for single-element dicts
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
            if not isinstance(dic, dict):
                raise KeyError

            trail.append(dic)

            if key == "*":
                if len(dic) > 1:
                    raise KeyError(f"Multiple candidates for wildcard: {list(dic)}")
                key = list(dic)[0]

            dic = dic[key]

    except KeyError as err:
        if default == KeyError:
            raise KeyError(f"drsvl: {err} not found for {chain=}")

        return default

    return trail[-collapse] if collapse else dic


def dflat(
    dic,
    sep: str = ".",
    only: Optional[int] = None,
    # skiplast: Optional[int] = None,
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
    skiplast : Optional[int]
        Do not flatten up to n hops from each leaf

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

    def rec(src: Mapping, tar: Mapping, trail: list[str]):
        for k, v in src.items():
            subtrail = trail + [str(k)]
            if descend(v, len(subtrail)):
                rec(v, tar, subtrail)
            else:
                tar[sep.join(subtrail)] = v

        return tar

    # step 1: flatten completely
    return rec(dic, {}, [])


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
    >>> from ktz.collections import dmerge
    >>> d1 = dict(foo=dict(a=1, b=2), bar=3)
    >>> d2 = dict(foo=dict(a=3, c=4), xyz=5)
    >>> dmerge(d1, d2)
    {'foo': {'a': 3, 'b': 2, 'c': 4}, 'bar': 3, 'xyz': 5}

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


def _dconv(dic: dict, fns):
    res = {}
    for k, v in dic.items():
        if isinstance(v, Mapping):
            res[k] = _dconv(v, fns)
            continue

        res[k] = v
        for fn, argc in fns:
            assert argc in {1, 2}, "converter functions accept 1 or 2 arguments"
            res[k] = fn(res[k]) if argc == 1 else fn(res[k], k)

    return res


def dconv(dic: dict, *convert: Callable[[A, B], C]):
    """Convert a dictionary deeply.

    A pipeline of converter functions may be provided which transform
    the values of the given mapping. It always returns a deep copy of
    the mapping as a dictionary. The converter functions are applied
    in the given order.

    Parameters
    ----------
    dic : dict
        Mapping to be copied and transformed
    *convert : Callable[[A, B], C]
        Converter functions

    Examples
    --------
    >>> from ktz.collections import dconv
    >>> dconv(dict(a=1, d=dict(b=2, c=3)), lambda v: v + 2)
    {'a': 3, 'd': {'b': 4, 'c': 5}}
    >>> dconv(dict(a=1, d=dict(b=2, c=3)), lambda v, k: True if k == 'b' else False)
    {'a': False, 'd': {'b': True, 'c': False}}

    """

    # Obtain function signatures for converter functions to determine
    # whether additionally the key needs to be given to the converter.
    # As with nearly all cases, "don't ask for permission but for
    # forgiveness" is a bad anti-pattern because we never know whether
    # we accidentally catch a TypeError produced by the invoker.
    fns = [(fn, len(signature(fn).parameters)) for fn in convert]
    return _dconv(dic, fns)


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
