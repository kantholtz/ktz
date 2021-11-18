# -*- coding: utf-8 -*-

from ktz.typing import A

import hashlib
from itertools import zip_longest

from typing import Optional
from typing import Callable

from collections.abc import Iterable


def _apply_fns(col, fn, fns):
    col = list(col)

    if fn is not None:
        fns = [fn] * len(col)

    if fns is not None and len(fns):
        trans = []
        for fn, x in zip_longest(fns, col):
            trans.append(x if fn is None else fn(x))
        col = trans

    return col


def decode_line(
    encoded: bytes,
    sep: str = ",",
    fn: Callable[[A], str] = None,
    fns: Iterable[Optional[Callable[[str], A]]] = None,
) -> tuple[str | A]:
    """Decode a value list bytestring

    Takes a unicode encoded bytestring separated by the seperator
    token(s) and returns a value tuple. If fns are given, these are
    used to transform the separated values.

    Parameters
    ----------
    encoded : bytes
        The value bytestring
    sep : str
        Separator token
    fn  : Optional[Callable[[A], str]]
        Optional converter applied to all, overrides fns
    fns : Iterable[Optional[Callable[[str], A]]]
        Optional converter functions

    Returns
    -------
    tuple[str | A]
        The separated values

    Examples
    --------
    FIXME: Add docs.

    """

    mapped = list(map(str.strip, encoded.decode("unicode_escape").split(sep)))
    mapped = _apply_fns(col=mapped, fn=fn, fns=fns)

    return tuple(mapped)


def encode_line(
    data: Iterable[str | A],
    sep: str = ",",
    fn: Callable[[A], str] = None,
    fns: Iterable[Optional[Callable[[A], str]]] = None,
) -> bytes:
    """Encode a value collection

    Take a collection of either string values or other (requires fns)
    and produces a bytestring representation with values separated by
    sep. If fns are set, values are mapped accordingly. Adds a newline.

    Parameters
    ----------
    data : Iterable[str | A]
        Collection to be encoded
    sep : str
        Separator token
    fn  : Optional[Callable[[A], str]]
        Optional converter applied to all, overrides fns
    fns : Iterable[Optional[Callable[[A], str]]]
        Optional converter functions

    Returns
    -------
    bytes
        Encoded bytestring

    Examples
    --------
    FIXME: Add docs.

    """

    data = _apply_fns(col=data, fn=fn, fns=fns)
    assert all(sep not in s for s in data)

    return ((sep).join(data)).encode("unicode_escape") + b"\n"


def args_hash(
    *args,
) -> str:
    """Produce a hash value for the provided args

    All provided arguments are transformed by their __str__
    implementation.

    Parameters
    ----------
    *args : To be hashed arguments

    Returns
    -------
    str
        Hash value for the arguments

    Examples
    --------
    FIXME: Add docs.
    """

    bytestr = "".join(map(str, args)).encode()
    return hashlib.sha224(bytestr).hexdigest()
