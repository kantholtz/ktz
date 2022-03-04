# -*- coding: utf-8 -*-

"""
Extensions for dataclasses.

This module offers
  Index: an inverted index for dataclasses
  Builder: to iteratively build (immutable) data objects
"""

import ktz

from dataclasses import fields
from dataclasses import asdict
from collections import defaultdict

from typing import Any
from typing import Generic
from typing import TypeVar
from collections.abc import Iterable

# must be a dataclass
# I omit the somwhat wonky type checking for that
T = TypeVar("T")


class Index(Generic[T]):
    """
    Maintain an inverted index for dataclasses.

    Provided instances are saved to indexes based on their
    properties. The can then be retrieved fast by those properties.

    Methods
    -------
    get:
      Retrieve an object by key
    dis:
      Retrieve the union result for multi-key queries
    con:
      Retrieve the intersection result for multi-key queries

    """

    @property
    def flat(self) -> set[T]:
        """Return all indexed dataclasses.

        Note that this does not return a copy. You must create a copy
        before modifying this datastructure, otherwise you can expect
        undefined behaviour.

        Returns
        -------
        set[T]
            Set of all indexed dataclasses

        """
        return self._flat

    # ---

    def _raise_keyerror(self, key):
        raise KeyError(f"Field '{key}' not indexed.")

    def _agg(self, **kwargs) -> list[set[T]]:
        agg = []
        for key, val in kwargs.items():
            if key not in self._idxs:
                self._raise_keyerror(key)

            sub = set()
            for val in val if type(val) is set else {val}:
                sub |= self._idxs[key].get(val, set())

            agg.append(sub)

        return agg

    def get(self, **kwargs) -> set[T]:
        """
        Shortcut for single-field queries.

        If a single field is given, no distinction between dis() and
        con() exists. So this is a nice shortcut for simple queries.

        Parameters
        ----------
        **kwargs : field-key pair

        Returns
        -------
        set[T]
            Subset of indexed dataclasses

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> from ktz.dataclasses import Index
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...
        >>> idx = Index(A).add([A(x=1), A(x=2)])
        >>> idx.get(x=2)
        {A(x=2)}

        """
        assert len(kwargs) < 2
        return self.dis(**kwargs)

    def dis(self, **kwargs) -> set[T]:
        """
        ∨ : Obtain unionized subset of queried dataclasses.

        Each indexed field is searched for the associated dataclasses
        and the union of these results is returned.

        Parameters
        ----------
        **kwargs : kwargs
            field-key pairs

        Returns
        -------
        set[T]
            Subset of indexed dataclasses

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> from ktz.dataclasses import Index
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...     y: int
        ...
        >>> idx = Index(A).add([A(x=2, y=1), A(x=2, y=2), A(x=3, y=3)])
        >>> idx.dis(x=2, y=3)
        {A(x=2, y=1), A(x=3, y=3)}

        """
        agg = self._agg(**kwargs)
        return {} if not agg else set.union(*agg)

    def con(self, **kwargs) -> set[T]:
        """
        ∧ : Obtain intersection of queried dataclasses.

        Each indexed field is searched for the associated dataclasses
        and the intersection of these results is returned.

        Parameters
        ----------
        **kwargs : kwargs
            field-key pairs

        Returns
        -------
        set[T]
            Subset of indexed dataclasses

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> from ktz.dataclasses import Index
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...     y: int
        ...
        >>> idx = Index(A).add([A(x=2, y=1), A(x=2, y=2), A(x=3, y=3)])
        >>> idx.con(x=2, y=1)
        {A(x=2, y=1)}

        """
        # as per python complexity wiki, intersection is
        #   O(min(len(s), len(t))
        # so the full set can always be provided even for single element lookups
        # TODO: it can degenerate for multiple sets?
        # https://stackoverflow.com/questions/30845469/time-complexity-of-python-set-intersection-for-n-sets

        agg = self._agg(**kwargs)
        return {} if not agg else set.intersection(*agg)

    def keys(self, field: str) -> set[Any]:
        """
        Obtain indexed keys for the given field.

        This enumerates the possible keys that can be used to retrieve
        indexed dataclasses

        Parameters
        ----------
        field : str
            The field of interest

        Returns
        -------
        set[Any]
            All known keys

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> from ktz.dataclasses import Index
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...
        >>> idx = Index(A).add([A(x=2), A(x=3), A(x=5)])
        >>> idx.keys('x')
        {2, 3, 5}

        """
        if field not in self._idxs:
            self._raise_keyerror(field)

        return set(self._idxs[field])

    def add(self, ts: Iterable[T]) -> "Index":
        """
        Add instances to index.

        Parameters
        ----------
        ts : Iterable[T]
            The dataclass instances to be indexed

        Returns
        -------
        "Index"
            self

        """
        for t, dic in ((t, asdict(t)) for t in ts):
            self._flat.add(t)
            for key in self._idxs:
                self._idxs[key][dic[key]].add(t)

        return self

    def __init__(
        self,
        Klass,
        includes: Iterable[str] = None,
        excludes: Iterable[str] = None,
    ):
        """
        Create a new index.

        Indexes dataclasses of the specified type. Subsets of the
        indexed dataclasses are returned by Index.con and Index.dis.

        Parameters
        ----------
        Klass : T
            Class declaration of the dataclass
        includes : Iterable[str]
            Only include the provided fields
        excludes : Iterable[str]
            Exclude the provided fields

        Raises
        ------
        KeyError
            If includes or excludes contain unknown field names.

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> from ktz.dataclasses import Index
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...
        >>> idx = Index(A).add([A(x=2), A(x=3), A(x=5)])

        """
        includes = set(includes) if includes else set()
        excludes = set(excludes) if excludes else set()

        fieldnames = set(field.name for field in fields(Klass))

        for col in (includes, excludes):
            rem = col - fieldnames
            if rem:
                raise KeyError(f"Unknown fields: {rem}")

        self._idxs = defaultdict(dict)
        for name in fieldnames:

            if includes and name not in includes:
                continue

            if excludes and name in excludes:
                continue

            self._idxs[name] = defaultdict(set)

        self._flat = set()


# ---


class Builder:
    """
    Simple builder to incrementally build a dataclass.

    An instance of this class maintains a kwargs dictionary
    which can be incrementally popularized. Calling the
    instance attempts to build the provided dataclass object.

    Methods
    -------
    __call__:
      Assemble the dataclass
    get:
      Obtain an already added value
    add:
      Add another property

    """

    def __init__(self, Klass, immutable: bool = False):
        """Create a builder.

        Parameters
        ----------
        Klass : dataclass
            Dataclass constructor
        immutable : bool
            whether multiple assignments are allowed

        Examples
        --------
        >>> from dataclasses import dataclass
        >>> from ktz.dataclasses import Builder
        >>> @dataclass
        ... class Product:
        ...     a: int
        ...     b: str
        >>> build = Builder(Klass=Product)
        >>> build.add(a=3)
        >>> build.get('a')
        3
        >>> build.add(b="foo")
        >>> build()
        Product(a=3, b='foo')

        """
        self._Klass = Klass
        self._kwargs = {}
        self._immutable = immutable

    def __call__(self):
        """Assemble the dataclass instance.

        Examples
        --------
        FIXME: Add docs.

        """
        return self._Klass(**self._kwargs)

    def get(self, key: str):
        """Get the value of the provided key.

        Parameters
        ----------
        key : str
        """
        return self._kwargs[key]

    def add(self, **kwargs):
        """Add properties to the dataclass.

        Parameters
        ----------
        **kwargs : Any

        Raises
        ------
        ktz.Error
            If immutable=True, this is raised when a key is provided
            multiple times.

        """
        for key, val in kwargs.items():

            if self._immutable and key in self._kwargs:
                raise ktz.Error(f"cannot overwrite {key}")

            self._kwargs[key] = val
