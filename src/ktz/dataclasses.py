# -*- coding: utf-8 -*-

"""
Extensions for dataclasses.

This module offers
  Index: an inverted index for dataclasses
  Builder: to iteratively build (immutable) data objects
"""

from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import fields
from typing import Any, Generic, TypeVar

import ktz

# must be a dataclass
# I omit the somwhat wonky type checking for that
T = TypeVar("T")


class Index(Generic[T]):
    """
    Maintain an inverted index for dataclasses.

    Provided instances are saved to indexes based on their
    properties. They can then be retrieved fast by those properties.

    Methods
    -------
    get:
      Retrieve an object by key
    dis:
      Retrieve the union result for multi-key queries
    con:
      Retrieve the intersection result for multi-key queries

    """

    def _raise_if_frozen(self):
        # we could also catch the AttributeError
        # raised for frozenset.add but explicit is
        # better than implicit
        if isinstance(self._flat, frozenset):
            raise ktz.Error("Cannot mutate a frozen Index")

    def __len__(self) -> int:
        """
        Number of indexed dataclasses.

        Returns
        -------
        int
            Indexed dataclasses

        Examples
        --------
        >>> idx = Index(A).add([A(x=0), A(x=1)])
        >>> len(idx)
        2
        """
        return len(self.flat)

    def __iter__(self) -> Iterator[T]:
        return iter(self.flat)

    def __delitem__(self, obj) -> None:
        """
        Delete a record from the index.

        Parameters
        ----------
        obj : T
            The object to be removed

        Examples
        --------
        >>> from ktz.dataclasses import Index
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...
        >>> a0, a1 = A(x=0), A(x=1)
        ... idx = Index(A).add([a0, a1])
        >>> del idx[a0]
        >>> del idx[A(x=1)]  # hash based
        >>> len(idx)
        0
        """
        self._raise_if_frozen()
        assert isinstance(self._flat, set)

        self._flat.remove(obj)
        for key, idx in self._idxs.items():
            idx[getattr(obj, key)].remove(obj)

    def freeze(self) -> "Index":
        """
        Do not allow changes to the Index.

        This removes the ability to mutate the Index state
        by freezing the underlying data management.

        Examples
        --------
        >>> from ktz.dataclasses import Index
        >>> from dataclasses import dataclass
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...
        >>> idx = Index(A)
        >>> idx.freeze()
        >>> idx.add(A(x=0))
        Traceback (most recent call last):
          Input In [11] in <cell line: 1>
            idx.add(A(x=0))
          File ~/Complex/scm/ktz/ktz/dataclasses.py:325 in add
            raise ktz.Error("Cannot mutate a frozen Index")
        Error: Cannot mutate a frozen Index

        >>> idx.unfreeze()
        >>> idx.add(A(x=0))
        """
        self._flat = frozenset(self._flat)
        return self

    def unfreeze(self) -> "Index":
        """
        Allow changes to the Index (the default).

        This adds the ability to mutate the Index state
        to allow adding and deleting objects. See Index.freeze
        for an usage example.
        """
        self._flat = set(self._flat)
        return self

    @property
    def flat(self) -> set[T] | frozenset[T]:
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

    def has(self, **kwargs) -> bool:
        """
        Test whether a value is indexed for one or more instances.

        This is basically a shortcut for single-key checks. If more
        kwargs are provided, use a len check on con or dis instead.

        Exmples
        -------
        >>> from dataclasses import dataclass
        >>> from ktz.dataclasses import Index
        >>> @dataclass(frozen=True)
        ... class A:
        ...     x: int
        ...
        >>> idx = Index(A).add([A(x=1), A(x=2)])
        >>> idx.has(x=2)
        True
        """
        assert len(kwargs) == 1, "use con or dis for multiple values"
        return bool(len(self.con(**kwargs)))

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
        assert len(kwargs) < 2, "only one query keyword is allowed"
        return self.dis(**kwargs)

    def gets(self, **kwargs) -> T:
        """
        Shortcut for single-field, single-value queries.

        If a single field is given, no distinction between dis() and
        con() exists. So this is a nice shortcut for simple queries
        where only one value is expected.

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
        assert len(kwargs) < 2, "only one query keyword is allowed"
        res = self.dis(**kwargs)

        assert len(res) == 1, "accessed single value with multiple results"
        return res.pop()

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
        return set() if not agg else set.union(*agg)

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
        return set() if not agg else set.intersection(*agg)

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

    def add(self, ts: T | Iterable[T]) -> "Index":
        """
        Add instances to index.

        Parameters
        ----------
        ts : T | Iterable[T]
            The dataclass instances to be indexed. You may
            provide both single instances or iterables over
            such instances.

        Returns
        -------
        "Index"
            self

        """
        self._raise_if_frozen()

        try:
            gen = iter(ts)  # type: ignore
        except TypeError:
            gen = iter([ts])

        for t in gen:
            self._raise_if_frozen()
            assert isinstance(self._flat, set)

            self._flat.add(t)
            for key in self._idxs:
                self._idxs[key][getattr(t, key)].add(t)

        return self

    def __init__(
        self,
        Klass,
        includes: Iterable[str] | None = None,
        excludes: Iterable[str] | None = None,
        additional: Iterable[str] | None = None,  # TODO test and document
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
        additional = set(additional) if additional else set()

        fieldnames = set(field.name for field in fields(Klass))

        for col in (includes, excludes):
            rem = col - fieldnames
            if rem:
                raise KeyError(f"Unknown fields: {rem}")

        self._idxs = defaultdict(dict)
        for name in fieldnames | additional:
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
