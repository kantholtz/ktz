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
    Maintain an inverted index for dataclasses
    """

    @property
    def flat(self) -> set[T]:
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

    def dis(self, **kwargs) -> set[T]:
        """∨ : Obtain unionized subset of queried dataclasses

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
        >>> idx.get(x=2, y=3)
        {A(x=2, y=1), A(x=3, y=3)}

        """
        agg = self._agg(**kwargs)
        return {} if not agg else set.union(*agg)

    def con(self, **kwargs) -> set[T]:
        """∧ : Obtain intersection of queried dataclasses

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
        >>> idx.get(x=2, y=1)
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
        """Obtain indexed keys for the given field

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
        """Add instances to index

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
        """Create a new index

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
