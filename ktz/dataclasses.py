# -*- coding: utf-8 -*-

from dataclasses import fields
from dataclasses import asdict
from collections import defaultdict

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

    def get(self, **kwargs) -> set[T]:
        # as per python complexity wiki, intersection is
        #   O(min(len(s), len(t))
        # so the full set can always be provided even for
        # single element lookups
        agg = [self._flat]

        for key, val in kwargs.items():
            assert key in self._idxs, f"invalid kwarg: {key}"
            agg.append(self._idxs[key].get(val, set()))

        return set.intersection(*agg)

    def keys(self, key: str):
        assert key in self._idxs, f"invalid key: {key}"
        return list(self._idxs[key])

    def add(self, ts: Iterable[T]):
        for t, dic in ((t, asdict(t)) for t in ts):
            self._flat.add(t)
            for key in self._idxs:
                self._idxs[key][dic[key]].add(t)

    def __init__(
        self,
        Klass,
        includes: Iterable[str] = None,
        excludes: Iterable[str] = None,
    ):
        if excludes:
            excludes = set(excludes)
        if includes:
            includes = set(includes)

        print("create index")
        self._idxs = defaultdict(dict)

        for field in fields(Klass):

            if includes and field.name not in includes:
                continue

            if excludes and field.name in excludes:
                continue

            self._idxs[field.name] = defaultdict(set)
            print(f"  add index for {field.name}")

        # flat set of all annotations
        self._flat = set()
