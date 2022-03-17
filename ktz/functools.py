# -*- coding: utf-8 -*-

"""Nice function-functions."""

import pickle
from pathlib import Path
from functools import wraps
from dataclasses import field
from dataclasses import replace
from dataclasses import dataclass

from ktz.filesystem import path as kpath

from typing import Union
from typing import TypeVar
from typing import Generic


T = TypeVar("T")


@dataclass
class Maybe(Generic[T]):
    """Maybe it contains data, maybe not.

    It is used for Cascade, do determine whether to execute functions,
    load data from cache, or ignore supposed data all together.

    Parameters
    ----------
    Generic[T] : T
        Whatever is saved in the cache

    """

    cache: Path
    data: Union[T, None] = field(default=None)

    # extra field required as data can be None
    # when loaded from cache
    loaded: bool = field(default=False)
    skip: bool = field(default=False)


class Cascade:
    """
    Cascading and cached function execution.

    This class is used to iteratively work with data. If a long
    pipeline requires much data to be processed linearly but some
    steps are very resoure intensive, this cascade is used to
    automatically resume the latest step and omit all previous
    steps. This is heavily used for iterative development of data
    processing pipelines in ipython notebooks.

    """

    path: Path
    data: dict[str, Maybe]

    def save(self, **kwargs: T):
        """Save provided data to cache.

        It will not overwrite data already saved to the cache but not
        retrieved in the current run. Only previously unseen data is
        saved.

        Parameters
        ----------
        **kwargs : T
            Key-value pairs of to-be-cached values.

        Examples
        --------
        >>> # see Cascade.__init__

        """
        for name, data in kwargs.items():
            maybe = self.data[name]

            if maybe.skip:
                continue

            with maybe.cache.open(mode="wb") as fd:
                pickle.dump(data, fd)

            self.data[name] = replace(
                maybe,
                loaded=True,
                data=data,
            )

    def unless(self, name: str):
        """Decorate cached functions.

        This decorator handles whether a function is called or if data
        needs to be retrieved from the cache.

        Parameters
        ----------
        name : str
            Data key

        Examples
        --------
        FIXME: Add docs.

        """

        def decorator(fn):
            @wraps(fn)
            def maybe_execute(*args, **kwargs):
                maybe = self.data[name]

                if maybe.loaded:
                    return maybe.data

                if not maybe.skip:
                    return fn(*args, **kwargs)

                return None

            return maybe_execute

        return decorator

    def __init__(
        self,
        path: Union[str, Path] = None,
        invalidate: bool = False,
        **kwargs: Union[str, Path],
    ):
        """Create a cached function cascade.

        Parameters
        ----------
        path : Union[str, Path]
            Optional basepath to be prepended to all cache files
        invalidate : bool
            This will invalidate all cache files and overwrite them.
        **kwargs : Union[str, Path]
            Register data keys

        Examples
        --------
        FIXME: Add docs.

        """
        self.path = kpath(path) if path else kpath(".")
        self.data = {}

        found = False
        while kwargs:

            name, cachefile = kwargs.popitem()  # lifo
            cache = self.path / cachefile

            assert name not in self.data

            if not cache.is_file() or found or invalidate:
                self.data[name] = Maybe(cache=cache, skip=found)
                continue

            with cache.open(mode="rb") as fd:
                found = True

                self.data[name] = Maybe(
                    cache=cache,
                    data=pickle.load(fd),
                    loaded=True,
                    skip=True,
                )
