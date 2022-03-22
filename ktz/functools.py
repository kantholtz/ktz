# -*- coding: utf-8 -*-

"""Nice function-functions."""

import pickle
import logging
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
log = logging.getLogger(__name__)


@dataclass
class Maybe(Generic[T]):
    """
    Maybe it contains data, maybe not.

    Used for Cascade, to determine whether to execute functions,
    load data from cache, or ignore supposed data all together.

    Development Notes
    -----------------
    Data can be in one of the following states:

    * skip: Data is not loaded, function is not executed and cache is not written
    * cached: A cache file exists where data can be loaded, function is not executed
    * loaded: Data was loaded (either from cache or by function execution)

    Parameters
    ----------
    Generic[T] : T
        Whatever is saved in the cache

    """

    cache: Path

    skip: bool
    loaded: bool
    cached: bool

    data: T = field(default=None)

    def __str__(self):
        """State information."""
        return (
            f"Maybe {self.cache} "
            f"skip={self.skip} loaded={self.loaded} cached={self.cached}"
        )


class Cascade:
    """
    Cascading and cached function execution.

    This class is used to iteratively work with data. If a long
    pipeline requires much data to be processed linearly but some
    steps are very resoure intensive, this cascade is used to
    automatically resume the latest step and omit all previous
    steps. This is heavily used for iterative development of data
    processing pipelines in ipython notebooks.

    Examples
    --------
    FIXME: add docs

    """

    path: Path
    data: dict[str, Maybe]

    def get(self, name: str):
        """Retrieve a cached object.

        Retrieve object of the provided name from the cache.

        Parameters
        ----------
        name : str
            Data key

        Raises
        ------
        KeyError
            If no cache file exists yet

        """
        assert name in self.data, f"cascade: unregistered name {name}"

        maybe = self.data[name]

        if not maybe.cached:
            raise KeyError(f"Cascade: {name} is not cached yet.")

        if maybe.loaded:
            return maybe.data

        with maybe.cache.open(mode="rb") as fd:
            log.info(f"cascade: loading {name} from {maybe.cache.name}")
            cached = pickle.load(fd)

        self.data[name] = replace(maybe, loaded=True, data=cached)
        return cached

    def cache(self, name: str):
        """
        Decorate cache functions.

        This decorator handles whether a function is called or if data
        needs to be retrieved from the cache.

        Parameters
        ----------
        name : str
            Data key

        """
        assert name in self.data, f"cascade: unregistered name {name}"

        def decorator(fn):
            @wraps(fn)
            def maybe_execute(*args, **kwargs):
                maybe = self.data[name]

                if maybe.skip:
                    log.info(f"cascade: skipping {name}")
                    return None

                if maybe.loaded:
                    log.info(f"cascade: return loaded data for {name}")
                    return maybe.data

                if maybe.cached:
                    return self.get(name=name)

                log.info(f"cascade: cache miss for {name}")
                ret = fn(*args, **kwargs)

                log.info(f"cascade: saving {name} to {maybe.cache.name}")
                with maybe.cache.open(mode="wb") as fd:
                    pickle.dump(ret, fd)

                self.data[name] = replace(
                    maybe,
                    cached=True,
                    loaded=True,
                    data=ret,
                )

                return ret

            return maybe_execute

        return decorator

    def when(self, *names: str):
        """
        Decorate conditionally executed function.

        This decorator handles whether a function is invooked at all.
        It does not work with any returned data. Decorated functions
        always return None.

        """
        assert all(
            name in self.data for name in names
        ), f"cascade: unregistered names {names}"

        def decorator(fn):
            @wraps(fn)
            def maybe_execute(*args, **kwargs):
                maybes = [self.data[name] for name in names]

                if not all(maybe.loaded for maybe in maybes):
                    return None

                fn(*args, **kwargs)

            return maybe_execute

        return decorator

    def __init__(
        self,
        path: Union[str, Path] = None,
        **kwargs: Union[str, Path],
    ):
        """
        Create a cached function cascade.

        Parameters
        ----------
        path : Union[str, Path]
            Optional basepath to be prepended to all cache files
        **kwargs : Union[str, Path]
            Register data keys

        """
        self.path = kpath(path) if path else kpath(".")
        self.data = {}

        found = False
        while kwargs:

            name, cachefile = kwargs.popitem()  # lifo
            cache = self.path / cachefile

            assert name not in self.data

            maybe = Maybe(
                cache=cache,
                loaded=False,
                cached=cache.is_file(),
                skip=found,
            )

            self.data[name] = maybe

            if not found and maybe.cached:
                found = True
                log.info(f"cascade: will resume at {name}")
