# -*- coding: utf-8 -*-

"""Nice function-functions."""

import logging
import pickle
from dataclasses import dataclass, field, replace
from functools import wraps
from pathlib import Path
from typing import Generic, TypeVar, Union

from ktz.filesystem import path

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
    """Cascading cached function execution.

    This class is used to iteratively work with data. If a long
    pipeline requires much data to be processed linearly but some
    steps are very resoure intensive, this cascade is used to
    automatically resume the latest step and omit all previous
    steps. This is heavily used for iterative development of data
    processing pipelines in ipython notebooks.

    Raises
    ------
    KeyError
        Thrown if a value is requested which has not been cached yet

    Examples
    --------
    >>> # FIRST SESSION:
    >>> !mkdir .cache
    >>> from ktz.functools import Cascade                                                                                [70/193]
    >>> # this defines two to be cached values x and y
    >>> # where y depends on x
    >>> run = Cascade(prefix='.cache', x='x.pkl', y='y.pkl')
    >>> outside = 1
    >>> @run.cache("x")
    ... def f():
    ...     return outside
    ...
    >>> @run.cache("y")
    ... def g(a):
    ...     return a + 1
    ...
    >>> x = f()
    >>> x
    1
    >>> outside += 1
    >>> outside
    2
    >>> # x is cached now!
    >>> x = f()
    >>> x
    1
    >>> # executing g is now preventing f
    >>> # to be run in the future
    >>> y = g(x)
    >>> y
    2
    >>>
    Do you really want to exit ([y]/n)? y
    >>> # SECOND SESSION:
    >>> from ktz.functools import Cascade                                                                                [70/193]
    >>> # this defines two to be cached values x and y
    >>> # where y depends on x
    >>> run = Cascade(prefix='.cache', x='x.pkl', y='y.pkl')
    >>> outside = 100
    >>> @run.cache("x")
    ... def f():
    ...     return outside
    ...
    >>> @run.cache("y")
    ... def g(a):
    ...     return a + 1
    >>> # f is not executed as g has already
    >>> # computed and cached a value
    >>> x = f()
    >>> x is None
    True
    >>> # g is not executed and the cached value
    >>> # is used instead
    >>> y = g(x)
    >>> y
    2

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

        This decorator handles whether a function is invoked at all.
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
        prefix: Union[str, Path] = None,
        **kwargs: Union[str, Path],
    ):
        """
        Create a cached function cascade.

        Parameters
        ----------
        prefix : Union[str, Path]
            Optional basepath to be prepended to all cache files
        **kwargs : Union[str, Path]
            Register data keys

        """
        self.path = path(prefix) if path else path(".")
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
