# -*- coding: utf-8 -*-

import tempfile
from pathlib import Path

import pytest
from ktz import functools as ft


@pytest.fixture
def cachedir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestCascade:
    def test_single_function(self, cachedir):
        cachefile = "x"

        run = ft.Cascade(prefix=cachedir, x=cachefile)

        # to check whether the function is not called
        # again we use a tracking variable from the
        # outside scope
        outside = 1

        @run.cache("x")
        def foo():  # type: ignore
            return outside

        # from function execution
        x = foo()
        assert x == 1

        # change state
        outside += 1

        # from memory
        x = foo()
        assert x == 1

        # a new run must use the cachefile now
        run = ft.Cascade(prefix=cachedir, x=cachefile)

        @run.cache("x")
        def foo():
            return outside

        # from cache
        x = foo()
        assert x == 1

        # from memory
        x = foo()
        assert x == 1

    def test_multi_function_stepwise(self, cachedir):
        outside = 1

        def _create_run():
            cachefile1 = "x"
            cachefile2 = "y"
            cachefile3 = "z"

            run = ft.Cascade(
                prefix=cachedir,
                x=cachefile1,
                y=cachefile2,
                z=cachefile3,
            )

            @run.cache("x")
            def f():
                return outside

            @run.cache("y")
            def g(x):
                return x + 1

            @run.cache("z")
            def h(x):
                return x + 1

            return run, f, g, h

        # first run
        #   x is saved to cache

        run, f, g, h = _create_run()

        x = f()
        assert x == 1

        # new run will not execute
        # formerly used functions

        # second run
        #  x is loaded from cache
        #  y is computed

        outside = 2
        run, f, g, h = _create_run()

        # from cache
        x = f()
        assert x == 1

        # from function, saved to cache
        y = g(x)
        assert y == 2

        # third run
        #   x is now None
        #   y is loaded from cached
        #   z is computed

        outside = 3
        run, f, g, h = _create_run()

        x = f()
        assert x is None

        y = g(x)
        assert y == 2

        z = h(y)
        assert z == 3

    def test_multi_function_whole(self, cachedir):
        outside = 1

        def _create_run():
            cachefile1 = "x"
            cachefile2 = "y"
            cachefile3 = "z"

            run = ft.Cascade(
                prefix=cachedir,
                x=cachefile1,
                y=cachefile2,
                z=cachefile3,
            )

            @run.cache("x")
            def f():
                return outside

            @run.cache("y")
            def g(x):
                return x + 1

            @run.cache("z")
            def h(x):
                return x + 1

            return run, f, g, h

        # all return values are computed

        run, f, g, h = _create_run()
        x = f()
        assert x == 1

        y = g(x)
        assert y == 2

        z = h(y)
        assert z == 3

        # last return value is loaded from cache

        run, f, g, h = _create_run()
        x = f()
        assert x is None

        y = g(x)
        assert y is None

        z = h(y)
        assert z == 3

    def test_get(self, cachedir):
        outside = 1
        run = ft.Cascade(prefix=cachedir, x="x")

        @run.cache("x")
        def f():  # type: ignore
            return outside

        # from computed value
        assert f() == 1
        assert run.get("x") == 1

        outside = 2
        run = ft.Cascade(prefix=cachedir, x="x")

        @run.cache("x")
        def f(x):
            # never called
            return outside  # pragma: no cover

        # from cache
        assert f() == 1  # type: ignore
        assert run.get("x") == 1

    def test_get_raises(self, cachedir):
        run = ft.Cascade(prefix=cachedir, x="x")

        with pytest.raises(KeyError):
            run.get("x")

    def test_when(self, cachedir):
        outside = 1

        def _create_run():
            run = ft.Cascade(prefix=cachedir, x="x", y="y")

            @run.cache("x")
            def f():
                pass

            @run.cache("y")
            def g():
                pass

            @run.when("x")
            def when():
                nonlocal outside
                outside += 1

            return run, f, g, when

        run, f, _, when = _create_run()

        # f was not executed yet
        when()
        assert outside == 1

        # when f was executed
        f()
        when()
        assert outside == 2

        # when f's return value was loaded from cache
        run, f, g, when = _create_run()

        when()
        assert outside == 2

        f()
        when()
        assert outside == 3

        # not run if skipped
        g()  # save g to cache
        run, f, g, when = _create_run()

        when()
        assert outside == 3

        f()
        when()
        assert outside == 3
