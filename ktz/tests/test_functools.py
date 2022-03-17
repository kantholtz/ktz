# -*- coding: utf-8 -*-

from ktz import functools as ft

import pytest

import tempfile
from pathlib import Path


@pytest.fixture
def cachedir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestCascade:
    def test_single_function_cascade(self, cachedir):
        cachefile = "x.pkl"

        run = ft.Cascade(path=cachedir, x=cachefile)

        # to check whether the function is not called
        # again we use a tracking variable from the
        # outside scope
        outside = 1

        @run.unless("x")
        def foo():
            return outside

        x = foo()
        run.save(x=x)
        assert x == 1

        outside += 1
        x = foo()
        assert x == 1

        # a new run must use the cachefile now

        run = ft.Cascade(path=cachedir, x=cachefile)
        x = foo()
        assert x == 1

    def test_multi_function_cascade(self, cachedir):
        outside = 1

        def _create_run():
            cachefile1 = "x.pkl"
            cachefile2 = "y.pkl"

            run = ft.Cascade(
                path=cachedir,
                x=cachefile1,
                y=cachefile2,
            )

            @run.unless("x")
            def foo():
                return outside + 1

            @run.unless("y")
            def bar(x):
                return x + 1

            return run, foo, bar

        run, foo, bar = _create_run()

        x = foo()
        run.save(x=x)
        assert x == 2

        outside = 1

        y = bar(x)
        run.save(y=y)
        assert y == 3

        # new run will not execute
        # formerly used functions

        outside = 500

        run, foo, bar = _create_run()

        x = foo()
        # this will not overwrite the cachefile
        run.save(x=x)
        assert x is None

        y = bar(x)
        run.save(y=y)
        assert y == 3

        # new run will not execute
        # formerly used functions

        outside = 500

        run, foo, bar = _create_run()

        x = foo()
        # this will not overwrite the cachefile
        run.save(x=x)
        assert x is None

        y = bar(x)
        run.save(y=y)
        assert y == 3
