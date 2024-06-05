# -*- coding: utf-8 -*-

import tempfile
from contextlib import ExitStack
from typing import Any, Callable, Iterable

import pytest
import yaml
from ktz.collections import (
    Incrementer,
    buckets,
    dconv,
    dflat,
    dmerge,
    drslv,
    lflat,
    ryaml,
    unbucket,
)


class TestBuckets:
    #
    #  without key and without mapper
    #
    def test_buckets_simple(self):
        ref = {1: [2], 3: [4]}
        ret = buckets(((1, 2), (3, 4)))
        assert ret == ref

    def test_buckets_multiple(self):
        ref = {1: [2, 3, 4], 5: [6, 7]}
        ret = buckets(((1, 2), (1, 3), (1, 4), (5, 6), (5, 7)))
        assert ret == ref

    #
    #  with key and without mapper
    #
    def test_buckets_key(self):
        ref = {0: [0, 2, 4], 1: [1, 3, 5]}
        ret = buckets(range(6), key=lambda _, x: (x % 2, x))
        assert ret == ref

    def test_buckets_index(self):
        ref = {1: [0], 2: [1], 3: [2]}
        ret = buckets(range(3), key=lambda i, x: (i + 1, x))
        assert ret == ref

    #
    #  without key and with mapper
    #
    def test_buckets_mapper(self):
        ref = {1: 9, 5: 13}
        ret = buckets(
            col=((1, 2), (1, 3), (1, 4), (5, 6), (5, 7)),
            mapper=sum,
        )
        assert ref == ret

    def f(self, y: Callable[[Iterable[Any]], int], *args) -> int:
        return y(*args)

    def g(self):
        return self.f(sum, 1, 2, 3)

    #
    #  with key and with mapper
    #
    def test_buckets_key_mapper(self):
        ref = {0: 6, 1: 9}
        ret = buckets(
            range(6),
            key=lambda _, x: (x % 2, x),
            mapper=sum,
        )

        assert ret == ref


class TestUnbucket:
    def test_unbucket(self):
        ref = [(1, 2), (5, 6), (5, 7)]
        ret = unbucket({1: [2], 5: [6, 7]})
        assert ret == ref


class TestLFlat:
    def test_lflat(self):
        ref = 1, 2
        ret = lflat([[[1]], [2]])
        assert tuple(ret) == ref

    def test_lflat_depth_1(self):
        ref = [1], 2
        ret = lflat([[1], 2], depth=1)
        assert tuple(ret) == ref

    def test_lflat_depth_2(self):
        ref = 1, [2]
        ret = lflat([[1], [[2]]], depth=2)
        assert tuple(ret) == ref

    def test_lflat_str(self):
        ref = "foo", "bar"
        ret = lflat([[["foo"]], ["bar"]])
        assert tuple(ret) == ref

    def test_lflat_tuple(self):
        ref = "foo", "bar"
        ret = lflat(((("foo")), ("bar")))
        assert tuple(ret) == ref


class TestIncrementer:
    def test_increment_default(self):
        incr = Incrementer()
        assert incr["a"] == 0
        assert incr["b"] == 1
        assert incr["a"] == 0
        assert incr["c"] == 2

    def test_increment_fn_iterable(self):
        incr = Incrementer(fn=range(10, 100))

        assert incr["a"] == 10
        assert incr["b"] == 11
        assert incr["a"] == 10
        assert incr["c"] == 12

    def test_increment_fn_iterator(self):
        incr = Incrementer(fn=iter(range(10, 100)))

        assert incr["a"] == 10
        assert incr["b"] == 11
        assert incr["a"] == 10
        assert incr["c"] == 12

    def test_increment_setitem(self):
        incr = Incrementer()
        with pytest.raises(KeyError):
            incr["foo"] = 3

    def test_increment_freeze_unfreeze(self):
        incr = Incrementer()
        assert incr["a"] == 0

        incr.freeze()
        with pytest.raises(NameError):
            incr["b"]

        incr.unfreeze()
        assert incr["b"] == 1


class TestDMerge:
    def test_flat(self):
        d1 = dict(foo=1, bar=2)
        d2 = dict(foo=3, xyz=4)

        res = dmerge(d1, d2)
        assert res == dict(foo=3, bar=2, xyz=4)

    def test_none(self):
        d1 = dict(foo=1, bar=2)
        d2 = dict(foo=None, xyz=4)

        res = dmerge(d1, d2)
        assert res == dict(foo=1, bar=2, xyz=4)

    def test_empty(self):
        res = dmerge()
        assert res == {}

    def test_deep(self):
        d1 = dict(foo=dict(a=1, b=2), bar=3)
        d2 = dict(foo=dict(a=3, c=4), xyz=5)

        res = dmerge(d1, d2)
        assert res == dict(foo=dict(a=3, b=2, c=4), bar=3, xyz=5)

    def test_multiple(self):
        d1 = dict(foo=1, d1=1)
        d2 = dict(foo=3, xyz=4, d2=2)
        d3 = dict(foo=5, xyz=6, d3=3)

        res = dmerge(d1, d2, d3)
        assert res == dict(foo=5, xyz=6, d1=1, d2=2, d3=3)


class TestDFlat:
    def test_empty(self):
        res = dflat({})
        assert res == {}

    def test_simple(self):
        res = dflat(dict(foo=1, bar=2))
        assert res == dict(foo=1, bar=2)

    def test_deep(self):
        d = {
            "1": {
                "1": {
                    "1": "1.1.1",
                    "2": "1.1.2",
                },
                "2": {
                    "1": "1.2.1",
                },
            },
            "2": {
                "1": "2.1",
            },
        }

        res = dflat(d)
        assert res == {
            "1.1.1": "1.1.1",
            "1.1.2": "1.1.2",
            "1.2.1": "1.2.1",
            "2.1": "2.1",
        }

    def test_only(self):
        d = {
            "1": {
                "1": {
                    "1": "1.1.1",
                    "2": "1.1.2",
                },
                "2": {
                    "1": "1.2.1",
                },
            },
            "2": {
                "1": "2.1",
            },
        }

        res = dflat(d, only=2)
        assert res == {
            "1.1": {"1": "1.1.1", "2": "1.1.2"},
            "1.2": {"1": "1.2.1"},
            "2.1": "2.1",
        }

    def test_sep(self):
        d = {"1": {"1": "1.1"}}
        res = dflat(d, sep=" ")
        assert res == {"1 1": "1.1"}

    def test_dict_leaves(self):
        d = {"1": {}, "2": {"1": {}, "2": {}}}
        res = dflat(d)
        assert res == {"1": {}, "2.1": {}, "2.2": {}}

    # TODO release 0.3
    # def test_skiplast(self):
    #     d = {
    #         "1": {
    #             "1": {
    #                 "1": "1.1.1",
    #                 "2": "1.1.2",
    #             },
    #             "2": {
    #                 "1": "1.2.1",
    #             },
    #         },
    #         "2": {
    #             "1": "2.1",
    #         },
    #     }

    #     res = dflat(d, skiplast=0)
    #     assert res == dflat(d)

    #     res = dflat(d, skiplast=1)
    #     assert res == {
    #         "1.1": {
    #             "1": "1.1.1",
    #             "2": "1.1.2",
    #         },
    #         "1.2": {
    #             "1": "1.2.1",
    #         },
    #         "2.1": "2.1",
    #     }

    #     res = dflat(d, skiplast=2)
    #     assert res == d


class TestDrslv:
    def test_empty_dic(self):
        d = {}

        # defaults to a key error
        with pytest.raises(KeyError):
            drslv(d, "foo")

        res = drslv(d, "foo", default=None)
        assert res is None

    def test_small(self):
        d = dict(foo=dict(bar="deep"), flat="flat")

        res = drslv(d, "flat")
        assert res == "flat"

        res = drslv(d, "foo.bar")
        assert res == "deep"

    def test_too_deep(self):
        d = dict(foo=dict(bar="deep"), flat="flat")

        with pytest.raises(KeyError):
            drslv(d, "foo.bar.baz")

    def test_sep(self):
        d = dict(foo=dict(bar="deep"), flat="flat")

        res = drslv(d, "foo bar", sep=" ")
        assert res == "deep"

    def test_collapse(self):
        d = {
            "1": {
                "1": {
                    "1": "1.1.1",
                    "2": "1.1.2",
                },
                "2": {
                    "1": "1.2.1",
                },
            },
        }

        res = drslv(d, "1.1.1")
        assert res == "1.1.1"

        res = drslv(d, "1.1.1", collapse=1)
        assert res == {"1": "1.1.1", "2": "1.1.2"}

        res = drslv(d, "1.1.1", collapse=2)
        assert res == {"1": {"1": "1.1.1", "2": "1.1.2"}, "2": {"1": "1.2.1"}}

        res = drslv(d, "1.1", collapse=1)
        assert res == {"1": {"1": "1.1.1", "2": "1.1.2"}, "2": {"1": "1.2.1"}}

        with pytest.raises(KeyError):
            drslv(d, "1.1.3", collapse=2)

    def test_wildcard(self):
        d = dict(a=dict(foo=dict(b=3)))
        assert drslv(d, "a.*.b") == 3

    def test_wildcard_error(self):
        d = dict(a=dict(x=1), b=dict(y=1))
        with pytest.raises(KeyError):
            drslv(d, "*.x")


class TestDConv:
    def test_empty_dic(self):
        d1 = {}
        d2 = dconv(d1)

        assert d1 == d2
        assert d1 is not d2

    def test_single_fn(self):
        d1 = dict(a=2)
        d2 = dconv(d1, lambda t: str(t))
        assert d2 == dict(a="2")

    def test_multiple_fn(self):
        def stoi(v):
            if isinstance(v, str):
                return int(v)
            return v

        def plus1(v):
            if isinstance(v, int):
                return v + 1
            return v

        d1 = dict(a=1, b="2", c=[])
        d2 = dconv(d1, stoi, plus1)

        assert d2 == dict(a=2, b=3, c=[])

    def test_key(self):
        def fn(_, k):
            if k == "foo":
                return True
            return False

        d1 = dict(foo=1, bar=2)
        d2 = dconv(d1, fn)

        assert d2 == dict(foo=True, bar=False)

    def test_deep(self):
        d1 = dict(foo=1, bar=dict(a=2, b=3))
        d2 = dconv(d1, lambda v: v + 2)
        assert d2 == dict(foo=3, bar=dict(a=4, b=5))


class TestRyaml:
    def test_noargs(self):
        ret = ryaml()
        assert ret == {}

    def test_only_overwrites(self):
        ret = ryaml(foo=1)
        assert ret == dict(foo=1)

    def test_single(self):
        d1 = dict(foo=1, bar=2)

        with tempfile.NamedTemporaryFile(mode="w") as fd:
            yaml.dump(d1, fd)
            ret = ryaml(fd.name)

        assert ret == d1

    def test_multi(self):
        d1 = dict(foo=1, bar=2)
        d2 = dict(foo=3, xyz=4)

        with ExitStack() as stack:
            fd1 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))
            fd2 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))

            yaml.dump(d1, fd1)
            yaml.dump(d2, fd2)

            ret = ryaml(fd1.name, fd2.name)
            assert ret == dict(foo=3, bar=2, xyz=4)
            return

        assert False

    def test_multi_deep(self):
        d1 = dict(foo=dict(a=1, b=2), bar=2)
        d2 = dict(foo=dict(a=3, c=4), xyz=5)

        with ExitStack() as stack:
            fd1 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))
            fd2 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))

            yaml.dump(d1, fd1)
            yaml.dump(d2, fd2)

            ret = ryaml(fd1.name, fd2.name)
            assert ret == dict(foo=dict(a=3, b=2, c=4), bar=2, xyz=5)
            return

        assert False

    def test_overwrites(self):
        d1 = dict(foo=1, bar=2)
        d2 = dict(foo=3, xyz=4)

        with ExitStack() as stack:
            fd1 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))
            fd2 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))

            yaml.dump(d1, fd1)
            yaml.dump(d2, fd2)

            ret = ryaml(fd1.name, fd2.name, xyz=5)
            assert ret == dict(foo=3, bar=2, xyz=5)
            return

        assert False

    def test_overwrites_deep(self):
        d1 = dict(foo=1, bar=2)
        d2 = dict(foo=3, xyz=dict(a=1, b=2))

        with ExitStack() as stack:
            fd1 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))
            fd2 = stack.enter_context(tempfile.NamedTemporaryFile(mode="w"))

            yaml.dump(d1, fd1)
            yaml.dump(d2, fd2)

            ret = ryaml(fd1.name, fd2.name, xyz=dict(a=3, c=3))
            assert ret == dict(foo=3, bar=2, xyz=dict(a=3, b=2, c=3))

        assert False
