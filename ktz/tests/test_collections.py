# -*- coding: utf-8 -*-

import yaml
import pytest
import tempfile

from contextlib import ExitStack

from ktz.collections import flat
from ktz.collections import ryaml
from ktz.collections import merge
from ktz.collections import buckets
from ktz.collections import unbucket
from ktz.collections import Incrementer


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
            ((1, 2), (1, 3), (1, 4), (5, 6), (5, 7)),
            mapper=sum,
        )
        assert ref == ret

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


class TestFlat:
    def test_flat(self):
        ref = 1, 2
        ret = flat([[[1]], [2]])
        assert tuple(ret) == ref

    def test_flat_idempotent(self):
        ref = 1, 2
        ret = flat(flat([[1], [2]]))
        assert tuple(ret) == ref

    def test_flat_depth_1(self):
        ref = [1], 2
        ret = flat([[1], 2], depth=1)
        assert tuple(ret) == ref

    def test_flat_depth_2(self):
        ref = 1, [2]
        ret = flat([[1], [[2]]], depth=2)
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


class TestMerge:
    def test_merge_flat(self):
        d1 = dict(foo=1, bar=2)
        d2 = dict(foo=3, xyz=4)

        merge(d1, d2)
        assert d1 == dict(foo=3, bar=2, xyz=4)

    def test_merge_none(self):
        d1 = dict(foo=1, bar=2)
        d2 = dict(foo=None, xyz=4)

        merge(d1, d2)
        assert d1 == dict(foo=1, bar=2, xyz=4)

    def test_merge_deep(self):
        d1 = dict(foo=dict(a=1, b=2), bar=3)
        d2 = dict(foo=dict(a=3, c=4), xyz=5)

        merge(d1, d2)
        assert d1 == dict(foo=dict(a=3, b=2, c=4), bar=3, xyz=5)


class TestRyaml:
    def test_noargs(self):
        ret = ryaml()
        assert ret == {}

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
