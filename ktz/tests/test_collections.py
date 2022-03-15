# -*- coding: utf-8 -*-

import pytest
from ktz import collections as kcol


class TestBuckets:

    #
    #  without key and without mapper
    #
    def test_buckets_simple(self):
        ref = {1: [2], 3: [4]}
        ret = kcol.buckets(((1, 2), (3, 4)))
        assert ret == ref

    def test_buckets_multiple(self):
        ref = {1: [2, 3, 4], 5: [6, 7]}
        ret = kcol.buckets(((1, 2), (1, 3), (1, 4), (5, 6), (5, 7)))
        assert ret == ref

    #
    #  with key and without mapper
    #
    def test_buckets_key(self):
        ref = {0: [0, 2, 4], 1: [1, 3, 5]}
        ret = kcol.buckets(range(6), key=lambda _, x: (x % 2, x))
        assert ret == ref

    def test_buckets_index(self):
        ref = {1: [0], 2: [1], 3: [2]}
        ret = kcol.buckets(range(3), key=lambda i, x: (i + 1, x))
        assert ret == ref

    #
    #  without key and with mapper
    #
    def test_buckets_mapper(self):
        ref = {1: 9, 5: 13}
        ret = kcol.buckets(
            ((1, 2), (1, 3), (1, 4), (5, 6), (5, 7)),
            mapper=sum,
        )
        assert ref == ret

    #
    #  with key and with mapper
    #
    def test_buckets_key_mapper(self):
        ref = {0: 6, 1: 9}
        ret = kcol.buckets(
            range(6),
            key=lambda _, x: (x % 2, x),
            mapper=sum,
        )

        assert ret == ref


class TestUnbucket:
    def test_unbucket(self):
        ref = [(1, 2), (5, 6), (5, 7)]
        ret = kcol.unbucket({1: [2], 5: [6, 7]})
        assert ret == ref


class TestFlat:
    def test_flat(self):
        ref = 1, 2
        ret = kcol.flat([[[1]], [2]])
        assert tuple(ret) == ref

    def test_flat_idempotent(self):
        ref = 1, 2
        ret = kcol.flat(kcol.flat([[1], [2]]))
        assert tuple(ret) == ref

    def test_flat_depth_1(self):
        ref = [1], 2
        ret = kcol.flat([[1], 2], depth=1)
        assert tuple(ret) == ref

    def test_flat_depth_2(self):
        ref = 1, [2]
        ret = kcol.flat([[1], [[2]]], depth=2)
        assert tuple(ret) == ref


class TestIncrementer:
    def test_increment_default(self):
        incr = kcol.Incrementer()
        assert incr["a"] == 0
        assert incr["b"] == 1
        assert incr["a"] == 0
        assert incr["c"] == 2

    def test_increment_fn_iterable(self):
        incr = kcol.Incrementer(fn=range(10, 100))

        assert incr["a"] == 10
        assert incr["b"] == 11
        assert incr["a"] == 10
        assert incr["c"] == 12

    def test_increment_fn_iterator(self):
        incr = kcol.Incrementer(fn=iter(range(10, 100)))

        assert incr["a"] == 10
        assert incr["b"] == 11
        assert incr["a"] == 10
        assert incr["c"] == 12

    def test_increment_setitem(self):
        incr = kcol.Incrementer()
        with pytest.raises(KeyError):
            incr["foo"] = 3
