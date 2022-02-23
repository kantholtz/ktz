# -*- coding: utf-8 -*-

from ktz import collections as kcol


class TestPartition:
    def test_partition(self):
        ref = {False: [0, 0], True: [1, 2, 3]}
        ret = kcol.partition([0, 1, 0, 2, 3])
        assert ret == ref

    def test_partition_order(self):
        ref = {False: [10, 20], True: [1, 2, 3]}
        ret = kcol.partition([1, 10, 2, 3, 20], fn=lambda x: x < 10)
        assert ret == ref

    def test_partition_multikey(self):
        ref = {"0": [0], "1": [1, 1]}
        ret = kcol.partition([1, 0, 1], fn=str)
        assert ret == ref


class TestSplit:
    def test_split_simple(self):
        ref = [1, 2, 3], [0, 0]
        ret = kcol.split([1, 0, 2, 0, 3])
        assert ret == ref

    def test_split_complex(self):
        ref = [1, 2], [10, 20]
        ret = kcol.split([1, 10, 2, 20], fn=lambda x: x < 10)
        assert ret == ref


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
