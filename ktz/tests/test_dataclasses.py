from dataclasses import dataclass

import pytest

import ktz
from ktz.dataclasses import Builder, Index


@dataclass(frozen=True, order=True)
class A:
    i: int
    f: float
    s: str


a = [
    A(i=1, f=2.3, s="hello"),  # 0
    A(i=1, f=2.3, s="hello again"),  # 1
    A(i=2, f=2.3, s="hello"),  # 2
    A(i=3, f=2.5, s="different"),  # 3
]


@pytest.fixture
def idx():
    return Index(A).add(a)


class TestIndex:
    def test_indexing_keys(self, idx):
        assert idx.keys("i") == {1, 2, 3}
        assert idx.keys("f") == {2.3, 2.5}
        assert idx.keys("s") == {"hello", "hello again", "different"}

    def test_keyerror(self, idx):
        with pytest.raises(KeyError):
            idx.keys("asd")

        with pytest.raises(KeyError):
            idx.con(asd=5)

        with pytest.raises(KeyError):
            idx.dis(asd=5)

    def _test_single(self, fn):
        assert fn() == {}

        assert fn(i=1) == {a[0], a[1]}
        assert fn(i=2) == {a[2]}
        assert fn(i=3) == {a[3]}

        assert fn(f=2.3) == {a[0], a[1], a[2]}
        assert fn(f=2.5) == {a[3]}

        assert fn(s="hello") == {a[0], a[2]}
        assert fn(s="hello again") == {a[1]}
        assert fn(s="different") == {a[3]}

    def test_flat(self, idx):
        assert sorted(a) == sorted(idx.flat)

    def test_iter(self, idx):
        assert sorted(a) == sorted(idx)

    def test_get_single(self, idx):
        self._test_single(idx.get)

    def test_con_single(self, idx):
        self._test_single(idx.con)

    def test_dis_single(self, idx):
        self._test_single(idx.dis)

    def test_dis_multiple_fields(self, idx):
        assert idx.dis(i=2, s="different") == {a[2], a[3]}
        assert idx.dis(f=2.3, s="hello") == {a[0], a[1], a[2]}

    def test_con_multiple_fields(self, idx):
        assert idx.con(i=1, s="hello") == {a[0]}
        assert idx.con(i=1, f=2.3) == {a[0], a[1]}

    def test_dis_multiple_keys(self, idx):
        assert idx.dis(i=2, s={"hello again", "different"}) == {a[1], a[2], a[3]}
        assert idx.dis(i={1, 2}, s={"hello again", "different"}) == set(a)

    def test_con_multiple_keys(self, idx):
        assert idx.con(i={1, 2}, s="hello") == {a[0], a[2]}
        assert idx.con(i={1, 2}, s={"hello", "hello again"}) == {a[0], a[1], a[2]}

    def test_includes(self):
        idx = Index(A, includes={"i"}).add(a)

        assert idx.con(i=1) == {a[0], a[1]}
        assert idx.dis(i=1) == {a[0], a[1]}

        with pytest.raises(KeyError):
            idx.con(f=2.3)

        with pytest.raises(KeyError):
            idx.dis(f=2.3)

        with pytest.raises(KeyError):
            idx.con(s="hello")

        with pytest.raises(KeyError):
            idx.dis(s="hello")

    def test_excludes(self):
        idx = Index(A, excludes={"f", "s"}).add(a)

        assert idx.con(i=1) == {a[0], a[1]}
        assert idx.dis(i=1) == {a[0], a[1]}

        with pytest.raises(KeyError):
            idx.con(f=2.3)

        with pytest.raises(KeyError):
            idx.dis(f=2.3)

        with pytest.raises(KeyError):
            idx.con(s="hello")

        with pytest.raises(KeyError):
            idx.dis(s="hello")

    def test_wrong_includeexlude(self):
        with pytest.raises(KeyError):
            Index(A, excludes={"nope"})
        with pytest.raises(KeyError):
            Index(A, includes={"nope"})

    def test_add_single(self, idx):
        idx = Index(A).add(a[0])
        assert idx.get(i=1) == {a[0]}

    def test_gets(self, idx):
        idx = Index(A).add(a[0])
        assert idx.gets(i=1) == a[0]

    def test_len(self, idx):
        assert len(idx) == len(a)

    def test_has(self, idx):
        assert idx.has(i=1)
        assert idx.has(f=2.3)
        assert idx.has(s="hello")

        assert not idx.has(i=5)
        assert not idx.has(f=2.2)
        assert not idx.has(s="nope")

        with pytest.raises(KeyError):
            idx.has(foo="bar")

        with pytest.raises(AssertionError):
            idx.has(i=1, f=2.3)

    def test_delitem(self, idx):
        del idx[a[3]]

        assert a[3] not in idx.flat

        assert idx.get(i=a[3].i) == set()
        assert idx.get(f=a[3].f) == set()
        assert idx.get(s=a[3].s) == set()


@dataclass
class Product:
    a: int
    b: str


class TestBuilder:
    def test_building(self):
        build = Builder(Klass=Product)

        build.add(a=3)
        assert build.get("a") == 3

        build.add(b="foo")
        assert build.get("b") == "foo"

        product = build()
        assert product == Product(a=3, b="foo")

    def test_building_incomplete(self):
        build = Builder(Klass=Product)
        build.add(a=3)

        with pytest.raises(TypeError):
            build()

    def test_building_overwrite(self):
        build = Builder(Klass=Product)

        build.add(a=3)
        build.add(a=4)
        build.add(b="foo")
        build.add(b="bar")

        product = build()
        assert product == Product(a=4, b="bar")

    def test_building_immutable(self):
        build = Builder(Klass=Product, immutable=True)

        build.add(a=3)

        with pytest.raises(ktz.Error):
            build.add(a=4)
