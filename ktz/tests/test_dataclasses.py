from ktz.dataclasses import Index

import pytest

from dataclasses import dataclass


@dataclass(frozen=True)
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

    def test_con_single(self, idx):
        assert idx.con() == {}

        assert idx.con(i=1) == {a[0], a[1]}
        assert idx.con(i=2) == {a[2]}
        assert idx.con(i=3) == {a[3]}

        assert idx.con(f=2.3) == {a[0], a[1], a[2]}
        assert idx.con(f=2.5) == {a[3]}

        assert idx.con(s="hello") == {a[0], a[2]}
        assert idx.con(s="hello again") == {a[1]}
        assert idx.con(s="different") == {a[3]}

    def test_dis_single(self, idx):
        assert idx.dis() == {}

        assert idx.dis(i=1) == {a[0], a[1]}
        assert idx.dis(i=2) == {a[2]}
        assert idx.dis(i=3) == {a[3]}

        assert idx.dis(f=2.3) == {a[0], a[1], a[2]}
        assert idx.dis(f=2.5) == {a[3]}

        assert idx.dis(s="hello") == {a[0], a[2]}
        assert idx.dis(s="hello again") == {a[1]}
        assert idx.dis(s="different") == {a[3]}

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
