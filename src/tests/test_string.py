# -*- coding: utf-8 -*-

from ktz import string as kstr


class TestEncodeDecodeLine:

    #
    #   encoding
    #

    def test_encode(self):
        encoded = kstr.encode_line(["a", "b", "cd"])

        expected = b"a,b,cd\n"
        assert encoded == expected

    def test_encode_fn(self):
        encoded = kstr.encode_line(
            [1, 2, 34],
            fn=str,
        )

        expected = b"1,2,34\n"
        assert encoded == expected

    def test_encode_fns(self):
        encoded = kstr.encode_line(
            ["1", 2, 3, "4"],
            fns=(
                None,
                str,
                lambda x: str(x - 1),
            ),
        )

        expected = b"1,2,2,4\n"
        assert encoded == expected

    def test_encode_sep(self):
        encoded = kstr.encode_line(
            list("abc"),
            sep="|",
        )

        expected = b"a|b|c\n"
        assert encoded == expected

    #
    #   decoding
    #

    def test_decode(self):
        decoded = kstr.decode_line(b"a,b,cd")

        expected = "a", "b", "cd"
        assert decoded == expected

    def test_decode_fn(self):
        decoded = kstr.decode_line(b"1,2,3", fn=int)

        expected = 1, 2, 3
        assert decoded == expected

    def test_decode_fns(self):
        decoded = kstr.decode_line(
            b"a,1,3,b",
            fns=(
                None,
                int,
                lambda s: int(s) - 1,
            ),
        )

        expected = "a", 1, 2, "b"
        assert decoded == expected

    def test_decode_sep(self):
        decoded = kstr.decode_line(b"a|b|c", sep="|")

        expected = "a", "b", "c"
        assert decoded == expected

    #
    #   now kith
    #
    def test_both(self):
        col = tuple("abc")
        ret = kstr.decode_line(kstr.encode_line(col))
        assert ret == col

    def test_both_sep(self):
        col = tuple("abc")
        sep = "|"
        ret = kstr.decode_line(kstr.encode_line(col, sep=sep), sep=sep)
        assert ret == col

    def test_both_int(self):
        col = 1, 2, 3
        ret = kstr.decode_line(kstr.encode_line(col, fn=str), fn=int)
        assert ret == col


class TestHashing:
    def test_hashing(self):
        hash1 = kstr.args_hash(1, 2, 3)
        hash2 = kstr.args_hash(1, 2, 3)
        assert hash1 == hash2
