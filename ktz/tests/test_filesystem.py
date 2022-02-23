# -*- coding: utf-8 -*-

import ktz
from ktz import filesystem as fs

import pytest
import tempfile
from pathlib import Path


class TestPath:
    def test_path(self):
        path = fs.path("foo")
        assert isinstance(path, Path)
        assert not path.exists()

        with tempfile.NamedTemporaryFile() as fd:
            path = fs.path(fd.name)
            assert path.exists()

    def test_path_exists(self):
        with tempfile.NamedTemporaryFile() as fd:
            path = fs.path(fd.name, exists=True)
            assert path.exists()

        with tempfile.TemporaryDirectory() as name:
            path = fs.path(name, exists=True)
            assert path.exists()

        with pytest.raises(ktz.Error):
            fs.path("foo", exists=True)

    def test_path_is_file(self):
        with pytest.raises(ktz.Error):
            fs.path("foo", is_file=True)

        with tempfile.NamedTemporaryFile() as fd:
            path = fs.path(fd.name, is_file=True)
            assert path

        with pytest.raises(ktz.Error):
            with tempfile.TemporaryDirectory() as name:
                fs.path(name, is_file=True)

    def test_path_is_dir(self):
        with pytest.raises(ktz.Error):
            fs.path("foo", is_dir=True)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = fs.path(tmpdir, is_dir=True)
            assert path

        with pytest.raises(ktz.Error):
            with tempfile.NamedTemporaryFile() as fd:
                path = fs.path(fd.name, is_dir=True)

    def test_path_create(self):
        with tempfile.TemporaryDirectory() as prefix:
            dirpath = Path(prefix) / "foo" / "bar"
            assert not dirpath.exists()

            path = fs.path(dirpath, create=True)
            assert path.exists() and path.is_dir()

    def test_message(self):
        fs.path("foo", message="Foo {path} bar {path_abbrv}")


class TestPathRotate:
    def test_rotate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = fs.path(tmpdir)
            path = root / "test.txt"

            # rotate once

            content1 = "foo"
            with path.open(mode="w") as fd:
                fd.write(content1)

            fs.path_rotate(current=path, keep=2)

            assert not path.exists()
            assert (root / "test.1.txt").exists()

            with (root / "test.1.txt").open(mode="r") as fd:
                assert content1 == fd.read()

            # rotate second time

            content2 = "bar"
            with path.open(mode="w") as fd:
                fd.write(content2)

            fs.path_rotate(current=path, keep=2)

            assert not path.exists()
            assert (root / "test.1.txt").exists()
            assert (root / "test.2.txt").exists()

            with (root / "test.1.txt").open(mode="r") as fd:
                assert content2 == fd.read()

            with (root / "test.2.txt").open(mode="r") as fd:
                assert content1 == fd.read()

            # rotate third time, keep parameter should work

            content3 = "hello fren"
            with path.open(mode="w") as fd:
                fd.write(content3)

            fs.path_rotate(current=path, keep=2)

            assert not path.exists()
            assert (root / "test.1.txt").exists()
            assert (root / "test.2.txt").exists()
            assert not (root / "test.3.txt").exists()

            with (root / "test.1.txt").open(mode="r") as fd:
                assert content3 == fd.read()

            with (root / "test.2.txt").open(mode="r") as fd:
                assert content2 == fd.read()


class TestGitHash:
    def test_githash(self):
        # don't know how to verify return value
        # for now should suffice that it is callable
        # errors would concern upstream lib anyway
        fs.git_hash()
