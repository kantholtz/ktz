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
