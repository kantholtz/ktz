# -*- coding: utf-8 -*-

"""Things to do with the filesystem."""

import logging
import pathlib

import ktz

log = logging.getLogger(__name__)


def path(
    name: str | pathlib.Path,
    create: bool = False,
    exists: bool = False,
    is_dir: bool | None = None,
    is_file: bool | None = None,
) -> pathlib.Path:
    """
    Create paths.

    This simply gathers usual operations on Path objects for which
    normally multiple calls to the pathlib are required.

    Parameters
    ----------
    name : str | pathlib.Path
        The path in question
    create : bool
        Whether to create a directory if it does not exist
    exists : bool
        Check if the path exists, otherwise raise
    is_dir : bool | None
        Check if path is a directory, otherwise raise
    is_file : bool | None
        Check if path is a file, otherwise raise

    Returns
    -------
    pathlib.Path
        A Path instance

    Raises
    ------
    ktz.Error
        Raised if any of the constraints are violated

    Examples
    --------
    >>> from ktz.filesystem import path
    >>> somedir = path('foo/bar', create=True)
    >>> path(somedir, exists=True)
    PosixPath('foo/bar')
    >>> path(somedir, is_dir=True)
    PosixPath('foo/bar')
    >>> path(somedir, is_file=True)
    Traceback (most recent call last):
      (...)
    Error: foo/bar exists but is not a file

    """
    path = pathlib.Path(name)

    if (exists or is_file or is_dir) and not path.exists():
        raise ktz.Error(f"{path} does not exist")

    if is_file and not path.is_file():
        raise ktz.Error(f"{path} exists but is not a file")

    if is_dir and not path.is_dir():
        raise ktz.Error(f"{path} exists but is not a directory")

    if create:
        path.mkdir(exist_ok=True, parents=True)

    return path


def path_rotate(
    current: str | pathlib.Path,
    keep: int | None = None,
):
    """
    Rotate a file.

    Given a file "foo.tar", rotating it will produce "foo.1.tar".
    If "foo.1.tar" already exists then "foo.1.tar" -> "foo.2.tar".
    And so on. Also works for directories.

    If 'keep' is set to a positive integer, keeps at most
    that much files.

    Parameters
    ----------
    current : str | pathlib.Path
        Target file or directory
    keep : int
        How many rotated files to keep at most

    Examples
    --------
    >>> from ktz.filesystem import path_rotate
    >>> ! touch test.txt
    >>> path_rotate('test.txt', keep=2)
    >>> ! ls
    test.1.txt
    >>> ! touch test.txt
    >>> ! ls
    test.txt  test.1.txt
    >>> path_rotate('test.txt', keep=2)
    >>> ! ls
    test.1.txt  test.2.txt
    >>> ! touch test.txt
    >>> ! ls
    test.txt test.1.txt test.2.txt
    >>> path_rotate('test.txt', keep=2)
    >>> ! ls
    test.1.txt  test.2.txt

    """
    current = path(current, exists=True)
    if keep:
        assert keep > 1

    def _new(
        p: pathlib.Path,
        n: int | None = None,
        suffixes: list[str] | None = None,
    ) -> pathlib.Path:
        name = p.name.split(".")[0]  # .stem returns foo.tar for foo.tar.gz
        return p.parent / "".join([name, "." + str(n)] + (suffixes or []))

    def _rotate(p: pathlib.Path):
        if p.exists():
            old_n, *suffixes = p.suffixes

            n = int(old_n[1:]) + 1
            new = _new(p, n=n, suffixes=suffixes)

            _rotate(new)

            if keep and n <= keep:
                p.rename(new)
            else:
                p.unlink()

    new = _new(current, n=1, suffixes=current.suffixes)
    _rotate(new)
    current.rename(new)
