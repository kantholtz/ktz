# -*- coding: utf-8 -*-

"""Things to do with the filesystem."""

import ktz

import pathlib
import logging

import git


from typing import Union
from typing import Optional


log = logging.getLogger(__name__)


def path(
    name: Union[str, pathlib.Path],
    create: bool = False,
    exists: bool = False,
    is_dir: Optional[bool] = None,
    is_file: Optional[bool] = None,
    message: str = None,
) -> pathlib.Path:
    """
    Create paths.

    This simply gathers usual operations on Path objects for which
    normally multiple calls to the pathlib are required.

    Parameters
    ----------
    name : Union[str, pathlib.Path]
        The path in question
    create : bool
        Whether to create a directory if it does not exist
    exists : bool
        Check if the path exists, otherwise raise
    is_dir : Optional[bool]
        Check if path is a directory, otherwise raise
    is_file : Optional[bool]
        Check if path is a file, otherwise raise
    message : str
        A message to be logged

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

    if message:
        path_abbrv = f"{path.parent.name}/{path.name}"
        log.info(message.format(path=path, path_abbrv=path_abbrv))

    return path


def path_rotate(current: Union[str, pathlib.Path], keep: int = None):
    """
    Rotate a file.

    Given a file "foo.tar", rotating it will produce "foo.1.tar".
    If "foo.1.tar" already exists then "foo.1.tar" -> "foo.2.tar".
    And so on. Also works for directories.

    If 'keep' is set to a positive integer, keeps at most
    that much files.

    Parameters
    ----------
    current : Union[str, pathlib.Path]
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
        n: int = None,
        suffixes: list[str] = None,
    ) -> pathlib.Path:
        name = p.name.split(".")[0]  # .stem returns foo.tar for foo.tar.gz
        return p.parent / "".join([name, "." + str(n)] + suffixes)

    def _rotate(p: pathlib.Path):
        if p.exists():
            old_n, *suffixes = p.suffixes

            n = int(old_n[1:]) + 1
            new = _new(p, n=n, suffixes=suffixes)

            _rotate(new)

            if n <= keep:
                p.rename(new)
            else:
                p.unlink()

    new = _new(current, n=1, suffixes=current.suffixes)
    _rotate(new)
    current.rename(new)


def git_hash() -> str:
    """
    Obtain the current git hash.

    Returns
    -------
    str
        Current git hash

    Examples
    --------
    >>> from ktz.filesystem import git_hash
    >>> git_hash()
    'bedee821a4c1e1217cee783b33ad3bea98dbbb9d'

    """
    repo = git.Repo(search_parent_directories=True)
    # dirty = '-dirty' if repo.is_dirty else ''
    return str(repo.head.object.hexsha)
