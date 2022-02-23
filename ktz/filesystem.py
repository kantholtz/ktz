# -*- coding: utf-8 -*-

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

    Quickly create and check pathlib.Paths

    Parameters
    ----------

    name : Union[str, pathlib.Path]
      The target file or directory

    create : bool
      Create as directory

    exists : bool
      Checks whether the file or directory exists

    is_file : bool
      Checks whether the target is a file

    is_file : bool
      Checks whether the target is a directory

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

    Rotates a file

    Given a file "foo.tar", rotating it will produce "foo.1.tar".
    If "foo.1.tar" already exists then "foo.1.tar" -> "foo.2.tar".
    And so on. Also works for directories.

    If 'keep' is set to a positive integer, keeps at most
    that much files.

    """
    current = path(current)
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

    if current.exists():
        new = _new(current, n=1, suffixes=current.suffixes)
        _rotate(new)
        current.rename(new)


def git_hash() -> str:
    repo = git.Repo(search_parent_directories=True)
    # dirty = '-dirty' if repo.is_dirty else ''
    return str(repo.head.object.hexsha)
