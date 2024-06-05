# KTZ - Python Tools

[![Documentation](https://img.shields.io/badge/Documentation-Latest-success?style=for-the-badge)](https://kantholtz.github.io/ktz/)
[![KTZ on PyPI](https://img.shields.io/pypi/v/ktz?style=for-the-badge)](https://pypi.org/project/ktz)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=for-the-badge)](https://github.com/psf/black)


Kantholtz' personal Python toolbox. Check out the [documentation here](https://kantholtz.github.io/ktz/).


## Installation

Python 3.11 is required.

```console
pip install ktz
```


For a local installation with all dev dependencies:

``` console
$ git clone https://github.com/kantholtz/ktz.git
$ cd ktz
$ conda create -n ktz python=3.11
$ conda activate ktz
$ poetry install --with dev

# run tests
$ poetry run pytest

# to continually run tests
$ poetry run ptw -c

# static typing
$ poetry run pyright src

# to check code coverage
$ poetry run coverage run -m pytest
$ poetry run coverage report
$ poetry run coverage html

# build package
$ poetry build
```
