# KTZ - Python Tools

[![Documentation](https://img.shields.io/badge/Documentation-Latest-success?style=for-the-badge)](https://kantholtz.github.io/ktz/)
[![Master Checks](https://img.shields.io/github/workflow/status/kantholtz/ktz/Current%20Master%20Checks?style=for-the-badge&label=Master%20Checks)](https://github.com/kantholtz/ktz/actions/workflows/development.yml)
[![KTZ on PyPI](https://img.shields.io/pypi/v/ktz?style=for-the-badge)](https://pypi.org/project/ktz)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=for-the-badge)](https://github.com/psf/black)


Kantholtz' personal Python toolbox. Check out the [documentation here](https://kantholtz.github.io/ktz/).


## Installation

Python 3.9 is required.

``` console
$ conda create -n ktz python=3.9
$ conda activate ktz
$ pip install ktz
```

For a local installation with all dev dependencies:

``` console
$ git clone https://github.com/kantholtz/ktz.git
$ cd ktz
$ conda create -n ktz python=3.9
$ conda activate ktz
$ pip install .[dev]

# run tests
$ pytest

# to continually run tests
$ ptw -c

# to check code coverage
$ coverage run -m pytest
$ coverage report
$ coverage html
```
